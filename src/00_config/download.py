"""Transferências para a landing com política HTTP e auditoria por tentativa."""
import errno
import json
import os
import random
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from landing import (ArquivoInvalido, caminho_manifesto, confirmar_arquivo, planejar_arquivos,
                     validar_existente, validar_parquet)


# Colunas originais preservadas; as adicionais são migradas de forma aditiva.
COLUNAS_LOG = {
    "dataset": "STRING", "arquivo": "STRING", "fonte": "STRING",
    "tamanho_mb": "DOUBLE", "checksum": "STRING", "status": "STRING",
    "ingested_at": "TIMESTAMP", "mensagem": "STRING",
    "execucao_id": "STRING", "evento": "STRING", "categoria": "STRING",
    "ano": "INT", "mes": "INT", "mes_ano": "STRING", "destino": "STRING",
    "operacao": "STRING", "tentativa": "INT", "http_status": "INT",
    "origem_arquivo": "STRING",
    "erro_tipo": "STRING", "retentavel": "BOOLEAN", "vai_repetir": "BOOLEAN",
    "bytes_recebidos": "BIGINT", "bytes_esperados": "BIGINT", "tamanho_bytes": "BIGINT",
    "duracao_ms": "BIGINT", "espera_s": "DOUBLE", "linhas": "BIGINT",
    "etag": "STRING", "last_modified": "STRING", "content_type": "STRING",
    "retry_after": "STRING", "periodos_solicitados": "STRING",
    "arquivos_previstos": "INT", "arquivos_ok": "INT",
    "arquivos_existentes": "INT", "arquivos_erro": "INT",
}
SCHEMA_LOG = ", ".join(f"{nome} {tipo}" for nome, tipo in COLUNAS_LOG.items())


class FalhaIngestao(RuntimeError):
    def __init__(self, resultados):
        self.resultados = resultados
        falhas = [r for r in resultados if r["status"] == "erro"]
        super().__init__(f"Ingestão incompleta: {len(falhas)}/{len(resultados)} partição(ões) falharam: "
                         + ", ".join(f"{r['categoria']}/{r['mes_ano']} ({r['erro_tipo']})" for r in falhas))


class FalhaTransferencia(Exception):
    def __init__(self, mensagem, tipo, retentavel=False):
        super().__init__(mensagem)
        self.tipo, self.retentavel = tipo, retentavel


def registrar_evento(spark, catalogo, evento):
    registro = {"ingested_at": datetime.now(timezone.utc), **evento}
    # Log estruturado também no stdout do job, inclusive se a escrita Delta falhar.
    print(json.dumps(registro, default=str, ensure_ascii=False))
    spark.createDataFrame(
        [tuple(registro.get(c) for c in COLUNAS_LOG)], SCHEMA_LOG,
    ).write.mode("append").saveAsTable(f"{catalogo}.governanca.log_ingestao")


def _politica(contrato, max_tentativas):
    config = {
        "max_tentativas": 3, "timeout_conexao_s": 15, "timeout_leitura_s": 60,
        "timeout_transferencia_s": 900, "backoff_inicial_s": 2,
        "espera_max_s": 120,
        "status_retentaveis": [408, 429, 500, 502, 503, 504],
        **contrato["fonte"].get("http", {}),
    }
    if max_tentativas is not None:
        config["max_tentativas"] = max_tentativas
    if type(config["max_tentativas"]) is not int or not 1 <= config["max_tentativas"] <= 10:
        raise ValueError("max_tentativas deve ser inteiro de 1 a 10")
    for nome in ("timeout_conexao_s", "timeout_leitura_s", "timeout_transferencia_s",
                 "backoff_inicial_s", "espera_max_s"):
        if not isinstance(config[nome], (int, float)) or not 0 < config[nome] <= 3600:
            raise ValueError(f"{nome} deve estar entre 0 e 3600 segundos")
    if not isinstance(config["status_retentaveis"], list) or any(
        type(s) is not int or s not in {408, 429, *range(500, 600)}
        for s in config["status_retentaveis"]
    ):
        raise ValueError("status_retentaveis aceita somente 408, 429 e códigos 5xx")
    return config


def _classificar_erro(exc, politica):
    if isinstance(exc, FalhaTransferencia):
        return exc.tipo, exc.retentavel
    if isinstance(exc, requests.HTTPError):
        codigo = exc.response.status_code
        tipo = {401: "autenticacao", 403: "acesso_negado", 404: "nao_encontrado",
                410: "removido", 429: "limite_requisicoes"}.get(codigo, f"http_{codigo}")
        return tipo, codigo in politica["status_retentaveis"]
    # SSL é subtipo de ConnectionError e não deve ser retentado como falha de rede.
    if isinstance(exc, requests.exceptions.SSLError):
        return "tls", False
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout", True
    if isinstance(exc, (requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ContentDecodingError)):
        return "transferencia_interrompida", True
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "conexao", True
    if isinstance(exc, requests.exceptions.TooManyRedirects):
        return "redirecionamentos", False
    if isinstance(exc, requests.RequestException):
        return "requisicao", False
    if isinstance(exc, ArquivoInvalido):
        return "parquet_invalido", True
    if isinstance(exc, PermissionError):
        return "permissao_armazenamento", False
    if isinstance(exc, OSError):
        return "disco_cheio" if exc.errno == errno.ENOSPC else "armazenamento", False
    return "erro_interno", False


def _espera_retry(cabecalho, tentativa, politica):
    """Retry-After é um mínimo: um valor acima do orçamento encerra o arquivo."""
    espera = min(politica["espera_max_s"],
                 politica["backoff_inicial_s"] * 2 ** (tentativa - 1) + random.random())
    if cabecalho:
        try:
            valor = float(cabecalho)
        except ValueError:
            try:
                data = parsedate_to_datetime(cabecalho)
                if data.tzinfo is None:
                    data = data.replace(tzinfo=timezone.utc)
                valor = (data - datetime.now(timezone.utc)).total_seconds()
            except (ValueError, TypeError, OverflowError):
                valor = 0
        espera = max(espera, valor)
    return float(espera)


def _baixar_http(session, plano, temporario, politica, dados):
    inicio = time.monotonic()
    with session.get(
        plano["fonte"], stream=True,
        timeout=(politica["timeout_conexao_s"], politica["timeout_leitura_s"]),
        headers={"Accept-Encoding": "identity", "User-Agent": "case-nyc-taxi-ingestion/2.0"},
    ) as resposta:
        dados.update(http_status=resposta.status_code, etag=resposta.headers.get("ETag"),
                     last_modified=resposta.headers.get("Last-Modified"),
                     content_type=resposta.headers.get("Content-Type"),
                     retry_after=resposta.headers.get("Retry-After"))
        # Não aceitar 204/206 ou redirecionamento não resolvido como arquivo completo.
        if resposta.status_code != 200:
            raise requests.HTTPError(f"HTTP {resposta.status_code} para {plano['arquivo']}", response=resposta)
        tipo = resposta.headers.get("Content-Type", "").lower()
        if any(t in tipo for t in ("text/html", "application/json", "text/xml", "application/xml")):
            raise FalhaTransferencia(f"Resposta 200 contém {tipo}, não Parquet", "conteudo_inesperado", True)
        tamanho = resposta.headers.get("Content-Length")
        # Requests descomprime o corpo: Content-Length só é comparável sem encoding.
        if tamanho and resposta.headers.get("Content-Encoding", "identity").lower() == "identity":
            try:
                dados["bytes_esperados"] = int(tamanho)
                if dados["bytes_esperados"] < 0:
                    raise ValueError("Content-Length negativo")
            except ValueError as exc:
                raise FalhaTransferencia("Content-Length inválido", "protocolo_http") from exc
        with open(temporario, "wb") as arquivo:
            for bloco in resposta.iter_content(chunk_size=1024 * 1024):
                if time.monotonic() - inicio > politica["timeout_transferencia_s"]:
                    raise FalhaTransferencia("Tempo máximo de transferência excedido", "timeout", True)
                if bloco:
                    arquivo.write(bloco)
                    dados["bytes_recebidos"] += len(bloco)
        if (dados.get("bytes_esperados") is not None
                and dados["bytes_recebidos"] != dados["bytes_esperados"]):
            raise FalhaTransferencia("Download truncado: bytes recebidos divergem de Content-Length",
                                     "tamanho_divergente", True)


def _transferir(plano, politica, emitir, session, execucao_id, origem_raw=None):
    inicio_arquivo = time.monotonic()
    emitir(evento="inicio_arquivo", status="iniciado", tentativa=0)
    ultimo = {}
    for tentativa in range(1, politica["max_tentativas"] + 1):
        inicio = time.monotonic()
        dados = {"tentativa": tentativa, "bytes_recebidos": 0, "espera_s": 0.0}
        emitir(evento="inicio_tentativa", status="iniciado", tentativa=tentativa)
        temporario = None
        try:
            pasta = os.path.dirname(plano["destino"])
            os.makedirs(pasta, exist_ok=True)
            if os.path.exists(plano["destino"]):
                dados["operacao"] = "validacao_existente"
                # Arquivo publicado é imutável: corrupção pede intervenção, não sobrescrita.
                try:
                    meta = validar_existente(plano)
                except ArquivoInvalido as exc:
                    raise FalhaTransferencia(str(exc), "existente_invalido") from exc
                status = "ja_existia"
                publicar_manifesto = not os.path.exists(caminho_manifesto(plano["destino"]))
            else:
                # Reaproveita a landing plana anterior, sem apagar nem rebaixar os arquivos.
                legado = plano["legado"]
                origem = origem_raw or (legado if os.path.isfile(legado) else None)
                dados["operacao"] = "copia_raw" if origem_raw else ("migracao_legado" if origem else "download")
                dados["origem_arquivo"] = origem
                with tempfile.NamedTemporaryFile(delete=False, dir=pasta, prefix=".ingestao-", suffix=".part") as arq:
                    temporario = arq.name
                if origem:
                    shutil.copyfile(origem, temporario)
                    dados["bytes_recebidos"] = os.path.getsize(temporario)
                else:
                    _baixar_http(session, plano, temporario, politica, dados)
                meta = validar_parquet(temporario)
                os.replace(temporario, plano["destino"])
                temporario = None
                status = "ok"
                publicar_manifesto = True
            if publicar_manifesto:
                confirmar_arquivo(plano, {**meta, **{k: dados.get(k) for k in (
                    "etag", "last_modified", "origem_arquivo", "operacao",
                )}}, execucao_id)
            dados.update(meta, status=status, tamanho_mb=round(meta["tamanho_bytes"] / 1024**2, 3),
                         duracao_ms=int((time.monotonic() - inicio) * 1000))
        except Exception as exc:
            tipo, retentavel = _classificar_erro(exc, politica)
            if dados.get("operacao") != "download":
                retentavel = False
            espera = _espera_retry(dados.get("retry_after"), tentativa, politica) if retentavel else 0.0
            repetir = retentavel and tentativa < politica["max_tentativas"] and espera <= politica["espera_max_s"]
            dados.update(status="retry" if repetir else "erro", erro_tipo=tipo,
                         mensagem=str(exc)[:2000], retentavel=retentavel, vai_repetir=repetir,
                         espera_s=espera, duracao_ms=int((time.monotonic() - inicio) * 1000))
            if retentavel and espera > politica["espera_max_s"]:
                dados["mensagem"] += f"; Retry-After exige {espera:.0f}s, acima do orçamento; reagende a partição"
        finally:
            if temporario and os.path.exists(temporario):
                try:
                    os.remove(temporario)
                except OSError as exc:
                    dados.update(status="erro", erro_tipo="limpeza_temporario",
                                 mensagem=str(exc)[:2000], retentavel=False, vai_repetir=False)
        emitir(evento="tentativa", **dados)
        ultimo = dados
        if dados.get("vai_repetir"):
            time.sleep(dados["espera_s"])
        else:
            break
    ultimo = {**ultimo, "duracao_ms": int((time.monotonic() - inicio_arquivo) * 1000)}
    return emitir(evento="fim_arquivo", **ultimo)


def ingerir_particoes(contrato, spark, ano, meses, catalogo, max_tentativas=None,
                     execucao_id=None, origem_volume=None, destino_volume=None):
    """Processa todas as partições pedidas; registra o resumo antes de falhar o lote."""
    planos = planejar_arquivos(contrato, ano, meses, destino_volume)
    politica = _politica(contrato, max_tentativas)
    execucao_id = execucao_id or str(uuid.uuid4())
    base = {"dataset": contrato["dataset"], "categoria": contrato["taxi_type"],
            "execucao_id": execucao_id, "ano": planos[0]["ano"],
            "periodos_solicitados": ",".join(p["mes_ano"] for p in planos)}

    def evento_lote(**dados):
        registrar_evento(spark, catalogo, {**base, **dados})

    inicio = time.monotonic()
    evento_lote(evento="inicio_lote", status="iniciado", arquivos_previstos=len(planos))
    resultados = []
    with requests.Session() as session:
        for plano in planos:
            def emitir(**dados):
                evento = {**base, **{k: v for k, v in plano.items() if k not in ("legado", "relativo")}, **dados}
                registrar_evento(spark, catalogo, evento)
                return evento
            origem = None
            if origem_volume:
                # Prefere a organização particionada; aceita a raiz antiga para migração.
                origem = os.path.join(origem_volume, *plano["relativo"].split("/"))
                if not os.path.isfile(origem):
                    origem = os.path.join(origem_volume, plano["arquivo"])
                plano["fonte"] = origem
            resultados.append(_transferir(plano, politica, emitir, session, execucao_id, origem))
    falhas = sum(r["status"] == "erro" for r in resultados)
    evento_lote(evento="fim_lote", status="erro" if falhas else "ok", arquivos_previstos=len(planos),
                arquivos_ok=sum(r["status"] == "ok" for r in resultados),
                arquivos_existentes=sum(r["status"] == "ja_existia" for r in resultados),
                arquivos_erro=falhas, duracao_ms=int((time.monotonic() - inicio) * 1000))
    if falhas:
        raise FalhaIngestao(resultados)
    return resultados
