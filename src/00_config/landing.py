"""Planejamento, validação e descoberta dos arquivos publicados na landing."""
import glob
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import PurePosixPath
from urllib.parse import urlsplit


class ArquivoInvalido(ValueError):
    """Conteúdo incompleto, inválido ou diferente do manifesto publicado."""


def normalizar_periodos(ano, meses):
    ano = str(ano).strip()
    if not re.fullmatch(r"[0-9]{4}", ano) or not 1900 <= int(ano) <= 9999:
        raise ValueError("ano deve ter quatro dígitos e ser >= 1900")
    normalizados = []
    for mes in meses:
        valor = str(mes).strip()
        if not re.fullmatch(r"[0-9]{1,2}", valor) or not 1 <= int(valor) <= 12:
            raise ValueError(f"Mês inválido: {mes!r}; informe de 01 a 12")
        valor = valor.zfill(2)
        if valor not in normalizados:
            normalizados.append(valor)
    if not normalizados:
        raise ValueError("Informe ao menos um mês")
    return ano, normalizados


def planejar_arquivos(contrato, ano, meses, destino_volume=None):
    ano, meses = normalizar_periodos(ano, meses)
    fonte = contrato["fonte"]
    categoria = contrato["taxi_type"]
    if not re.fullmatch(r"[a-z0-9_]+", categoria):
        raise ValueError("taxi_type inválido para o caminho da landing")
    planos = []
    for mes in meses:
        campos = {"taxi_type": categoria, "ano": ano, "mes": mes}
        url = fonte["url_template"].format(**campos)
        nome = PurePosixPath(urlsplit(url).path).name
        particao = fonte["particao_template"].format(**campos)
        relativo = PurePosixPath(particao) / nome
        if (relativo.is_absolute() or ".." in relativo.parts or "\\" in str(relativo)
                or ":" in str(relativo) or not nome.endswith(".parquet")):
            raise ValueError(f"Caminho de landing inválido: {relativo}")
        raiz = destino_volume or fonte["volume"]
        planos.append({
            "dataset": contrato["dataset"], "categoria": categoria,
            "ano": int(ano), "mes": int(mes), "mes_ano": f"{mes}-{ano}",
            "arquivo": nome, "fonte": url,
            "destino": os.path.join(raiz, *relativo.parts),
            "legado": os.path.join(raiz, nome),
            "relativo": str(relativo),
        })
    return planos


def validar_parquet(caminho):
    """Lê o footer Parquet e calcula SHA-256 de TODO o arquivo, sem coletar linhas."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    tamanho = os.path.getsize(caminho)
    if tamanho < 12:
        raise ArquivoInvalido("Arquivo vazio ou menor que um Parquet válido")
    try:
        with pq.ParquetFile(caminho) as parquet:
            linhas = parquet.metadata.num_rows
            if parquet.metadata.num_columns == 0:
                raise ArquivoInvalido("Parquet sem colunas")
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise ArquivoInvalido(f"Footer Parquet inválido: {exc}") from exc
    digest = hashlib.sha256()
    with open(caminho, "rb") as arquivo:
        for bloco in iter(lambda: arquivo.read(8 * 1024 * 1024), b""):
            digest.update(bloco)
    return {"tamanho_bytes": tamanho, "checksum": digest.hexdigest(), "linhas": linhas}


def caminho_manifesto(caminho):
    return f"{caminho}.manifesto.json"


def ler_manifesto(caminho):
    try:
        with open(caminho_manifesto(caminho), encoding="utf-8") as arquivo:
            return json.load(arquivo)
    except (ValueError, TypeError) as exc:
        raise ArquivoInvalido(f"Manifesto inválido de {caminho}") from exc


def confirmar_arquivo(plano, metadados, execucao_id):
    """O manifesto é o marcador de publicação; só é escrito após o arquivo final."""
    documento = {
        **{k: plano[k] for k in ("dataset", "categoria", "ano", "mes", "mes_ano", "arquivo", "fonte")},
        **metadados, "algoritmo_checksum": "sha256", "status": "confirmado",
        "execucao_id": execucao_id, "confirmado_em": datetime.now(timezone.utc).isoformat(),
    }
    destino = caminho_manifesto(plano["destino"])
    temporario = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False,
                                         dir=os.path.dirname(destino), suffix=".part") as arquivo:
            temporario = arquivo.name
            json.dump(documento, arquivo, ensure_ascii=False)
        os.replace(temporario, destino)
    finally:
        if temporario and os.path.exists(temporario):
            os.remove(temporario)


def validar_existente(plano):
    meta = validar_parquet(plano["destino"])
    if os.path.exists(caminho_manifesto(plano["destino"])):
        manifesto = ler_manifesto(plano["destino"])
        if not isinstance(manifesto, dict):
            raise ArquivoInvalido("Manifesto deve ser objeto JSON")
        if manifesto.get("status") != "confirmado":
            raise ArquivoInvalido("Manifesto sem status confirmado")
        for chave in ("dataset", "categoria", "ano", "mes", "arquivo"):
            if manifesto.get(chave) != plano[chave]:
                raise ArquivoInvalido(f"Manifesto diverge da partição: {chave}")
        if any(manifesto.get(k) != meta[k] for k in ("tamanho_bytes", "checksum")):
            raise ArquivoInvalido("Tamanho ou SHA-256 diverge do manifesto publicado")
    return meta


def listar_arquivos_landing(contrato):
    """Preserva subpastas e rejeita Parquets sem marcador de publicação válido.

    O hash completo é verificado na ingestão; aqui a validação é de metadados,
    evitando ler todos os bytes na definição do grafo SDP.
    """
    fonte = contrato["fonte"]
    arquivos = sorted(p for p in glob.glob(os.path.join(fonte["volume"], fonte["glob"]))
                      if os.path.isfile(p))
    for caminho in arquivos:
        try:
            manifesto = ler_manifesto(caminho)
        except FileNotFoundError as exc:
            raise ArquivoInvalido(f"Arquivo sem manifesto confirmado: {caminho}; reexecute a ingestão") from exc
        if not isinstance(manifesto, dict):
            raise ArquivoInvalido(f"Manifesto deve ser objeto JSON: {caminho}")
        try:
            plano = planejar_arquivos(contrato, manifesto["ano"], [manifesto["mes"]])[0]
        except (KeyError, ValueError, TypeError) as exc:
            raise ArquivoInvalido(f"Partição inválida no manifesto: {caminho}") from exc
        if (os.path.normpath(plano["destino"]) != os.path.normpath(caminho)
                or manifesto.get("dataset") != contrato["dataset"]
                or manifesto.get("categoria") != contrato["taxi_type"]
                or manifesto.get("status") != "confirmado"
                or manifesto.get("tamanho_bytes") != os.path.getsize(caminho)
                or not re.fullmatch(r"[0-9a-f]{64}", str(manifesto.get("checksum", "")))):
            raise ArquivoInvalido(f"Arquivo/manifesto inconsistente: {caminho}")
    return arquivos
