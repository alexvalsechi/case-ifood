# =============================================================================
# Motores genéricos do case_ifood — sem lógica específica de dataset.
# Qualquer dataset novo é onboardado apenas com um contrato YAML em contracts/.
# Convenção DAMA-DMBOK: metadados e lineage registrados em governanca.log_ingestao.
# =============================================================================
import fnmatch
import hashlib
import os
import shutil
import time
from datetime import datetime, timezone

import yaml

# Caminho base do projeto no workspace
BASE_PATH = "/Workspace/Users/alexvalsechibosque@gmail.com/case_ifood"
CONTRACTS_DIR = f"{BASE_PATH}/contracts"


def carregar_contrato(nome_dataset):
    """Carrega um contrato YAML pelo nome do dataset (ex.: 'yellow_trips')."""
    caminho = f"{CONTRACTS_DIR}/{nome_dataset}.yml"
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Contrato não encontrado: {caminho}")
    with open(caminho, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def listar_contratos():
    """Lista todos os datasets com contrato definido."""
    contratos = []
    for nome in sorted(os.listdir(CONTRACTS_DIR)):
        if nome.endswith(".yml"):
            c = carregar_contrato(nome[:-4])
            contratos.append(c)
    return contratos


def _log_path(catalogo):
    return f"{catalogo}.governanca.log_ingestao"


def criar_tabelas_governanca(catalogo, spark):
    """Cria as tabelas de log/métricas de governança (idempotente)."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {_log_path(catalogo)} (
            dataset STRING,
            arquivo STRING,
            fonte STRING,
            tamanho_mb DOUBLE,
            checksum STRING,
            status STRING,
            ingested_at TIMESTAMP,
            mensagem STRING
        ) USING DELTA
        COMMENT 'Log de execuções do motor de ingestão (DAMA: Metadata Management / lineage)'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalogo}.governanca.dq_resultados (
            dataset STRING,
            tabela STRING,
            dimensao STRING,
            regra STRING,
            metrica DOUBLE,
            status STRING,
            avaliado_em TIMESTAMP,
            exemplos STRING COMMENT 'JSON com ate 5 registros que violaram a regra (chave + colunas envolvidas)'
        ) USING DELTA
        COMMENT 'Resultados do motor de qualidade por dimensão DAMA-DMBOK'
    """)
    # migracao idempotente: coluna exemplos adicionada apos a primeira versao
    if "exemplos" not in spark.table(f"{catalogo}.governanca.dq_resultados").columns:
        spark.sql(
            f"ALTER TABLE {catalogo}.governanca.dq_resultados "
            "ADD COLUMNS (exemplos STRING COMMENT 'JSON com ate 5 registros que violaram a regra')"
        )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalogo}.governanca.obs_metricas (
            dataset STRING,
            tabela STRING,
            metrica STRING,
            valor DOUBLE,
            status STRING,
            coletado_em TIMESTAMP
        ) USING DELTA
        COMMENT 'Métricas de observabilidade por camada (freshness, contagens)'
    """)


def _checksum_arquivo(caminho, max_bytes=64 * 1024 * 1024):
    """Checksum md5 dos primeiros max_bytes (suficiente para detectar mudanças)."""
    h = hashlib.md5()
    with open(caminho, "rb") as f:
        h.update(f.read(max_bytes))
    return h.hexdigest()


def executar_ingestao(contrato, spark, origem_volume=None, destino_volume=None,
                      catalogo="case_ifood", filtro_arquivos=None):
    """MOTOR DE INGESTÃO GENÉRICO orientado a contrato.

    Move arquivos parquet da origem (volume raw) para a landing zone declarada
    no contrato, com metadados (tamanho, checksum, timestamp) e log em
    {catalogo}.governanca.log_ingestao. Idempotente: arquivos já presentes
    com o mesmo tamanho são pulados. Funciona para QUALQUER dataset parquet
    em volume — basta um contrato.

    Args:
        contrato: dict do contrato YAML (carregar_contrato).
        spark: SparkSession.
        origem_volume: volume de origem; default = contrato.fonte.volume.
        destino_volume: volume landing de destino; default = volume do contrato.
        catalogo: catálogo UC onde ficam as tabelas de governança.
        filtro_arquivos: lista opcional de nomes; None = todos os que casam com o glob.
    """
    fonte = contrato["fonte"]
    origem = origem_volume or fonte["volume"]
    destino = destino_volume or fonte["volume"]
    glob = fonte["glob"]
    dataset = contrato["dataset"]

    os.makedirs(destino, exist_ok=True)
    criar_tabelas_governanca(catalogo, spark)

    candidatos = sorted(
        f for f in os.listdir(origem)
        if fnmatch.fnmatch(f, glob)
        and (filtro_arquivos is None or f in filtro_arquivos)
    )
    if not candidatos:
        print(f"[aviso] nenhum arquivo em {origem} casa com o glob '{glob}'")

    resultados = []
    for nome in candidatos:
        origem_completa = os.path.join(origem, nome)
        destino_completo = os.path.join(destino, nome)
        tamanho_mb = os.path.getsize(origem_completa) / (1024 * 1024)
        checksum = _checksum_arquivo(origem_completa)
        status, mensagem = "ok", ""
        inicio = time.time()

        if os.path.exists(destino_completo) and os.path.getsize(destino_completo) > 0:
            status = "ja_existia"
        else:
            try:
                shutil.copy2(origem_completa, destino_completo)
            except Exception as exc:
                status, mensagem = "erro", str(exc)

        duracao = round(time.time() - inicio, 2)
        resultados.append({
            "dataset": dataset, "arquivo": nome, "fonte": origem_completa,
            "tamanho_mb": round(tamanho_mb, 2), "checksum": checksum,
            "status": status, "ingested_at": datetime.now(timezone.utc),
            "mensagem": mensagem,
        })
        print(f"[{status}] {nome}: {tamanho_mb:.1f} MB ({duracao}s)")

        spark.createDataFrame(
            [(dataset, nome, origem_completa, round(tamanho_mb, 2), checksum,
              status, datetime.now(timezone.utc), mensagem)],
            schema="dataset STRING, arquivo STRING, fonte STRING, tamanho_mb DOUBLE, "
                   "checksum STRING, status STRING, ingested_at TIMESTAMP, mensagem STRING",
        ).write.mode("append").saveAsTable(_log_path(catalogo))

    return resultados


def baixar_da_fonte(contrato, spark, ano, meses, catalogo="case_ifood", max_tentativas=3):
    """MOTOR DE DOWNLOAD GENÉRICO orientado a contrato.

    Baixa os parquets da fonte oficial (URL declarada no contrato,
    fonte.url_template) direto para o volume landing do contrato, com retry,
    arquivo temporário (só move se completar), checksum e log em
    {catalogo}.governanca.log_ingestao. Idempotente: arquivo já presente
    com tamanho > 0 é pulado. Funciona para QUALQUER dataset com
    url_template no contrato — basta informar ano e meses.
    """
    import tempfile

    import requests

    fonte = contrato["fonte"]
    url_template = fonte["url_template"]
    destino = fonte["volume"]
    taxi_type = contrato["taxi_type"]
    dataset = contrato["dataset"]

    os.makedirs(destino, exist_ok=True)
    criar_tabelas_governanca(catalogo, spark)

    resultados = []
    for mes in meses:
        nome = url_template.split("/")[-1].format(taxi_type=taxi_type, ano=ano, mes=mes)
        url = url_template.format(taxi_type=taxi_type, ano=ano, mes=mes)
        destino_completo = os.path.join(destino, nome)
        status, mensagem, checksum = "ok", "", ""
        inicio = time.time()

        if os.path.exists(destino_completo) and os.path.getsize(destino_completo) > 0:
            status = "ja_existia"
            tamanho_mb = os.path.getsize(destino_completo) / (1024 * 1024)
        else:
            ultimo_erro = None
            for tentativa in range(1, max_tentativas + 1):
                try:
                    with requests.get(url, stream=True, timeout=300) as resp:
                        resp.raise_for_status()
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                            tmp_path = tmp.name
                            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                                tmp.write(chunk)
                    shutil.move(tmp_path, destino_completo)
                    tamanho_mb = os.path.getsize(destino_completo) / (1024 * 1024)
                    ultimo_erro = None
                    break
                except Exception as exc:
                    ultimo_erro = exc
                    print(f"[erro] tentativa {tentativa}/{max_tentativas} para {nome}: {exc}")
                    time.sleep(2 * tentativa)
            if ultimo_erro is not None:
                status, mensagem = "erro", str(ultimo_erro)
                tamanho_mb = 0.0
            else:
                checksum = _checksum_arquivo(destino_completo)

        duracao = round(time.time() - inicio, 2)
        resultados.append({
            "dataset": dataset, "arquivo": nome, "fonte": url,
            "tamanho_mb": round(tamanho_mb, 2), "checksum": checksum,
            "status": status, "ingested_at": datetime.now(timezone.utc),
            "mensagem": mensagem,
        })
        print(f"[{status}] {nome}: {tamanho_mb:.1f} MB ({duracao}s)")

        spark.createDataFrame(
            [(dataset, nome, url, round(tamanho_mb, 2), checksum,
              status, datetime.now(timezone.utc), mensagem)],
            schema="dataset STRING, arquivo STRING, fonte STRING, tamanho_mb DOUBLE, "
                   "checksum STRING, status STRING, ingested_at TIMESTAMP, mensagem STRING",
        ).write.mode("append").saveAsTable(_log_path(catalogo))

        if status == "erro":
            raise RuntimeError(f"Falha ao baixar {nome} apos {max_tentativas} tentativas") from ultimo_erro

    return resultados


def copiar_glob(origem_glob_dir, glob, destino_dir):
    """Copia arquivos que casam com o glob de uma pasta para outra (uso manual)."""
    os.makedirs(destino_dir, exist_ok=True)
    copiados = []
    for f in sorted(os.listdir(origem_glob_dir)):
        if fnmatch.fnmatch(f, glob):
            shutil.copy2(os.path.join(origem_glob_dir, f), os.path.join(destino_dir, f))
            copiados.append(f)
    return copiados
