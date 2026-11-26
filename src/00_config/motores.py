# =============================================================================
# Motores genéricos do case_nyc_taxi — sem lógica específica de dataset.
# Qualquer dataset novo é onboardado apenas com um contrato YAML em contracts/.
# Convenção DAMA-DMBOK: metadados e lineage registrados em governanca.log_ingestao.
# =============================================================================
import fnmatch
import os
import shutil

import yaml

from download import COLUNAS_LOG, FalhaIngestao, ingerir_particoes
from landing import normalizar_periodos

# Caminho base do projeto: relativo a este arquivo (00_config/) — portável
# entre workspaces e para cópias deployadas via DAB (sem path absoluto).
BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTRACTS_DIR = os.path.join(BASE_PATH, "contracts")


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
    # Evolução aditiva: mantém o histórico de ingestão anterior e acrescenta
    # os campos de partição, execução, HTTP, integridade e tentativa.
    existentes = set(spark.table(_log_path(catalogo)).columns)
    adicionais = [f"{nome} {tipo}" for nome, tipo in COLUNAS_LOG.items() if nome not in existentes]
    if adicionais:
        spark.sql(f"ALTER TABLE {_log_path(catalogo)} ADD COLUMNS ({', '.join(adicionais)})")
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


def executar_ingestao(contrato, spark, origem_volume=None, destino_volume=None,
                      catalogo="case_nyc_taxi", filtro_arquivos=None, ano=None,
                      meses=None, execucao_id=None):
    """Copia raw para as mesmas partições e com a mesma auditoria do download.

    O modo raw exige ano/meses ou um filtro de nomes gerados pelo url_template.
    Aceita a landing plana anterior e a origem já particionada. Nunca apaga a
    origem. Cada arquivo deve ser um Parquet válido antes de ser publicado.
    """
    if ano is None or meses is None:
        if not filtro_arquivos:
            raise ValueError("Informe ano e meses (ou filtro_arquivos) para planejar as partições raw")
        import re
        periodos = []
        for nome in filtro_arquivos:
            match = re.search(r"_(\d{4})-(\d{2})\.parquet$", nome)
            if not match:
                raise ValueError(f"Nome sem período: {nome}")
            periodos.append(match.groups())
        anos = {a for a, _ in periodos}
        if len(anos) != 1:
            raise ValueError("Execute um ano por chamada de ingestão")
        ano, meses = periodos[0][0], [m for _, m in periodos]
    ano, meses = normalizar_periodos(ano, meses)
    criar_tabelas_governanca(catalogo, spark)
    return ingerir_particoes(
        contrato, spark, ano, meses, catalogo, execucao_id=execucao_id,
        origem_volume=origem_volume or contrato["fonte"]["volume"],
        destino_volume=destino_volume,
    )


def baixar_da_fonte(contrato, spark, ano, meses, catalogo="case_nyc_taxi",
                    max_tentativas=None, execucao_id=None):
    """Baixa e valida as partições declaradas no contrato, auditando cada tentativa.

    Erros temporários usam backoff/Retry-After. Falhas permanentes não são
    retentadas; o lote processa os demais meses e falha com o resumo completo.
    A landing antiga é copiada para a organização particionada sem remoção.
    """
    ano, meses = normalizar_periodos(ano, meses)
    criar_tabelas_governanca(catalogo, spark)
    return ingerir_particoes(contrato, spark, ano, meses, catalogo,
                             max_tentativas=max_tentativas, execucao_id=execucao_id)


def copiar_glob(origem_glob_dir, glob, destino_dir):
    """Copia arquivos que casam com o glob de uma pasta para outra (uso manual)."""
    os.makedirs(destino_dir, exist_ok=True)
    copiados = []
    for f in sorted(os.listdir(origem_glob_dir)):
        if fnmatch.fnmatch(f, glob):
            shutil.copy2(os.path.join(origem_glob_dir, f), os.path.join(destino_dir, f))
            copiados.append(f)
    return copiados
