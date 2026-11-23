# =============================================================================
# Pipeline SDP medallion — bronze -> silver -> gold, ORIENTADA A CONTRATOS.
# Nenhuma lógica específica de dataset: tudo deriva dos YAML em contracts/
# (ingestão: bronze/silver) e contracts/gold/ (visões de consumo).
# Onboarding de novo dataset = novo contrato; o loop gera bronze/silver dele.
# Nova unificação/agregado gold = novo contrato de gold em contracts/gold/.
# =============================================================================
import os

import yaml
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CONTRACTS_DIR = "/Workspace/Users/alexvalsechibosque@gmail.com/case_ifood/contracts"


def _carregar_contratos():
    contratos = []
    for nome in sorted(os.listdir(CONTRACTS_DIR)):
        if nome.endswith(".yml"):
            with open(os.path.join(CONTRACTS_DIR, nome), "r", encoding="utf-8") as fh:
                contratos.append(yaml.safe_load(fh))
    return contratos


CONTRATOS = _carregar_contratos()


def _expectations_do_contrato(contrato):
    """Deriva expectations do SDP das regras declaradas no contrato."""
    drops = {}
    for regra in contrato.get("regras_qualidade", {}).get("validade", {}).get("regras", []):
        drops[f"validade_{regra['coluna']}"] = regra["expressao"]
    for i, expressao in enumerate(contrato.get("regras_qualidade", {}).get("consistencia", {}).get("regras", [])):
        drops[f"consistencia_{i}"] = expressao
    criticas = [c for c in contrato["colunas_obrigatorias"] if c != "passenger_count"]
    for coluna in criticas:
        drops[f"obrigatoria_{coluna}"] = f"{coluna} IS NOT NULL"
    warns = {}
    if "passenger_count" in contrato["colunas_obrigatorias"]:
        warns["obrigatoria_passenger_count"] = "passenger_count IS NOT NULL"
    return drops, warns


# ---------------------------------------------------------------- bronze -----
# Streaming table por dataset, alimentada por UM FLUXO POR ARQUIVO: cada parquet
# da landing (glob do contrato) e lido com o PROPRIO schema e normalizado para o
# schema_bronze canônico do contrato via cast. Isso contorna as variações de tipo
# físico (int/bigint/double) e de capitalização entre os arquivos mensais da TLC.
import glob as _glob

for _contrato in CONTRATOS:
    _dataset = _contrato["dataset"]
    _volume = _contrato["fonte"]["volume"]
    _arquivos = sorted(
        os.path.basename(p)
        for p in _glob.glob(f"{_volume}/{_contrato['fonte']['glob']}")
    )
    _nome_bronze = _contrato["destinos"]["bronze"]
    _schema_bronze = _contrato["schema_bronze"]
    _comentario = _contrato["descricao"].strip()

    # tabela streaming vazia; os fluxos por arquivo fazem o append
    dp.create_streaming_table(
        name=_nome_bronze,
        comment=f"[bronze] {_comentario}",
        # parquets da TLC usam TIMESTAMP_NTZ; habilita a feature Delta na bronze
        table_properties={"delta.feature.timestampNtz": "supported"},
    )

    def _criar_fluxo_arquivo(caminho_arquivo, nome_fluxo, schema_bronze, destino):
        @dp.append_flow(target=destino, name=nome_fluxo, once=True,
                        comment=f"Ingestão normalizada de {os.path.basename(caminho_arquivo)}")
        def fluxo():
            df = spark.read.format("parquet").load(caminho_arquivo)
            df_cols_ci = {c.lower(): c for c in df.columns}
            colunas = []
            for coluna, tipo in schema_bronze.items():
                variantes = [orig for key, orig in df_cols_ci.items()
                             if key == coluna.lower()]
                if variantes:
                    colunas.append(F.coalesce(*[F.col(v).cast(tipo) for v in variantes])
                                   .alias(coluna))
                else:
                    colunas.append(F.lit(None).cast(tipo).alias(coluna))
            return (
                df.select(*colunas)
                .withColumn("_arquivo_origem", F.lit(os.path.basename(caminho_arquivo)))
                .withColumn("_ingestao_ts", F.current_timestamp())
            )

        return fluxo

    for _arquivo in _arquivos:
        _criar_fluxo_arquivo(
            caminho_arquivo=f"{_volume}/{_arquivo}",
            nome_fluxo=f"{_dataset}_{_arquivo.replace('.', '_').replace('-', '_')}",
            schema_bronze=_schema_bronze,
            destino=_nome_bronze,
        )

# ---------------------------------------------------------------- silver -----
# Materialized view por dataset: mapeia colunas do contrato, gera trip_id
# (hash da chave de dedupe), deduplica e aplica expectations do contrato.
for _contrato in CONTRATOS:
    _dataset = _contrato["dataset"]
    _nome_bronze = _contrato["destinos"]["bronze"]
    _nome_silver = _contrato["destinos"]["silver"]
    _mapeamento = _contrato["mapeamento_silver"]
    _chave = _contrato["chave_dedupe_origem"]
    _taxi_type = _contrato["taxi_type"]
    _drops, _warns = _expectations_do_contrato(_contrato)
    _comentario = _contrato["descricao"].strip()

    def _criar_silver(contrato, nome_bronze, nome_silver, mapeamento, chave,
                      taxi_type, drops, warns, comentario):
        @dp.materialized_view(
            name=nome_silver,
            comment=f"[silver] {comentario} Deduplicado por trip_id; expectations do contrato.",
            cluster_by_auto=True,
        )
        @dp.expect_all_or_drop(drops)
        @dp.expect_all(warns)
        def silver():
            df = spark.read.table(nome_bronze)
            # trip_id calculado sobre as colunas ORIGINAIS (antes do mapeamento)
            hash_cols = [F.col(c).cast("string") for c in chave]
            df = (
                df.withColumn("trip_id", F.xxhash64(*hash_cols))
                .withColumn("taxi_type", F.lit(taxi_type))
            )
            selecionadas = []
            for padrao, origem in mapeamento.items():
                origens = origem if isinstance(origem, list) else [origem]
                if len(origens) == 1:
                    expressao = F.col(origens[0])
                else:
                    expressao = F.coalesce(*[F.col(o) for o in origens])
                if "datetime" in padrao.lower():
                    expressao = expressao.cast("timestamp")
                selecionadas.append(expressao.alias(padrao))
            return (
                df.select(*selecionadas, F.col("trip_id"), F.col("taxi_type"))
                .dropDuplicates(["trip_id"])
            )

        return silver

    _criar_silver(_contrato, _nome_bronze, _nome_silver, _mapeamento, _chave,
                  _taxi_type, _drops, _warns, _comentario)

# ------------------------------------------------------------------ gold -----
# Visões de consumo geradas a partir dos contratos de gold em contracts/gold/:
# cada YAML declara os membros (datasets de ingestão a unificar) e os agregados
# derivados (dimensões, medidas e clusterização). Nada de gold hardcoded aqui.
GOLD_CONTRACTS_DIR = os.path.join(CONTRACTS_DIR, "gold")

_FUNCOES_AGREGACAO = ("count", "count_distinct", "sum", "avg", "min", "max")


def _carregar_contratos_gold(silver_por_dataset):
    """Carrega e valida contratos de gold; membros resolvem para tabelas silver."""
    contratos_gold = []
    if not os.path.isdir(GOLD_CONTRACTS_DIR):
        return contratos_gold
    for nome_arquivo in sorted(os.listdir(GOLD_CONTRACTS_DIR)):
        if not nome_arquivo.endswith(".yml"):
            continue
        with open(os.path.join(GOLD_CONTRACTS_DIR, nome_arquivo), "r", encoding="utf-8") as fh:
            contrato = yaml.safe_load(fh)
        nao_resolvidos = [m for m in contrato.get("membros", [])
                          if m not in silver_por_dataset]
        if nao_resolvidos:
            raise ValueError(
                f"Contrato de gold '{nome_arquivo}': membros sem contrato de "
                f"ingestão correspondente: {nao_resolvidos}"
            )
        for agregado in contrato.get("agregados", []):
            for medida in agregado.get("medidas", []):
                if medida["funcao"] not in _FUNCOES_AGREGACAO:
                    raise ValueError(
                        f"Contrato de gold '{nome_arquivo}', agregado "
                        f"'{agregado['nome']}': função '{medida['funcao']}' não "
                        f"suportada (permitidas: {list(_FUNCOES_AGREGACAO)})"
                    )
        contratos_gold.append(contrato)
    return contratos_gold


_SILVER_POR_DATASET = {c["dataset"]: c["destinos"]["silver"] for c in CONTRATOS}
CONTRATOS_GOLD = _carregar_contratos_gold(_SILVER_POR_DATASET)


def _expressao_dimensao(dim):
    """Dimensão de agrupamento: coluna com alias e transformação opcionais."""
    if not isinstance(dim, dict):
        return F.col(dim)
    coluna = dim["coluna"]
    if dim.get("transformacao") == "to_date":
        expr = F.to_date(F.col(coluna))
    else:
        expr = F.col(coluna)
    return expr.alias(dim.get("alias", coluna))


def _expressao_medida(medida):
    """Medida declarativa: função da whitelist aplicada à coluna do contrato."""
    alias, funcao, coluna = medida["alias"], medida["funcao"], medida["coluna"]
    if funcao == "count":
        return F.count(F.lit(1)).alias(alias)  # equivale a count(*)
    if funcao == "count_distinct":
        return F.countDistinct(F.col(coluna)).alias(alias)
    fn = {"sum": F.sum, "avg": F.avg, "min": F.min, "max": F.max}[funcao]
    return fn(F.col(coluna)).alias(alias)


for _contrato_gold in CONTRATOS_GOLD:
    _nome_gold = _contrato_gold["nome"]
    _silvers = [_SILVER_POR_DATASET[m] for m in _contrato_gold["membros"]]
    _kwargs_gold = {"comment": f"[gold] {_contrato_gold['descricao'].strip()}"}
    if _contrato_gold.get("cluster_by_auto"):
        _kwargs_gold["cluster_by_auto"] = True

    def _criar_gold_consolidado(nome, silvers, kwargs):
        @dp.materialized_view(name=nome, **kwargs)
        def consolidado():
            dfs = [spark.read.table(t) for t in silvers]
            resultado = dfs[0]
            for df in dfs[1:]:
                resultado = resultado.unionByName(df, allowMissingColumns=True)
            return resultado

        return consolidado

    _criar_gold_consolidado(_nome_gold, _silvers, _kwargs_gold)

    for _agregado in _contrato_gold.get("agregados", []):
        _dimensoes = [_expressao_dimensao(d) for d in _agregado["agrupar_por"]]
        _medidas = [_expressao_medida(m) for m in _agregado["medidas"]]
        _kwargs_agg = {"comment": f"[gold] {_agregado['descricao'].strip()}"}
        if _agregado.get("cluster_by"):
            _kwargs_agg["cluster_by"] = _agregado["cluster_by"]

        def _criar_gold_agregado(nome, tabela_base, dimensoes, medidas, kwargs):
            @dp.materialized_view(name=nome, **kwargs)
            def agregado():
                return (
                    spark.read.table(tabela_base)
                    .groupBy(*dimensoes)
                    .agg(*medidas)
                )

            return agregado

        _criar_gold_agregado(_agregado["nome"], _nome_gold, _dimensoes,
                             _medidas, _kwargs_agg)
