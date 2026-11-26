"""Testes estruturais dos contratos YAML do case_nyc_taxi — rodam headless no CI.

Validam as regras do SDD: campos obrigatórios, convenções de destinos
(bronze/silver/quarentena), referências cruzadas (membros do gold, colunas do
schema, enriquecimentos) e a whitelist de funções de agregação.
"""
import os

import yaml

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTRATOS_DIR = os.path.join(RAIZ, "src", "contracts")
GOLD_DIR = os.path.join(CONTRATOS_DIR, "gold")
REFS_ARQUIVO = os.path.join(CONTRATOS_DIR, "references", "dimensoes_tlc.yml")

FUNCOES_PERMITIDAS = {"count", "count_distinct", "sum", "avg", "min", "max"}


def _carregar(caminho):
    with open(caminho, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _contratos_ingestao():
    arquivos = sorted(
        n for n in os.listdir(CONTRATOS_DIR)
        if n.endswith(".yml") and os.path.isfile(os.path.join(CONTRATOS_DIR, n))
    )
    return [(nome, _carregar(os.path.join(CONTRATOS_DIR, nome))) for nome in arquivos]


def _contratos_gold():
    arquivos = sorted(n for n in os.listdir(GOLD_DIR) if n.endswith(".yml"))
    return [_carregar(os.path.join(GOLD_DIR, n)) for n in arquivos]


def _dimensoes_referencia():
    return _carregar(REFS_ARQUIVO)["dimensoes"]


# ------------------------------------------------------------- ingestão ----
def test_contratos_de_ingestao_tem_campos_obrigatorios():
    obrigatorios = [
        "dataset", "taxi_type", "descricao", "fonte", "destinos",
        "chave_dedupe_origem", "mapeamento_silver", "schema_bronze",
        "colunas_obrigatorias", "regras_qualidade",
    ]
    contratos = _contratos_ingestao()
    assert contratos, "nenhum contrato de ingestão encontrado"
    for nome, contrato in contratos:
        for campo in obrigatorios:
            assert campo in contrato, f"{nome}: campo obrigatório ausente '{campo}'"
        assert contrato["chave_dedupe_origem"], f"{nome}: chave_dedupe_origem vazio"


def test_destinos_seguem_convencao_bronze_silver_quarentena():
    for nome, contrato in _contratos_ingestao():
        dataset = contrato["dataset"]
        destinos = contrato["destinos"]
        assert destinos["bronze"] == f"case_nyc_taxi.bronze.{dataset}", nome
        assert destinos["silver"] == f"case_nyc_taxi.silver.{dataset}", nome
        assert destinos["quarentena"] == (
            f"case_nyc_taxi.governanca.quarantine_{dataset}"
        ), nome


def test_mapeamento_silver_aponta_para_colunas_do_schema_bronze():
    for nome, contrato in _contratos_ingestao():
        schema = set(contrato["schema_bronze"])
        for destino, origens in contrato["mapeamento_silver"].items():
            origens = [origens] if isinstance(origens, str) else origens
            for origem in origens:
                assert origem in schema, (
                    f"{nome}: mapeamento '{destino}' usa '{origem}' "
                    "fora do schema_bronze"
                )


def test_colunas_obrigatorias_estao_mapeadas_na_silver():
    for nome, contrato in _contratos_ingestao():
        mapeadas = set(contrato["mapeamento_silver"])
        for coluna in contrato["colunas_obrigatorias"]:
            assert coluna in mapeadas, (
                f"{nome}: coluna obrigatória '{coluna}' sem mapeamento silver"
            )


def test_regras_qualidade_declara_dimensoes_dama():
    for nome, contrato in _contratos_ingestao():
        regras = contrato["regras_qualidade"]
        for dimensao in ("completude", "validade", "unicidade",
                         "consistencia", "pontualidade"):
            assert dimensao in regras, f"{nome}: dimensão DAMA '{dimensao}' ausente"


# ------------------------------------------------------------------ gold ----
def test_gold_membros_existem_como_datasets_de_ingestao():
    datasets = {c["dataset"] for _, c in _contratos_ingestao()}
    for contrato in _contratos_gold():
        for membro in contrato["membros"]:
            assert membro in datasets, f"membro '{membro}' sem contrato de ingestão"


def test_gold_tem_nomes_unicos_consolidado_e_agregados():
    nomes = set()
    for contrato in _contratos_gold():
        assert contrato["nome"] not in nomes, contrato["nome"]
        nomes.add(contrato["nome"])
        for agregado in contrato.get("agregados", []):
            assert agregado["nome"] not in nomes, agregado["nome"]
            nomes.add(agregado["nome"])


def test_gold_funcoes_de_agregacao_na_whitelist():
    for contrato in _contratos_gold():
        for agregado in contrato.get("agregados", []):
            for medida in agregado["medidas"]:
                assert medida["funcao"] in FUNCOES_PERMITIDAS, (
                    f"{agregado['nome']}: função '{medida['funcao']}' fora da whitelist"
                )


def test_gold_enriquecimentos_referenciam_dims_validas():
    colunas_dims = {
        ref["destinos"]["silver"]: {c["origem"] for c in ref["colunas"]}
        for ref in _dimensoes_referencia()
    }
    por_dataset = {c["dataset"]: c for _, c in _contratos_ingestao()}
    for contrato in _contratos_gold():
        mapeadas = set()
        for membro in contrato["membros"]:
            mapeadas |= set(por_dataset[membro]["mapeamento_silver"])
        for enrich in contrato.get("enriquecimentos", []):
            assert enrich["dimensao"] in colunas_dims, (
                f"enriquecimento aponta dimensão inexistente: {enrich['dimensao']}"
            )
            assert enrich["chave_fato"] in mapeadas, (
                f"chave_fato '{enrich['chave_fato']}' não é coluna silver dos membros"
            )
            for col in enrich["colunas"]:
                assert col["origem"] in colunas_dims[enrich["dimensao"]], (
                    f"coluna de dimensão inexistente: {col['origem']}"
                )


# ------------------------------------------------------- dimensões TLC -----
def test_dimensoes_de_referencia_consistentes():
    refs = _dimensoes_referencia()
    assert refs, "nenhuma dimensão de referência declarada"
    vistos = set()
    for ref in refs:
        destino_silver = ref["destinos"]["silver"]
        assert destino_silver not in vistos, destino_silver
        vistos.add(destino_silver)
        colunas = {c["nome"] for c in ref["colunas"]}
        assert ref["chave"] in colunas, f"{destino_silver}: chave fora das colunas"
        if ref["fonte"]["tipo"] == "linhas":
            for linha in ref["fonte"]["linhas"]:
                assert set(linha) == colunas, (
                    f"{destino_silver}: linha do dicionário fora das colunas"
                )
        else:
            assert ref["fonte"]["tipo"] == "volume_csv", destino_silver
            assert ref["fonte"]["path"].startswith("/Volumes/"), destino_silver


# ---------------------------------------------------------------- motores --
def test_motor_carrega_todos_os_contratos_de_ingestao(monkeypatch):
    import motores

    monkeypatch.setattr(motores, "CONTRACTS_DIR", CONTRATOS_DIR)
    datasets = {c["dataset"] for c in motores.listar_contratos()}
    esperados = {c["dataset"] for _, c in _contratos_ingestao()}
    assert datasets == esperados
