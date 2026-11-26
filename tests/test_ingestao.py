"""Resiliência da landing com Parquets reais e HTTP simulado, sem Databricks."""
import copy
import hashlib
import inspect
import json
import os
import runpy
import sys
import threading
import types
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
import yaml

import download
import landing
import motores


class SparkLog:
    """Exercita a serialização das linhas que o motor entregaria ao Spark."""
    def __init__(self):
        self.eventos = []
        self.schema = None

    def createDataFrame(self, linhas, schema):
        self.schema = schema
        colunas = [campo.strip().split()[0] for campo in schema.split(",")]
        self.eventos.extend(dict(zip(colunas, linha)) for linha in linhas)
        return self

    @property
    def write(self):
        return self

    def mode(self, modo):
        assert modo == "append"
        return self

    def saveAsTable(self, tabela):
        assert tabela.endswith(".governanca.log_ingestao")


class Resposta:
    def __init__(self, corpo=b"", status=200, headers=None, falha=None):
        self.corpo, self.status_code, self.falha = corpo, status, falha
        self.headers = {"Content-Length": str(len(corpo)), **(headers or {})}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_content(self, chunk_size):
        yield self.corpo[:10]
        if self.falha:
            raise self.falha
        yield self.corpo[10:]


class Sessao:
    def __init__(self, respostas):
        self.respostas = iter(respostas)
        self.chamadas = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get(self, url, **kwargs):
        self.chamadas.append((url, kwargs))
        resposta = next(self.respostas)
        if isinstance(resposta, Exception):
            raise resposta
        return resposta


@pytest.fixture
def contrato(tmp_path):
    raiz = Path(__file__).resolve().parents[1]
    doc = yaml.safe_load((raiz / "src/contracts/green_trips.yml").read_text(encoding="utf-8"))
    doc["fonte"]["volume"] = str(tmp_path / "landing")
    return doc


@pytest.fixture
def parquet(tmp_path):
    caminho = tmp_path / "exemplo.parquet"
    pq.write_table(pa.table({"VendorID": [1, 2], "total_amount": [10.5, 20.0]}), caminho)
    return caminho.read_bytes()


@pytest.fixture
def ambiente(monkeypatch):
    spark = SparkLog()
    esperas = []
    monkeypatch.setattr(download.time, "sleep", esperas.append)
    monkeypatch.setattr(download.random, "random", lambda: 0.0)

    def preparar(respostas):
        sessao = Sessao(respostas)
        monkeypatch.setattr(download.requests, "Session", lambda: sessao)
        return spark, sessao, esperas
    return preparar


def executar(contrato, spark, meses=("05",), **kwargs):
    return download.ingerir_particoes(contrato, spark, "2023", meses, "case_nyc_taxi", **kwargs)


def test_download_particionado_manifesto_e_auditoria(contrato, parquet, ambiente):
    spark, sessao, _ = ambiente([Resposta(parquet)])
    resultado = executar(contrato, spark)[0]
    destino = Path(contrato["fonte"]["volume"]) / "categoria=green/mes_ano=05-2023/green_tripdata_2023-05.parquet"
    assert destino.read_bytes() == parquet
    manifesto = landing.ler_manifesto(str(destino))
    assert manifesto["checksum"] == hashlib.sha256(parquet).hexdigest()
    assert manifesto["linhas"] == 2
    assert resultado["destino"] == str(destino)
    assert [e["evento"] for e in spark.eventos] == [
        "inicio_lote", "inicio_arquivo", "inicio_tentativa", "tentativa", "fim_arquivo", "fim_lote",
    ]
    assert len({e["execucao_id"] for e in spark.eventos}) == 1
    assert spark.eventos[-1]["arquivos_ok"] == spark.eventos[-1]["arquivos_previstos"] == 1
    assert sessao.chamadas[0][1]["timeout"] == (15, 60)
    assert landing.listar_arquivos_landing(contrato) == [str(destino)]
    assert not list(destino.parent.glob("*.part"))


def test_categorias_e_periodos_nao_colidem(contrato):
    yellow = copy.deepcopy(contrato)
    yellow["taxi_type"] = "yellow"
    planos = landing.planejar_arquivos(contrato, 2023, [5, "05", 6])
    planos += landing.planejar_arquivos(yellow, "2024", [5, 6])
    assert len({p["destino"] for p in planos}) == 4
    assert planos[1]["mes_ano"] == "06-2023"
    assert "categoria=yellow" in planos[2]["destino"]


@pytest.mark.parametrize("ano,meses", [(2023, []), (2023, [0]), (2023, [13]),
                                        (2023, ["../05"]), ("abc", [5]), (2023, [True])])
def test_periodo_invalido_rejeitado_antes_do_http(contrato, ano, meses):
    with pytest.raises(ValueError):
        landing.planejar_arquivos(contrato, ano, meses)


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 204, 206, 302])
def test_http_permanente_nao_repete_nem_publica(contrato, ambiente, status):
    spark, sessao, esperas = ambiente([Resposta(status=status)])
    with pytest.raises(download.FalhaIngestao):
        executar(contrato, spark)
    assert len(sessao.chamadas) == 1 and esperas == []
    assert spark.eventos[-2]["http_status"] == status
    assert spark.eventos[-1]["arquivos_erro"] == 1
    assert not list(Path(contrato["fonte"]["volume"]).rglob("*.parquet"))
    assert not list(Path(contrato["fonte"]["volume"]).rglob("*.part"))


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_http_temporario_recupera_com_backoff(contrato, parquet, ambiente, status):
    spark, sessao, esperas = ambiente([Resposta(status=status), Resposta(parquet)])
    assert executar(contrato, spark)[0]["status"] == "ok"
    assert len(sessao.chamadas) == 2 and esperas == [2.0]
    tentativas = [e for e in spark.eventos if e["evento"] == "tentativa"]
    assert tentativas[0]["vai_repetir"] and tentativas[0]["http_status"] == status


@pytest.mark.parametrize("erro,tipo,retenta", [
    (requests.ConnectTimeout(), "timeout", True),
    (requests.ReadTimeout(), "timeout", True),
    (requests.ConnectionError("DNS"), "conexao", True),
    (requests.exceptions.SSLError("certificado"), "tls", False),
    (requests.exceptions.TooManyRedirects(), "redirecionamentos", False),
])
def test_excecoes_de_rede_classificadas(contrato, parquet, ambiente, erro, tipo, retenta):
    spark, sessao, _ = ambiente([erro, Resposta(parquet)])
    if retenta:
        executar(contrato, spark)
    else:
        with pytest.raises(download.FalhaIngestao):
            executar(contrato, spark)
    assert [e for e in spark.eventos if e["evento"] == "tentativa"][0]["erro_tipo"] == tipo
    assert len(sessao.chamadas) == (2 if retenta else 1)


def test_retry_after_respeitado_e_sem_espera_apos_ultima_tentativa(contrato, ambiente):
    spark, sessao, esperas = ambiente([Resposta(status=429, headers={"Retry-After": "8"})] * 3)
    with pytest.raises(download.FalhaIngestao):
        executar(contrato, spark)
    assert len(sessao.chamadas) == 3 and esperas == [8.0, 8.0]


def test_retry_after_acima_do_orcamento_falha_sem_repetir_cedo(contrato, ambiente):
    spark, sessao, esperas = ambiente([Resposta(status=503, headers={"Retry-After": "3600"})])
    with pytest.raises(download.FalhaIngestao) as exc:
        executar(contrato, spark)
    assert len(sessao.chamadas) == 1 and not esperas
    assert "reagende" in exc.value.resultados[0]["mensagem"]


def test_retry_after_data_e_cabecalho_malformado(contrato):
    politica = download._politica(contrato, None)
    data = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
    assert 28 <= download._espera_retry(data, 1, politica) <= 30
    assert 2 <= download._espera_retry("invalido", 1, politica) <= 3


@pytest.mark.parametrize("resposta,tipo", [
    (Resposta(b"<html>erro</html>", headers={"Content-Type": "text/html"}), "conteudo_inesperado"),
    (Resposta(b"nao e parquet"), "parquet_invalido"),
    (Resposta(b""), "parquet_invalido"),
    (Resposta(b"abc", headers={"Content-Length": "100"}), "tamanho_divergente"),
    (Resposta(b"abc", falha=requests.exceptions.ChunkedEncodingError()), "transferencia_interrompida"),
])
def test_conteudo_invalido_e_truncado_nunca_publicado(contrato, ambiente, resposta, tipo):
    spark, _, _ = ambiente([resposta] * 3)
    with pytest.raises(download.FalhaIngestao) as exc:
        executar(contrato, spark)
    assert exc.value.resultados[0]["erro_tipo"] == tipo
    assert not list(Path(contrato["fonte"]["volume"]).rglob("*.manifesto.json"))
    assert not list(Path(contrato["fonte"]["volume"]).rglob("*.part"))


def test_falha_de_um_mes_nao_oculta_os_demais(contrato, parquet, ambiente):
    spark, sessao, _ = ambiente([Resposta(status=404), Resposta(parquet)])
    with pytest.raises(download.FalhaIngestao) as exc:
        executar(contrato, spark, meses=["05", "06"])
    assert [r["status"] for r in exc.value.resultados] == ["erro", "ok"]
    assert len(sessao.chamadas) == 2
    assert spark.eventos[-1]["arquivos_previstos"] == 2
    assert spark.eventos[-1]["arquivos_erro"] == spark.eventos[-1]["arquivos_ok"] == 1


def test_reexecucao_valida_conteudo_e_nao_faz_http(contrato, parquet, ambiente):
    spark, sessao, _ = ambiente([Resposta(parquet)])
    executar(contrato, spark)
    segunda = executar(contrato, spark)[0]
    assert segunda["status"] == "ja_existia" and segunda["checksum"]
    assert len(sessao.chamadas) == 1
    assert spark.eventos[-1]["arquivos_existentes"] == 1


def test_corrupcao_de_existente_nao_e_ignorada(contrato, parquet, ambiente):
    spark, sessao, _ = ambiente([Resposta(parquet)])
    destino = executar(contrato, spark)[0]["destino"]
    pq.write_table(pa.table({"VendorID": [9, 9], "total_amount": [10.5, 20.0]}), destino)
    with pytest.raises(download.FalhaIngestao) as exc:
        executar(contrato, spark)
    assert exc.value.resultados[0]["erro_tipo"] == "existente_invalido"
    assert len(sessao.chamadas) == 1


def test_migracao_da_raiz_para_particoes_sem_apagar_origem(contrato, parquet, ambiente):
    spark, sessao, _ = ambiente([])
    raiz = Path(contrato["fonte"]["volume"])
    raiz.mkdir()
    legado = raiz / "green_tripdata_2023-05.parquet"
    legado.write_bytes(parquet)
    resultado = executar(contrato, spark)[0]
    assert resultado["operacao"] == "migracao_legado" and legado.read_bytes() == parquet
    assert Path(resultado["destino"]).read_bytes() == parquet
    assert landing.listar_arquivos_landing(contrato) == [resultado["destino"]]
    assert not sessao.chamadas


def test_raw_mes_ausente_falha_em_vez_de_sucesso_vazio(contrato, tmp_path, parquet, ambiente):
    spark, sessao, _ = ambiente([])
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "green_tripdata_2023-06.parquet").write_bytes(parquet)
    with pytest.raises(download.FalhaIngestao) as exc:
        executar(contrato, spark, meses=[5, 6], origem_volume=str(raw))
    assert [r["status"] for r in exc.value.resultados] == ["erro", "ok"]
    assert not sessao.chamadas


def test_erro_de_disco_nao_repete_http(contrato, parquet, ambiente, monkeypatch):
    spark, sessao, _ = ambiente([Resposta(parquet)])
    def sem_permissao(*args):
        raise PermissionError("Volume sem permissão")
    monkeypatch.setattr(download.os, "replace", sem_permissao)
    with pytest.raises(download.FalhaIngestao) as exc:
        executar(contrato, spark)
    assert exc.value.resultados[0]["erro_tipo"] == "permissao_armazenamento"
    assert len(sessao.chamadas) == 1
    assert not list(Path(contrato["fonte"]["volume"]).rglob("*.part"))


def test_bronze_exige_manifesto_e_preserva_caminhos(contrato, parquet, ambiente):
    plano = landing.planejar_arquivos(contrato, 2023, [5])[0]
    destino = Path(plano["destino"])
    destino.parent.mkdir(parents=True)
    destino.write_bytes(parquet)
    with pytest.raises(landing.ArquivoInvalido, match="sem manifesto"):
        landing.listar_arquivos_landing(contrato)
    spark, sessao, _ = ambiente([])
    executar(contrato, spark)
    assert landing.listar_arquivos_landing(contrato) == [str(destino)]
    assert not sessao.chamadas


def test_evolucao_do_log_preserva_colunas_originais():
    class SparkGovernanca:
        def __init__(self):
            self.sqls = []
        def sql(self, sql):
            self.sqls.append(sql)
        def table(self, tabela):
            if tabela.endswith("dq_resultados"):
                return types.SimpleNamespace(columns=["exemplos"])
            return types.SimpleNamespace(columns=list(download.COLUNAS_LOG)[:8])
    spark = SparkGovernanca()
    motores.criar_tabelas_governanca("case_nyc_taxi", spark)
    alter = next(s for s in spark.sqls if s.startswith("ALTER TABLE"))
    assert "execucao_id STRING" in alter and "http_status INT" in alter
    assert "DROP" not in alter


def test_pipeline_mantem_identidade_do_fluxo_ao_mudar_pasta(contrato, parquet, ambiente, tmp_path, monkeypatch):
    spark, _, _ = ambiente([Resposta(parquet)])
    caminho = executar(contrato, spark)[0]["destino"]
    contratos = tmp_path / "contracts"
    contratos.mkdir()
    (contratos / "green.yml").write_text(yaml.safe_dump(contrato), encoding="utf-8")
    registrados = []
    dp = types.ModuleType("pyspark.pipelines")
    dp.create_streaming_table = lambda **kw: None
    def append_flow(**kw):
        def decorar(fn):
            registrados.append((kw, inspect.getclosurevars(fn).nonlocals["caminho_arquivo"]))
            return fn
        return decorar
    dp.append_flow = append_flow
    dp.materialized_view = lambda **kw: lambda fn: fn
    dp.expect_all = lambda *a: lambda fn: fn
    pyspark = types.ModuleType("pyspark")
    pyspark.pipelines = dp
    sql = types.ModuleType("pyspark.sql")
    sql.functions = types.ModuleType("pyspark.sql.functions")
    window = types.ModuleType("pyspark.sql.window")
    window.Window = object
    for nome, modulo in {"pyspark": pyspark, "pyspark.pipelines": dp,
                         "pyspark.sql": sql, "pyspark.sql.functions": sql.functions,
                         "pyspark.sql.window": window}.items():
        monkeypatch.setitem(sys.modules, nome, modulo)
    pipeline = Path(__file__).resolve().parents[1] / "src/02_pipelines/pipeline_case_nyc_taxi.py"
    fake = types.SimpleNamespace(conf=types.SimpleNamespace(get=lambda _: str(contratos)))
    runpy.run_path(str(pipeline), init_globals={"spark": fake})
    assert registrados == [({"target": contrato["destinos"]["bronze"],
                             "name": "green_trips_green_tripdata_2023_05_parquet", "once": True,
                             "comment": "Ingestão normalizada de green_tripdata_2023-05.parquet"}, caminho)]


def test_http_real_local_503_seguido_de_parquet(contrato, parquet, monkeypatch):
    chamadas = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            chamadas.append(self.path)
            self.send_response(503 if len(chamadas) == 1 else 200)
            self.send_header("Content-Length", str(len(parquet) if len(chamadas) > 1 else 0))
            self.end_headers()
            if len(chamadas) > 1:
                self.wfile.write(parquet)
        def log_message(self, *args):
            pass
    servidor = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=servidor.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(download.time, "sleep", lambda _: None)
    contrato["fonte"]["url_template"] = (
        f"http://127.0.0.1:{servidor.server_port}/{{taxi_type}}_tripdata_{{ano}}-{{mes}}.parquet"
    )
    try:
        spark = SparkLog()
        assert executar(contrato, spark)[0]["status"] == "ok"
        assert chamadas == ["/green_tripdata_2023-05.parquet"] * 2
    finally:
        servidor.shutdown()
        servidor.server_close()
        thread.join(timeout=5)
