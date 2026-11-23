"""Regressão do bug do guard do taxi zone lookup (2026-11-23):

O CSV oficial da TLC passou a vir com o cabeçalho entre aspas
('"LocationID","Borough",...') e o guard rejeitava o arquivo válido.
Estes testes garantem que header com e sem aspas é aceito e que um CSV
de schema errado continua rejeitado.
"""
import io

import pytest

import atualizar_taxi_zone_lookup as lookup

CSV_COM_ASPAS = (
    b'"LocationID","Borough","Zone","service_zone"\r\n'
    b'1,"EWR","Newark Airport","EWR"\r\n'
)


class _FakeFS:
    def __init__(self):
        self.arquivos = {}

    def mkdirs(self, caminho):
        self.arquivos[caminho] = "<dir>"

    def put(self, caminho, conteudo, overwrite=False):
        self.arquivos[caminho] = conteudo


class _FakeDbutils:
    def __init__(self):
        self.fs = _FakeFS()


def _stub_download(monkeypatch, corpo):
    def fake_urlopen(requisicao, timeout=None):
        return io.BytesIO(corpo)

    monkeypatch.setattr(lookup, "urlopen", fake_urlopen)


def test_header_com_aspas_e_aceito(monkeypatch):
    _stub_download(monkeypatch, CSV_COM_ASPAS)
    dbutils = _FakeDbutils()
    resultado = lookup.atualizar(dbutils)
    assert resultado["rows"] == 1
    assert dbutils.fs.arquivos[lookup.DESTINO].startswith('"LocationID"')


def test_header_sem_aspas_tambem_e_aceito(monkeypatch):
    _stub_download(monkeypatch, CSV_COM_ASPAS.replace(b'"', b""))
    resultado = lookup.atualizar(_FakeDbutils())
    assert resultado["rows"] == 1


def test_csv_com_schema_errado_e_rejeitado(monkeypatch):
    _stub_download(monkeypatch, b'"Outro","Schema"\r\n1,"x"\r\n')
    with pytest.raises(ValueError):
        lookup.atualizar(_FakeDbutils())
