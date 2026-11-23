"""Baixa o snapshot oficial de Taxi Zones para o Volume de landing.

Execute no Databricks, onde `dbutils` está disponível, antes de atualizar o
pipeline SDP. O pipeline consome somente o arquivo salvo no Volume.
"""
from datetime import datetime, timezone
from hashlib import sha256
from urllib.request import Request, urlopen


URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DESTINO = "/Volumes/case_ifood/landing/reference/taxi_zone_lookup.csv"
MANIFESTO = "/Volumes/case_ifood/landing/reference/taxi_zone_lookup.manifest"
ARQUIVO_BASE = "/Volumes/case_ifood/landing/reference/taxi_zone_lookup"


def atualizar(dbutils):
    requisicao = Request(URL, headers={"User-Agent": "case-ifood-reference-ingestion/1.0"})
    with urlopen(requisicao, timeout=60) as resposta:
        conteudo = resposta.read()
    # O CSV oficial pode vir com o cabeçalho entre aspas; normalizar antes de validar
    cabecalho = (
        conteudo.split(b"\r\n", 1)[0].split(b"\n", 1)[0].replace(b'"', b"")
    )
    if not cabecalho.startswith(b"LocationID,Borough,Zone,service_zone"):
        raise ValueError("O CSV recebido não corresponde ao schema esperado da TLC.")

    dbutils.fs.mkdirs("/Volumes/case_ifood/landing/reference")
    csv = conteudo.decode("utf-8-sig")
    data_snapshot = datetime.now(timezone.utc).date().isoformat()
    diretorio_snapshot = f"{ARQUIVO_BASE}/snapshot_date={data_snapshot}"
    arquivo_snapshot = f"{diretorio_snapshot}/taxi_zone_lookup.csv"
    manifesto_snapshot = f"{diretorio_snapshot}/taxi_zone_lookup.manifest"
    dbutils.fs.mkdirs(diretorio_snapshot)
    dbutils.fs.put(arquivo_snapshot, csv, overwrite=True)
    dbutils.fs.put(DESTINO, csv, overwrite=True)
    checksum = sha256(conteudo).hexdigest()
    manifesto = (
        f"url={URL}\n"
        f"snapshot_date={data_snapshot}\n"
        f"downloaded_at={datetime.now(timezone.utc).isoformat()}\n"
        f"sha256={checksum}\n"
    )
    dbutils.fs.put(manifesto_snapshot, manifesto, overwrite=True)
    dbutils.fs.put(MANIFESTO, manifesto, overwrite=True)
    return {
        "path": DESTINO,
        "snapshot_path": arquivo_snapshot,
        "rows": max(csv.count("\n") - 1, 0),
        "sha256": checksum,
    }


if __name__ == "__main__":
    print(atualizar(dbutils))  # `dbutils` é fornecido pelo runtime Databricks.
