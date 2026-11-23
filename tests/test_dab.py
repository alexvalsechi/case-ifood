"""Valida a estrutura do DAB (databricks.yml + resources) sem precisar do CLI.

Garante que o bundle parseia, tem os targets dev/prod esperados e que todo
caminho referenciado (biblioteca do pipeline, notebook do job), relativo ao
YAML resource que o declara, existe no repo.
"""
import glob
import os

import yaml

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATABRICKS_YML = os.path.join(RAIZ, "databricks.yml")


def test_databricks_yml_possui_targets_dev_e_prod():
    with open(DATABRICKS_YML, encoding="utf-8") as fh:
        bundle = yaml.safe_load(fh)
    assert bundle["bundle"]["name"], "bundle sem nome"
    targets = bundle["targets"]
    assert targets["dev"]["mode"] == "development"
    assert targets["dev"].get("default") is True
    assert targets["prod"]["mode"] == "production"


def test_resources_sao_yaml_validos_com_caminhos_existentes():
    recursos = sorted(glob.glob(os.path.join(RAIZ, "resources", "*.yml")))
    assert recursos, "nenhum resource em resources/"
    for caminho in recursos:
        with open(caminho, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        for pipeline in doc.get("resources", {}).get("pipelines", {}).values():
            conf = pipeline.get("configuration", {})
            assert "case_ifood.contracts_dir" in conf, (
                "pipeline sem a configuração case_ifood.contracts_dir (portabilidade)"
            )
            for lib in pipeline.get("libraries", []):
                alvo = os.path.normpath(os.path.join(os.path.dirname(caminho), lib["file"]["path"]))
                assert os.path.isfile(alvo), f"biblioteca inexistente: {alvo}"
        for job in doc.get("resources", {}).get("jobs", {}).values():
            for task in job.get("tasks", []):
                notebook = task.get("notebook_task", {}).get("notebook_path")
                if notebook:
                    # DAB resolve cada caminho relativo à pasta do YAML resource.
                    alvo = os.path.normpath(os.path.join(os.path.dirname(caminho), notebook))
                    assert os.path.isfile(alvo), f"notebook do job inexistente: {alvo}"
