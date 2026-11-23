"""Valida a estrutura do DAB (databricks.yml + resources) sem precisar do CLI.

Garante que o bundle parseia, tem os targets dev/prod esperados e que todo
caminho referenciado (biblioteca do pipeline, notebook do job) existe no repo.
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
            for lib in pipeline.get("libraries", []):
                alvo = os.path.join(RAIZ, lib["file"]["path"])
                assert os.path.isfile(alvo), f"biblioteca inexistente: {alvo}"
        for job in doc.get("resources", {}).get("jobs", {}).values():
            for task in job.get("tasks", []):
                notebook = task.get("notebook_task", {}).get("notebook_path")
                if notebook:
                    # notebook no repo: sem extensão (workspace) ou .ipynb (fonte)
                    sem_ext = os.path.join(RAIZ, notebook)
                    existe = os.path.exists(sem_ext) or os.path.exists(sem_ext + ".ipynb")
                    assert existe, f"notebook do job inexistente: {notebook}"
