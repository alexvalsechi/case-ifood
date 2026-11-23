"""Configuração dos testes: coloca o repo e seus pacotes no sys.path.

Os testes rodam headless (GitHub Actions), sem Spark nem DBR — testam a lógica
pura: contratos YAML, guard do lookup e estrutura do DAB.
"""
import os
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

for caminho in (RAIZ, os.path.join(RAIZ, "00_config"), os.path.join(RAIZ, "01_ingestao")):
    if caminho not in sys.path:
        sys.path.insert(0, caminho)
