"""Gera PNGs e SVGs editáveis da arquitetura e dos testes deste repositório.

Uso na raiz do projeto: python docs/arquitetura/gerar_diagrama.py
Dependências locais: Pillow e PyYAML. Nenhuma conexão ao Databricks é feita.
"""

from __future__ import annotations

import ast
import hashlib
import html
import json
import math
import os
from pathlib import Path

import yaml
from PIL import Image, ImageDraw, ImageFont, PngImagePlugin


ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent
WIDTH, HEIGHT = 3600, 3000
SOURCES = [
    "00_config/motores.py",
    "01_ingestao/ingestao_parametrizada.ipynb",
    "02_pipelines/pipeline_case_ifood.py",
    "03_qualidade/qualidade_contratos.ipynb",
    "contracts/green_trips.yml",
    "contracts/yellow_trips.yml",
    "contracts/gold/trips.yml",
    "contracts/references/dimensoes_tlc.yml",
    "01_ingestao/atualizar_taxi_zone_lookup.py",
    "06_analises/consultas_gold.ipynb",
    "databricks.yml",
    "resources/case_ifood_medallion.pipeline.yml",
    "resources/case_ifood_update_e_qualidade.job.yml",
    ".github/workflows/tests.yml",
    "tests/test_contratos.py",
    "tests/test_dab.py",
    "tests/test_lookup_guard.py",
    "tests/conftest.py",
    "requirements-dev.txt",
    "SDD.md",
]
C = {
    "bg": "#F4F7FB", "ink": "#162B46", "muted": "#52657B",
    "line": "#CED9E6", "white": "#FFFFFF", "teal": "#087E83",
    "teal_bg": "#E7F5F3", "bronze": "#A8622D", "bronze_bg": "#FBF0E5",
    "silver": "#466B91", "silver_bg": "#EAF1F9", "gold": "#947013",
    "gold_bg": "#FBF5DB", "purple": "#7051A3", "purple_bg": "#F0ECF8",
    "gray": "#6C7887", "gray_bg": "#EDF1F5", "red": "#A0443C",
}


def font_path(style):
    windows = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts"
    choices = {
        "regular": [windows / "segoeui.ttf", Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")],
        "bold": [windows / "segoeuib.ttf", Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")],
        "mono": [windows / "consola.ttf", Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf")],
    }
    return next(p for p in choices[style] if p.is_file())


class Canvas:
    """As mesmas primitivas produzem o bitmap e o vetor, sem texto gerado por IA."""

    def __init__(self, width=WIDTH, height=HEIGHT):
        self.width, self.height = width, height
        self.image = Image.new("RGB", (width, height), C["bg"])
        self.draw = ImageDraw.Draw(self.image)
        self.svg = []
        self.fonts = {}
        self.text_boxes = []

    def font(self, size, style="regular"):
        key = size, style
        if key not in self.fonts:
            self.fonts[key] = ImageFont.truetype(str(font_path(style)), size)
        return self.fonts[key]

    def rect(self, x, y, w, h, fill, stroke=None, radius=22, dashed=False):
        self.draw.rounded_rectangle((x, y, x + w, y + h), radius=radius, fill=fill,
                                    outline=None if dashed else stroke, width=2)
        dash = ' stroke-dasharray="11 8"' if dashed else ""
        self.svg.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
                        f'fill="{fill}" stroke="{stroke or "none"}" stroke-width="2"{dash}/>')
        if dashed and stroke:
            # Contorno tracejado no PNG, incluindo os cantos.
            pts = []
            for cx, cy, start in [(x+w-radius, y+radius, -90), (x+w-radius, y+h-radius, 0),
                                  (x+radius, y+h-radius, 90), (x+radius, y+radius, 180)]:
                pts.extend((cx+radius*math.cos(math.radians(a)), cy+radius*math.sin(math.radians(a)))
                           for a in range(start, start+91, 3))
            self._dashed(pts+[pts[0]], stroke, 2)

    def _dashed(self, points, color, width):
        cursor = 0.0
        for (x1, y1), (x2, y2) in zip(points, points[1:]):
            length = math.hypot(x2-x1, y2-y1)
            pos = 0.0
            while pos < length:
                segment = min(11 - cursor if cursor < 11 else 19 - cursor, length-pos)
                if cursor < 11:
                    a, b = pos/length, (pos+segment)/length
                    self.draw.line((x1+(x2-x1)*a, y1+(y2-y1)*a,
                                    x1+(x2-x1)*b, y1+(y2-y1)*b), fill=color, width=width)
                pos += segment
                cursor = (cursor + segment) % 19

    def path(self, points, color, width=4, dashed=False, arrow=False):
        if dashed:
            self._dashed(points, color, width)
        else:
            self.draw.line(points, fill=color, width=width, joint="curve")
        coords = " ".join(f'{x},{y}' for x, y in points)
        dash = ' stroke-dasharray="11 8"' if dashed else ""
        self.svg.append(f'<polyline points="{coords}" fill="none" stroke="{color}" '
                        f'stroke-width="{width}" stroke-linejoin="round"{dash}/>')
        if arrow:
            x1, y1 = points[-2]
            x2, y2 = points[-1]
            angle = math.atan2(y2-y1, x2-x1)
            vertices = [(x2, y2)] + [(x2-15*math.cos(angle+a), y2-15*math.sin(angle+a))
                                     for a in (-0.48, 0.48)]
            self.draw.polygon(vertices, fill=color)
            self.svg.append(f'<polygon points="{" ".join(f"{x},{y}" for x,y in vertices)}" fill="{color}"/>')

    def text(self, x, y, value, size=26, color=None, style="regular"):
        color = color or C["ink"]
        font = self.font(size, style)
        ascent, _ = font.getmetrics()
        self.draw.text((x, y+ascent), value, font=font, fill=color, anchor="ls")
        family = "Consolas, 'DejaVu Sans Mono', monospace" if style == "mono" else "'Segoe UI', 'DejaVu Sans', sans-serif"
        weight = "700" if style == "bold" else "400"
        self.svg.append(f'<text x="{x}" y="{y+ascent}" font-family="{family}" font-size="{size}" '
                        f'font-weight="{weight}" fill="{color}">{html.escape(value)}</text>')
        bbox = self.draw.textbbox((x, y+ascent), value, font=font, anchor="ls")
        self.text_boxes.append(bbox)
        if bbox[0] < 0 or bbox[1] < 0 or bbox[2] > self.width or bbox[3] > self.height:
            raise ValueError(f"Texto fora da imagem: {value}")
        return font.getlength(value)

    def paragraph(self, x, y, text, width, size=26, color=None, style="regular", line=36):
        font = self.font(size, style)
        lines = []
        for raw in text.split("\n"):
            current = ""
            for word in raw.split():
                proposal = f"{current} {word}" if current else word
                if font.getlength(proposal) > width and current:
                    lines.append(current)
                    current = word
                else:
                    current = proposal
                if font.getlength(current) > width:
                    raise ValueError(f"Palavra maior que a coluna ({width}px): {current}")
            lines.append(current)
        for value in lines:
            self.text(x, y, value, size, color, style)
            y += line
        return y


class Card:
    def __init__(self, canvas, x, y, w, h, number, title, color, tint, dashed=False):
        self.c, self.x, self.y, self.w, self.bottom = canvas, x+26, y+124, w-52, y+h-24
        if canvas.font(34, "bold").getlength(title) > w-114:
            raise ValueError(f"Título excede cartão: {title}")
        canvas.rect(x, y, w, h, C["white"], color if dashed else C["line"], dashed=dashed)
        canvas.rect(x+22, y+22, 48, 45, tint, radius=12)
        canvas.text(x+33, y+28, number, 22, color, "bold")
        canvas.text(x+88, y+24, title, 34, C["ink"], "bold")
        canvas.path([(x+26, y+94), (x+w-26, y+94)], color, 3)

    def p(self, text, size=26, color=None, style="regular", line=36, gap=13):
        self.y = self.c.paragraph(self.x, self.y, text, self.w, size, color, style, line) + gap
        if self.y-gap > self.bottom:
            raise ValueError(f"Conteúdo excede cartão: {text}; y={self.y} limite={self.bottom}")

    def label(self, text, color, size=23):
        self.p(text, size, color, "bold", line=31, gap=8)

    def rule(self, gap=18):
        self.c.path([(self.x, self.y), (self.x+self.w, self.y)], C["line"], 2)
        self.y += gap


def load_yaml(path):
    return yaml.safe_load((ROOT / path).read_text(encoding="utf-8"))


def model():
    green = load_yaml("contracts/green_trips.yml")
    yellow = load_yaml("contracts/yellow_trips.yml")
    gold = load_yaml("contracts/gold/trips.yml")
    refs = load_yaml("contracts/references/dimensoes_tlc.yml")["dimensoes"]
    counts = {}
    for name in ("contratos", "lookup_guard", "dab"):
        tree = ast.parse((ROOT / f"tests/test_{name}.py").read_text(encoding="utf-8"))
        counts[name] = sum(isinstance(n, ast.FunctionDef) and n.name.startswith("test_")
                           for n in tree.body)
    # A composição é explícita: falhar é preferível a publicar um desenho desatualizado.
    assert gold["membros"] == [green["dataset"], yellow["dataset"]]
    assert {r["dataset"] for r in refs} == {
        "dim_taxi_zone", "dim_vendor", "dim_rate_code", "dim_payment_type", "dim_trip_type"}
    assert len(gold["enriquecimentos"]) == 6
    assert gold["nome"] == "case_ifood.gold.trips_consolidado"
    assert gold["agregados"][0]["nome"] == "case_ifood.gold.trips_diario"
    assert counts == {"contratos": 11, "lookup_guard": 3, "dab": 2}
    audits = []
    for contract in (yellow, green):
        q = contract["regras_qualidade"]
        audits.append(len(q["completude"]["colunas"]) + len(q["completude"]["excessoes"])
                      + len(q["validade"]["regras"]) + 1
                      + len(q["consistencia"]["regras"]) + 1)
    assert audits == [12, 12]
    return green, yellow, gold, refs, counts, audits


def heading(c, title, subtitle, eyebrow):
    c.text(64, 42, eyebrow, 25, C["teal"], "bold")
    c.text(64, 91, title, 65, style="bold")
    c.text(66, 182, subtitle, 30, C["muted"])


def band(c, x, y, w, title, body):
    c.rect(x, y, w, 180, C["teal_bg"], "#ADD9D5")
    c.text(x+26, y+20, title, 29, C["teal"], "bold")
    c.paragraph(x+26, y+72, body, w-52, 27, line=36)


def save(c, name, title, description):
    provenance = {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in SOURCES}
    metadata = json.dumps({"sources_sha256": provenance, "scope": "static source analysis"},
                          ensure_ascii=False)
    vector = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{c.width}" height="{c.height}" '
              f'viewBox="0 0 {c.width} {c.height}" role="img" aria-labelledby="title desc">'
              f'<title id="title">{html.escape(title)}</title>'
              f'<desc id="desc">{html.escape(description)}</desc>'
              f'<metadata>{html.escape(metadata)}</metadata>'
              f'<rect width="{c.width}" height="{c.height}" fill="{C["bg"]}"/>'
              + "\n".join(c.svg) + "</svg>\n")
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / f"{name}.svg").write_text(vector, encoding="utf-8")
    info = PngImagePlugin.PngInfo()
    info.add_text("Description", description)
    info.add_text("Source provenance", metadata)
    c.image.save(OUT / f"{name}.png", pnginfo=info, optimize=True, dpi=(200, 200))
    preview = c.image.copy()
    preview.thumbnail((1800, 1500))
    preview_name = "arquitetura-previa" if name == "arquitetura-projeto" else f"{name}-previa"
    preview.save(OUT / f"{preview_name}.png", optimize=True)
    print(f"{name}: PNG {c.width}x{c.height}, SVG editável e prévia; {len(c.text_boxes)} linhas.")


def architecture(data):
    _, _, gold, refs, counts, _ = data
    c = Canvas(height=3000)
    heading(c, "Arquitetura do lakehouse NYC Taxi",
            "case-ifood  /  contratos YAML  /  Unity Catalog: case_ifood  /  implementação versionada",
            "VISÃO GERAL  ·  DADOS, DIMENSÕES E ENTREGA")

    ci = Card(c, 64, 270, 1080, 320, "01", "CI · GitHub Actions", C["teal"], C["teal_bg"])
    ci.p("Push em main ou pull request → pytest", 29, style="bold", line=40, gap=9)
    ci.p(f"{sum(counts.values())} testes: contratos, lookup e estrutura DAB.", 27, line=37, gap=9)
    ci.p("bundle validate -t dev: etapa condicional a DATABRICKS_HOST. Sem deploy no workflow.", 26, C["muted"], line=36)

    dab = Card(c, 1204, 270, 1100, 320, "02", "DAB · deploy pela CLI", C["teal"], C["teal_bg"])
    dab.p("databricks.yml + resources/*.yml", 28, style="mono", line=39, gap=9)
    dab.p("databricks bundle deploy -t <target>", 26, C["teal"], "mono", line=36, gap=9)
    dab.p("Targets dev / prod; contratos via workspace.file_path. Define pipeline e Job no workspace.", 27, line=37)

    job = Card(c, 2364, 270, 1172, 320, "03", "Job operacional", C["purple"], C["purple_bg"])
    job.p("case_ifood_update_e_qualidade", 28, style="mono", line=39, gap=9)
    job.p("pipeline_update → qualidade_contratos", 29, C["purple"], "bold", line=40, gap=9)
    job.p("Update normal; auditoria depende do update. Ingestão e lookup são executados à parte.", 27, line=37)
    c.path([(2304, 430), (2364, 430)], C["teal"], 4, dashed=True, arrow=True)

    band(c, 64, 630, 1706, "CONTRATOS DE INGESTÃO E REFERÊNCIAS",
         "contracts/*.yml + contracts/references/dimensoes_tlc.yml\nFonte, schema, mapeamento, chave, regras e dimensões Bronze/Silver.")
    band(c, 1810, 630, 1726, "CONTRATO DE CONSUMO · GOLD",
         "contracts/gold/trips.yml\nMembros, seis enriquecimentos por LEFT JOIN, agrupamentos e medidas.")

    c.text(64, 854, "FLUXO DE DADOS  ·  Lakeflow Spark Declarative Pipelines", 29, C["muted"], "bold")
    c.text(64, 896, "Pipeline serverless + Photon  /  execução disparada por update  /  MVs = materialized views", 26, C["muted"])
    c.path([(2673, 810), (2673, 930)], C["teal"], 3, dashed=True, arrow=True)

    src = Card(c, 64, 930, 610, 650, "04", "Fonte e landing", C["teal"], C["teal_bg"])
    src.p("NYC TLC · Yellow + Green", 29, style="bold", line=40)
    src.p("Parquet mensal → notebook de ingestão parametrizada.", 27, line=38)
    src.p("origem=url: download HTTP\norigem=raw: arquivos existentes", 25, style="mono", line=35)
    src.rule()
    src.p("/Volumes/case_ifood/\nlanding/files", 27, C["teal"], "mono", line=38)
    src.p("Glob dos contratos: 2023.\nJanela válida: jan–mai/2023.", 27, line=38)
    src.p("Preparação da landing fora do Job versionado.", 25, C["muted"], line=35)

    br = Card(c, 734, 930, 610, 650, "05", "Bronze", C["bronze"], C["bronze_bg"])
    br.p("bronze.yellow_trips\nbronze.green_trips", 28, style="mono", line=39)
    br.label("STREAMING TABLES", C["bronze"])
    br.p("append_flow(once=True)\nUm fluxo por arquivo Parquet; leitura batch.", 27, line=38)
    br.rule()
    br.p("Schema canônico, casts e normalização dos nomes. Coluna ausente recebe NULL.", 27, line=38)
    br.p("+ _arquivo_origem\n+ _ingestao_ts", 28, C["bronze"], "mono", line=39)
    br.p("Preserva dados antes da seleção de conformidade.", 25, C["muted"], line=35)

    si = Card(c, 1404, 930, 610, 650, "06", "Silver", C["silver"], C["silver_bg"])
    si.p("silver.yellow_trips\nsilver.green_trips", 28, style="mono", line=39)
    si.p("MV privada com expect_all: métricas no event log e classificação das rejeições.", 27, line=38)
    si.rule()
    si.p("Mapeamento + trip_id (xxhash64) + deduplicação determinística.", 27, line=38)
    si.label("CONFORMES → SILVER", C["silver"])
    si.p("Inválidos e duplicatas excedentes → quarentena.", 27, line=38)
    si.p("passenger_count nulo: WARN; a linha é mantida.", 26, C["muted"], line=36)

    go = Card(c, 2074, 930, 760, 650, "07", "Gold enriquecida", C["gold"], C["gold_bg"])
    go.p(gold["nome"].removeprefix("case_ifood."), 30, style="mono", line=41, gap=9)
    go.p("Green + Yellow: unionByName, seguido de 6 LEFT JOINs com dimensões Silver.", 28, line=39, gap=10)
    go.p("IDs preservados; sem correspondência, descrições ficam NULL.", 26, C["muted"], line=36, gap=10)
    go.rule()
    go.p("gold.trips_diario", 30, style="mono", line=41, gap=9)
    go.p("Deriva do consolidado por data_corrida + taxi_type.", 28, line=39, gap=10)
    go.p("count(*) · avg(total_amount)\navg(passenger_count) · sum(trip_distance)", 26, style="mono", line=36, gap=10)
    go.p("MVs; clustering automático no consolidado e por data no diário.", 25, C["muted"], line=35)

    co = Card(c, 2894, 930, 642, 650, "08", "Consumo SQL", C["gold"], C["gold_bg"])
    co.p("06_analises/\nconsultas_gold.ipynb", 27, style="mono", line=38)
    co.p("Lê gold.trips_consolidado.", 27, line=38)
    co.rule()
    co.p("Média de total_amount por mês para Yellow Taxi.", 29, line=40)
    co.p("Média de passageiros por hora em maio para toda a frota.", 29, line=40)
    co.p("Consultas versionadas; execução fora do Job operacional.", 26, C["muted"], line=36)
    for x1, x2 in ((674, 734), (1344, 1404), (2014, 2074), (2834, 2894)):
        c.path([(x1, 1172), (x2, 1172)], C["ink"], 5, arrow=True)

    c.text(64, 1642, "REFERÊNCIAS TLC  ·  cinco dimensões na Silver enriquecem a Gold", 29, C["teal"], "bold")
    c.path([(2454, 1720), (2454, 1580)], C["ink"], 4, arrow=True)
    c.text(2500, 1648, "dimensões Silver → joins na Gold", 26, C["muted"])

    rf = Card(c, 64, 1720, 760, 570, "09", "Fontes de referência", C["teal"], C["teal_bg"])
    rf.p("Taxi Zone Lookup · CSV oficial", 29, style="bold", line=40)
    rf.p("atualizar_taxi_zone_lookup.py", 27, style="mono", line=38)
    rf.p("/Volumes/case_ifood/landing/\nreference/taxi_zone_lookup.csv", 26, style="mono", line=36)
    rf.p("Snapshot por data + manifesto SHA-256; antes do update.", 26, line=36)
    rf.rule()
    rf.p("Vendor, tarifa, pagamento e despacho: dicionários versionados no YAML.", 28, line=39)

    di = Card(c, 884, 1720, 1130, 570, "10", "Dimensões Bronze → Silver", C["silver"], C["silver_bg"])
    di.p("contracts/references/dimensoes_tlc.yml", 27, style="mono", line=38)
    for ref in refs:
        di.p(ref["dataset"], 29, C["silver"], "mono", line=39, gap=8)
    di.rule()
    di.p("MVs de referência; Silver aplica casts e deduplica pela chave da dimensão.", 27, line=37)

    en = Card(c, 2074, 1720, 1462, 570, "11", "Enriquecimentos do consolidado", C["gold"], C["gold_bg"])
    en.label("6 LEFT JOINs · 5 DIMENSÕES · 10 ATRIBUTOS DESCRITIVOS", C["gold"], 25)
    en.p("dim_vendor → vendor_name\ndim_rate_code → rate_code_name\ndim_payment_type → payment_type_name\ndim_trip_type → trip_type_name", 28, style="mono", line=40, gap=12)
    en.p("dim_taxi_zone × 2 → pickup_* e dropoff_*", 28, style="mono", line=40, gap=10)
    en.p("Cada localidade recebe borough, zone e service_zone. As tabelas dimensionais permanecem na Silver.", 28, line=39)
    c.path([(824, 1930), (884, 1930)], C["ink"], 4, arrow=True)
    c.path([(2014, 1930), (2074, 1930)], C["ink"], 4, arrow=True)

    c.text(64, 2344, "OPERAÇÃO, QUALIDADE E GOVERNANÇA", 29, C["purple"], "bold")
    log = Card(c, 64, 2400, 1090, 330, "12", "Rastreabilidade", C["purple"], C["purple_bg"])
    log.p("governanca.log_ingestao: status por arquivo.\nquarantine_<dataset>: motivos, origem e timestamps.", 27, line=38)
    log.p("Expectations: métricas no event log da pipeline. Linhagem por arquivo é preservada na quarentena.", 26, line=36)
    dq = Card(c, 1214, 2400, 1110, 330, "13", "Auditoria após o update", C["purple"], C["purple_bg"])
    dq.p("Notebook qualidade_contratos → Silver", 29, style="bold", line=40, gap=9)
    dq.p("5 dimensões DAMA; 24 verificações.\nAppend em governanca.dq_resultados; qualquer FAIL lança exceção no notebook.", 27, line=38)
    lim = Card(c, 2384, 2400, 1152, 330, "14", "Capacidades declaradas", C["gray"], C["gray_bg"], dashed=True)
    lim.p("obs_metricas: DDL definido, sem coletor.\nOwner, tags e PII declarados nos YAML; aplicação de GRANTs/tags não localizada.", 27, line=38)
    lim.p("Runtime do workspace não verificado.", 26, C["muted"], line=36)

    c.path([(64, 2770), (3536, 2770)], C["line"], 2)
    c.text(64, 2798, "LEGENDA", 23, C["muted"], "bold")
    for x, label, color, dashed in ((240, "Dados", C["ink"], False), (520, "Configuração / deploy manual", C["teal"], True)):
        c.path([(x, 2820), (x+64, 2820)], color, 4, dashed=dashed, arrow=True)
        c.text(x+82, 2798, label, 26, color)
    c.text(64, 2856, "Nota: soma_distancia_km é o alias do contrato; o código soma trip_distance sem converter a unidade.", 26, C["muted"])
    c.text(64, 2898, "Auditoria definida no repo; célula inicial e import do notebook de qualidade requerem ajuste para execução portátil.", 26, C["muted"])
    c.text(64, 2940, "Fonte: código, notebooks, YAMLs e workflow versionados · sem comprovação de execução no GitHub ou no Databricks.", 25, C["muted"])
    save(c, "arquitetura-projeto", "Arquitetura do lakehouse NYC Taxi — case-ifood",
         "Arquitetura versionada: CI, deploy manual DAB, Job update e qualidade, medallion, cinco dimensões Silver e seis LEFT JOINs na Gold. Execução remota não verificada.")


def quality(data):
    green, yellow, _, _, counts, audits = data
    c = Canvas(height=2600)
    heading(c, "Testes automatizados e qualidade dos dados",
            "case-ifood  /  o que o GitHub verifica  /  o que a pipeline e a auditoria avaliam no Databricks",
            "MAPA DE VERIFICAÇÕES  ·  CI E DADOS")
    band(c, 64, 270, 1706, f"GITHUB ACTIONS · {sum(counts.values())} TESTES CONFIGURADOS",
         "push em main + pull_request · Ubuntu · Python 3.12\npip install -r requirements-dev.txt → pytest tests -v")
    band(c, 1810, 270, 1726, f"DATABRICKS · 5 DIMENSÕES DAMA · {sum(audits)} VERIFICAÇÕES",
         "Auditoria sobre silver.yellow_trips e silver.green_trips.\n12 verificações por dataset, após o update da pipeline.")

    contracts = Card(c, 64, 500, 1706, 430, "01", f"Contratos e carregador · {counts['contratos']} testes", C["teal"], C["teal_bg"])
    contracts.label("tests/test_contratos.py", C["teal"], 25)
    contracts.p("5 · Ingestão: campos obrigatórios, destinos Bronze/Silver/quarentena, mapeamento no schema, obrigatórias mapeadas e dimensões DAMA declaradas.", 29, line=41)
    contracts.p("4 · Gold: membros existentes, nomes únicos, funções permitidas e enriquecimentos com dimensões/colunas válidas.", 29, line=41)
    contracts.p("1 · Referências: chave, colunas e fonte coerentes.\n1 · Motor: listar_contratos() carrega todos os datasets.", 28, line=39)

    lookup = Card(c, 64, 970, 1706, 260, "02", f"Lookup Taxi Zones · {counts['lookup_guard']} testes", C["teal"], C["teal_bg"])
    lookup.p("tests/test_lookup_guard.py · HTTP e dbutils simulados.", 28, style="mono", line=39)
    lookup.p("CSV com aspas aceito · CSV sem aspas aceito · schema incorreto rejeitado.", 29, line=40)

    dab = Card(c, 64, 1270, 1706, 260, "03", f"Estrutura DAB · {counts['dab']} testes", C["teal"], C["teal_bg"])
    dab.p("tests/test_dab.py", 28, style="mono", line=39)
    dab.p("Targets dev/prod; YAMLs dos resources; configuração contracts_dir e existência dos arquivos de pipeline/notebook referenciados.", 29, line=40)

    c.rect(1810, 500, 1726, 1030, C["white"], C["line"])
    c.text(1836, 526, "AUDITORIA SILVER · qualidade_contratos.ipynb", 32, C["purple"], "bold")
    c.text(1836, 585, "Dimensão", 26, C["muted"], "bold")
    c.text(2170, 585, "Regra avaliada em cada dataset", 26, C["muted"], "bold")
    c.text(3290, 585, "Qtd.", 26, C["muted"], "bold")
    yc = int(yellow["regras_qualidade"]["completude"]["max_pct_nulo"] * 100)
    gc = int(green["regras_qualidade"]["completude"]["max_pct_nulo"] * 100)
    yp = int(yellow["regras_qualidade"]["completude"]["excessoes"]["passenger_count"] * 100)
    gp = int(green["regras_qualidade"]["completude"]["excessoes"]["passenger_count"] * 100)
    rows = [
        ("Completude", f"Nulos em vendor_id, total_amount, pickup e dropoff: Yellow ≤ {yc}%; Green ≤ {gc}%.\npassenger_count: Yellow ≤ {yp}%; Green ≤ {gp}%.", "5", 180),
        ("Validade", "total_amount ≥ 0 e trip_distance ≥ 0;\npassenger_count NULL ou entre 0 e 8;\npickup de 2023-01-01 até antes de 2023-06-01.", "4", 185),
        ("Unicidade", "Nenhuma duplicata pela chave trip_id.", "1", 120),
        ("Consistência", "dropoff_datetime ≥ pickup_datetime.", "1", 120),
        ("Pontualidade", "Nenhuma corrida fora da janela histórica:\n2023-01-01 ≤ pickup < 2023-06-01.", "1", 150),
    ]
    y = 640
    for i, (dim, rule, number, height) in enumerate(rows):
        c.rect(1834, y, 1678, height, C["purple_bg"] if i % 2 == 0 else C["white"], radius=12)
        c.text(1854, y+25, dim, 28, C["purple"], "bold")
        c.paragraph(2170, y+23, rule, 1060, 28, line=39)
        c.text(3310, y+25, number, 32, C["purple"], "bold")
        y += height + 8
    c.text(1836, 1460, "Total: 12 por dataset × 2 datasets = 24 verificações em dados.", 28, C["purple"], "bold")

    bv = Card(c, 64, 1580, 1706, 340, "04", "Validação DAB · job independente", C["teal"], C["teal_bg"])
    bv.p("databricks bundle validate --target dev", 30, C["teal"], "mono", line=41)
    bv.p("Executada se DATABRICKS_HOST não estiver vazio. HOST e TOKEN vêm dos secrets; o CLI precisa autenticar no workspace.", 28, line=39)
    bv.p("A validação não faz deploy e não roda o Job.", 28, C["muted"], line=39)

    rt = Card(c, 1810, 1580, 1726, 340, "05", "Qualidade durante a transformação", C["silver"], C["silver_bg"])
    rt.p("MV privada: expect_all registra métricas no event log. O código classifica e roteia as linhas pelas regras DROP.", 28, line=39)
    rt.p("Conformes → Silver; inválidas + duplicatas excedentes → quarentena. passenger_count nulo gera WARN e permanece na Silver.", 28, line=39)

    report = Card(c, 64, 2010, 1706, 340, "06", "Relatério e interrupção por falha", C["purple"], C["purple_bg"])
    report.p("Job: pipeline_update → qualidade_contratos", 30, C["purple"], "bold", line=41)
    report.p("Append em case_ifood.governanca.dq_resultados: PASS/FAIL, métrica, horário e até 5 exemplos JSON por regra com falha.", 28, line=39)
    report.p("Após gravar, qualquer FAIL lança exceção no notebook.", 28, line=39)

    scope = Card(c, 1810, 2010, 1726, 340, "07", "Como interpretar as verificações", C["gray"], C["gray_bg"])
    scope.p("CI testa lógica Python e estrutura dos contratos; não executa Spark, joins, agregações ou dados reais.", 28, line=39)
    scope.p("Auditoria lê a Silver após a quarentena; PASS não mede descartes. Pontualidade usa a janela histórica. Não há check de volume mínimo.", 28, line=39)

    c.path([(64, 2400), (3536, 2400)], C["line"], 2)
    c.text(64, 2430, "ESCOPO: configuração e implementação versionadas; resultados de execuções remotas não foram consultados.", 27, C["muted"], "bold")
    c.text(64, 2480, "Pré-requisito de runtime: ajustar a célula inicial (Markdown marcado como código) e o import fixo do notebook de qualidade.", 26, C["muted"])
    c.text(64, 2526, "CI sem lint, análise de segurança ou cobertura configurados · não há deploy automático no workflow atual.", 26, C["muted"])
    save(c, "testes-e-qualidade", "Testes automatizados e qualidade dos dados — case-ifood",
         "GitHub Actions: 16 testes Python, validação DAB condicional. Databricks: expectations, quarentena e 24 verificações de auditoria Silver em cinco dimensões DAMA. Execução remota não verificada.")


def create():
    data = model()
    architecture(data)
    quality(data)


if __name__ == "__main__":
    create()
