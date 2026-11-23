"""Gera SVG editável e PNG da arquitetura reconstruída deste repositório.

Uso na raiz do projeto: python docs/arquitetura/gerar_diagrama.py
Dependências locais: Pillow e PyYAML. Nenhuma conexão ao Databricks é feita.
"""

from __future__ import annotations

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
WIDTH, HEIGHT = 3600, 2740
SOURCES = [
    "00_config/motores.py",
    "01_ingestao/ingestao_parametrizada.ipynb",
    "02_pipelines/pipeline_case_ifood.py",
    "03_qualidade/qualidade_contratos.ipynb",
    "contracts/green_trips.yml",
    "contracts/yellow_trips.yml",
    "contracts/gold/trips.yml",
    "README.md",
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

    def __init__(self):
        self.image = Image.new("RGB", (WIDTH, HEIGHT), C["bg"])
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
        if bbox[0] < 0 or bbox[1] < 0 or bbox[2] > WIDTH or bbox[3] > HEIGHT:
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


def create():
    green = yaml.safe_load((ROOT/"contracts/green_trips.yml").read_text(encoding="utf-8"))
    yellow = yaml.safe_load((ROOT/"contracts/yellow_trips.yml").read_text(encoding="utf-8"))
    gold = yaml.safe_load((ROOT/"contracts/gold/trips.yml").read_text(encoding="utf-8"))
    # Falha explicitamente se os principais contratos deixarem de corresponder à imagem.
    assert gold["membros"] == [green["dataset"], yellow["dataset"]]
    assert gold["nome"] == "case_ifood.gold.trips_consolidado"
    assert gold["agregados"][0]["nome"] == "case_ifood.gold.trips_diario"
    assert green["fonte"]["volume"] == yellow["fonte"]["volume"] == "/Volumes/case_ifood/landing/files"
    assert [m["alias"] for m in gold["agregados"][0]["medidas"]] == [
        "qtd_corridas", "media_total_amount", "media_passageiros", "soma_distancia_km"]
    c = Canvas()
    c.text(64, 45, "LAKEHOUSE NYC TAXI", 24, C["teal"], "bold")
    c.text(64, 88, "Arquitetura implementada no repositório", 62, C["ink"], "bold")
    c.text(66, 174, "case-ifood  /  Databricks  /  contratos YAML → ingestão → bronze → silver → gold", 29, C["muted"])

    # Legenda: três semânticas de ligação e um estado de implementação.
    for x, label, color, dashed in [(1920,"Dados",C["ink"],False), (2160,"Contrato / configuração",C["teal"],True),
                                    (2710,"Log / auditoria",C["purple"],False), (3100,"Só declarado",C["gray"],True)]:
        c.path([(x, 62), (x+58, 62)], color, 4, dashed=dashed)
        c.text(x+72, 42, label, 24, color)

    for x, w in [(64,1686),(1780,1756)]:
        c.rect(x, 248, w, 342, C["teal_bg"], "#ADD9D5")
    c.text(90, 266, "CONTRATOS DE INGESTÃO + SILVER", 25, C["teal"], "bold")
    c.text(90, 310, "contracts/green_trips.yml  +  contracts/yellow_trips.yml", 28, C["ink"], "mono")
    c.paragraph(90, 358, "fonte · destinos · schema_bronze · mapeamento_silver · chave_dedupe_origem · regras_qualidade", 1620, 26, line=35)
    c.text(90, 445, "carregar_contrato() → motores de ingestão", 25, C["teal"], "mono")
    c.text(90, 485, "_carregar_contratos() → geração das tabelas bronze / silver", 25, C["teal"], "mono")
    c.text(90, 525, "listar_contratos() → notebook de qualidade", 25, C["teal"], "mono")
    c.text(1806, 266, "CONTRATO DE CONSUMO / GOLD", 25, C["teal"], "bold")
    c.text(1806, 310, "contracts/gold/trips.yml → _carregar_contratos_gold()", 28, C["ink"], "mono")
    c.text(1806, 363, "membros → _criar_gold_consolidado()", 27, C["teal"], "mono")
    c.text(1806, 407, "agrupar_por → _expressao_dimensao()", 27, C["teal"], "mono")
    c.text(1806, 451, "medidas → _expressao_medida() → _criar_gold_agregado()", 27, C["teal"], "mono")
    c.paragraph(1806, 509, "Valida os membros e as funções permitidas: count, count_distinct, sum, avg, min, max.", 1660, 25, C["muted"], line=34)

    c.rect(530, 670, 3028, 985, "#EAF0F7", "#CEDAE8", radius=28)
    c.text(556, 690, "DATABRICKS  ·  case_ifood", 27, C["muted"], "bold")
    c.text(1632, 690, "LAKEFLOW SDP  ·  pyspark.pipelines as dp", 25, C["muted"], "bold")
    # Configuração não transporta linhas: barramento tracejado separado do fluxo de dados.
    c.path([(900,590),(900,629),(2842,629)], C["teal"], 3, dashed=True)
    for x in (1076,2174,2842):
        c.path([(x,629),(x,790)],C["teal"],3,dashed=True,arrow=True)
    c.path([(3510,590),(3510,790)], C["teal"], 3, dashed=True, arrow=True)

    src = Card(c,64,790,430,790,"01","Fonte",C["teal"],C["teal_bg"])
    src.label("NYC TAXI & LIMOUSINE",C["teal"],22)
    src.p("Yellow + Green Taxi",31,style="bold",line=42)
    src.p("Arquivos mensais Parquet publicados pela TLC.",28,line=39)
    src.rule()
    src.p("d37ci6vzurychx.\ncloudfront.net/\ntrip-data/",24,style="mono",line=34)
    src.p("URL montada com taxi_type, ano e mês pelo url_template do contrato.",26,line=37)
    src.rule()
    src.p("Grão: uma linha por corrida.",27,style="bold",line=38)
    src.p("Escopo contratado: janeiro a maio de 2023.",26,C["muted"],line=36)

    ing = Card(c,552,790,550,790,"02","Ingestão",C["teal"],C["teal_bg"])
    ing.p("01_ingestao/\ningestao_parametrizada.ipynb",24,style="mono",line=33,gap=14)
    ing.p("carregar_contrato()",28,C["teal"],"mono",line=38)
    ing.p("Widgets: taxi_type, catalogo, ano, meses, origem.",26,line=36)
    ing.rule()
    ing.label("origem=url",C["teal"])
    ing.p("baixar_da_fonte()",27,style="mono",line=37,gap=8)
    ing.p("HTTP stream, até 3 tentativas; arquivo temporário → landing.",26,line=35,gap=14)
    ing.rule()
    ing.label("origem=raw  (default)",C["teal"])
    ing.p("executar_ingestao()",27,style="mono",line=37,gap=8)
    ing.p("Origem e destino usam a própria landing por padrão. Arquivos existentes são registrados como ja_existia.",25,line=35)

    land = Card(c,1160,790,410,790,"03","Landing",C["teal"],C["teal_bg"])
    land.label("VOLUME UC",C["teal"])
    land.p("/Volumes/\ncase_ifood/\nlanding/files",28,style="mono",line=39)
    land.rule()
    land.p("Parquet mensal",29,style="bold",line=40)
    land.p("yellow_tripdata_\n2023-*.parquet\n\ngreen_tripdata_\n2023-*.parquet",24,style="mono",line=33,gap=19)
    land.p("glob avaliado ao construir o pipeline.",26,line=37)
    land.p("Os widgets filtram a ingestão; o glob da pipeline continua fixo em 2023.",25,C["muted"],line=35)

    bronze = Card(c,1630,790,570,790,"04","Bronze",C["bronze"],C["bronze_bg"])
    bronze.p("bronze.green_trips\nbronze.yellow_trips",29,style="mono",line=39)
    bronze.label("STREAMING TABLES",C["bronze"])
    bronze.p("dp.create_streaming_table()",25,style="mono",line=35)
    bronze.p("_criar_fluxo_arquivo()\n@dp.append_flow(once=True)",25,style="mono",line=35)
    bronze.p("Um fluxo por arquivo; leitura batch com spark.read parquet.",26,line=36)
    bronze.rule()
    bronze.p("Cast para schema_bronze; nomes normalizados sem diferença de caixa; colunas ausentes → NULL.",26,line=36)
    bronze.p("+ _arquivo_origem\n+ _ingestao_ts",27,C["bronze"],"mono",line=38)
    bronze.p("Dados anteriores às regras de descarte da silver.",25,C["muted"],line=32)

    silver = Card(c,2258,790,610,790,"05","Silver",C["silver"],C["silver_bg"])
    silver.p("silver.green_trips\nsilver.yellow_trips",29,style="mono",line=39,gap=9)
    silver.p("_criar_silver()\n@dp.materialized_view",25,style="mono",line=34,gap=9)
    silver.p("xxhash64(chave original) → trip_id; mapeamento, timestamps e taxi_type; dedupe determinístico.",25,line=35,gap=12)
    silver.rule()
    silver.p("MV privada · @dp.expect_all",24,C["silver"],"mono",line=33,gap=9)
    silver.label("DROP · regras + duplicatas → quarentena",C["red"],23)
    silver.p("governanca.quarantine_<dataset>: motivos, colunas originais e lineage para reprocessamento.",24,line=33,gap=10)
    silver.p("Obrigatórias + validade + consistência; pickup na janela de jan–mai/2023.",24,line=33,gap=10)
    silver.label("WARN · expect_all",C["silver"],23)
    silver.p("passenger_count nulo: registro mantido. Faixa válida: NULL ou 0–8.",25,line=35,gap=11)

    gc = Card(c,2926,790,610,790,"06","Gold",C["gold"],C["gold_bg"])
    gc.p("gold.trips_consolidado",29,style="mono",line=39,gap=7)
    gc.p("_criar_gold_consolidado()",24,C["gold"],"mono",line=34,gap=7)
    gc.p("green → yellow; unionByName com allowMissingColumns=True.",25,line=35,gap=10)
    gc.rule(16)
    gc.p("gold.trips_diario",29,style="mono",line=39,gap=7)
    gc.p("_criar_gold_agregado()",25,C["gold"],"mono",line=35,gap=7)
    gc.p("Consolidado → groupBy:\nto_date(pickup_datetime)\nas data_corrida, taxi_type",24,style="mono",line=33,gap=10)
    gc.p("qtd_corridas: count(*)\nmedia_total_amount: avg\nmedia_passageiros: avg\nsoma_distancia_km: sum",24,style="mono",line=33,gap=12)
    gc.p("MVs; auto clustering no consolidado; diário por data_corrida.",24,C["muted"],line=33)

    for x1,x2 in [(494,552),(1102,1160),(1570,1630),(2200,2258),(2868,2926)]:
        c.path([(x1,1100),(x2,1100)],C["ink"],5,arrow=True)
    c.text(554,1606,"Todas as tabelas bronze / silver / gold usam o catálogo case_ifood. MVs = materialized views.",25,C["muted"])

    # Telemetria efetivamente escrita e avaliação independente da silver.
    c.path([(827,1580),(827,1730),(580,1730),(580,1830)],C["purple"],4,arrow=True)
    c.text(852,1708,"log por arquivo",25,C["purple"])
    c.path([(2563,1580),(2563,1730),(1860,1730),(1860,1830)],C["purple"],4,arrow=True)
    c.text(1890,1752,"leitura da silver após o pipeline",25,C["purple"])
    c.text(64,1764,"OPERAÇÃO E QUALIDADE",27,C["purple"],"bold")

    log = Card(c,64,1830,1010,480,"07","Log de ingestão",C["purple"],C["purple_bg"])
    log.p("case_ifood.governanca.log_ingestao",28,style="mono",line=39)
    log.p("Append por executar_ingestao() e baixar_da_fonte().",26,line=37)
    log.p("Dataset, arquivo, fonte, tamanho, checksum, status, timestamp e mensagem. Estados: ok / ja_existia / erro.",26,line=37)
    log.rule()
    log.p("Skip: destino existente e não vazio. MD5 parcial dos primeiros 64 MiB; não é comparado para decidir o skip.",25,C["muted"],line=36)

    dq = Card(c,1120,1830,1420,480,"08","Auditoria de qualidade · DAMA-DMBOK",C["purple"],C["purple_bg"])
    dq.p("case_ifood.governanca.dq_resultados",29,style="mono",line=40,gap=8)
    dq.p("03_qualidade/qualidade_contratos.ipynb → listar_contratos()",25,style="mono",line=35,gap=10)
    dq.p("Completude · validade · unicidade · consistência · pontualidade (janela histórica).",27,line=37,gap=9)
    dq.p("Nulos: yellow ≤ 1%, green ≤ 5%; passenger_count: ≤ 5% / ≤ 10%.",26,line=36,gap=9)
    dq.p("_registrar() + _exemplos_violacoes() → PASS/FAIL, métrica e JSON com até 5 exemplos em falhas; append Delta.",25,line=35,gap=9)
    dq.p("Qualquer FAIL lança Exception após gravar. Mede a silver já filtrada; não quantifica descartes da bronze.",25,C["purple"],line=35)

    obs = Card(c,2586,1830,950,480,"09","Observabilidade declarada",C["gray"],C["gray_bg"],dashed=True)
    obs.p("case_ifood.governanca.obs_metricas",27,style="mono",line=38)
    obs.p("criar_tabelas_governanca() cria a tabela Delta de métricas.",26,line=37)
    obs.rule()
    obs.p("Escritor / coletor não localizado no checkout.",28,C["gray"],"bold",line=39)
    obs.p("Freshness, contagens e leitura do event log são citados na documentação; o motor 04_observabilidade não está versionado aqui.",25,C["muted"],line=36)

    # Notas factuais: sem desenhar componentes não comprovados como fluxos executáveis.
    c.text(64,2350,"LIMITES DA REPRESENTAÇÃO",25,C["muted"],"bold")
    notes = [
        (64,1114,"Orquestração e consumo", "O Job de ingestão → pipeline → qualidade → observabilidade → segurança é descrito no README; sua definição não está no repo. 04/05/06 também estão ausentes. SQL/BI é consumo previsto."),
        (1210,1114,"Rastreabilidade e segurança", "_arquivo_origem e _ingestao_ts são preservados na quarentena. owner, domínio, tags e PII são declarados nos YAML; aplicação de GRANTs e tags não foi localizada."),
        (2356,1180,"Semântica que importa", "Deduplicação por dataset na silver; a gold apenas une. Janela contratual fixa em jan–mai/2023. soma_distancia_km soma trip_distance sem conversão de unidade no código; médias ignoram NULL."),
    ]
    for x,w,title,body in notes:
        c.rect(x,2396,w,220,C["white"],C["line"],radius=18,dashed=True)
        c.text(x+24,2412,title,28,C["ink"],"bold")
        c.paragraph(x+24,2461,body,w-48,24,C["muted"],line=33)
    c.text(64,2653,"Base: código Python, notebooks e 3 contratos YAML  ·  referências e reprodução no README",25,C["muted"])
    c.text(64,2690,"Análise estática do checkout; execução, configuração do Job e estado do workspace Databricks não foram verificados.",24,C["muted"])

    provenance = {p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in SOURCES}
    description = ("Arquitetura reconstruída do repositório case-ifood. Fluxos de dados sólidos, "
                   "configuração YAML tracejada em teal e log/auditoria em roxo. "
                   "Observabilidade, Job e segurança não são apresentados como implementação verificada.")
    metadata = json.dumps({"sources_sha256":provenance,"scope":"static source analysis"},ensure_ascii=False)
    vector = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" '
              f'viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-labelledby="title desc">'
              f'<title id="title">Arquitetura do lakehouse NYC Taxi — case-ifood</title>'
              f'<desc id="desc">{html.escape(description)}</desc>'
              f'<metadata>{html.escape(metadata)}</metadata>'
              f'<rect width="{WIDTH}" height="{HEIGHT}" fill="{C["bg"]}"/>'
              + "\n".join(c.svg) + "</svg>\n")
    OUT.mkdir(parents=True,exist_ok=True)
    (OUT/"arquitetura-projeto.svg").write_text(vector,encoding="utf-8")
    png_metadata = PngImagePlugin.PngInfo()
    png_metadata.add_text("Description",description)
    png_metadata.add_text("Source provenance",metadata)
    c.image.save(OUT/"arquitetura-projeto.png",pnginfo=png_metadata,optimize=True,dpi=(200,200))
    preview = c.image.copy()
    preview.thumbnail((1800,1370))
    preview.save(OUT/"arquitetura-previa.png",optimize=True)
    print(f"Gerados: PNG {WIDTH}x{HEIGHT}, SVG editável e prévia. {len(c.text_boxes)} blocos de texto.")


if __name__ == "__main__":
    create()
