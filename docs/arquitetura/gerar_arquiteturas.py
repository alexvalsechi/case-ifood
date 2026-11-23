"""Gera as duas visões de arquitetura usadas no README, sem acesso remoto.

Uso na raiz do projeto: python docs/arquitetura/gerar_arquiteturas.py
Dependência: Pillow. SVG e PNG compartilham o mesmo desenho e conteúdo.
"""

from __future__ import annotations

import html
import math
import os
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont


OUT = Path(__file__).resolve().parent
WIDTH = 1440
COLORS = {
    "bg": "#F5F7FB", "white": "#FFFFFF", "ink": "#1A2D45",
    "muted": "#53657A", "line": "#D8E1EC", "teal": "#087A7D",
    "teal_bg": "#E5F4F2", "bronze": "#935924", "bronze_bg": "#FBF0E4",
    "silver": "#416C97", "silver_bg": "#EAF2FB", "gold": "#85660F",
    "gold_bg": "#FAF4D9", "purple": "#71509C", "purple_bg": "#F1ECF8",
    "red": "#A84644", "red_bg": "#FBEFEB",
}


def font_path(bold=False):
    windows = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts"
    candidates = [
        windows / ("segoeuib.ttf" if bold else "segoeui.ttf"),
        Path("/usr/share/fonts/truetype/dejavu")
        / ("DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf"),
    ]
    return next(p for p in candidates if p.is_file())


class Canvas:
    def __init__(self, height):
        self.width, self.height = WIDTH, height
        self.image = Image.new("RGB", (WIDTH, height), COLORS["bg"])
        self.draw = ImageDraw.Draw(self.image)
        self.svg = []
        self.fonts = {}

    def font(self, size, bold=False):
        key = (size, bold)
        if key not in self.fonts:
            self.fonts[key] = ImageFont.truetype(str(font_path(bold)), size)
        return self.fonts[key]

    def rect(self, x, y, w, h, fill, stroke=None, radius=18):
        self.draw.rounded_rectangle((x, y, x+w, y+h), radius, fill=fill,
                                    outline=stroke, width=2)
        self.svg.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
            f'fill="{fill}" stroke="{stroke or "none"}" stroke-width="2"/>')

    def path(self, points, color=None, arrow=False, width=3):
        color = color or COLORS["muted"]
        self.draw.line(points, fill=color, width=width, joint="curve")
        coords = " ".join(f"{x},{y}" for x, y in points)
        self.svg.append(f'<polyline points="{coords}" fill="none" stroke="{color}" '
                        f'stroke-width="{width}" stroke-linejoin="round"/>')
        if arrow:
            x1, y1 = points[-2]
            x2, y2 = points[-1]
            angle = math.atan2(y2-y1, x2-x1)
            vertices = [(x2, y2)] + [
                (x2-12*math.cos(angle+a), y2-12*math.sin(angle+a))
                for a in (-0.5, 0.5)]
            self.draw.polygon(vertices, fill=color)
            coords = " ".join(f"{x},{y}" for x, y in vertices)
            self.svg.append(f'<polygon points="{coords}" fill="{color}"/>')

    def text(self, x, y, value, size=24, color=None, bold=False):
        color = color or COLORS["ink"]
        font = self.font(size, bold)
        ascent, _ = font.getmetrics()
        self.draw.text((x, y+ascent), value, font=font, fill=color, anchor="ls")
        bbox = self.draw.textbbox((x, y+ascent), value, font=font, anchor="ls")
        if min(bbox[:2]) < 0 or bbox[2] > self.width or bbox[3] > self.height:
            raise ValueError(f"Texto fora do canvas: {value}")
        self.svg.append(f'<text x="{x}" y="{y+ascent}" font-family="Segoe UI, DejaVu Sans, sans-serif" '
                        f'font-size="{size}" font-weight="{700 if bold else 400}" '
                        f'fill="{color}">{html.escape(value)}</text>')

    def wrap(self, value, width, size=24, bold=False):
        lines = []
        font = self.font(size, bold)
        for paragraph in value.split("\n"):
            line = ""
            for word in paragraph.split():
                proposed = f"{line} {word}" if line else word
                if font.getlength(proposed) > width and line:
                    lines.append(line)
                    line = word
                else:
                    line = proposed
                if font.getlength(line) > width:
                    raise ValueError(f"Palavra excede coluna: {line}")
            lines.append(line)
        return lines

    def paragraph(self, x, y, value, width, size=24, color=None, bold=False, leading=None):
        leading = leading or round(size*1.4)
        for line in self.wrap(value, width, size, bold):
            self.text(x, y, line, size, color, bold)
            y += leading
        return y

    def save(self, stem, title, description):
        self.image.save(OUT / f"{stem}.png", optimize=True)
        svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.width}" '
               f'height="{self.height}" viewBox="0 0 {self.width} {self.height}" '
               f'role="img" aria-labelledby="title desc">'
               f'<title id="title">{html.escape(title)}</title>'
               f'<desc id="desc">{html.escape(description)}</desc>'
               f'<rect width="100%" height="100%" fill="{COLORS["bg"]}"/>'
               + "".join(self.svg) + "</svg>\n")
        (OUT / f"{stem}.svg").write_text(svg, encoding="utf-8")


def header(c, title, subtitle):
    c.text(56, 42, "CASE IFOOD  /  DATABRICKS", 20, COLORS["teal"], True)
    c.text(56, 82, title, 43, bold=True)
    c.text(56, 145, subtitle, 24, COLORS["muted"])


def general():
    c = Canvas(1236)
    header(c, "Da configuração à consulta", "Visão geral · contratos YAML orientam a ingestão, a qualidade e a Gold")

    c.rect(56, 212, 1328, 122, COLORS["white"], COLORS["line"])
    c.rect(78, 236, 55, 55, COLORS["teal_bg"], radius=14)
    c.text(91, 246, "01", 25, COLORS["teal"], True)
    c.text(157, 232, "Databricks Asset Bundle", 30, bold=True)
    c.text(157, 278, "Valida e publica a definição do pipeline e dos Jobs · targets dev e prod", 24, COLORS["muted"])
    c.path([(720, 334), (720, 370)], arrow=True)

    c.rect(56, 382, 1328, 178, COLORS["white"], COLORS["line"])
    c.rect(78, 406, 55, 55, COLORS["teal_bg"], radius=14)
    c.text(91, 416, "02", 25, COLORS["teal"], True)
    c.text(157, 402, "Bootstrap do ambiente · execução sob demanda", 30, bold=True)
    c.text(157, 450, "Cria catálogo, schemas, volumes e tabelas de governança no Unity Catalog.", 24)
    c.text(157, 491, "Baixa Parquet Yellow/Green e CSV Taxi Zones da TLC para os volumes de landing.", 24, COLORS["muted"])
    c.path([(720, 560), (720, 597)], arrow=True)

    c.text(56, 612, "03  PIPELINE SDP · BRONZE → SILVER → GOLD", 22, COLORS["teal"], True)
    c.text(1076, 612, "CONSUMO", 22, COLORS["teal"], True)
    cards = [
        (56, "Bronze", "bronze", "Lê a landing e\nnormaliza o schema.\nPreserva a origem."),
        (396, "Silver", "silver", "Padroniza e deduplica.\nAplica regras e separa\ninválidos na quarentena."),
        (736, "Gold", "gold", "Unifica Yellow + Green.\nAdiciona dimensões e\nagregado diário."),
        (1076, "Consulta SQL", "teal", "Disponibiliza os dados\npara análises no\nnotebook de respostas."),
    ]
    for x, title, color, body in cards:
        c.rect(x, 664, 308, 236, COLORS["white"], COLORS["line"])
        c.rect(x+18, 682, 272, 57, COLORS[f"{color}_bg"], radius=12)
        c.text(x+34, 692, title, 29, COLORS[color], True)
        c.paragraph(x+24, 762, body, 266, 24, leading=35)
    for x in (364, 704, 1044):
        c.path([(x+3, 782), (x+26, 782)], arrow=True, color=COLORS["teal"])
    c.text(56, 921, "Dimensões: fornecedores, tarifas, pagamentos, tipo de viagem e zonas de embarque/desembarque.", 23, COLORS["muted"])

    c.rect(56, 988, 1328, 138, COLORS["purple_bg"], radius=18)
    c.text(78, 1007, "Depois da materialização: auditoria de qualidade → observabilidade", 28, COLORS["purple"], True)
    c.text(78, 1051, "O Job operacional atualiza o pipeline, audita a Silver e registra métricas de execução.", 24)
    c.text(78, 1088, "FAIL na auditoria interrompe o Job; a Gold já foi materializada nessa atualização.", 23, COLORS["muted"])
    c.text(56, 1163, "Requer configuração inicial do workspace; ver guia no README. Execução remota não verificada.", 22, COLORS["muted"])
    c.save("arquitetura-visao-geral", "Arquitetura: da configuração à consulta",
           "DAB publica pipeline e Jobs; bootstrap cria o ambiente e baixa fontes TLC; pipeline lê landing, materializa Bronze e Silver, enriquece Gold e permite consulta SQL. Auditoria e observabilidade executam após a materialização.")


def stage(c, y, number, title, color, purpose, checks, tag=None):
    """Etapa com duas colunas largas e altura calculada pelo texto real."""
    x, w = 112, 1272
    content_y = y+109
    left_width, right_width = 552, 580
    left_lines = c.wrap(purpose, left_width, 24)
    right_lines = c.wrap(checks, right_width, 24)
    body_height = max(len(left_lines), len(right_lines))*34
    height = 135+body_height
    c.rect(x, y, w, height, COLORS["white"], COLORS["line"])
    c.rect(x+20, y+18, w-40, 54, COLORS[f"{color}_bg"], radius=11)
    c.text(x+36, y+24, title, 29, COLORS[color], True)
    if tag:
        # Uma legenda curta ocupa a extremidade direita da faixa do título.
        tag_w = c.font(20, True).getlength(tag)
        c.text(x+w-38-tag_w, y+33, tag, 20, COLORS[color], True)
    c.text(x+28, y+83, "POR QUE / O QUE FAZ", 17, COLORS["muted"], True)
    c.text(x+643, y+83, "VALIDAÇÕES / COMPORTAMENTO", 17, COLORS["muted"], True)
    c.path([(x+615, y+87), (x+615, y+height-24)], COLORS["line"], width=2)
    c.paragraph(x+28, content_y, purpose, left_width, 24, leading=34)
    c.paragraph(x+643, content_y, checks, right_width, 24, leading=34)
    c.rect(35, y+20, 55, 55, COLORS[f"{color}_bg"], radius=16)
    c.text(47, y+29, number, 25, COLORS[color], True)
    c.path([(62, y+75), (62, y+height+21)], COLORS["line"], arrow=True, width=3)
    return y+height+33


def detailed():
    stages = [
        ("01", "Versionamento e publicação com DAB", "teal",
         "Git versiona código, contratos YAML e recursos. O bundle publica o pipeline SDP e os Jobs de bootstrap e operação no workspace.",
         "GitHub Actions executa 16 testes Python; validação DAB é condicional aos secrets. O workflow não faz deploy nem executa Spark.", None),
        ("02", "Bootstrap e leitura das fontes TLC", "teal",
         "Cria catálogo, schemas, volumes e governança. Baixa Yellow/Green (jan–mai/2023) e o CSV Taxi Zones; o pipeline passa a ler a landing.",
         "CREATE IF NOT EXISTS; pula Parquet existente e não vazio. Registra status/checksum. Confere cabeçalho do CSV e salva snapshot com manifesto.", "SOB DEMANDA"),
        ("03", "Bronze · rastreabilidade da origem", "bronze",
         "Lê cada Parquet com o schema físico do arquivo e converte para o schema canônico do contrato. Mantém os dados para investigação e reprocessamento.",
         "Normaliza tipos e capitalização; colunas ausentes viram NULL. Acrescenta _arquivo_origem e _ingestao_ts. Regras de negócio são aplicadas na Silver.", None),
        ("04", "Silver · conformidade e deduplicação", "silver",
         "Padroniza nomes e datas; cria trip_id e taxi_type. Expectations registram métricas. Mantém uma linha conforme por trip_id; violações e duplicatas excedentes vão à quarentena com motivo e origem.",
         "Obrigatórias críticas não nulas; valor e distância ≥ 0; passageiros NULL ou 0–8; pickup de 01/jan a antes de 01/jun/2023; dropoff ≥ pickup. Passageiros NULL: WARN, mantém a linha.", None),
        ("05", "Referências · códigos com significado", "silver",
         "Materializa cinco dimensões em Bronze/Silver: vendor, rate_code, payment_type, trip_type e taxi_zone. Quatro vêm do YAML e uma do CSV oficial.",
         "Aplica os tipos do contrato e deduplica pela chave da dimensão. O lookup externo teve seu cabeçalho verificado no bootstrap. Essas referências enriquecem a Gold.", "EM PARALELO"),
        ("06", "Gold · dados prontos para análise", "gold",
         "Une Yellow + Green em trips_consolidado. Faz seis LEFT JOINs com cinco dimensões (zonas usadas duas vezes). trips_diario agrega contagem e médias por dia/tipo.",
         "Na carga, valida membros, dimensões e funções de agregação declaradas. LEFT JOIN preserva as corridas e as chaves; referência não encontrada resulta em descrição NULL.", None),
        ("07", "Auditoria de qualidade · após o pipeline", "purple",
         "Audita as Silvers em cinco dimensões DAMA: completude, validade, unicidade, consistência e pontualidade. Grava métricas e exemplos em governanca.dq_resultados.",
         "24 verificações (12 por dataset). Qualquer FAIL interrompe o Job após registrar o relatório. A Gold já foi materializada; a auditoria não desfaz nem impede essa publicação.", None),
        ("08", "Observabilidade · saúde e histórico", "purple",
         "Lê as tabelas declaradas nos contratos e o event log do SDP. Registra volume, tempo desde a última escrita e resultados das expectations em governanca.obs_metricas.",
         "Sinaliza tabelas vazias/ausentes e expectations reprovadas, sem gate por métrica. Erros técnicos podem falhar a tarefa. Depende do sucesso da auditoria de qualidade.", None),
    ]
    # O mesmo cálculo de quebra de linhas determina a altura final do canvas.
    probe = Canvas(100)
    heights = [135+max(len(probe.wrap(s[3], 552)), len(probe.wrap(s[4], 580)))*34+33
               for s in stages]
    height = 224+sum(heights)+221
    c = Canvas(height)
    header(c, "Etapas, propósito e validações", "Visão detalhada · leia de cima para baixo; as dimensões são uma ramificação do pipeline")
    y = 224
    for number, title, color, purpose, checks, tag in stages:
        y = stage(c, y, number, title, color, purpose, checks, tag)
    c.rect(112, y+3, 1272, 132, COLORS["gold_bg"], radius=18)
    c.text(140, y+22, "Consumo · SQL / notebook de respostas", 30, COLORS["gold"], True)
    c.text(140, y+70, "Gold disponível: trips_consolidado e trips_diario. Notebook consulta o consolidado.", 24)
    c.text(56, y+174, "Requer configuração inicial do workspace; ver guia no README. Execução remota não verificada.", 22, COLORS["muted"])
    c.save("arquitetura-detalhada", "Arquitetura detalhada: etapas, propósito e validações",
           "Oito etapas descrevem versionamento e DAB, bootstrap, Bronze, Silver e quarentena, cinco dimensões de referência, Gold enriquecida por seis LEFT JOINs, auditoria DAMA após materialização e observabilidade. Consumo SQL consulta a Gold; a auditoria posterior não é uma barreira antes de materializar a Gold.")


if __name__ == "__main__":
    general()
    detailed()
    print("Gerados: arquitetura-visao-geral e arquitetura-detalhada (SVG + PNG).")
