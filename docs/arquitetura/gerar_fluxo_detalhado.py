"""Fluxo visual da arquitetura atual: ícones vetoriais, conexões e fontes locais.

Invocado por gerar_arquiteturas.py. Os textos de objetos, contratos e jobs são
obtidos dos arquivos versionados. Nenhum serviço remoto é acessado.
"""

from __future__ import annotations

import hashlib
import math
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
W, H = 3600, 2680
P = {
    "ink": "#183B49", "muted": "#536A77", "line": "#CCDCE3",
    "white": "#FFFFFF", "data": "#127D87", "config": "#72848E",
    "purple": "#7755A5", "purple_bg": "#F2EDF8",
    "coral": "#F15C47", "teal_bg": "#E8F5F4",
    "bronze": "#A4652F", "bronze_bg": "#FCF0E4",
    "silver": "#47769F", "silver_bg": "#EDF4FB",
    "gold": "#987311", "gold_bg": "#FFF7D8",
    "red": "#B25050", "red_bg": "#FBEFEE",
}


def read_model():
    """Obtém nomes reais e registra a origem de cada componente desenhado."""
    sources = [
        "databricks.yml", ".github/workflows/tests.yml",
        "resources/case_nyc_taxi_bootstrap.job.yml",
        "resources/case_nyc_taxi_update_e_qualidade.job.yml",
        "resources/case_nyc_taxi_medallion.pipeline.yml",
        "src/contracts/yellow_trips.yml", "src/contracts/green_trips.yml",
        "src/contracts/references/dimensoes_tlc.yml", "src/contracts/gold/trips.yml",
        "src/00_config/motores.py", "src/01_ingestao/bootstrap_ambiente.ipynb",
        "src/00_config/download.py", "src/00_config/landing.py",
        "analysis/observabilidade_ingestao.sql", "tests/test_ingestao.py",
        "src/01_ingestao/ingestao_parametrizada.ipynb",
        "src/01_ingestao/atualizar_taxi_zone_lookup.py",
        "src/02_pipelines/pipeline_case_nyc_taxi.py",
        "src/03_qualidade/qualidade_contratos.ipynb",
        "src/04_observabilidade/observabilidade_contratos.ipynb",
        "analysis/consultas_gold.ipynb",
        "docs/arquitetura/gerar_arquiteturas.py",
        "docs/arquitetura/gerar_fluxo_detalhado.py",
    ]

    def load(path):
        return yaml.safe_load((ROOT / path).read_text(encoding="utf-8-sig"))

    datasets = [load(f"src/contracts/{taxi}_trips.yml") for taxi in ("yellow", "green")]
    references = load("src/contracts/references/dimensoes_tlc.yml")["dimensoes"]
    gold = load("src/contracts/gold/trips.yml")
    bootstrap = load("resources/case_nyc_taxi_bootstrap.job.yml")["resources"]["jobs"]["case_nyc_taxi_bootstrap"]
    operational = load("resources/case_nyc_taxi_update_e_qualidade.job.yml")["resources"]["jobs"]["case_nyc_taxi_update_e_qualidade"]
    parameters = bootstrap["tasks"][0]["notebook_task"]["base_parameters"]
    # Mudanças na topologia exigem revisão do desenho, em vez de uma figura desatualizada.
    assert [t["task_key"] for t in operational["tasks"]] == [
        "pipeline_update", "qualidade_contratos", "observabilidade_contratos"
    ]
    assert len(references) == 5 and len(gold["enriquecimentos"]) == 6
    assert len(gold["agregados"]) == 1
    catalog = parameters["catalogo"]
    assert all(c["destinos"]["silver"].startswith(catalog + ".") for c in datasets)
    checks = []
    for contract in datasets:
        rules = contract["regras_qualidade"]
        checks.append(
            len(rules["completude"]["colunas"])
            + len(rules["completude"].get("excessoes", {}))
            + len(rules["validade"]["regras"])
            + len(rules["consistencia"]["regras"]) + 2
        )
    return {
        "datasets": datasets, "references": references, "gold": gold,
        "catalog": catalog, "checks": checks,
        "parquets": len(datasets) * len(parameters["meses"].split(",")),
        "sources_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sources},
    }


class Flow:
    """Primitivas comuns ao SVG editável e ao PNG, com ícones desenhados localmente."""

    def __init__(self, canvas):
        self.c = canvas
        self.nodes = []
        self.edges = []

    def ellipse(self, box, color, fill=None, width=3):
        x, y, w, h = box
        self.c.draw.ellipse((x, y, x + w, y + h), fill=fill, outline=color, width=width)
        self.c.svg.append(
            f'<ellipse cx="{x+w/2}" cy="{y+h/2}" rx="{w/2}" ry="{h/2}" '
            f'fill="{fill or "none"}" stroke="{color}" stroke-width="{width}"/>'
        )

    def polygon(self, points, color, fill=None, width=3):
        if fill:
            self.c.draw.polygon(points, fill=fill)
        self.c.draw.line(points + [points[0]], fill=color, width=width, joint="curve")
        coords = " ".join(f"{x},{y}" for x, y in points)
        self.c.svg.append(f'<polygon points="{coords}" fill="{fill or "none"}" stroke="{color}" stroke-width="{width}"/>')

    def line(self, points, color, width=4, dashed=False, arrow=False):
        if not dashed:
            self.c.path(points, color=color, width=width, arrow=arrow)
            return
        # Comprimentos contínuos nas quinas para manter PNG/SVG equivalentes.
        phase = 0.0
        for (x1, y1), (x2, y2) in zip(points, points[1:]):
            length = math.hypot(x2 - x1, y2 - y1)
            distance = 0.0
            while distance < length:
                step = min((12 if phase < 12 else 22) - phase, length - distance)
                if phase < 12:
                    a, b = distance / length, (distance + step) / length
                    self.c.draw.line((x1+(x2-x1)*a, y1+(y2-y1)*a, x1+(x2-x1)*b, y1+(y2-y1)*b), fill=color, width=width)
                distance += step
                phase = (phase + step) % 22
        coords = " ".join(f"{x},{y}" for x, y in points)
        self.c.svg.append(f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="{width}" stroke-dasharray="12 10" stroke-linejoin="round"/>')
        if arrow:
            x1, y1 = points[-2]
            x2, y2 = points[-1]
            angle = math.atan2(y2-y1, x2-x1)
            tip = [(x2, y2)] + [(x2-14*math.cos(angle+a), y2-14*math.sin(angle+a)) for a in (-0.5, 0.5)]
            self.c.draw.polygon(tip, fill=color)
            self.c.svg.append('<polygon points="' + " ".join(f"{x},{y}" for x,y in tip) + f'" fill="{color}"/>')

    def icon(self, name, x, y, size=52, color=None):
        color = color or P["data"]
        s = size / 64

        def pt(a, b):
            return (x + a*s, y + b*s)

        def line(coords):
            self.line([pt(a,b) for a,b in coords], color, width=3)

        def rect(a,b,w,h,r=5):
            self.c.rect(x+a*s,y+b*s,w*s,h*s,P["white"],color,radius=r)

        def ellipse(a,b,w,h,fill=None):
            self.ellipse((x+a*s,y+b*s,w*s,h*s),color,fill)

        if name == "database":
            line([(8,17),(8,48)])
            line([(56,17),(56,48)])
            ellipse(8,39,48,18)
            ellipse(8,25,48,18)
            ellipse(8,8,48,18,P["white"])
        elif name == "cloud":
            # Silhueta feita de arcos fechados, com seta de download reconhecível.
            ellipse(7,21,22,27,P["white"])
            ellipse(20,9,30,35,P["white"])
            ellipse(39,23,20,25,P["white"])
            self.c.rect(x+12*s,y+30*s,42*s,18*s,P["white"],radius=0)
            line([(12,48),(53,48)])
            line([(32,28),(32,57)])
            line([(23,48),(32,57),(41,48)])
        elif name == "file":
            self.polygon([pt(12,5),pt(39,5),pt(53,19),pt(53,59),pt(12,59)],color,P["white"])
            line([(39,5),(39,19),(53,19)])
            for a in (29,38,47):
                line([(22,a),(43,a)])
        elif name == "folder":
            self.polygon([pt(5,17),pt(25,17),pt(31,24),pt(58,24),pt(58,54),pt(5,54)],color,P["white"])
            line([(5,31),(58,31)])
        elif name == "git":
            line([(20,12),(20,51)])
            line([(20,39),(44,25),(44,12)])
            for a,b in ((20,10),(20,53),(44,10)):
                ellipse(a-5,b-5,10,10,P["white"])
        elif name == "stack":
            for dy in (0,14,28):
                line([(6,15+dy),(32,3+dy),(58,15+dy),(32,27+dy),(6,15+dy)])
        elif name == "play":
            ellipse(5,5,54,54,P["white"])
            self.polygon([pt(27,19),pt(46,32),pt(27,45)],color,color)
        elif name == "shield":
            self.polygon([pt(32,4),pt(55,14),pt(51,39),pt(44,51),pt(32,60),pt(20,51),pt(13,39),pt(9,14)],color,P["white"])
            line([(20,31),(29,40),(45,23)])
        elif name == "filter":
            self.polygon([pt(6,11),pt(58,11),pt(39,33),pt(39,54),pt(25,60),pt(25,33)],color,P["white"])
        elif name == "chart":
            line([(6,6),(6,57),(60,57)])
            for a,b in ((17,33),(31,22),(45,8)):
                rect(a,b,9,53-b,0)
        elif name == "monitor":
            rect(4,8,56,39)
            line([(26,47),(26,57),(15,57),(48,57),(38,57),(38,47)])
            line([(15,23),(9,28),(15,33)])
            line([(49,23),(55,28),(49,33)])
            line([(36,19),(28,38)])
        elif name == "join":
            ellipse(4,13,36,37)
            ellipse(24,13,36,37)
        elif name == "taxi":
            self.polygon([pt(8,30),pt(16,16),pt(48,16),pt(56,30),pt(60,33),pt(60,49),pt(4,49),pt(4,33)],color,P["white"])
            rect(26,7,13,9,2)
            line([(12,30),(52,30)])
            ellipse(9,44,12,12,P["white"])
            ellipse(43,44,12,12,P["white"])
        elif name == "warning":
            self.polygon([pt(32,5),pt(61,57),pt(3,57)],color,P["white"])
            line([(32,22),(32,39)])
            ellipse(30,45,4,4,color)
        elif name == "pulse":
            line([(3,33),(15,33),(23,17),(33,49),(42,25),(49,33),(61,33)])
        else:
            raise ValueError(f"Ícone não implementado: {name}")

    def card(self, key, x, y, w, h, title, icon, color=None, tint=None, subtitle=None):
        color = color or P["data"]
        tint = tint or P["teal_bg"]
        if self.c.font(31, True).getlength(title) > w-112:
            raise ValueError(f"Título excede o bloco {key}: {title}")
        self.c.rect(x,y,w,h,P["white"],P["line"],radius=18)
        self.c.rect(x+2,y+2,w-4,82,tint,radius=16)
        self.icon(icon,x+22,y+15,50,color)
        self.c.text(x+88,y+21,title,31,color,True)
        self.nodes.append({"id":key,"title":title,"box":[x,y,w,h]})
        if subtitle:
            self.c.paragraph(x+24,y+101,subtitle,w-48,25,P["muted"],leading=34)
        return y+102

    def text(self, x, y, value, width, size=28, color=None, bold=False, leading=37):
        end = self.c.paragraph(x,y,value,width,size,color or P["ink"],bold,leading)
        for node in reversed(self.nodes):
            if "box" not in node:
                continue
            nx,ny,nw,nh = node["box"]
            if nx < x < nx+nw and ny+82 <= y < ny+nh:
                if end-leading+size > ny+nh-9:
                    raise ValueError(f"Texto excede o bloco {node['id']}: {value}")
                break
        return end

    def edge(self, source, target, points, kind="data", label=None, label_at=None):
        styles = {
            "data": (P["data"],False), "config":(P["config"],True),
            "task":(P["purple"],False), "metric":(P["purple"],True),
            "reject":(P["red"],False),
        }
        color,dashed = styles[kind]
        self.line(points,color,4,dashed,True)
        self.edges.append({"from":source,"to":target,"kind":kind,"label":label})
        if label and label_at:
            self.c.text(*label_at,label,23,color,True)

    def pill(self, x, y, text, width, color=None, tint=None, icon=None, size=25):
        color = color or P["data"]
        self.c.rect(x,y,width,57,tint or P["teal_bg"],radius=11)
        if icon:
            self.icon(icon,x+12,y+9,38,color)
        self.c.text(x+(60 if icon else 18),y+11,text,size,color,True)


def generate(canvas_type):
    model = read_model()
    c = canvas_type(H, width=W)
    f = Flow(c)
    short = lambda name: name.removeprefix(model["catalog"] + ".")
    datasets = model["datasets"]
    c.text(72,40,"LAKEHOUSE NYC TAXI  /  ARQUITETURA IMPLEMENTADA",24,P["data"],True)
    c.text(72,84,"Da origem TLC à Gold: componentes e conexões",58,P["ink"],True)
    c.text(74,165,"Leia da esquerda para a direita. Corridas e referências seguem caminhos próprios até o enriquecimento.",29,P["muted"])

    # Entrega e declaração: fora do fluxo de dados para não sugerir deploy automático.
    f.card("github",72,235,750,220,"GitHub · código e CI","git")
    f.text(98,337,"src/ + analysis/ + resources/\nTestes Python · validação DAB condicional",690,28)
    f.card("dab",920,235,740,220,"Databricks Asset Bundle","stack")
    f.text(946,337,"CLI: validate → deploy · targets dev/prod\ndatabricks.yml + resources/*.yml",685,28)
    f.edge("github","dab",[(822,345),(920,345)],"config", "manual", (835,307))
    f.card("contracts",1780,235,1748,220,"Contratos declarativos · src/contracts/","file")
    for x,heading,body,width in (
        (1808,"Corridas","yellow_trips.yml + green_trips.yml",566),
        (2406,"Referências","references/dimensoes_tlc.yml",530),
        (2980,"Consumo","gold/trips.yml",515),
    ):
        c.text(x,336,heading,28,P["ink"],True)
        c.text(x,380,body,24,P["muted"])
    f.edge("dab","platform",[(1290,455),(1290,490),(1530,490),(1530,520)],"config","publica arquivos, pipeline e jobs",(770,480))
    f.edge("contracts","platform",[(2650,455),(2650,520)],"config","configura ingestão, schema, regras, dimensões e Gold",(2690,476))

    # Limite da plataforma e orquestração.
    c.rect(550,520,2500,1900,P["white"],P["coral"],radius=18)
    c.rect(552,522,2496,77,P["coral"],radius=16)
    f.icon("stack",580,535,49,P["white"])
    c.text(650,539,"DATABRICKS  ·  Lakehouse NYC Taxi",36,P["white"],True)
    c.text(2380,546,"Workspace + Unity Catalog",28,P["white"])
    c.text(600,622,"PREPARAÇÃO SOB DEMANDA",23,P["data"],True)
    c.text(1320,622,"JOB OPERACIONAL  ·  case_nyc_taxi_update_e_qualidade",23,P["purple"],True)
    f.card("bootstrap",600,670,620,255,"Bootstrap do ambiente","play")
    f.text(625,773,"case_nyc_taxi_bootstrap\nCria UC e chama os motores de ingestão\nCREATE ... IF NOT EXISTS",568,27,leading=37)
    f.card("update",1320,670,390,255,"Pipeline update","play",P["purple"],P["purple_bg"])
    f.text(1345,773,"pipeline_update\nAtualização normal\nBronze → Silver → Gold",338,27,leading=37)
    f.card("audit",1800,670,515,255,"Auditoria DAMA","shield",P["purple"],P["purple_bg"])
    f.text(1825,771,f"{sum(model['checks'])} verificações na Silver\nFAIL bloqueia a próxima task",463,27,leading=36)
    f.pill(1823,854,"governanca.dq_resultados",466,P["purple"],P["purple_bg"],"database",23)
    f.card("observe",2400,670,590,255,"Observabilidade","chart",P["purple"],P["purple_bg"])
    f.text(2425,771,"Contagens + freshness + expectations\nLê tabelas de corridas/Gold e event log",538,26,leading=36)
    f.pill(2423,854,"governanca.obs_metricas",542,P["purple"],P["purple_bg"],"database",23)
    f.edge("bootstrap","update",[(1220,795),(1320,795)],"config","depois",(1230,750))
    f.edge("update","audit",[(1710,795),(1800,795)],"task")
    f.edge("audit","observe",[(2315,795),(2400,795)],"task")

    f.pill(600,973,"Lakeflow Spark Declarative Pipelines · case_nyc_taxi_medallion · serverless + Photon",1680,size=28)
    f.pill(2360,973,"event_log() · métricas nativas",620,P["purple"],P["purple_bg"],"pulse",26)
    f.nodes.append({"id":"eventlog","title":"event_log()"})
    f.edge("update","pipeline",[(1515,925),(1515,973)],"config")
    f.edge("eventlog","observe",[(2670,973),(2670,925)],"metric")
    f.edge("bootstrap","download",[(600,850),(575,850),(575,1140),(600,1140)],"config")

    # Entradas externas, sem adicionar tecnologias que não existem no projeto.
    c.text(72,1040,"ORIGENS OFICIAIS  ·  NYC TLC",25,P["ink"],True)
    f.card("parquets",72,1110,410,370,"Corridas TLC","cloud")
    f.icon("taxi",98,1222,58,"#AF7A13")
    c.text(174,1234,"Yellow Taxi",30,P["ink"],True)
    f.icon("taxi",98,1304,58,"#388564")
    c.text(174,1316,"Green Taxi",30,P["ink"],True)
    f.text(98,1404,f"Parquet · jan–mai/2023\n{model['parquets']} arquivos no bootstrap",354,26,leading=34)
    f.card("manual",72,1535,410,190,"Notebook manual","file")
    f.text(97,1632,"ingestao_parametrizada\norigem=url ou raw",354,25,leading=32)
    f.card("csv",72,1760,410,240,"Taxi Zones","cloud")
    f.text(98,1863,"taxi_zone_lookup.csv\nCSV oficial de referência\nSnapshot por data",354,26,leading=35)
    f.card("dictionary",72,2060,410,170,"Dicionários YAML","file")
    f.text(98,2162,"Vendor · tarifa\nPagamento · despacho",354,25,leading=33)

    # Ingestão: download e volume são elementos distintos, ligados por dados.
    f.card("download",600,1110,460,250,"Download Parquet","cloud")
    f.text(625,1214,"HTTP · retry por tipo de falha\nBackoff + Retry-After\nTemporário → Parquet validado\nSkip: footer + hash conferidos",408,25,leading=32)
    f.card("files",600,1430,460,240,"Volume landing.files","folder")
    f.text(625,1537,"categoria=green | yellow\nmes_ano=MM-AAAA\nParquet + manifesto",408,26,leading=35)
    f.pill(600,1688,"governanca.log_ingestao",460,icon="database",size=23)
    f.nodes.append({"id":"ingestion_log","title":"governanca.log_ingestao"})
    f.edge("parquets","download",[(482,1280),(600,1280)])
    f.edge("manual","download",[(482,1608),(535,1608),(535,1335),(600,1335)],"config","url",(490,1300))
    f.edge("manual","files",[(482,1700),(560,1700),(560,1595),(600,1595)],"config","raw",(492,1663))
    f.edge("download","files",[(830,1360),(830,1430)])
    f.edge("download","ingestion_log",[(1060,1300),(1090,1300),(1090,1715),(1060,1715)],"metric")
    c.text(600,1752,"SHA-256 completo · log por tentativa",23,P["muted"])

    f.card("lookup",600,1820,460,178,"Atualizar lookup TLC","cloud")
    f.text(625,1922,"Cabeçalho CSV validado\nSHA-256 + snapshot datado",408,25,leading=32)
    f.card("reference_volume",600,2060,460,170,"landing.reference","folder")
    f.text(625,2163,"CSV atual + snapshots\nManifesto de origem e hash",408,25,leading=32)
    f.edge("csv","lookup",[(482,1890),(600,1890)])
    f.edge("lookup","reference_volume",[(830,1998),(830,2060)])

    # Bronze, MV privada de validação, Silver e a saída de rejeitados.
    f.card("bronze",1140,1110,500,380,"Bronze · corridas","database",P["bronze"],P["bronze_bg"])
    f.text(1165,1213,"\n".join(short(d['destinos']['bronze']) for d in datasets),448,26,P["bronze"],True,36)
    f.text(1165,1306,"Streaming tables · casts\nSchema canônico · NULL ausente\n_arquivo_origem · _ingestao_ts\nappend_flow(once=True) por arquivo\nLeitura batch de cada Parquet",448,25,leading=33)
    f.edge("files","bronze",[(1060,1545),(1100,1545),(1100,1280),(1140,1280)])

    f.card("validation",1720,1110,560,300,"Validação · MV privada","filter",P["silver"],P["silver_bg"])
    f.text(1745,1214,"_yellow_trips_validacao\n_green_trips_validacao",508,25,P["silver"],True,34)
    f.text(1745,1296,"Mapeamento · trip_id (xxhash64)\nexpect_all + motivos_rejeicao",508,26,leading=36)
    f.edge("bronze","validation",[(1640,1280),(1720,1280)])
    f.edge("validation","eventlog",[(2240,1110),(2240,1065),(2670,1065),(2670,1030)],"metric")

    f.card("silver",1720,1510,560,270,"Silver · conformes","database",P["silver"],P["silver_bg"])
    f.text(1745,1611,"\n".join(short(d['destinos']['silver']) for d in datasets),508,26,P["silver"],True,36)
    f.text(1745,1702,"Materialized views · trip_id único\nPassageiro NULL: WARN, mantém",508,25,leading=32)
    f.edge("validation","silver",[(2000,1410),(2000,1510)],label="conformes",label_at=(1825,1442))

    f.card("quarantine",1140,1580,500,270,"Quarentena","warning",P["red"],P["red_bg"])
    f.text(1165,1682,"\n".join(short(d['destinos']['quarentena']) for d in datasets),448,23,P["red"],True,32)
    f.text(1165,1765,"Rejeições + duplicatas excedentes\nMotivo · _orig_* · arquivo · datas",448,24,leading=32)
    f.edge("validation","quarantine",[(1720,1340),(1680,1340),(1680,1700),(1640,1700)],"reject")

    # Referências passam por Bronze e Silver antes de compor os seis joins.
    f.card("bronze_dims",1140,1900,500,330,"Bronze · referências","database",P["bronze"],P["bronze_bg"])
    f.text(1165,2002,"CSV de zonas + 4 dicionários\nTabelas bronze.dim_*\nMaterialized views de referência",448,26,leading=36)
    f.text(1165,2150,"Tipos declarados nos contratos",448,24,P["muted"])
    f.edge("reference_volume","bronze_dims",[(1060,2140),(1140,2140)])
    f.edge("dictionary","bronze_dims",[(482,2170),(530,2170),(530,2260),(1390,2260),(1390,2230)])

    f.card("silver_dims",1720,1900,560,330,"Silver · 5 dimensões","database",P["silver"],P["silver_bg"])
    dimensions = [ref["dataset"] for ref in model["references"]]
    f.text(1745,2005,"\n".join(dimensions),508,25,P["silver"],True,32)
    f.text(1745,2185,"Casts + deduplicação pela chave",508,23,P["muted"])
    f.edge("bronze_dims","silver_dims",[(1640,2050),(1720,2050)])

    # Gold une fatos e adiciona as referências; consulta do case lê o consolidado.
    f.card("gold",2360,1110,620,670,"Gold · unificação e consumo","join",P["gold"],P["gold_bg"])
    f.text(2386,1212,"Yellow + Green · unionByName\n6 LEFT JOINs com dimensões Silver\nTaxi Zone: embarque + desembarque",566,28,leading=38)
    f.text(2386,1350,"Sem correspondência: mantém a corrida\ne deixa a descrição NULL",566,25,P["muted"],leading=34)
    f.pill(2384,1454,short(model["gold"]["nome"]),572,P["gold"],P["gold_bg"],"database",27)
    f.text(2402,1526,"IDs preservados + descrições TLC",535,25)
    f.edge("gold","daily",[(2670,1570),(2670,1620)])
    f.pill(2384,1620,short(model["gold"]["agregados"][0]["nome"]),572,P["gold"],P["gold_bg"],"database",27)
    f.text(2402,1690,"Por data/táxi: count, avg e sum\nMVs · clustering automático / por data",535,25,leading=33)
    f.nodes.append({"id":"daily","title":model['gold']['agregados'][0]['nome']})
    f.edge("silver","gold",[(2280,1550),(2360,1550)])
    f.edge("silver_dims","gold",[(2280,2050),(2320,2050),(2320,1330),(2360,1330)],label="enriquecimento",label_at=(2362,1830))

    # Controles detalhados em uma caixa sob a Gold, sem criar um gate antes dela.
    f.card("rules",2360,1900,620,330,"Regras · Silver e auditoria","shield",P["silver"],P["silver_bg"])
    f.text(2386,2004,"Obrigatórias críticas não nulas\nValor e distância ≥ 0 · passageiros 0–8/NULL\nPickup: jan–mai/2023 · dropoff ≥ pickup\nUnicidade: deduplicação por trip_id",566,25,leading=35)
    f.text(2386,2150,"Auditoria: completude, validade, unicidade,\nconsistência e janela histórica",566,24,P["purple"],leading=32)

    c.text(3120,1222,"CONSUMO",26,P["ink"],True)
    f.card("sql",3120,1280,408,485,"Consulta SQL","monitor")
    f.text(3145,1386,"analysis/\nconsultas_gold.ipynb",356,26,P["ink"],True,36)
    f.text(3145,1490,"Lê trips_consolidado\n\nMédia mensal por corrida\nYellow Taxi\n\nPassageiros/hora em maio\nYellow + Green",356,25,leading=34)
    f.edge("gold","sql",[(2980,1483),(3120,1483)])

    # Fundação comum: os nomes das tabelas no desenho omitem só o catálogo.
    c.rect(600,2310,2380,75,P["ink"],radius=12)
    f.icon("shield",619,2320,48,P["white"])
    c.text(688,2327,"Unity Catalog · case_nyc_taxi",30,P["white"],True)
    c.text(1190,2330,"landing  |  bronze  |  silver  |  gold  |  governanca     ·     Delta Lake + Parquet",27,P["white"])
    c.text(72,2343,"4 dicionários em YAML → Bronze",24,P["data"],True)

    # Leitura e legendas: contexto preciso sem multiplicar setas de controle.
    c.text(72,2470,"COMO LER AS SETAS",24,P["ink"],True)
    legend = [
        (72,"Dados", "data"), (600,"Configuração / chamada manual", "config"),
        (1430,"Dependência entre tasks", "task"), (2190,"Métricas / logs", "metric"),
        (2930,"Rejeições", "reject"),
    ]
    styles = {"data":(P["data"],False),"config":(P["config"],True),"task":(P["purple"],False),"metric":(P["purple"],True),"reject":(P["red"],False)}
    for x,label,kind in legend:
        color,dashed = styles[kind]
        f.line([(x,2542),(x+68,2542)],color,4,dashed,True)
        c.text(x+90,2523,label,26,P["ink"])
    c.text(72,2594,"A auditoria ocorre após materializar a Gold. O modo raw usa a landing existente por padrão. Todos os nomes de tabela pertencem ao catálogo case_nyc_taxi.",26,P["muted"])

    ids = {n["id"] for n in f.nodes} | {"platform", "pipeline", "ingest"}
    assert all(edge["from"] in ids and edge["to"] in ids for edge in f.edges)
    metadata = {
        "scope":"arquitetura derivada do checkout local",
        "nodes":f.nodes, "connections":f.edges,
        "sources_sha256":model["sources_sha256"],
        "source_catalog":model["catalog"],
    }
    c.save(
        "arquitetura-detalhada",
        "Arquitetura detalhada do case NYC Taxi: fluxo, ícones e conexões",
        "Fluxo implementado de GitHub e DAB ao bootstrap Databricks; Parquets Yellow/Green, CSV Taxi Zones e dicionários YAML; download, volumes, Bronze, validação privada, Silver e quarentena; cinco dimensões, seis LEFT JOINs, Gold e consultas SQL. Inclui jobs, logs, auditoria DAMA e observabilidade.",
        metadata,
    )
    print(f"Fluxo detalhado: {W}x{H}, {len(f.nodes)} blocos e {len(f.edges)} conexões; fontes verificadas no checkout.")
