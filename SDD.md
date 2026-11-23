# SDD — Case iFood: guia de contratos YAML para IA

> **LEITURA OBRIGATÓRIA.** Qualquer tarefa que envolva a pasta `case_ifood/` — criar, editar ou responder perguntas sobre datasets, contratos, pipeline, visões gold ou ingestão — começa lendo este documento até o fim. Ele define como o projeto é operado: **nada se resolve editando código do pipeline; tudo se resolve criando ou editando contratos YAML.**

## 1. Arquitetura em uma página

```text
contracts/yellow_trips.yml, green_trips.yml  (contratos de INGESTÃO)
        │  lidos pelo pipeline SDP 02_pipelines/pipeline_case_ifood.py
        ▼
case_ifood.bronze.<dataset>   →  dados brutos + metadados (_arquivo_origem, _ingestao_ts)
        ▼
case_ifood.silver.<dataset>  →  limpos, tipados, deduplicados (trip_id), expectations
        ▼  ┌─────────────────────────────────────────────────────────┐
contracts/gold/trips.yml  (contrato de GOLD)  ── lê os silvers ──▶│ unificação (membros)     │
        │                                                        │ + agregados declarativos │
        ▼                                                        └─────────────────────────┘
case_ifood.gold.<visao>    →  camada de consumo (SQL/BI)
```

**Regra de ouro:** o pipeline `02_pipelines/pipeline_case_ifood.py` não contém nenhuma lógica específica de dataset. No startup ele lê todos os YAML de `contracts/` (ingestão) e `contracts/gold/` (gold) e **gera** as tabelas. Portanto:

* Novo dataset = novo YAML em `contracts/`.
* Nova visão de consumo = item novo em `agregados` (ou novo YAML) em `contracts/gold/`.
* Editar o `.py` é reservado a mudanças de comportamento **genérico** do motor (raro) — nunca para atender um dataset ou visão específica.

## 2. Contrato de ingestão (`contracts/*.yml`)

Referências vivas: `contracts/yellow_trips.yml` e `contracts/green_trips.yml`. Um arquivo por dataset, nome do arquivo = nome do dataset.

| Campo | Significado | Regras |
| --- | --- | --- |
| `dataset` | Identificador único do dataset (snake_case) | É a chave usada no campo `membros` dos contratos de gold |
| `taxi_type` | Rótulo gravado na coluna `taxi_type` da silver | Literal (ex.: `yellow`, `green`) |
| `dominio` / `owner` / `descricao` | Governança e contexto | Declare o grain na descrição (ex.: "uma linha por corrida") |
| `fonte.volume` | Volume UC de landing | Hoje `/Volumes/case_ifood/landing/files` |
| `fonte.glob` | Padrão de arquivos a ingerir | Ex.: `yellow_tripdata_2023-*.parquet`; um append flow por arquivo casado |
| `fonte.formato` | Formato de leitura | `parquet` (único suportado no motor atual) |
| `destinos.bronze` / `destinos.silver` / `destinos.quarentena` | Nomes completos das tabelas geradas | `case_ifood.bronze.<dataset>` / `case_ifood.silver.<dataset>` / `case_ifood.governanca.quarantine_<dataset>` |
| `chave_dedupe_origem` | Colunas ORIGINAIS (nomes da bronze) que formam a chave de dedupe | Viram o hash `trip_id` (xxhash64) na silver |
| `mapeamento_silver` | Dicionário `nome_silver -> coluna_bronze` (ou lista, para coalesce) | Colunas fora do mapa são descartadas; nomes contendo `datetime` recebem cast automático para timestamp |
| `schema_bronze` | Dicionário `coluna -> tipo Spark` | Cada arquivo é lido com o próprio schema físico e normalizado via cast (int/bigint/double e capitalização variam entre arquivos mensais) |
| `colunas_obrigatorias` | Colunas de consumo exigidas | `IS NOT NULL` é avaliado como regra de rejeição e enviado à quarentena, exceto `passenger_count` (warn, mantido) |
| `regras_qualidade.validade` | Lista `{coluna, expressao}` | Expressões SQL avaliadas sobre nomes **SILVER** (pós-mapeamento); falha = linha desviada para `destinos.quarentena` e mantida na bronze |
| `regras_qualidade.consistencia` | Lista de expressões | Idem validade (ex.: `dropoff_datetime >= pickup_datetime`); motivo é registrado em `motivos_rejeicao` |
| `regras_qualidade.completude` / `pontualidade` / `unicidade` | Documentação DAMA-DMBOK | Completude e pontualidade são auditadas depois; a unicidade é aplicada na Silver por `trip_id`, com as duplicatas excedentes enviadas à quarentena |
| `colunas_pii` / `tags` | Segurança e metadados | Alimentam os motores de segurança/governança |

## 3. Contrato de gold (`contracts/gold/*.yml`)

Referência viva: `contracts/gold/trips.yml`. Cada YAML declara **uma unificação** (o consolidado) e **N agregados** derivados dela. Cada agregado lê SEMPRE o consolidado do próprio contrato.

| Campo | Significado | Regras |
| --- | --- | --- |
| `nome` | Nome completo do consolidado | `case_ifood.gold.<nome>`; deve ser único entre todas as tabelas do projeto |
| `descricao` | Contexto da visão | Vira o comentário da tabela (prefixado `[gold] `) |
| `membros` | Lista de `dataset` de ingestão que compõem o consolidado | **A ordem importa**: o primeiro membro define o layout de colunas; os seguintes apenas acrescentam colunas próprias (`unionByName` com colunas ausentes). Membro inexistente = pipeline falha na carga com erro claro |
| `cluster_by_auto` | `true` habilita liquid clustering automático no consolidado | Alternativa: `cluster_by: [<colunas>]` |
| `agregados[].nome` | Nome completo da MV agregada | `case_ifood.gold.<nome>`; único no projeto |
| `agregados[].descricao` | Contexto | Vira comentário da tabela |
| `agregados[].agrupar_por` | Dimensões: `{coluna, alias?, transformacao?}` (ou string simples) | `transformacao` suportada: `to_date` (gera dimensão de data a partir de timestamp) |
| `agregados[].medidas` | `{alias, funcao, coluna}` | `funcao` ∈ `count`, `count_distinct`, `sum`, `avg`, `min`, `max` (fora disso = pipeline falha na carga). `coluna: "*"` só com `count` (equivale a `count(*)`) |
| `agregados[].cluster_by` | Colunas/alias de clusterização | Escolher dimensões usadas em filtros/joins |

## 4. Qual caminho seguir? (árvore de decisão)

Diante de "quero uma visão / tabela nova na gold":

```text
O dado já existe em algum silver (contrato de ingestão já criado)?
├── NÃO  → criar contrato de INGESTÃO em contracts/ (seção 2).
│         Depois, se quiser consumo: ir para o ramo SIM.
└── SIM  → A visão agrega dados JÁ unificados por um contrato de gold existente?
          ├── SIM  → EDITAR o contrato existente: adicionar um item em `agregados`
          │         (não cria arquivo novo; ex.: agrupar corridas por localidade
          │          = novo agregado em contracts/gold/trips.yml).
          └── NÃO  → CRIAR novo YAML em contracts/gold/ (unificação nova:
                    outros membros, grain diferente, ex.: pontos de táxi).
                    Um consolidado por grain — nunca misturar grains no
                                       mesmo consolidado.
```

Restrições em todos os caminhos:

* `membros` precisam existir como `dataset` de ingestão (validado na carga).
* Função fora da whitelist falha a carga do pipeline com mensagem explícita.
* **Nunca renomear/mover tabela gold existente sem checar a lineage antes** — `case_ifood.gold.trips_consolidado` tem consumidor downstream (notebook) que lê pelo nome.

## 5. Protocolo de interação com o usuário

Pedidos de visão costumam vir vagos ("quero agrupar corridas por localidade"). Antes de escrever qualquer YAML, perguntar **nesta ordem** (combinar o que já puder inferir; sugerir default, não fazer interrogatório):

1. **Nome da tabela gold** — sugerir a convenção `case_ifood.gold.<sujeito>_<granularidade>` (ex.: `corridas_por_localidade`).
2. **Dimensões de agrupamento** — listar as colunas silver relevantes ao pedido (ex.: para "localidade": `pickup_location_id`, `dropoff_location_id`; para "tipo": `taxi_type`, `vendor_id`).
3. **Medidas** — quais métricas (contagem, médias, somas) e com qual função da whitelist; sugerir aliases em PT.
4. **Dimensão temporal?** — se precisa de quebra por dia (`transformacao: to_date` sobre `pickup_datetime`).
5. **Clusterização** — sugerir as dimensões de maior uso em filtros.

Depois: montar o YAML, **mostrar ao usuário e confirmar**, e só então executar o fluxo da seção 6. Nunca presumir medida que o usuário não confirmou (ex.: "total" pede SUM, "média" pede AVG — a função vem da palavra do usuário, não do nome da coluna).

## 6. Fluxo de execução obrigatório

1. Editar/criar o YAML do contrato (e nada mais).
2. Confirmar o contrato com o usuário.
3. **Dry run** do pipeline SDP `feda0232-181f-4451-b8d2-2f9aa024dd51` — validar antes de materializar.
4. **Update** do pipeline (`fullRefresh: false` — update normal; **jamais** full refresh sem pedido explícito do usuário).
5. **Verificar**: `COUNT(*)` da tabela nova (não pode ser 0 nem vazio de sentido), cruzar com a fonte (ex.: soma de `qtd_*` do agregado = contagem do consolidado), e conferir o schema gerado (`DESCRIBE TABLE`).
6. Reportar ao usuário com números, não apenas "concluído".

## 7. Gotchas do projeto

* **Ordem de `membros` define o schema do consolidado**: hoje `[green_trips, yellow_trips]` — o layout segue o green e `airport_fee` fica por último. Mudar a ordem muda a ordem (não o conteúdo) das colunas.
* `passenger_count` é nulo em ~2,7% das corridas (não registrado pelos motoristas) — por isso é expectation **warn**, não drop. Agregações de média o ignoram (comportamento SQL padrão).
* Janela de validade SLA: corridas fora de `2023-01-01` a `2023-06-01` vão para `destinos.quarentena` e permanecem na bronze — regra de `pickup_datetime` nos contratos.
* **Expressões de `regras_qualidade` usam nomes SILVER** (pós-`mapeamento_silver`), nunca os nomes originais da fonte (ex.: `tpep_pickup_datetime` vira `pickup_datetime`).
* Agregados leem apenas o consolidado do próprio contrato — não apontam para silver, nem para consolidado de outro contrato.
* `mapeamento_silver` com lista = coalesce na ordem (usado para variantes de capitalização na bronze, ex.: `Airport_fee`/`airport_fee`).
* Uma MV privada avalia `expect_all` para conservar métricas de regras no event log e adiciona `motivos_rejeicao`. Linhas rejeitadas e duplicatas válidas excedentes por `trip_id` são materializadas em `destinos.quarentena`; inclui campos Silver mapeados, campos originais da Bronze prefixados `_orig_`, `_arquivo_origem`, `_ingestao_ts` e `quarentena_ts`. WARNs, como `passenger_count` nulo, não são rejeitados. Para reprocessar após corrigir uma regra, atualize/reexecute o pipeline a partir da Bronze.

## 8. Exemplo ponta a ponta — "visão que agrupa corridas por localidade"

Diálogo esperado: IA pergunta nome (`corridas_por_localidade` sugerido), dimensão (`pickup_location_id`; usuário pode pedir também `taxi_type`), medidas (`qtd_corridas` count, `media_total_amount` avg) e clusterização (`pickup_location_id`). Como `trips` já unifica green+yellow, o caminho é **adicionar um agregado ao contrato existente** `contracts/gold/trips.yml`:

```yaml
# (item adicionado à lista `agregados` existente em contracts/gold/trips.yml —
#  o agregado trips_diario permanece intocado)
  - nome: case_ifood.gold.corridas_por_localidade
    descricao: >
      Corridas e faturamento médio por localidade de embarque e tipo de táxi.
    agrupar_por:
      - coluna: pickup_location_id
        alias: localidade_embarque
      - coluna: taxi_type
    medidas:
      - alias: qtd_corridas
        funcao: count
        coluna: "*"
      - alias: media_total_amount
        funcao: avg
        coluna: total_amount
    cluster_by: [localidade_embarque]
```

Seguem-se os passos da seção 6: dry run → update → verificação (`SUM(qtd_corridas)` deve fechar com `COUNT(*)` de `trips_consolidado`).
