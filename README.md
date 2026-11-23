# Case iFood — Lakehouse NYC Taxi (medallion + DAMA-DMBOK)

Solução do case técnico iFood (Data Architect) na Databricks: ingestão orientada a **contratos de dados** e pipeline lakehouse medallion **bronze → silver → gold** no Unity Catalog, com motores de qualidade, observabilidade e segurança de metadados.

## Estrutura

```text
case_ifood/
├── 00_config/            # Constantes compartilhadas e motores genéricos
│   └── motores.py        # Motor de ingestão genérico orientado a contrato
├── 01_ingestao/          # Notebook parametrizado de ingestão (wrapper do motor)
├── 02_pipelines/         # Pipeline SDP medallion (lê os contratos)
│   └── pipeline_case_ifood.py
├── 03_qualidade/         # Motor de qualidade (dimensões DAMA-DMBOK)
├── 04_observabilidade/   # Motor de observabilidade (freshness, contagens, event log)
├── 05_seguranca/         # Grants Unity Catalog + tags de metadados
├── 06_analises/          # Respostas às perguntas do case
├── contracts/            # Contratos de dados (YAML) — fonte única da verdade
│   └── gold/            # Contratos de gold — unificações e visões de consumo
├── SDD.md                # Guia de contratos para IA — LEITURA OBRIGATÓRIA
└── README.md
```

## Unity Catalog

| Objeto | Papel |
| --- | --- |
| `case_ifood.landing.files` (volume) | Landing zone — arquivos parquet raw |
| `case_ifood.bronze` | Dados brutos como chegaram + metadados de ingestão |
| `case_ifood.silver` | Dados limpos, tipados, deduplicados, colunas obrigatórias |
| `case_ifood.gold` | Camada de consumo (SQL/BI) |
| `case_ifood.governanca` | Qualidade (`dq_resultados`), observabilidade (`obs_metricas`), log de ingestão (`log_ingestao`) |

## Modelo de dados (medallion)

- **bronze.yellow_trips / bronze.green_trips** — streaming tables alimentadas pelo volume landing, com colunas de metadados (`_arquivo_origem`, `_ingestao_timestamp`, `_taxi_type`).
- **silver.yellow_trips / silver.green_trips** — dedupe pela chave do contrato, tipos corretos, colunas obrigatórias do case: `VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime` (green: equivalentes `lpep_*`).
- **gold.trips_consolidado** — união yellow + green com `taxi_type` (consumo analítico) e **gold.trips_diario** — agregado diário.

## Dicionário de colunas (camada de consumo)

| Coluna | Descrição |
| --- | --- |
| `vendor_id` | ID do fornecedor de tecnologia do taxímetro |
| `passenger_count` | Nº de passageiros informado pelo motorista |
| `total_amount` | Valor total cobrado (tarifa + taxas + gorjeta + pedágio) |
| `pickup_datetime` / `dropoff_datetime` | Início e fim da corrida |
| `taxi_type` | yellow ou green |
| `trip_id` | Chave de dedupe (hash determinística da corrida) |

## Contratos de dados

Cada dataset tem um YAML em `contracts/` (fonte, destino, grain, chave de dedupe, schema esperado, regras de qualidade por dimensão DAMA, owner, SLA de freshness, PII). Ele alimenta **todos** os motores: ingestão, SDP, qualidade, observabilidade e segurança. Visões de consumo (gold) são declaradas em `contracts/gold/`: membros a unificar + agregados derivados.

> 📖 **Leitura obrigatória (IA e onboarding):** `SDD.md` — dicionário campo a campo dos contratos de ingestão e de gold, árvore de decisão para novas visões, protocolo de interação e fluxo de execução (YAML → dry run → update → verificação). Nenhuma visão/dataset novo se resolve editando o código do pipeline.

### Runbook: onboarding de um novo dataset

1. Copie um contrato em `contracts/` e ajuste fonte, destino, schema e regras.
2. Coloque os parquets no volume landing (o motor de ingestão aceita qualquer parquet em volume raw).
3. Rode o notebook `01_ingestao` com `taxi_type=<novo>`.
4. Rode um update da pipeline SDP — as tabelas são geradas a partir dos contratos.
5. Nenhuma linha de código específica de dataset precisa ser escrita.

## Orquestração

Lakeflow Job `case_ifood_ingestao_orquestrador`: ingestão → pipeline update → qualidade → observabilidade → segurança, com job parameters `taxi_type`, `ano`, `meses`.

## Análises (entregáveis do case)

- `06_analises/analise_media_total_amount_mes` — média de `total_amount` por mês, yellow.
- `06_analises/analise_media_passageiros_hora_maio` — média de `passenger_count` por hora do dia em maio, todos os táxis.
