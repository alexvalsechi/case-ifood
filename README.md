# Case iFood — Lakehouse NYC Taxi (medallion + DAMA-DMBOK)

Solução do case técnico iFood (Data Architect) na Databricks: ingestão orientada a **contratos de dados** e pipeline lakehouse medallion **bronze → silver → gold** no Unity Catalog, com motores de qualidade, observabilidade e segurança de metadados.

## Diagrama da arquitetura implementada

![Arquitetura do projeto: contratos, ingestão, bronze, silver, gold, log de ingestão, qualidade e limites de observabilidade](docs/arquitetura/arquitetura-projeto.png)

[Imagem PNG em alta resolução](docs/arquitetura/arquitetura-projeto.png) · [SVG editável](docs/arquitetura/arquitetura-projeto.svg) · [Gerador do diagrama](docs/arquitetura/gerar_diagrama.py)

O diagrama foi reconstruído a partir do código e dos contratos deste checkout. As setas distinguem dados, configuração por YAML e log/auditoria. Contornos tracejados identificam capacidades apenas declaradas ou com implementação parcial. A execução e a configuração do workspace Databricks não foram verificadas.

### Evidências no repositório

| Etapa | Implementação e contrato que a orienta |
| --- | --- |
| Seleção e despacho da ingestão | [`01_ingestao/ingestao_parametrizada.ipynb`](01_ingestao/ingestao_parametrizada.ipynb), células 0 e 1: `carregar_contrato(f"{taxi_type}_trips")`, widgets `taxi_type`, `catalogo`, `ano`, `meses`, `origem`; despacho para `baixar_da_fonte()` ou `executar_ingestao()`. As células são numeradas a partir de zero. |
| Fonte HTTP → landing | [`00_config/motores.py`](00_config/motores.py), `baixar_da_fonte()`: `fonte.url_template` → download HTTP em partes, até 3 tentativas e arquivo temporário → `fonte.volume`. Arquivo existente e não vazio é pulado. O checksum MD5 cobre somente os primeiros 64 MiB. |
| Caminho `raw` | `executar_ingestao()` aceita volumes de origem e destino, mas o notebook não os fornece: ambos assumem `fonte.volume`. Portanto, a chamada padrão reconhece arquivos já presentes na landing e registra `ja_existia`; não estabelece uma segunda zona raw. O critério real de skip é destino existente com tamanho maior que zero. |
| Contratos dos datasets | [`contracts/green_trips.yml`](contracts/green_trips.yml) e [`contracts/yellow_trips.yml`](contracts/yellow_trips.yml): fonte, glob, schema canônico, nomes de destino, chave de deduplicação, mapeamento e regras. O pipeline tem seu próprio carregador `_carregar_contratos()`; o motor de ingestão e o notebook de qualidade usam os carregadores de `motores.py`. |
| Landing → bronze | [`02_pipelines/pipeline_case_ifood.py`](02_pipelines/pipeline_case_ifood.py): `dp.create_streaming_table()` + `_criar_fluxo_arquivo()` com `@dp.append_flow(once=True)`, um fluxo por arquivo encontrado pelo glob durante a construção do pipeline. A leitura de cada Parquet é batch via `spark.read`; aplica casts para `schema_bronze`, normalização de caixa, NULL para colunas ausentes e adiciona `_arquivo_origem` e `_ingestao_ts`. |
| Bronze → silver e quarentena | `_criar_silver_e_quarentena()` cria uma MV privada de validação e duas saídas públicas. Calcula `trip_id = xxhash64(...)` sobre a chave original, aplica `mapeamento_silver` e escolhe deterministicamente uma linha válida por `trip_id`. |
| Qualidade durante a transformação | `_expectations_do_contrato()` aplica `expect_all` na MV privada para manter as métricas de violações no event log; as mesmas regras classificam as linhas em `motivos_rejeicao`. Silver recebe só linhas válidas. Registros inválidos e duplicatas válidas excedentes vão para `case_ifood.governanca.quarantine_<dataset>`, com colunas originais prefixadas `_orig_`, campos mapeados, `_arquivo_origem`, `_ingestao_ts`, `taxi_type`, `trip_id` e `quarentena_ts`. `passenger_count` nulo é WARN e continua na Silver, não na quarentena. |
| Dimensões de referência | [`contracts/references/dimensoes_tlc.yml`](contracts/references/dimensoes_tlc.yml) declara os dicionários versionados (vendor, tarifa, pagamento e despacho) e a Taxi Zone Lookup oficial. `01_ingestao/atualizar_taxi_zone_lookup.py` baixa o CSV para `/Volumes/case_ifood/landing/reference/`; o pipeline publica dimensões Bronze e Silver. |
| Silver → gold | [`contracts/gold/trips.yml`](contracts/gold/trips.yml) → `_carregar_contratos_gold()` valida membros, dimensões e funções; `_criar_gold_consolidado()` lê green e yellow, aplica `unionByName(allowMissingColumns=True)` e executa os `LEFT JOINs` declarados. Gera `case_ifood.gold.trips_consolidado` com IDs originais mais nomes de vendor, tarifa, pagamento, despacho e zonas de embarque/desembarque. Sem correspondência, a linha permanece e a descrição fica nula. |
| Agregado diário | `_expressao_dimensao()`, `_expressao_medida()` e `_criar_gold_agregado()` geram `case_ifood.gold.trips_diario` a partir do consolidado: `to_date(pickup_datetime)` como `data_corrida`, `taxi_type`, `count(*)`, `avg(total_amount)`, `avg(passenger_count)` e `sum(trip_distance)`. Clustering por `data_corrida`. O alias `soma_distancia_km` é literal do contrato; não existe conversão de unidade no código. |
| Log operacional | Os dois motores de ingestão fazem append em `case_ifood.governanca.log_ingestao`: dataset, arquivo, fonte, tamanho, checksum, status, timestamp e mensagem. `criar_tabelas_governanca()` cria as tabelas Delta de governança. |
| Auditoria posterior de qualidade | [`03_qualidade/qualidade_contratos.ipynb`](03_qualidade/qualidade_contratos.ipynb), células 1 e 2: `listar_contratos()` → leitura das tabelas silver → completude, validade, unicidade, consistência e pontualidade. `_registrar()` e `_exemplos_violacoes()` produzem métricas PASS/FAIL e até 5 exemplos JSON para regras com falha, gravados em append em `case_ifood.governanca.dq_resultados`. Qualquer FAIL lança exceção depois da persistência. |
| Observabilidade e segurança | `criar_tabelas_governanca()` contém o DDL de `case_ifood.governanca.obs_metricas`, mas nenhum escritor/coletor foi localizado. `owner`, `dominio`, `colunas_pii` e `tags` estão nos contratos; não foi localizada aplicação de GRANTs ou tags neste checkout. |

### Limites e diferenças em relação à documentação

- O Job `case_ifood_ingestao_orquestrador` e a sequência ingestão → pipeline → qualidade → observabilidade → segurança são descritos abaixo, mas a definição do Job não está versionada. Os diretórios `04_observabilidade`, `05_seguranca` e `06_analises` citados na estrutura também não estão presentes neste checkout. A imagem distingue essas referências da implementação verificável.
- Os glob dos contratos selecionam arquivos de **2023** e a validade do pickup está fixada em **2023-01-01 ≤ pickup < 2023-06-01**. Alterar `ano` ou `meses` no notebook de ingestão não modifica essas regras do pipeline. A dimensão de pontualidade testa essa janela histórica, não freshness operacional.
- A auditoria lê a **silver após a quarentena**; PASS não inclui a quantidade de rejeições, que deve ser consultada em `case_ifood.governanca.quarantine_<dataset>` ou nas métricas do event log. Não existe regra explícita de volume mínimo e o percentual de nulos é zero quando a tabela está vazia. Filtros SQL `NOT(expressão)` não contam resultados NULL como violações; completude é avaliada separadamente para as colunas configuradas.
- `_arquivo_origem` e `_ingestao_ts` não são selecionados pela silver e não chegam à gold; linhas rejeitadas os preservam na quarentena. Linhas conformes ainda não carregam essa linhagem até o consumo, e a execução não comprova a configuração de lineage nativa no workspace.
- No arquivo de qualidade versionado, a célula 0 contém texto Markdown, mas está marcada como `code`, sem `%md`. Esse conteúdo não é Python válido. A imagem representa a lógica das células 1 e 2; a execução integral desse notebook, como exportado, requer corrigir o tipo da célula inicial. Nenhum notebook foi alterado para produzir o diagrama.

Para reproduzir a imagem localmente, com **Pillow** e **PyYAML** instalados:

```powershell
python docs/arquitetura/gerar_diagrama.py
```

O gerador produz o PNG de **3600 × 2740**, o SVG com texto editável e uma prévia. Os arquivos finais incluem hashes SHA-256 das fontes em seus metadados para identificar a base usada na geração. A renderização utiliza fontes locais (Segoe UI/Consolas no Windows, com fallback DejaVu no Linux).

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
| `case_ifood.governanca` | Qualidade (`dq_resultados`), observabilidade (`obs_metricas`), log de ingestão (`log_ingestao`) e quarentena (`quarantine_yellow_trips`, `quarantine_green_trips`) |

## Modelo de dados (medallion)

- **bronze.yellow_trips / bronze.green_trips** — streaming tables alimentadas pelo volume landing, com colunas de metadados (`_arquivo_origem`, `_ingestao_timestamp`, `_taxi_type`).
- **silver.yellow_trips / silver.green_trips** — apenas registros válidos pelas regras DROP do contrato, deduplicados pela chave e mapeados para nomes padronizados. `passenger_count` nulo é aviso e permanece.
- **governanca.quarantine_yellow_trips / quarantine_green_trips** — linhas que falham regras DROP e duplicatas removidas da Silver, com motivo(s), colunas da Bronze prefixadas `_orig_`, arquivo de origem, timestamp de ingestão e timestamp de quarentena. Reprocessamento deve partir da Bronze após a correção da regra ou do dado.
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
