# Case iFood — Lakehouse NYC Taxi

Pipeline Databricks para corridas **Yellow e Green Taxi**, com contratos de dados em YAML, arquitetura **Bronze → Silver → Gold**, dimensões TLC e auditoria de qualidade baseada no DAMA-DMBOK. O recorte do case é **janeiro a maio de 2023**.

[Visão geral](#1-arquitetura-visão-geral) · [Como executar](#2-como-configurar-e-iniciar-o-ambiente) · [Arquitetura detalhada](#3-arquitetura-detalhada) · [Testes e qualidade](#4-testes-e-qualidade) · [Operação](#5-operação-e-evolução)

## 1. Arquitetura: visão geral

Esta visão acompanha o caminho da configuração do ambiente até a consulta dos dados. Os detalhes de implementação e as validações ficam na [visão detalhada](#3-arquitetura-detalhada).

![Visão geral: deploy pelo DAB, bootstrap do ambiente, origens TLC, transformação Bronze–Silver–Gold e consultas SQL](docs/arquitetura/arquitetura-visao-geral.png)

[Abrir em alta resolução](docs/arquitetura/arquitetura-visao-geral.png) · [SVG](docs/arquitetura/arquitetura-visao-geral.svg)

O **Databricks Asset Bundle (DAB)** publica os arquivos, a pipeline e os dois jobs no workspace. Depois do deploy, o **bootstrap** cria os objetos do Unity Catalog e baixa os arquivos oficiais da TLC. A **pipeline** lê a landing, normaliza a Bronze, valida e deduplica a Silver, e gera a Gold enriquecida para consulta SQL.

| Quando executar | Recurso | Responsabilidade |
| --- | --- | --- |
| Preparação inicial ou reposição de arquivos | `case_ifood_bootstrap` | Criar catálogo, schemas, volumes e tabelas de governança; baixar Parquets e o lookup de zonas. |
| Processamento dos arquivos disponíveis | `case_ifood_update_e_qualidade` | Executar `pipeline_update → qualidade_contratos → observabilidade_contratos`. |
| Consumo | `06_analises/consultas_gold.ipynb` | Consultar a Gold usando SQL. Executado separadamente dos jobs. |

Os contratos definem fontes, schemas, destinos, regras, dimensões e agregados. A qualidade é aplicada durante a transformação para a Silver e auditada novamente após o update da pipeline. A auditoria posterior pode falhar o job, mas a Gold já foi materializada nessa etapa.

## 2. Como configurar e iniciar o ambiente

**Sequência de primeira execução:** clonar → autenticar → configurar o checkout → validar e fazer deploy do DAB → executar bootstrap → validar a pipeline → executar o job operacional → consultar a Gold.

O provisionamento descrito aqui cria **objetos dentro de um workspace Databricks existente**. Workspace, metastore, armazenamento gerenciado e acesso a compute devem estar disponíveis antes do bootstrap.

### 2.1. Pré-requisitos

| Requisito | O que precisa estar disponível |
| --- | --- |
| Ferramentas locais | Git e [Databricks CLI com suporte a bundles](https://docs.databricks.com/aws/en/dev-tools/cli/install). Python 3.12 é usado pelo CI e é opcional para os testes locais. |
| Workspace | Unity Catalog habilitado e armazenamento gerenciado configurado para criar catálogo/volumes. Também é possível usar um catálogo `case_ifood` previamente criado pelo administrador. |
| Permissões | Criar/usar catálogo, schemas, volumes e tabelas; publicar/executar jobs e pipeline; consultar o event log da pipeline. |
| Compute e dependências | Pipeline serverless disponível. Definir compute para as tasks de notebook e disponibilizar `PyYAML`, `requests` e `pandas` nesse ambiente. |
| Rede | Acesso HTTPS a `d37ci6vzurychx.cloudfront.net`, origem dos Parquets e do CSV de Taxi Zones. |

### 2.2. Clonar e autenticar

Execute em um terminal local. Os exemplos de shell abaixo usam PowerShell.

```powershell
git clone https://github.com/alexvalsechi/case-ifood.git
cd case-ifood

databricks --version
databricks auth login --host https://<seu-workspace> --profile IFOOD_DEV
```

Conclua o login no navegador. No target `dev` de [`databricks.yml`](databricks.yml), adicione `workspace` ao bloco existente, preservando `mode`, `default` e `resources`:

```yaml
targets:
  dev:
    # Manter as configurações existentes deste target.
    workspace:
      host: https://<seu-workspace>
      profile: IFOOD_DEV
```

O perfil autentica a CLI no workspace escolhido. Referência: [autenticação de bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/authentication).

### 2.3. Configurar o checkout para o seu workspace

Há ajustes necessários na versão atual antes de reproduzir a execução em outro workspace. Os diagramas representam o fluxo após essa configuração; a execução remota não foi verificada nesta atualização da documentação.

| Onde configurar | Ajuste antes do primeiro deploy |
| --- | --- |
| Caminhos locais nos YAML de [`resources/`](resources/) | Os caminhos de código devem ser relativos ao YAML que os declara. Na biblioteca da pipeline, usar `../02_pipelines/pipeline_case_ifood.py`. Nas tasks de notebook, usar `../01_ingestao/bootstrap_ambiente.ipynb`, `../03_qualidade/qualidade_contratos.ipynb` e `../04_observabilidade/observabilidade_contratos.ipynb`, respectivamente. |
| Imports nos notebooks de bootstrap, ingestão, qualidade e observabilidade | Substituir o caminho fixo `/Workspace/Repos/alexvalsechibosque@gmail.com/case-ifood` pela raiz da cópia que será executada no workspace. Para execução via DAB, usar a raiz de arquivos publicada pelo bundle, de forma que `00_config`, `01_ingestao` e `contracts` pertençam ao mesmo deploy. |
| [`03_qualidade/qualidade_contratos.ipynb`](03_qualidade/qualidade_contratos.ipynb), primeira célula | Alterar o tipo da célula de `code` para `markdown`. Ela contém a apresentação do notebook e atualmente não é Python válido. |
| Tasks de notebook nos dois jobs | Configurar compute compatível com o workspace. Os YAML atuais não declaram cluster nem ambiente serverless para essas tasks; `serverless: true` está declarado na pipeline. |
| Catálogo e período | Manter `case_ifood`, ano `2023` e meses `01,02,03,04,05` para reproduzir o case. Contratos, lookup e resources contêm nomes/caminhos desse catálogo; mudar apenas o widget `catalogo` não remapeia a solução inteira. |

Exemplo de raiz de arquivos de um deploy: `/Workspace/Users/<seu-usuario>/.bundle/case-ifood/dev/files`. Confirme o caminho resolvido para o seu target; não use o diretório local do clone nos imports Databricks. No bootstrap, `REPO` deve apontar para essa raiz; nos demais notebooks, os imports devem apontar para `00_config` dentro dela. Faça os ajustes nas fontes locais, pois edições apenas nos arquivos publicados são sobrescritas no próximo deploy.

O motor [`motores.py`](00_config/motores.py) já encontra `contracts/` em relação à própria localização, e a pipeline recebe `${workspace.file_path}/contracts` pelo bundle. Para caminhos relativos, consulte o [exemplo oficial de migração para DAB](https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources).

> Esta atualização do README documenta os ajustes necessários; não modifica os notebooks nem os resources.

### 2.4. Validar e publicar os recursos

Na raiz do clone, depois dos ajustes anteriores:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle summary -t dev
```

O deploy publica os arquivos e cria/atualiza estes recursos:

| Tipo | Chave do recurso |
| --- | --- |
| Job de preparação | `case_ifood_bootstrap` |
| Pipeline medallion | `case_ifood_medallion` |
| Job operacional | `case_ifood_update_e_qualidade` |

O deploy configura os recursos; a criação dos objetos UC e o download dos dados acontecem ao executar o bootstrap.

### 2.5. Provisionar os objetos e baixar as origens

```powershell
databricks bundle run -t dev case_ifood_bootstrap
```

O job executa [`01_ingestao/bootstrap_ambiente.ipynb`](01_ingestao/bootstrap_ambiente.ipynb), com os parâmetros definidos em [`case_ifood_bootstrap.job.yml`](resources/case_ifood_bootstrap.job.yml):

| Parâmetro | Valor do case |
| --- | --- |
| `catalogo` | `case_ifood` |
| `ano` | `2023` |
| `meses` | `01,02,03,04,05` |

Ao terminar, conferir no Catalog Explorer:

- Catálogo `case_ifood` e schemas `landing`, `bronze`, `silver`, `gold`, `governanca`.
- Volumes `case_ifood.landing.files` e `case_ifood.landing.reference`.
- **Dez Parquets** esperados na landing: cinco meses de Yellow e cinco de Green.
- `taxi_zone_lookup.csv` no volume de referência, com snapshot datado e manifesto SHA-256.
- Tabelas `log_ingestao`, `dq_resultados` e `obs_metricas` no schema `governanca`.

O bootstrap usa `CREATE ... IF NOT EXISTS` e pula downloads quando o arquivo de destino já existe e tem tamanho maior que zero. O lookup é atualizado com um snapshot por data. Confira os logs: a checagem final de quantidade de Parquets apenas emite aviso quando faltam arquivos.

### 2.6. Validar e processar a pipeline

Primeiro, validar o grafo sem solicitar full refresh; depois, executar o job completo:

```powershell
databricks bundle run -t dev --validate-only case_ifood_medallion
databricks bundle run -t dev case_ifood_update_e_qualidade
```

A validação da pipeline verifica o grafo; os resultados de transformação e das auditorias são conferidos na execução. O job segue a dependência:

```text
pipeline_update             qualidade_contratos          observabilidade_contratos
Bronze → Silver → Gold  →   auditoria da Silver      →   contagens, freshness e event log
                            FAIL encerra esta sequência
```

A observabilidade só é iniciada se a task de qualidade for bem-sucedida. O ID da pipeline é passado à observabilidade pelo DAB. Referência dos comandos: [Databricks CLI — bundle](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands).

### 2.7. Conferir os resultados e consultar a Gold

No SQL Editor ou em um notebook SQL com acesso ao catálogo:

```sql
-- Conferir se as duas frotas chegaram à camada de consumo.
SELECT taxi_type, COUNT(*) AS qtd_corridas
FROM case_ifood.gold.trips_consolidado
GROUP BY taxi_type;

-- Conferir o agregado diário.
SELECT *
FROM case_ifood.gold.trips_diario
ORDER BY data_corrida, taxi_type
LIMIT 10;

-- Inspecionar as auditorias e métricas mais recentes.
SELECT * FROM case_ifood.governanca.dq_resultados
ORDER BY avaliado_em DESC LIMIT 24;

SELECT * FROM case_ifood.governanca.obs_metricas
ORDER BY coletado_em DESC LIMIT 30;
```

Como reconciliação adicional, compare `COUNT(*)` do consolidado com `SUM(qtd_corridas)` do agregado diário. Essa conferência é manual; não existe um gate de reconciliação implementado no job atual.

O notebook [`06_analises/consultas_gold.ipynb`](06_analises/consultas_gold.ipynb) contém as consultas do case: média de `total_amount` por corrida em cada mês para Yellow e média de `passenger_count` por hora do dia em maio para a frota toda. Ele consulta `gold.trips_consolidado` e não faz parte dos jobs.

## 3. Arquitetura detalhada

Esta visão mostra a responsabilidade das etapas e onde os controles são aplicados. O fluxo de dados e a sequência de execução são apresentados separadamente para facilitar a leitura.

![Arquitetura detalhada com contratos, preparação das origens, transformações, dimensões, quarentena, auditoria e observabilidade](docs/arquitetura/arquitetura-detalhada.png)

[Abrir em alta resolução](docs/arquitetura/arquitetura-detalhada.png) · [SVG](docs/arquitetura/arquitetura-detalhada.svg) · [Gerador das duas visões](docs/arquitetura/gerar_arquiteturas.py)

<details>
<summary>Responsabilidade e validação por etapa — descrição textual</summary>

| Etapa | O que faz e por que existe | Controles e resultados |
| --- | --- | --- |
| **CI e DAB** | Versionar a configuração e publicar jobs, pipeline e arquivos de forma repetível. | GitHub Actions executa os testes Python; validação DAB depende dos secrets configurados. Deploy é manual. Targets `dev` e `prod` alteram o modo de implantação e a propriedade `development` da pipeline. |
| **Contratos YAML** | Concentrar fonte, schema, mapeamento, chave, regras e modelos de consumo fora do código genérico. | O carregador Gold valida membros, referências de dimensões e funções de agregação suportadas. Os testes verificam nomes, destinos e estrutura declarada. |
| **Bootstrap e landing** | Preparar UC e disponibilizar as origens para a pipeline. Os Parquets vêm da TLC; quatro dicionários pequenos ficam versionados em YAML. | Download com arquivo temporário e até três tentativas; skip por arquivo existente/não vazio; log de ingestão. Lookup de zonas com validação do cabeçalho, snapshot e SHA-256. O MD5 dos Parquets cobre os primeiros 64 MiB. |
| **Bronze de corridas** | Normalizar as variações de tipo e capitalização dos Parquets mensais, preservando metadados de ingestão. | Um `append_flow(once=True)` por arquivo descoberto pelo glob. Leitura batch de cada Parquet, casts conforme `schema_bronze`, colunas ausentes como NULL, `_arquivo_origem` e `_ingestao_ts`. |
| **Silver e quarentena** | Padronizar nomes, aplicar regras e manter uma corrida válida por chave de negócio. Separar registros que exigem investigação. | `trip_id` determinístico com `xxhash64`; `expect_all` gera métricas; regras classificam rejeições; deduplicação determinística. Inválidos e duplicatas excedentes vão à quarentena com motivos e metadados. `passenger_count` nulo é WARN e permanece na Silver. |
| **Dimensões TLC** | Traduzir códigos em descrições de fornecedor, tarifa, pagamento, despacho e zona. | Cinco tabelas de referência na Bronze/Silver, casts e deduplicação pela chave. Não há auditoria DAMA específica para essas dimensões. |
| **Gold** | Unificar as duas frotas, enriquecer as corridas e produzir o agregado diário para consumo. | `unionByName(allowMissingColumns=True)`; seis `LEFT JOINs` com as dimensões Silver; validação da declaração de membros e agregações. Chave sem correspondência preserva a corrida e deixa a descrição nula. |
| **Auditoria posterior** | Conferir a Silver resultante nas cinco dimensões DAMA do case. | 24 verificações no total; persistência de PASS/FAIL, métrica, horário e até cinco exemplos por falha em `governanca.dq_resultados`. Qualquer FAIL lança exceção após a gravação. |
| **Observabilidade** | Registrar volume por tabela, tempo desde a última escrita/update e métricas das expectations. | Tabelas de corridas e Gold derivadas dos contratos; event log do último update; persistência em `governanca.obs_metricas`. Métricas sinalizam atenção sem um gate por limiar; erros técnicos ainda podem falhar o notebook. |
| **Consulta SQL** | Responder às perguntas do case sobre a Gold enriquecida. | Consultas no notebook de análises; volumes e reconciliação podem ser conferidos com os exemplos da seção de execução. |

</details>

### Modelo de consumo

| Objeto | Granularidade e uso |
| --- | --- |
| `case_ifood.gold.trips_consolidado` | Uma linha por corrida que permaneceu na Silver de cada frota, com `taxi_type`, IDs e descrições das dimensões. |
| `case_ifood.gold.trips_diario` | Uma linha por `data_corrida` e `taxi_type`: quantidade de corridas, média do valor total, média de passageiros e soma de distância. |
| `case_ifood.silver.dim_taxi_zone` | Zona, borough e service zone. Participa duas vezes da Gold: embarque e desembarque. |
| `case_ifood.silver.dim_vendor` | Descrição do fornecedor do taxímetro. |
| `case_ifood.silver.dim_rate_code` | Descrição do código de tarifa. |
| `case_ifood.silver.dim_payment_type` | Descrição da forma de pagamento. |
| `case_ifood.silver.dim_trip_type` | Descrição do tipo de despacho do Green Taxi. |

As cinco dimensões geram **seis joins** porque Taxi Zones atende embarque e desembarque. O agregado diário lê o consolidado enriquecido, conforme [`contracts/gold/trips.yml`](contracts/gold/trips.yml).

<details>
<summary>Implementações e contratos de referência</summary>

| Componente | Arquivo |
| --- | --- |
| Bundle e targets | [`databricks.yml`](databricks.yml) |
| Provisionamento lógico e download inicial | [`bootstrap_ambiente.ipynb`](01_ingestao/bootstrap_ambiente.ipynb) |
| Motores de ingestão e tabelas de governança | [`motores.py`](00_config/motores.py) |
| Ingestão individual parametrizada | [`ingestao_parametrizada.ipynb`](01_ingestao/ingestao_parametrizada.ipynb) |
| Atualização do CSV de zonas | [`atualizar_taxi_zone_lookup.py`](01_ingestao/atualizar_taxi_zone_lookup.py) |
| Transformações medallion | [`pipeline_case_ifood.py`](02_pipelines/pipeline_case_ifood.py) |
| Contratos de corridas | [`yellow_trips.yml`](contracts/yellow_trips.yml) e [`green_trips.yml`](contracts/green_trips.yml) |
| Contratos de referência e consumo | [`dimensoes_tlc.yml`](contracts/references/dimensoes_tlc.yml) e [`trips.yml`](contracts/gold/trips.yml) |
| Auditoria DAMA | [`qualidade_contratos.ipynb`](03_qualidade/qualidade_contratos.ipynb) |
| Coleta de métricas | [`observabilidade_contratos.ipynb`](04_observabilidade/observabilidade_contratos.ipynb) |
| Dependências entre tasks | [`case_ifood_update_e_qualidade.job.yml`](resources/case_ifood_update_e_qualidade.job.yml) |

</details>

## 4. Testes e qualidade

<details>
<summary>Mapa visual das verificações de código e dados</summary>

![Mapa dos 16 testes Python, validação condicional DAB, expectations e 24 verificações de auditoria Silver](docs/arquitetura/testes-e-qualidade.png)

[PNG em alta resolução](docs/arquitetura/testes-e-qualidade.png) · [SVG](docs/arquitetura/testes-e-qualidade.svg)

</details>

### Testes de código no GitHub

O workflow [`.github/workflows/tests.yml`](.github/workflows/tests.yml) executa **16 testes** com Ubuntu e Python 3.12 em pushes para `main` e pull requests.

| Arquivo | Quantidade | O que verifica |
| --- | ---: | --- |
| [`test_contratos.py`](tests/test_contratos.py) | 11 | Estrutura dos contratos, destinos, mapeamentos, regras, membros, dimensões, funções e carregamento pelo motor. |
| [`test_lookup_guard.py`](tests/test_lookup_guard.py) | 3 | Cabeçalho CSV com/sem aspas e rejeição de schema incorreto; HTTP e `dbutils` simulados. |
| [`test_dab.py`](tests/test_dab.py) | 2 | Targets e estrutura YAML, `contracts_dir` e existência dos arquivos referenciados segundo a resolução usada pelo teste local. |

O job de CI `bundle-validate` executa `databricks bundle validate --target dev` quando `DATABRICKS_HOST` está preenchido. Configure `DATABRICKS_HOST` e `DATABRICKS_TOKEN` nos secrets do GitHub para habilitar essa validação autenticada. **O workflow não faz deploy nem executa os jobs Databricks.**

Os testes Python não executam Spark, joins ou agregações com dados reais. A verificação local dos caminhos em `test_dab.py` considera a raiz do repositório e não substitui a resolução de caminhos do Databricks CLI. Lint, cobertura e análise de segurança não estão configurados no workflow atual.

Após corrigir os caminhos DAB para `../...`, o resolvedor de caminhos de `test_dab.py` também precisa ser alinhado ao diretório do YAML. Sem esse ajuste, esse teste pode acusar arquivo inexistente mesmo com o caminho correto para a CLI.

Para executar os testes localmente:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest tests -v
```

### Validações dos dados no Databricks

Durante a transformação, as expectations registram métricas e as regras separam Silver/quarentena. Depois do update, a auditoria aplica **12 verificações por dataset**, totalizando 24:

| Dimensão DAMA | Verificações por dataset | Regra |
| --- | ---: | --- |
| Completude | 5 | Nulos em `vendor_id`, `total_amount`, `pickup_datetime`, `dropoff_datetime`: Yellow ≤ 1%, Green ≤ 5%. Para `passenger_count`: Yellow ≤ 5%, Green ≤ 10%. |
| Validade | 4 | Valor e distância não negativos; passageiros nulos ou entre 0 e 8; pickup dentro de `2023-01-01 ≤ pickup < 2023-06-01`. |
| Unicidade | 1 | Ausência de duplicatas por `trip_id`. |
| Consistência | 1 | `dropoff_datetime >= pickup_datetime`. |
| Pontualidade | 1 | Ausência de corridas fora da janela histórica contratada. |

Os relatórios ficam em `case_ifood.governanca.dq_resultados`; registros rejeitados ficam em `case_ifood.governanca.quarantine_<dataset>`. Como a auditoria lê a Silver após a quarentena, um PASS deve ser interpretado junto dos volumes rejeitados e do event log.

## 5. Operação e evolução

### Próximas execuções

Para reprocessar os arquivos já disponíveis, executar novamente o job `case_ifood_update_e_qualidade`. Não é necessário executar um update separado da pipeline antes desse job: a primeira task já faz isso. Nenhum dos dois jobs possui agendamento versionado.

Para repor arquivos ausentes ou atualizar o lookup, executar o bootstrap sob demanda. Para uma única frota, o notebook de ingestão aceita `taxi_type=yellow|green`, `origem=url`, `ano` e `meses`. O caminho `origem=raw` usa o mesmo volume de origem/destino por padrão; não representa uma segunda zona raw separada.

### Evoluir os contratos

1. Ler [`SDD.md`](SDD.md), que descreve os campos e as convenções.
2. Para um novo dataset, criar o YAML em `contracts/`; para dimensões, usar `contracts/references/`.
3. Para consumo, declarar membros, enriquecimentos ou agregados em `contracts/gold/`.
4. Disponibilizar as origens, publicar as alterações e validar a pipeline antes do update normal.
5. Conferir schema, volume e reconciliação com a fonte após a execução.

Novos datasets e visões que usam os comportamentos já suportados são declarados em contratos. Mudanças de comportamento genérico pertencem aos motores.

### Targets de implantação

| Target | Configuração atual |
| --- | --- |
| `dev` | Padrão; `mode: development`; pipeline com `development: true`. |
| `prod` | `mode: production`; pipeline com `development: false`. |

Antes de usar `prod`, configurar workspace, identidade de execução, permissões e caminhos adequados ao ambiente. Os contratos e resources atuais usam o mesmo catálogo `case_ifood`; trocar de target, por si só, não cria isolamento dos dados.

### Limites atuais

- **Período:** os globs selecionam arquivos de 2023 e as regras aceitam pickups de janeiro a maio. Alterar widgets de download não altera os contratos.
- **Linhagem:** `_arquivo_origem` e `_ingestao_ts` estão na Bronze e na quarentena, mas não são projetados na Silver/Gold.
- **Auditoria:** não há gate explícito de volume mínimo. Tabela vazia resulta em percentual de nulos zero; filtros `NOT(expressão)` não contam NULL como violação. A completude é avaliada separadamente.
- **Observabilidade:** registra contagens das tabelas de corridas/quarentena e da Gold, sem incluir as dimensões de referência. Uma quarentena vazia também recebe status `falha` pela regra genérica de volume. A freshness não possui limiar de alerta; em MVs/streaming tables, usa o horário de início do último update como fallback.
- **Distância:** `soma_distancia_km` é o alias atual do agregado, mas o código soma `trip_distance` sem conversão de unidade.
- **Segurança:** `owner`, `dominio`, `colunas_pii` e `tags` documentam metadados nos contratos; não há motor de aplicação de GRANTs/tags versionado neste checkout.

## Estrutura do repositório

```text
case-ifood/
├── 00_config/          # Motores genéricos e tabelas de governança
├── 01_ingestao/        # Bootstrap, ingestão parametrizada e lookup TLC
├── 02_pipelines/       # Pipeline medallion orientada a contratos
├── 03_qualidade/       # Auditoria DAMA da Silver
├── 04_observabilidade/ # Contagens, freshness e event log
├── 06_analises/        # Consultas SQL na Gold
├── contracts/
│   ├── references/     # Cinco dimensões TLC
│   └── gold/           # Consolidação, enriquecimentos e agregado diário
├── resources/          # Pipeline, job de bootstrap e job operacional
├── .github/workflows/  # Testes e validação condicional do bundle
├── tests/              # Testes Python sem Spark
├── docs/arquitetura/   # Diagramas e geradores
├── databricks.yml      # Bundle e targets dev/prod
├── requirements-dev.txt
├── SDD.md              # Guia de contratos
└── README.md
```

Para regenerar as duas visões da arquitetura, com Pillow disponível:

```powershell
python docs/arquitetura/gerar_arquiteturas.py
```
