# Lakehouse NYC Taxi

Pipeline Databricks para corridas **Yellow e Green Taxi**, orientado a contratos YAML e organizado nas camadas **Bronze → Silver → Gold**. Inclui dimensões TLC, quarentena, auditoria de qualidade DAMA-DMBOK e consultas SQL. O recorte do case é janeiro a maio de 2023.

[Visão geral](#1-arquitetura-visão-geral) · [Configuração e execução](#2-configuração-e-execução) · [Arquitetura detalhada](#3-arquitetura-detalhada) · [Testes e qualidade](#4-testes-e-qualidade) · [Operação](#5-operação)

## 1. Arquitetura: visão geral

![Visão geral do fluxo: DAB, bootstrap, Bronze, Silver, Gold e consulta SQL](docs/arquitetura/arquitetura-visao-geral.png)

[PNG](docs/arquitetura/arquitetura-visao-geral.png) · [SVG editável](docs/arquitetura/arquitetura-visao-geral.svg)

O **Databricks Asset Bundle (DAB)** valida e publica os arquivos, a pipeline e os jobs no workspace. O job de **bootstrap** cria os objetos de Unity Catalog e baixa as fontes oficiais da TLC para a landing. A pipeline transforma as origens em Bronze, Silver e Gold; o notebook SQL consome as tabelas Gold.

| Recurso | Quando usar | O que faz |
| --- | --- | --- |
| `case_nyc_taxi_bootstrap` | Preparação inicial ou reposição dos arquivos | Cria catálogo, schemas, volumes e tabelas de governança; baixa os Parquets Yellow/Green e o lookup de Taxi Zones. |
| `case_nyc_taxi_medallion` | Processamento dos arquivos disponíveis | Gera Bronze, Silver, dimensões e Gold a partir dos contratos. |
| `case_nyc_taxi_update_e_qualidade` | Execução operacional | Executa `pipeline_update → qualidade_contratos → observabilidade_contratos`. |
| `analysis/consultas_gold.ipynb` | Consumo | Executa as consultas SQL do case; não é uma task dos jobs. |

O job operacional atualiza a pipeline primeiro. A auditoria avalia a Silver depois desse update; portanto, um FAIL interrompe as tasks seguintes, embora a Gold já tenha sido materializada. As cinco dimensões de referência alimentam seis `LEFT JOINs` na Gold, porque Taxi Zone é usada para embarque e desembarque.

## 2. Configuração e execução

### Pré-requisitos

- Git e [Databricks CLI com suporte a bundles](https://docs.databricks.com/aws/en/dev-tools/cli/install).
- Workspace Databricks com Unity Catalog, metastore associado e serverless habilitado para a pipeline e para jobs.
- Permissões para usar/criar catálogo, schemas, volumes e tabelas, publicar/executar jobs e pipeline, e consultar o event log.
- Acesso HTTPS a `d37ci6vzurychx.cloudfront.net`, origem dos Parquets TLC e do CSV de Taxi Zones.
- Python 3.12 é necessário para reproduzir o CI localmente; as dependências dos jobs Databricks estão declaradas nos resources.

O bootstrap prepara objetos do Unity Catalog dentro de um workspace existente. Ele não cria o workspace, o metastore ou o armazenamento de nuvem.

### 2.1. Clonar e autenticar

Os exemplos usam PowerShell:

```powershell
git clone https://github.com/alexvalsechi/case-nyc-taxi.git
cd case-nyc-taxi

databricks --version
databricks auth login --host https://<seu-workspace> --profile DATABRICKS_DEV
```

Conclua o login no navegador. No bloco existente `targets.dev` de [`databricks.yml`](databricks.yml), configure o host e o perfil sem remover as demais opções do target:

```yaml
workspace:
  host: https://<seu-workspace>
  profile: DATABRICKS_DEV
```

A [autenticação de bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/authentication) descreve os perfis aceitos pela CLI.

### 2.2. Validar e publicar o DAB

Na raiz do repositório:

```powershell
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle summary -t dev
```

Os caminhos da biblioteca da pipeline e dos notebooks estão relativos aos YAML que os declaram. Os notebooks descobrem o diretório `src/` pelo caminho em que estão executando e localizam seus módulos e contratos dentro dele. A pipeline recebe `${workspace.file_path}/src/contracts` pela configuração do DAB. As tasks de notebook têm ambientes serverless e dependências declarados nos jobs; a pipeline usa serverless e declara `PyYAML` no ambiente do resource.

A configuração e os exemplos de caminhos estão na [documentação do DAB](https://docs.databricks.com/aws/en/dev-tools/bundles/settings) e nos [exemplos oficiais de bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/examples).

### 2.3. Preparar catálogo e fontes

O job de bootstrap tem os parâmetros padrão do case: catálogo `case_nyc_taxi`, ano `2023` e meses `01,02,03,04,05`.

```powershell
databricks bundle run -t dev case_nyc_taxi_bootstrap
```

O job cria os schemas `landing`, `bronze`, `silver`, `gold` e `governanca`; os volumes `landing.files` e `landing.reference`; e as tabelas de governança. Em seguida, baixa os cinco Parquets mensais de cada frota e o lookup oficial de Taxi Zones. O CSV passa por uma checagem de cabeçalho e é gravado com snapshot datado e manifesto SHA-256.

O download publica os arquivos em **partições físicas por categoria e mês/ano**. O período representa a competência do arquivo TLC, derivada da URL, e não a data de cada corrida dentro do Parquet:

```text
/Volumes/case_nyc_taxi/landing/files/
├── categoria=green/
│   ├── mes_ano=05-2023/green_tripdata_2023-05.parquet
│   └── mes_ano=06-2023/green_tripdata_2023-06.parquet
└── categoria=yellow/
    ├── mes_ano=05-2023/yellow_tripdata_2023-05.parquet
    └── mes_ano=06-2023/yellow_tripdata_2023-06.parquet
```

O contrato declara `fonte.particao_template`, `fonte.glob` e a política `fonte.http`. Cada download usa um temporário `.part` na partição de destino. Antes da publicação, o motor verifica os bytes recebidos contra `Content-Length` quando comparável, lê o footer Parquet e calcula **SHA-256 de todo o arquivo**. Um manifesto `<arquivo>.manifesto.json` confirma tamanho, hash, linhas, categoria, período e execução. A Bronze preserva o caminho completo e exige esse marcador; ausência ou inconsistência interrompe o update. Isso evita depender de uma promessa de rename atômico do armazenamento em nuvem. A validação do footer não substitui a auditoria dos dados nas camadas seguintes.

Na reexecução, um arquivo existente só recebe `ja_existia` depois de validar footer, tamanho e checksum; um arquivo alterado ou corrompido falha sem sobrescrita automática. Os arquivos antigos na raiz são **copiados** para a nova organização, validados e confirmados, preservando a origem. A Bronze usa apenas o glob particionado. Os nomes dos append flows continuam derivados do nome do arquivo, preservando sua identidade nos checkpoints; valide a alteração em seu workspace antes do update. Não é necessário solicitar full refresh para essa mudança.

Os parâmetros e o job estão em [`bootstrap_ambiente.ipynb`](src/01_ingestao/bootstrap_ambiente.ipynb) e [`case_nyc_taxi_bootstrap.job.yml`](resources/case_nyc_taxi_bootstrap.job.yml). O bootstrap verifica as **partições exatas solicitadas**, processa todos os meses das duas categorias e falha se alguma partição não puder ser publicada. O job limita execuções simultâneas a uma; evite executar wrappers manuais ao mesmo tempo sobre as mesmas partições. Para baixar maio e junho, execute o notebook com `ano=2023` e `meses=05,06`. O download aceita outros anos; as regras atuais de Silver ainda aceitam somente janeiro a maio de 2023, enviando as demais corridas à quarentena.

### Falhas HTTP e observabilidade da ingestão

| Falha | Comportamento |
| --- | --- |
| HTTP 408, 429, 500, 502, 503, 504 | Até três tentativas por padrão, com backoff exponencial e jitter. |
| `Retry-After` | Respeita segundos ou data HTTP. Se exceder `espera_max_s`, falha a partição e pede reagendamento, sem tentar antes do prazo. |
| Timeout, conexão/DNS, transferência interrompida, tamanho divergente ou conteúdo inválido | Nova transferência completa dentro do orçamento de tentativas; temporários são removidos. |
| HTTP 400/401/403/404/410 e outros códigos fora da política | Falha sem repetição automática. URL, disponibilidade e permissões precisam ser corrigidas; uma resposta 403 não permite concluir que o arquivo existe. |
| TLS/certificado, excesso de redirects, requisição inválida, permissão ou falta de espaço | Falha identificada, sem retry automático. A validação TLS permanece habilitada. |
| Resposta 200 com HTML/JSON/XML, resposta vazia, 204 ou 206 | Não é confirmada como Parquet completo. |

Timeouts separados limitam conexão (15 s), inatividade na leitura (60 s) e tempo de transferência verificado entre blocos (900 s). O timeout de leitura limita quanto uma chamada bloqueada pode ultrapassar esse último limite. A política pode ser ajustada nos contratos. A classificação usa as [exceções documentadas do Requests](https://requests.readthedocs.io/en/latest/user/quickstart/#errors-and-exceptions) e o formato de [Retry-After do HTTP](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after).

`governanca.log_ingestao` recebe início/fim do lote, início do arquivo, início/resultado de cada tentativa e resultado final por partição. O histórico anterior é preservado por migração aditiva de colunas. Os novos eventos incluem `execucao_id`, categoria, período, destino, operação (download/cópia/migração), status HTTP, tipo de erro, decisão de retry, espera, duração, bytes, SHA-256, linhas, ETag e Last-Modified. Também são emitidos como JSON no stdout do job. Uma falha ao persistir a auditoria interrompe a execução. Para contar resultados por arquivo, filtre `evento = 'fim_arquivo'`; os eventos de tentativa e lote não representam novos arquivos ingeridos.

As consultas em [`analysis/observabilidade_ingestao.sql`](analysis/observabilidade_ingestao.sql) mostram resumo de execução, último resultado por partição, retries e volume transferido, partições sem resultado e execuções interrompidas. Funcionam mesmo quando o download falha antes do update SDP. Para notificações automáticas, configure um alerta SQL com essas consultas ou notificações de falha no job; este checkout não define destinatários nem um serviço externo de alertas.

### 2.4. Validar e executar o processamento

Valide o grafo da pipeline e execute o job operacional:

```powershell
databricks bundle run -t dev --validate-only case_nyc_taxi_medallion
databricks bundle run -t dev case_nyc_taxi_update_e_qualidade
```

A sequência operacional é:

```text
pipeline_update → qualidade_contratos → observabilidade_contratos
     Bronze/Silver/Gold        auditoria da Silver          métricas e event log
```

Se a auditoria gravar uma regra como `FAIL`, ela lança uma exceção e o job não inicia a task de observabilidade. A Gold já foi publicada no update da pipeline. A observabilidade registra métricas e sinaliza atenção; não define um limiar de negócio que bloqueie o job. Os jobs não têm agendamento declarado.

### 2.5. Conferir e consultar os resultados

No SQL Editor ou em um notebook com acesso ao catálogo:

```sql
SELECT taxi_type, COUNT(*) AS qtd_corridas
FROM case_nyc_taxi.gold.trips_consolidado
GROUP BY taxi_type;

SELECT *
FROM case_nyc_taxi.gold.trips_diario
ORDER BY data_corrida, taxi_type
LIMIT 10;

SELECT *
FROM case_nyc_taxi.governanca.dq_resultados
ORDER BY avaliado_em DESC
LIMIT 24;
```

Para conferir as métricas de observabilidade, consulte `case_nyc_taxi.governanca.obs_metricas`. Como reconciliação adicional, compare a contagem de linhas de `gold.trips_consolidado` com a soma de `qtd_corridas` em `gold.trips_diario`; essa conferência ainda é manual.

O notebook [`consultas_gold.ipynb`](analysis/consultas_gold.ipynb) responde às duas perguntas do case: média do valor `total_amount` por corrida e mês para Yellow Taxi; média de passageiros por hora do dia em maio para toda a frota.

## 3. Arquitetura detalhada

A imagem mostra os componentes implementados e suas conexões. Leia as origens à esquerda, o processamento no centro e a consulta SQL à direita. A faixa superior identifica a publicação pelo DAB e a sequência dos jobs; corridas e referências têm caminhos separados até a Gold.

![Fluxo detalhado com ícones e setas: GitHub e DAB, bootstrap, origens TLC, downloads, volumes, Bronze, validação, Silver, quarentena, dimensões, Gold, SQL, auditoria e observabilidade](docs/arquitetura/arquitetura-detalhada.png)

[PNG](docs/arquitetura/arquitetura-detalhada.png) · [SVG editável](docs/arquitetura/arquitetura-detalhada.svg) · [Gerador dos diagramas](docs/arquitetura/gerar_arquiteturas.py)

As setas identificam dados, configuração/chamadas manuais, dependências entre tasks, métricas/logs e rejeições. Os nomes de tabelas na imagem omitem apenas o catálogo `case_nyc_taxi`. O notebook de consumo lê `gold.trips_consolidado`; `gold.trips_diario` é uma saída adicional. Os nomes de objetos e os hashes das fontes são incorporados ao PNG e ao SVG a partir dos contratos e resources versionados.

| Etapa | Finalidade | Validações e comportamento |
| --- | --- | --- |
| **CI e DAB** | Versionar código, contratos e recursos; publicar a configuração do workspace. | GitHub Actions roda os testes Python; validação DAB é condicional aos secrets. Deploy e execução Databricks são manuais. |
| **Contratos YAML** | Declarar fontes, schemas, regras de qualidade, dimensões e modelos de consumo. | Pipeline verifica membros, referências de dimensões e funções de agregação da Gold. Testes estruturais conferem a configuração e os caminhos. |
| **Bootstrap e landing** | Criar objetos de UC e baixar as origens TLC para os volumes de landing. | Partições por categoria/mês-ano, validação de Parquet, SHA-256 completo, manifesto e auditoria por tentativa. Retry por classe de falha. O CSV de zonas tem cabeçalho validado e manifesto próprio. |
| **Bronze** | Preservar os dados de origem e normalizar variações dos Parquets mensais. | Casts para schema canônico, nomes normalizados, NULL para coluna ausente e inclusão de `_arquivo_origem` e `_ingestao_ts`. Um fluxo batch `append_flow(once=True)` é criado por arquivo encontrado. |
| **Silver e quarentena** | Padronizar colunas e manter uma linha conforme por `trip_id`. | Expectations registram métricas. Regras verificam campos obrigatórios, valor/distância não negativos, passageiros nulos ou entre 0 e 8, janela de datas e `dropoff_datetime >= pickup_datetime`. Inválidos e duplicatas excedentes ficam em quarentena com motivo e origem. Passageiro nulo é WARN e permanece na Silver. |
| **Dimensões TLC** | Traduzir códigos de fornecedor, tarifa, pagamento, despacho e zonas. | Cinco dimensões passam por Bronze/Silver, cast e deduplicação por chave. Quatro dicionários estão em YAML; Taxi Zones vem do CSV oficial. |
| **Gold** | Unificar Yellow/Green, enriquecer as corridas e gerar agregado diário. | `unionByName` aceita colunas ausentes. Seis `LEFT JOINs` preservam a corrida mesmo sem correspondência; a descrição correspondente fica NULL. `trips_diario` agrega por data e tipo de táxi. |
| **Auditoria DAMA** | Avaliar qualidade da Silver após o update do pipeline. | 12 regras por dataset, 24 no total. Grava PASS/FAIL, métrica, horário e até cinco exemplos por falha em `governanca.dq_resultados`; qualquer FAIL encerra a task após o registro. |
| **Observabilidade** | Registrar contagem, freshness e resultados de expectations. | Lê contratos e event log; grava em `governanca.obs_metricas`. Métricas podem sinalizar atenção; falhas técnicas de execução também podem interromper a task. |
| **Consumo SQL** | Responder às perguntas do case e apoiar análises. | Notebook SQL consulta a Gold consolidada e enriquecida. |

### Tabelas de consumo e dimensões

| Tabela | Conteúdo |
| --- | --- |
| `case_nyc_taxi.gold.trips_consolidado` | Corridas Yellow e Green com IDs TLC, `taxi_type` e descrições das dimensões. |
| `case_nyc_taxi.gold.trips_diario` | Contagem, médias de valor/passageiros e distância somada por data e táxi. |
| `case_nyc_taxi.silver.dim_vendor` | Nome do fornecedor do taxímetro. |
| `case_nyc_taxi.silver.dim_rate_code` | Nome do código de tarifa. |
| `case_nyc_taxi.silver.dim_payment_type` | Descrição da forma de pagamento. |
| `case_nyc_taxi.silver.dim_trip_type` | Descrição do tipo de despacho Green. |
| `case_nyc_taxi.silver.dim_taxi_zone` | Bairro, zona e zona de serviço para embarque e desembarque. |

A declaração de enriquecimento e do agregado diário fica em [`src/contracts/gold/trips.yml`](src/contracts/gold/trips.yml). As regras e os schemas de ingestão estão em [`src/contracts/yellow_trips.yml`](src/contracts/yellow_trips.yml), [`src/contracts/green_trips.yml`](src/contracts/green_trips.yml) e [`src/contracts/references/dimensoes_tlc.yml`](src/contracts/references/dimensoes_tlc.yml).

## 4. Testes e qualidade

<details>
<summary>Mapa visual dos testes de código e das verificações nos dados</summary>

![Testes Python, validação DAB, expectations e auditoria DAMA](docs/arquitetura/testes-e-qualidade.png)

[PNG](docs/arquitetura/testes-e-qualidade.png) · [SVG](docs/arquitetura/testes-e-qualidade.svg)

</details>

### Testes automatizados de código

O workflow [`.github/workflows/tests.yml`](.github/workflows/tests.yml) executa os testes em pushes para `main` e pull requests, com Ubuntu e Python 3.12.

| Arquivo | Qtde. | Cobertura |
| --- | ---: | --- |
| [`test_contratos.py`](tests/test_contratos.py) | 11 | Contratos de ingestão, dimensões e Gold; regras e carregamento pelo motor. |
| [`test_lookup_guard.py`](tests/test_lookup_guard.py) | 3 | Cabeçalho do CSV com/sem aspas, rejeição de schema incorreto; HTTP e `dbutils` simulados. |
| [`test_dab.py`](tests/test_dab.py) | 2 | Targets, YAMLs, configuração `contracts_dir` e paths dos arquivos declarados nos resources. |
| [`test_ingestao.py`](tests/test_ingestao.py) | 45 | Partições, manifestos, SHA-256, migração da landing antiga, reexecução, falhas HTTP/rede/disco, cobertura do lote, identidade dos fluxos e servidor HTTP local com 503 seguido de sucesso. |

O job `bundle-validate` roda `databricks bundle validate --target dev` quando `DATABRICKS_HOST` e `DATABRICKS_TOKEN` estão configurados nos secrets do GitHub. O workflow não faz deploy nem executa os jobs Databricks. Os testes Python não executam Spark ou resultados de joins/agregações em dados reais; lint, cobertura e análise de segurança não estão configurados.

Para rodar a suíte localmente:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest tests -v
```

### Auditoria da qualidade dos dados

A auditoria avalia cada dataset Silver em cinco dimensões DAMA, totalizando 12 verificações por dataset:

| Dimensão | Verificações por dataset | Regras resumidas |
| --- | ---: | --- |
| Completude | 5 | Percentual máximo de nulos em `vendor_id`, `total_amount`, timestamps e passageiros, com tolerâncias por frota. |
| Validade | 4 | `total_amount` e `trip_distance` não negativos; passageiros nulos ou de 0 a 8; pickup entre 01/01/2023 e 01/06/2023. |
| Unicidade | 1 | Não há duplicatas de `trip_id`. |
| Consistência | 1 | `dropoff_datetime >= pickup_datetime`. |
| Pontualidade | 1 | Nenhuma corrida fora da janela histórica contratada. |

Os resultados ficam em `case_nyc_taxi.governanca.dq_resultados`; rejeições são consultáveis em `case_nyc_taxi.governanca.quarantine_<dataset>`. A auditoria lê Silver após a quarentena, portanto, avalie PASS junto com o volume rejeitado e as métricas do event log.

## 5. Operação

### Reexecuções

Para processar de novo os arquivos presentes, execute `case_nyc_taxi_update_e_qualidade`. A primeira task já atualiza a pipeline. Execute `case_nyc_taxi_bootstrap` sob demanda para repor arquivos ou atualizar o lookup de Taxi Zones. Os jobs não têm schedule configurado.

O notebook individual [`ingestao_parametrizada.ipynb`](src/01_ingestao/ingestao_parametrizada.ipynb) permite escolher `taxi_type`, `ano`, `meses`, `origem` e `origem_volume`. O modo padrão `url` baixa da TLC; `raw` copia de uma origem plana ou particionada para a landing do contrato. Se `origem_volume` ficar vazio, reaproveita a própria landing. Para executá-lo fora do job de bootstrap, o ambiente precisa de PyYAML, requests, pandas e pyarrow, conforme as dependências do resource.

### Evoluir os dados

Leia [`SDD.md`](SDD.md) antes de alterar contratos. Um novo dataset é declarado em `src/contracts/`; dimensões em `src/contracts/references/`; unificações, enriquecimentos e agregados em `src/contracts/gold/`. O pipeline consome esses contratos para os comportamentos genéricos já suportados.

### Targets

| Target | Modo |
| --- | --- |
| `dev` | Desenvolvimento; target padrão. |
| `prod` | Produção. O target altera o modo da pipeline, mas os contratos atuais continuam usando o catálogo `case_nyc_taxi`. |

### Observações de escopo

- Os contratos selecionam dados de 2023, com janela de pickups de janeiro a maio. Alterar parâmetros de download não altera essa regra.
- `_arquivo_origem` e `_ingestao_ts` ficam na Bronze e na quarentena; não chegam à Silver/Gold.
- Não há regra de volume mínimo na auditoria. Uma Silver vazia tem percentual de nulos calculado como zero; completude é avaliada separadamente.
- Observabilidade inclui tabelas de corridas e Gold, sem as dimensões de referência. Uma quarentena vazia recebe status `falha` pela regra genérica de contagem zero; não há limiar de freshness configurado.
- `soma_distancia_km` é o nome declarado no agregado, mas `trip_distance` não é convertido de unidade antes da soma.
- Os contratos documentam campos de owner/tags/PII; não há grants ou aplicação de tags automatizados neste checkout.

## Estrutura do repositório

```text
case-nyc-taxi/
├── src/                    # Código fonte e contratos da solução
│   ├── 00_config/          # Motores genéricos e governança
│   ├── 01_ingestao/        # Bootstrap, ingestão parametrizada e lookup TLC
│   ├── 02_pipelines/       # Pipeline medallion orientada a contratos
│   ├── 03_qualidade/       # Auditoria DAMA
│   ├── 04_observabilidade/ # Métricas e event log
│   └── contracts/          # Contratos de ingestão, dimensões e consumo
│       ├── gold/
│       └── references/
├── analysis/               # Notebook com as respostas do case
│   └── consultas_gold.ipynb
├── resources/              # Pipeline e jobs DAB
├── tests/                  # Testes Python sem Spark
├── docs/                   # Diagramas e geradores
├── .github/                # CI e validação condicional do DAB
├── README.md
├── SDD.md                  # Guia de contratos
├── databricks.yml          # Bundle e targets dev/prod
└── requirements-dev.txt
```

Para gerar novamente os dois diagramas de arquitetura, com Pillow e PyYAML disponíveis:

```powershell
python docs/arquitetura/gerar_arquiteturas.py
```
