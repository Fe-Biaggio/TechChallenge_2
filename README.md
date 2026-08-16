# Tech Challenge Fase 2 — Pipeline Híbrido para Análise da Alfabetização no Brasil

**Pós-Tech FIAP | 1IAST | Fase 2**

---

## Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico do Brasil. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados e municípios com a meta de garantir que todas as crianças estejam alfabetizadas ao final do **2º ano do ensino fundamental até 2030**.

O INEP definiu o ponto de corte de **743 pontos na escala de proficiência do Saeb** como critério de alfabetização. Com base nisso, foi criado o **Indicador Criança Alfabetizada**, que expressa o percentual de estudantes que atingem esse patamar.

Compreender os fatores que influenciam a alfabetização exige integrar múltiplas fontes de dados — educacionais, territoriais, socioeconômicos — e disponibilizá-las de forma confiável para análise e tomada de decisão.

---

## O Desafio

Atuando como um **time de engenharia de dados de uma organização pública educacional**, o objetivo é construir uma **pipeline híbrida de dados (Batch + Streaming)** que integre diferentes fontes do indicador de alfabetização, garantindo qualidade, escalabilidade e eficiência de custos em ambiente de nuvem.

---

## Arquitetura da Solução

A pipeline segue a **Arquitetura Medalhão** com três camadas, com ingestão
híbrida (BigQuery com fallback automático para CSV) e streaming simulado.
Diagramas completos (arquitetura + fluxo de dados/joins) em
[`docs/arquitetura/README.md`](docs/arquitetura/README.md) — renderizam
nativamente no GitHub.

```
Fontes (BigQuery ou data/raw/*.csv)
      │
      ▼                                  Streaming (simulado)
┌─────────────┐     Batch      ┌──────────────┐   producer.py → consumer.py
│ Base dos    │ ─────────────► │    BRONZE    │        │
│ Dados       │                │ 8 tabelas,   │◄───────┘
│ (INEP)      │                │ raw, part.   │
│             │                │ por ano      │
└─────────────┘                └──────┬───────┘
                                       │  limpeza, decodificação de "rede",
                                       │  melt de metas, integração
                                       ▼
                                ┌──────────────┐
                                │    SILVER    │  10 tabelas tratadas
                                └──────┬───────┘
                                       │  agregação analítica
                                       ▼
                                ┌──────────────┐
                                │     GOLD     │  3 datasets analíticos
                                └──────┬───────┘
                                       │
                           ┌───────────┼───────────┐
                           ▼           ▼            ▼
                       Dashboard   Análise      Modelos ML
```

Qualidade de dados (`quality/validacao_dados.py`) valida as três camadas de
forma transversal — ver seção "Metodologia" abaixo.

### Bronze Layer — Dados Brutos

- Ingestão sem transformação: `SELECT *` (BigQuery) ou leitura direta do CSV (fallback)
- Histórico completo preservado, um diretório por tabela
- Formato: Parquet particionado por `ano` (tabelas de referência sem partição)
- 8 tabelas: `indicador_municipio`, `indicador_uf`, `meta_brasil`, `meta_uf`,
  `meta_municipio`, `alunos` (~3,87M registros), `diretorio_municipio`, `diretorio_uf`

### Silver Layer — Dados Tratados

Transformações aplicadas:
- Limpeza, tipagem e padronização de nomes de colunas
- Decodificação de `rede` (código numérico → rótulo, ver `REDE_MAP` em `config.py`)
- "Despivotagem" das tabelas de meta (`meta_alfabetizacao_2024..2030`, formato
  largo) para long (`ano_meta`, `valor_meta`), mantendo por ano-alvo apenas a
  vintage mais recente publicada
- Normalização de chaves (`id_municipio`, `sigla_uf`)
- **Integração das bases**: resultado municipal + diretório (nome/UF/região) +
  meta municipal → `alfabetizacao_integrado` (uma linha por ano × município)
- Tratamento de nulos **seletivo**: nulos estruturais (ex.: `proporcao_aluno_nivel_*`
  só existe a partir da vintage 2024; `proficiencia` nula para aluno ausente)
  não são imputados — imputar mediana nesses casos distorceria a distribuição real

### Gold Layer — Camada Analítica

3 datasets prontos para consumo:
- `indicador_alfabetizacao_municipio` — indicador por município/ano, gap vs.
  meta municipal, gap vs. indicador nacional, faixa de risco
- `comparacao_metas_resultados` — metas vs. resultados por UF/ano (rede
  Pública), com % de municípios da UF que atingiram sua meta municipal
- `evolucao_temporal` — série histórica do indicador (Brasil + UF), com
  variação ano a ano
- Preparado para dashboards, análises estatísticas e treinamento de modelos de ML

---

## Fontes de Dados

Dataset [Avaliação da Alfabetização](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72)
(INEP, via Base dos Dados) — projeto BigQuery `br_inep_avaliacao_alfabetizacao`,
mais o diretório de referência `br_bd_diretorios_brasil`:

| Tabela | Entidade | Chave | Descrição |
|--------|----------|-------|-----------|
| `municipio` | Resultado municipal | ano, id_municipio, serie, rede | Taxa de alfabetização por município |
| `uf` | Resultado estadual | ano, sigla_uf, serie, rede | Taxa de alfabetização por UF |
| `meta_alfabetizacao_brasil` | Meta nacional | ano, rede | Trajetória de metas 2024–2030 (rede Pública) |
| `meta_alfabetizacao_uf` | Meta estadual | ano, sigla_uf, rede | Trajetória de metas por UF (rede Pública) |
| `meta_alfabetizacao_municipio` | Meta municipal | ano, id_municipio, rede | Trajetória de metas por município (rede Municipal) |
| `alunos` | Microdados | ano, id_municipio, id_escola, id_aluno | ~3,87M registros individuais de desempenho |
| `br_bd_diretorios_brasil.municipio` | Diretório | id_municipio | Nome, UF, região de cada município |
| `br_bd_diretorios_brasil.uf` | Diretório | sigla | Nome e região de cada UF |

Não existe uma tabela de resultado nacional própria — a série Brasil na Gold é
derivada da coluna `taxa_alfabetizacao` já presente em `meta_alfabetizacao_brasil`.

**Dados externos opcionais para enriquecimento (não implementados nesta entrega):**
- Censo Escolar (INEP) — infraestrutura escolar
- IBGE Censo / PNAD — contexto socioeconômico
- Atlas do Desenvolvimento Humano — IDH municipal
- Cadastro Único / Bolsa Família — vulnerabilidade social
- FUNDEB — financiamento educacional

---

## Estrutura do Repositório

```
TechChallenge_2/
├── pipeline/
│   ├── batch/
│   │   ├── config.py                      # TABELAS, REDE_MAP, diretórios, credenciais
│   │   ├── utils.py                       # logger, I/O Parquet, métricas de qualidade
│   │   ├── 01_ingestao_bronze.py          # BigQuery com fallback → CSV raw → Bronze
│   │   ├── 02_processamento_silver.py     # Limpeza, melt de metas, integração → Silver
│   │   └── 03_agregacao_gold.py           # Agregações Silver → Gold
│   └── streaming/
│       ├── producer.py / consumer.py      # Fila em memória ou Kafka real
│       └── 04_simulacao_streaming.py      # Orquestrador da simulação
├── data/
│   ├── raw/                               # CSVs exportados (fallback sem GCP) + metadados
│   ├── bronze/                            # Dados brutos (Parquet) — gerado, não versionado
│   ├── silver/                            # Dados tratados e integrados — gerado, não versionado
│   └── gold/                              # Datasets analíticos — gerado, não versionado
├── quality/
│   └── validacao_dados.py                 # Regras de qualidade para Bronze/Silver/Gold
├── notebooks/
│   ├── 01_exploracao_dados.ipynb          # Análise exploratória das 8 fontes raw
│   ├── 02_pipeline_bronze_silver.ipynb    # Transformações, com o caso real de decisão de escopo
│   └── 03_camada_gold_analytics.ipynb     # Gráficos da Gold + aplicação em IA
├── docs/
│   ├── arquitetura/                       # Diagramas (Mermaid + PNG) e decisões
│   └── [IAST] - Tech Challenge - Fase 2.pdf  # Enunciado original
├── requirements.txt                       # Dependências Python
└── README.md
```

`data/raw/` traz 7 dos 8 CSVs (o de `alunos`, ~214MB, excede o limite de
100MB do GitHub — ver `.gitignore`); os demais ficam versionados para que o
pipeline rode via fallback sem precisar de credenciais GCP.

---

## Metodologia

### 1. Ingestão Batch ([`pipeline/batch/`](pipeline/batch/))
Processamento sob demanda das 8 tabelas fonte, com fallback automático:
- BigQuery (produção) → CSV local em `data/raw/` se GCP não estiver configurado
- Metas educacionais (nacional, estadual, municipal), resultado (município/UF),
  diretórios de referência e microdados de alunos

### 2. Ingestão Streaming ([`pipeline/streaming/`](pipeline/streaming/))
Simulação de eventos em tempo quase real (fila em memória por padrão, ou Kafka
real se configurado):
- Atualização de indicadores municipais
- Atualização/revisão de metas

### 3. Qualidade de Dados ([`quality/validacao_dados.py`](quality/validacao_dados.py))
Valida as 3 camadas (Bronze, Silver, Gold) — 21 tabelas, 0 alertas na última
execução contra os dados reais:
- Completude (limiar de nulos por coluna, calibrado por tabela — nulos
  estruturais como `proporcao_aluno_nivel_*` pré-2024 não geram alerta)
- Unicidade (duplicatas na chave primária de cada tabela)
- Integridade referencial (`sigla_uf` de `alfabetizacao_integrado` contra `diretorio_uf`)
- Domínio (siglas de UF válidas)

---

## Tecnologias Utilizadas

| Tecnologia | Uso | Justificativa |
|------------|-----|---------------|
| Python 3.10+ | Pipeline principal | Ecossistema rico para dados, único runtime em todas as camadas |
| pandas + pyarrow | Transformação e I/O Parquet | ~3,87M linhas (maior tabela) cabem confortavelmente em memória (~1,1GB) — um cluster distribuído seria over-engineering neste volume |
| Parquet + Snappy | Formato de armazenamento das 3 camadas | Compressão colunar eficiente, leitura seletiva de colunas, particionamento nativo |
| `basedosdados` (BigQuery) | Ingestão em produção | Client oficial da Base dos Dados; cai automaticamente para CSV local se não configurado |
| Kafka / fila em memória | Streaming | `kafka-python` para Kafka real; fila em memória (`threading.Queue`) por padrão — roda sem infraestrutura externa |
| Jupyter + matplotlib/seaborn | Notebooks analíticos | Exploração e visualização executadas contra os dados reais da pipeline |
| python-dotenv | Configuração | Credenciais/parâmetros via `.env`, fora do controle de versão |

**Considerados, não usados nesta entrega** (ficam em `requirements.txt` como
caminho de evolução, não como dependência ativa do código): PySpark/Delta
Lake/Iceberg — fariam sentido se o volume de dados não coubesse em um único
processo; Great Expectations — o volume de regras de qualidade deste projeto
não justificou a curva de configuração frente a um validador Python direto
(`quality/validacao_dados.py`).

---

## Decisões Arquiteturais

### Batch vs Streaming
- **Batch** para as 8 fontes históricas (resultado, meta, diretórios, alunos) — volume conhecido, sem necessidade de latência baixa
- **Streaming simulado** para eventos de atualização de indicador/meta — demonstra o padrão de ingestão incremental sem exigir um cluster Kafka real para rodar localmente

### BigQuery com fallback para CSV, não só um ou só outro
- BigQuery é o caminho de produção (sempre atualizado, sem duplicar dados),
  mas exige projeto GCP com faturamento ativo — indisponível no ambiente de
  desenvolvimento usado nesta entrega
- `01_ingestao_bronze.py` tenta o BigQuery e cai automaticamente para `data/raw/*.csv`
  (mesmo dado, exportado uma vez) sem exigir nenhuma mudança de código — o
  restante da pipeline roda idêntico nos dois caminhos

### Escopo de rede na comparação meta x resultado
- `indicador_municipio`/`indicador_uf` trazem mais de uma rede por (ano,
  entidade); `meta_municipio` só define metas para a rede Municipal e
  `meta_uf`/`meta_brasil` só para a rede Pública (agregado)
- Comparar a meta contra o resultado "headline" (melhor rede disponível)
  misturaria escopos e geraria um gap incorreto para qualquer município com
  mais de uma rede avaliada — por isso a Silver mantém o resultado headline e
  o resultado no escopo exato da meta como colunas separadas (ver
  `notebooks/02_pipeline_bronze_silver.ipynb` para um caso real)

### Data Lake vs Data Warehouse
- Parquet particionado em disco (data lake simples) em vez de um Data
  Warehouse gerenciado — no volume atual (~415MB no total, somando raw + bronze + silver), o overhead
  operacional de um DW não se paga; a estrutura de camadas já isola dado
  bruto de dado analítico, que é o benefício que se buscaria com um DW aqui

### Custo vs Performance
- Parquet + particionamento por `ano` reduz o volume lido por consulta a um
  ano específico
- Processamento local de ponta a ponta (Bronze→Silver→Gold) roda em ~35s
  neste volume — nenhuma infraestrutura de cluster a manter ou pagar
  ociosidade

---

## FinOps — Otimização de Custos

| Prática | Impacto |
|---------|---------|
| Armazenamento em Parquet com compressão Snappy | ~54% menos espaço em disco na tabela `alunos` (99MB em Parquet vs. 214MB no CSV de origem, mesmos ~3,87M registros) — mais leitura seletiva de colunas, que o CSV não permite |
| Particionamento por `ano` | Consultas a um ano não escaneiam o histórico completo |
| Processamento local (pandas) em vez de cluster distribuído | Sem custo de cluster gerenciado (EMR/Dataproc/Databricks) nem capacidade ociosa — justificável enquanto o maior dataset (~3,87M linhas) couber em memória de um processo |
| Fallback CSV para BigQuery | Evita custo de consultas BigQuery repetidas durante desenvolvimento/reexecução — o dado é consultado uma vez e reaproveitado localmente |
| Camada Gold pré-agregada | Elimina reprocessamento de `alunos` (maior tabela) a cada análise — dashboards e notebooks leem apenas os 3 datasets Gold, já pequenos |

**Estimativa de custo mensal:** com o volume atual (~415MB, execução sob
demanda), o caminho BigQuery fica dentro do tier gratuito do Google Cloud
(1TB de consultas/mês) para uso educacional/piloto; o caminho CSV tem custo
de armazenamento desprezível. Uma estimativa de cloud própria (compute +
storage gerenciado) depende do provedor e do volume de produção escolhidos —
não detalhada aqui por não ter sido implantada nesta entrega.

---

## Aplicação em IA

A camada Gold está preparada para alimentar:

- **Modelos preditivos de alfabetização por município** — prever municípios em risco de não atingir a meta 2030 com base em dados históricos e socioeconômicos
- **Clusters de vulnerabilidade educacional** — segmentação de municípios por perfil de risco para priorização de políticas públicas
- **Análise de desigualdade educacional** — identificação de disparidades regionais e fatores determinantes
- **Políticas públicas baseadas em dados** — simulação de cenários de investimento e impacto no indicador

---

## Como Reproduzir

### 1. Pré-requisitos

```bash
pip install -r requirements.txt
```

O pipeline roda **sem nenhuma configuração adicional**, usando os CSVs em
`data/raw/` (fallback). Para usar BigQuery em vez do fallback, copie
`.env.example` para `.env` e preencha `GCP_PROJECT_ID` — **atenção**: é o
Project ID do GCP, não o Billing Account ID da conta de faturamento (ver
comentário em `.env.example`).

### 2. Executar o Pipeline Batch

```bash
python pipeline/batch/01_ingestao_bronze.py
python pipeline/batch/02_processamento_silver.py
python pipeline/batch/03_agregacao_gold.py
```

### 3. Executar a Simulação de Streaming

```bash
python pipeline/streaming/04_simulacao_streaming.py
```

### 4. Validação de Qualidade

```bash
python quality/validacao_dados.py
```

### 5. Explorar via Notebooks (ordem recomendada)

```bash
jupyter lab
```

1. [`notebooks/01_exploracao_dados.ipynb`](notebooks/01_exploracao_dados.ipynb)
2. [`notebooks/02_pipeline_bronze_silver.ipynb`](notebooks/02_pipeline_bronze_silver.ipynb)
3. [`notebooks/03_camada_gold_analytics.ipynb`](notebooks/03_camada_gold_analytics.ipynb)

---

## Monitoramento da Pipeline

Mecanismos de observabilidade implementados (`pipeline/batch/utils.py`):
- Logs estruturados por etapa, com timestamp e nível (Bronze / Silver / Gold)
- Resumo de execução por tabela (sucesso/erro/registros) ao final de cada camada
- Métricas de qualidade por tabela a cada ingestão/transformação (nulos,
  duplicatas, registros) via `log_qualidade()`
- Tempo de execução por função (decorator `@timer`) — visível nos logs

**Não implementado nesta entrega** (item opcional do desafio): alertas
externos (e-mail/Slack/PagerDuty) e um dashboard de observabilidade dedicado
— os logs estruturados já dão a rastreabilidade necessária para o volume
atual, mas não substituem alertas ativos em um cenário de produção real.
