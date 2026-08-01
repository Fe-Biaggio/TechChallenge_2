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

A pipeline segue a **Arquitetura Medalhão** com três camadas:

```
Fontes Externas
      │
      ▼
┌─────────────┐     Batch      ┌──────────────┐
│  Base dos   │ ─────────────► │    BRONZE    │  Dados brutos, histórico preservado
│   Dados     │                └──────┬───────┘
│  (INEP,     │                       │
│  IBGE, etc) │     Streaming         ▼
│             │ ─────────────► ┌──────────────┐
└─────────────┘                │    SILVER    │  Limpeza, padronização, integração
                               └──────┬───────┘
                                      │
                                      ▼
                               ┌──────────────┐
                               │     GOLD     │  Datasets analíticos prontos
                               └──────┬───────┘
                                      │
                          ┌───────────┼───────────┐
                          ▼           ▼            ▼
                      Dashboard   Análise      Modelos ML
```

### Bronze Layer — Dados Brutos

- Ingestão das fontes sem transformações significativas
- Histórico completo preservado
- Formato: Parquet particionado por UF e ano

### Silver Layer — Dados Tratados

Transformações aplicadas:
- Limpeza e tratamento de valores ausentes
- Padronização de nomes e tipos
- Normalização de chaves de relacionamento
- Validação de consistência entre tabelas
- **Integração das bases** (metas + municípios + desempenho + dados de alunos)

### Gold Layer — Camada Analítica

Datasets prontos para consumo:
- Indicador de alfabetização por município
- Comparação entre metas e resultados alcançados
- Evolução temporal do indicador por UF e município
- Preparado para dashboards, análises estatísticas e treinamento de modelos de ML

---

## Fontes de Dados

| Entidade | Descrição | Fonte |
|----------|-----------|-------|
| UF | Unidades federativas | Base dos Dados |
| Meta Alfabetização Brasil | Meta nacional do indicador | Base dos Dados |
| Meta Alfabetização por UF | Metas por estado | Base dos Dados |
| Meta Alfabetização por Município | Metas municipais | Base dos Dados |
| Município | Dados territoriais municipais | Base dos Dados / IBGE |
| Dados de alunos | Microdados educacionais individuais | Base dos Dados |

**Dados externos opcionais para enriquecimento:**
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
│   │   ├── 01_ingestao_bronze.py          # Ingestão batch das fontes → Bronze
│   │   ├── 02_processamento_silver.py     # Transformações Bronze → Silver
│   │   └── 03_agregacao_gold.py           # Agregações Silver → Gold
│   └── streaming/
│       └── 04_simulacao_streaming.py      # Simulação de eventos em tempo real
├── data/
│   ├── bronze/                            # Dados brutos (Parquet)
│   ├── silver/                            # Dados tratados e integrados (Parquet)
│   └── gold/                              # Datasets analíticos (Parquet)
├── quality/
│   └── validacao_dados.py                 # Scripts de qualidade e validação
├── notebooks/
│   ├── 01_exploracao_dados.ipynb          # Análise exploratória das fontes
│   ├── 02_pipeline_bronze_silver.ipynb    # Demonstração das transformações
│   └── 03_camada_gold_analytics.ipynb    # Análise e visualização da camada Gold
├── docs/
│   └── arquitetura/                       # Diagramas e documentação técnica
├── reports/
│   └── figures/                           # Visualizações geradas pelos notebooks
├── requirements.txt                       # Dependências Python
└── README.md
```

---

## Metodologia

### 1. Ingestão Batch ([`pipeline/batch/`](pipeline/batch/))
Processamento periódico para ingestão de dados históricos:
- Dados de metas educacionais (nacionais, estaduais, municipais)
- Dados territoriais de municípios
- Microdados de desempenho de alunos

### 2. Ingestão Streaming ([`pipeline/streaming/`](pipeline/streaming/))
Simulação de eventos em tempo quase real:
- Atualização de indicadores
- Novas medições de desempenho
- Atualização de metas ou resultados

### 3. Qualidade de Dados ([`quality/validacao_dados.py`](quality/validacao_dados.py))
Mecanismos de validação incluídos:
- Verificação de duplicidade
- Detecção de valores ausentes
- Validação de chaves de relacionamento
- Consistência entre tabelas

---

## Tecnologias Utilizadas

| Tecnologia | Uso | Justificativa |
|------------|-----|---------------|
| Python 3.10+ | Pipeline principal | Ecossistema rico para dados |
| Apache Spark / PySpark | Processamento distribuído | Escalabilidade para grandes volumes |
| Apache Kafka | Streaming | Padrão de mercado para eventos em tempo real |
| Parquet | Formato de armazenamento | Compressão eficiente, leitura colunar |
| Delta Lake / Iceberg | Formato das camadas | ACID, time travel, schema evolution |
| Cloud (AWS / GCP / Azure) | Infraestrutura | Escalabilidade e gestão gerenciada |
| pandas, pyarrow | Processamento local | Transformações e validações |
| Great Expectations | Qualidade de dados | Validação declarativa e documentada |

---

## Decisões Arquiteturais

### Batch vs Streaming
- **Batch** para dados históricos de metas e microdados — volumes altos, processamento noturno
- **Streaming** para eventos de atualização de indicadores — baixa latência, processamento incremental

### Data Lake vs Data Warehouse
- Adotamos **Data Lakehouse** (Delta Lake / Iceberg) sobre storage em nuvem — combina a flexibilidade do Data Lake com as garantias ACID do Data Warehouse, sem duplicação de dados

### Custo vs Performance
- Parquet + particionamento por UF/ano reduz o volume lido por query em até 80%
- Processamento Spark sob demanda (serverless / spot instances) em vez de clusters permanentes
- Camada Gold pré-agregada elimina queries ad-hoc caras sobre microdados

---

## FinOps — Otimização de Custos

| Prática | Impacto |
|---------|---------|
| Armazenamento em Parquet com compressão Snappy | Redução de ~70% no volume vs CSV |
| Particionamento por UF e ano | Evita full-scans desnecessários |
| Processamento Spot / Preemptível | Redução de até 60-70% no custo de compute |
| Camada Gold pré-computada | Elimina reprocessamento em cada análise |
| Auto-scaling do cluster | Sem capacidade ociosa em períodos sem carga |
| Lifecycle policies no storage | Move dados antigos para armazenamento frio (cold/archive) |

**Estimativa de custo mensal:** a detalhar conforme cloud escolhida e volume de dados.

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

Mecanismos de observabilidade implementados:
- Logs estruturados por etapa (Bronze / Silver / Gold)
- Alertas de falha de ingestão
- Métricas de volume de dados processados por execução
- Tempo de execução por camada (latência do pipeline)

---

## Tecnologias Utilizadas

- Python 3.10+
- PySpark / Apache Spark
- Apache Kafka
- Delta Lake / Apache Iceberg
- Great Expectations
- pandas, pyarrow
- Cloud provider (a definir)
