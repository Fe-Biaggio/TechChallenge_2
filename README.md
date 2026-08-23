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
Fontes (BigQuery ou data/raw/*.csv)          Streaming (simulado)         Enriquecimento externo
      │                                    producer.py → consumer.py     (Atlas do Desenv. Humano)
      ▼ Batch                                       │                            │
┌─────────────┐                ┌──────────────┐     │                           │
│ Base dos    │ ─────────────► │    BRONZE    │◄────┘◄──────────────────────────┘
│ Dados       │                │ 9 tabelas    │  streaming_indicadores/
│ (INEP)      │                │ batch + 2    │  streaming_metas +
│             │                │ streaming +  │  streaming_rejeitados (auditoria)
└─────────────┘                │ 1 rejeitados │
                                └──────┬───────┘
                                       │  limpeza, decodificação de "rede",
                                       │  melt de metas, integração + IDHM
                                       ▼
                                ┌──────────────┐
                                │    SILVER    │  13 tabelas — 9 fontes tratadas + integração
                                └──────┬───────┘  + 2 streaming + 1 IDHM (linhagem própria)
                                       │  agregação analítica
                                       ▼
                                ┌──────────────┐
                                │     GOLD     │  4 datasets — 3 analíticos (com IDHM)
                                └──────┬───────┘  + 1 monitoramento de streaming
                                       │
                           ┌───────────┼───────────┐
                           ▼           ▼            ▼
                       Dashboard   Análise      Modelos ML

Qualidade (transversal) + Alertas ──valida──► Bronze / Silver / Gold
  quality/validacao_dados.py              data/monitoramento/alertas.jsonl
```

Qualidade de dados (`quality/validacao_dados.py`) valida as três camadas de
forma transversal — ver seção "Metodologia" abaixo.

**Batch vs. Streaming é o *modo de ingestão* (como o dado chega), não uma
divisão entre camadas** — os dois alimentam a Bronze, e Silver/Gold operam
sobre o que está lá independente da origem. O streaming simulado gera eventos
sintéticos (não o resultado oficial do INEP) para demonstrar o padrão de
ingestão incremental; por isso sua linhagem (`indicador_streaming`,
`meta_streaming` na Silver) fica separada da análise real, e alimenta só o
dataset de **monitoramento** na Gold, não `alfabetizacao_integrado`.

### Bronze Layer — Dados Brutos

- Ingestão sem transformação: `SELECT *` (BigQuery) ou leitura direta do CSV (fallback)
- Histórico completo preservado, um diretório por tabela
- Formato: Parquet particionado por `ano` (tabelas de referência sem partição)
- 9 tabelas batch: `indicador_municipio`, `indicador_uf`, `meta_brasil`, `meta_uf`,
  `meta_municipio`, `alunos` (~3,87M registros), `diretorio_municipio`, `diretorio_uf`,
  `idhm_municipio` (enriquecimento externo — ver seção "Fontes de Dados")
- 2 tabelas streaming (append-only, uma vez que `pipeline/streaming/04_simulacao_streaming.py`
  roda): `streaming_indicadores`, `streaming_metas`
- 1 tabela de auditoria (append-only, só existe se algum evento de streaming falhar
  na validação de schema): `streaming_rejeitados` — guarda o payload bruto e o
  motivo da rejeição (ver `pipeline/streaming/consumer.py`), em vez de descartar
  o evento inválido silenciosamente

### Silver Layer — Dados Tratados

Transformações aplicadas às tabelas batch:
- Limpeza, tipagem e padronização de nomes de colunas
- Decodificação de `rede` (código numérico → rótulo, ver `REDE_MAP` em `config.py`)
- "Despivotagem" das tabelas de meta (`meta_alfabetizacao_2024..2030`, formato
  largo) para long (`ano_meta`, `valor_meta`), mantendo por ano-alvo apenas a
  vintage mais recente publicada
- Normalização de chaves (`id_municipio`, `sigla_uf`)
- **Integração das bases**: resultado municipal + diretório (nome/UF/região) +
  meta municipal + IDHM (2010) → `alfabetizacao_integrado` (uma linha por ano × município)
- Tratamento de nulos **seletivo**: nulos estruturais (ex.: `proporcao_aluno_nivel_*`
  só existe a partir da vintage 2024; `proficiencia` nula para aluno ausente)
  não são imputados — imputar mediana nesses casos distorceria a distribuição real

As tabelas de streaming (`indicador_streaming`, `meta_streaming`) recebem
limpeza/tipagem equivalente, mas ficam fora da integração — ver nota acima.

`idhm_municipio` (enriquecimento externo) recebe limpeza e validação de domínio
(IDHM ∈ [0,1]) e entra na integração como LEFT JOIN por `id_municipio` — ver
"Fontes de Dados" para a fonte e o critério de vintage usado.

### Gold Layer — Camada Analítica

3 datasets analíticos (dado real) + 1 de monitoramento (dado de streaming):
- `indicador_alfabetizacao_municipio` — indicador por município/ano, gap vs.
  meta municipal, gap vs. indicador nacional, faixa de risco, IDHM e seus
  3 componentes (educação, longevidade, renda) quando disponível
- `comparacao_metas_resultados` — metas vs. resultados por UF/ano (rede
  Pública), com % de municípios da UF que atingiram sua meta municipal
- `evolucao_temporal` — série histórica do indicador (Brasil + UF), com
  variação ano a ano
- `monitoramento_streaming` — volume, cobertura territorial e janela temporal
  dos eventos de streaming processados (observabilidade da ingestão, não
  análise educacional)
- Os 3 primeiros ficam prontos para dashboards, análises estatísticas e
  treinamento de modelos de ML

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
| `atlas_desenvolvimento_humano_municipio` | Enriquecimento externo | ano, Codmun7 | IDHM e componentes (educação/longevidade/renda) por município — censos 1991/2000/2010 |

Não existe uma tabela de resultado nacional própria — a série Brasil na Gold é
derivada da coluna `taxa_alfabetizacao` já presente em `meta_alfabetizacao_brasil`.

### Limitação de dados — dicionário oficial indisponível

O dataset `br_inep_avaliacao_alfabetizacao` disponibiliza, no Base dos Dados,
uma tabela `dicionario` que traduziria oficialmente os códigos usados nas
demais tabelas — mas ela só é consultável via BigQuery, que exige projeto GCP
com faturamento ativo (indisponível nesta entrega, ver "BigQuery com fallback
para CSV" em Decisões Arquiteturais). O [link de download do
dicionário](https://basedosdados.org/api/tables/downloadTable?p=YnJfaW5lcF9hdmFsaWFjYW9fYWxmYWJldGl6YWNhbw==&q=ZGljaW9uYXJpbw==&d=ZmFsc2U=&s=)
também não pôde ser usado neste ambiente.

Sem o dicionário oficial, o significado das colunas abaixo foi **inferido** —
por convenção do INEP, pelo contexto do desafio e por validação empírica
cruzada com outras colunas — e não confirmado pela fonte oficial:

| Coluna | Valores brutos | Inferência | Base da inferência |
|---|---|---|---|
| `rede` | 0, 1, 2, 3, 4, 5 | Total, Federal, Estadual, Municipal, Privada, Pública (`REDE_MAP` em `config.py`) | Convenção padrão do INEP; `alunos` só traz 2/3/4 (redes reais por aluno), enquanto `indicador_municipio`/`indicador_uf` também trazem 0/5 — consistente com 0/5 sendo agregados, não redes reais |
| `serie` | sempre `2` | 2º ano do ensino fundamental | Contexto do desafio (o Compromisso Nacional mede alfabetização ao final do 2º ano); não há outro valor na base para comparar |
| `presenca` | 0 / 1 | Aluno ausente / presente na aplicação | `proficiencia` é 100% nula quando `presenca=0` e só 0,04% nula quando `presenca=1` — consistente com "ausente" |
| `preenchimento_caderno` | 0 / 1 | Caderno de prova não preenchido / preenchido | Mesmo padrão de `presenca`: `proficiencia` é 100% nula quando `preenchimento_caderno=0` |
| `alfabetizado` | 0 / 1 | Não atingiu / atingiu o ponto de corte de 743 pontos | Validado empiricamente contra `proficiencia`: **0 de 3.354.661** registros com proficiência preenchida contradizem o corte de 743 |

**Atenção ao usar `alfabetizado` como feature**: dos alunos com
`alfabetizado=0`, 513.338 (~13% da base de 3,87M) não têm `proficiencia`
registrada — ou seja, `alfabetizado=0` mistura "avaliado e abaixo do corte"
com "não avaliado" (mesmo código para os dois casos). Qualquer análise ou
modelo que use esse campo diretamente deve filtrar por `presenca=True`
primeiro — é exatamente por isso que `transformar_alunos()` em
`pipeline/batch/02_processamento_silver.py` não imputa `proficiencia` ausente
como se fosse zero ou mediana.

Se o acesso ao BigQuery for configurado no futuro (`GCP_PROJECT_ID` no
`.env`), a tabela `dicionario` deveria ser consultada para confirmar (ou
corrigir) essas inferências antes de qualquer uso em produção.

### Frequência de atualização da fonte

Mesmo com a pipeline estruturada para ingestão híbrida (batch periódico +
streaming simulado), a frequência real de atualização do indicador é limitada
pela própria fonte, não pela arquitetura: a página do
[dataset no Base dos Dados](https://basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72)
registra a última atualização em **23/09/2025**, mas até esta entrega os
dados publicados ainda cobrem só **2023 e 2024** — nenhuma linha de 2025
apareceu nas tabelas de resultado (`indicador_municipio`, `indicador_uf`) ou
de alunos.

Isso é esperado, não um defeito da pipeline: o Indicador Criança Alfabetizada
vem da Avaliação de Alfabetização do Saeb, aplicada uma vez por ciclo letivo e
consolidada/publicada pelo INEP com meses de defasagem — não é um dado
transacional que muda dia a dia. A atualização de 23/09/2025 registrada na
página do dataset provavelmente reflete manutenção de cadastro/metadados, não
necessariamente novo dado de resultado.

Implicação prática: reexecutar `01_ingestao_bronze.py` com mais frequência do
que a fonte publica não traz dado novo, só custo de consulta desnecessário
(BigQuery cobra por volume escaneado — ver FinOps). O streaming implementado
(ver "Decisões Arquiteturais") já assume essa realidade: é uma simulação que
demonstra o *padrão* de ingestão incremental, não uma tentativa de capturar
eventos reais de uma fonte que, na prática, não emite eventos com essa
cadência. Em produção, o agendamento do batch deveria acompanhar o ciclo de
publicação do INEP (tipicamente anual), não uma cadência diária/horária.

### Enriquecimento externo — Atlas do Desenvolvimento Humano (IDHM)

Implementado nesta entrega. Fonte: **Atlas do Desenvolvimento Humano no
Brasil** (IPEA / PNUD / Fundação João Pinheiro), o mesmo dataset sugerido no
enunciado, disponível como dataset público no
[Base dos Dados](https://basedosdados.org/dataset/cbfc7253-089b-44e2-8825-755e1419efc8).
Sem `GCP_PROJECT_ID` configurado para consultar via BigQuery nesta entrega, os
dados foram obtidos de uma redistribuição tabular do mesmo dado oficial
(censos 1991/2000/2010), mantida em
[github.com/mauriciocramos/IDHM](https://github.com/mauriciocramos/IDHM) —
mesmo código IBGE de 7 dígitos (`Codmun7`) usado como `id_municipio` no
restante da pipeline.

- Arquivo: `data/raw/atlas_desenvolvimento_humano_municipio.csv` (16.695
  linhas — 5.565 municípios × 3 vintagens censitárias)
- Colunas usadas: `IDHM` (índice geral) e seus 3 componentes — `IDHM_E`
  (educação), `IDHM_L` (longevidade), `IDHM_R` (renda)
- **2010 é a vintage de referência** para o enriquecimento — é o último censo
  demográfico com apuração oficial do IDHM municipal (o índice depende do
  Censo Demográfico do IBGE, decenal; não há vintage 2020/2022 pública)
- Entra como enriquecimento **estático** (não varia por ano do indicador de
  alfabetização) via LEFT JOIN por `id_municipio` em `alfabetizacao_integrado`
  e na Gold `indicador_alfabetizacao_municipio`
- Cobertura real obtida: **11.021 de 11.030** registros de
  `alfabetizacao_integrado` (99,9%) — os municípios sem correspondência são
  os poucos criados/desmembrados após o Censo 2010
- Ingestão opcional: se `data/raw/atlas_desenvolvimento_humano_municipio.csv`
  não existir, a pipeline roda normalmente e essas colunas ficam ausentes —
  mesmo padrão de fallback gracioso usado nas tabelas de streaming

**Dados externos sugeridos e não implementados nesta entrega** (caminho de
evolução futura):
- Censo Escolar (INEP) — infraestrutura escolar
- IBGE Censo / PNAD — contexto socioeconômico complementar
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
│   │   ├── 01_ingestao_bronze.py          # BigQuery com fallback → CSV raw → Bronze (+ IDHM externo)
│   │   ├── 02_processamento_silver.py     # Limpeza, melt de metas, integração → Silver
│   │   └── 03_agregacao_gold.py           # Agregações Silver → Gold
│   ├── streaming/
│   │   ├── producer.py / consumer.py      # Fila em memória ou Kafka real (+ persiste rejeitados)
│   │   └── 04_simulacao_streaming.py      # Orquestrador da simulação
│   └── monitoring/
│       └── alertas.py                     # disparar_alerta() — log + data/monitoramento/alertas.jsonl
├── data/
│   ├── raw/                               # CSVs exportados (fallback sem GCP) + metadados + IDHM
│   ├── bronze/                            # Dados brutos (Parquet) — gerado, não versionado
│   ├── silver/                            # Dados tratados e integrados — gerado, não versionado
│   ├── gold/                              # Datasets analíticos — gerado, não versionado
│   └── monitoramento/                     # alertas.jsonl — gerado, não versionado
├── quality/
│   └── validacao_dados.py                 # Regras de qualidade para Bronze/Silver/Gold
├── tests/                                 # pytest — qualidade, alertas, transformações Silver
├── notebooks/
│   ├── 01_exploracao_dados.ipynb          # Análise exploratória das 8 fontes raw
│   ├── 02_pipeline_bronze_silver.ipynb    # Transformações, com o caso real de decisão de escopo
│   └── 03_camada_gold_analytics.ipynb     # Gráficos da Gold + aplicação em IA
├── docs/
│   ├── arquitetura/                       # Diagramas (Mermaid + PNG) e decisões
│   └── [IAST] - Tech Challenge - Fase 2.pdf  # Enunciado original
├── reports/
│   ├── dashboard.html                     # Painel analítico — HTML autocontido, gerado
│   ├── dashboard_template.html            # Template do painel (estrutura/estilo/JS)
│   ├── dashboard_data.json                # Agregados Gold consumidos pelo painel (gerado)
│   └── gerar_dashboard_dados.py           # Gold (+ Silver/alunos) → dashboard.html
├── requirements.txt                       # Dependências Python
├── pytest.ini                             # Configuração dos testes
└── README.md
```

`data/raw/` traz 7 dos 8 CSVs (o de `alunos`, ~214MB, excede o limite de
100MB do GitHub — ver `.gitignore`); os demais ficam versionados para que o
pipeline rode via fallback sem precisar de credenciais GCP.

---

## Metodologia

### 1. Ingestão Batch ([`pipeline/batch/`](pipeline/batch/))
Processamento sob demanda das 8 tabelas fonte (Base dos Dados) + 1 de
enriquecimento externo (IDHM), com fallback automático:
- BigQuery (produção) → CSV local em `data/raw/` se GCP não estiver configurado
- Metas educacionais (nacional, estadual, municipal), resultado (município/UF),
  diretórios de referência, microdados de alunos e IDHM municipal

### 2. Ingestão Streaming ([`pipeline/streaming/`](pipeline/streaming/))
Simulação de eventos em tempo quase real (fila em memória por padrão, ou Kafka
real se configurado), que alimenta a Bronze e segue até Silver/Gold como
linhagem própria (ver nota em "Arquitetura da Solução"):
- Atualização de indicadores municipais
- Atualização/revisão de metas
- Consumer processa em micro-lotes e persiste em Parquet append-only —
  cada execução acrescenta eventos, não substitui os anteriores

### 3. Qualidade de Dados ([`quality/validacao_dados.py`](quality/validacao_dados.py))
Valida as 3 camadas (Bronze, Silver, Gold) — 23 tabelas batch sempre (incluindo
`idhm_municipio`), +5 de streaming (2 na Bronze, 2 na Silver, 1 na Gold) se
`04_simulacao_streaming.py` já rodou, 0 alertas na última execução contra os
dados reais (28/28 tabelas OK):
- Completude (limiar de nulos por coluna, calibrado por tabela — nulos
  estruturais como `proporcao_aluno_nivel_*` pré-2024 não geram alerta)
- Unicidade (duplicatas na chave primária de cada tabela)
- Integridade referencial (`sigla_uf` de `alfabetizacao_integrado` contra `diretorio_uf`)
- Domínio (siglas de UF válidas; `taxa_alfabetizacao` ∈ [0,100]; IDHM e
  componentes ∈ [0,1] via `verificar_dominio_numerico`)

Cada alerta de qualidade dispara `disparar_alerta()` (ver seção "Monitoramento"),
não fica só no log de console.

### 4. Testes automatizados ([`tests/`](tests/))
24 testes `pytest` cobrindo as funções puras da pipeline (não dependem de
dados reais nem de I/O em disco fora de `tmp_path`):
- `test_qualidade.py` — as 5 verificações de `quality/validacao_dados.py`
  (completude, unicidade, domínio de UF, domínio numérico, integridade referencial)
- `test_alertas.py` — persistência e leitura de `data/monitoramento/alertas.jsonl`
- `test_silver_transformacoes.py` — padronização de colunas, decodificação de
  rede, melt de metas (long + vintage mais recente), seleção de rede headline

```bash
python -m pytest tests/ -v
```

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
| pytest | Testes automatizados (`tests/`) | Funções puras da pipeline (qualidade, alertas, transformações Silver) testadas isoladamente, sem depender de `data/raw/` |

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
| Camada Gold pré-agregada | Elimina reprocessamento de `alunos` (maior tabela) na maior parte das análises — as 3 visões de `indicador_alfabetizacao_municipio`/`comparacao_metas_resultados`/`evolucao_temporal` do dashboard leem só a Gold, já pequena. A visão "Aluno" é a exceção deliberada: agrega direto da Silver (não há rollup de aluno pronto na Gold) — mas roda uma vez em `gerar_dashboard_dados.py`, não a cada carregamento da página |

**Estimativa de custo mensal:** com o volume atual (~415MB, execução sob
demanda), o caminho BigQuery fica dentro do tier gratuito do Google Cloud
(1TB de consultas/mês) para uso educacional/piloto; o caminho CSV tem custo
de armazenamento desprezível. Uma estimativa de cloud própria (compute +
storage gerenciado) depende do provedor e do volume de produção escolhidos —
não detalhada aqui por não ter sido implantada nesta entrega.

---

## Aplicação em IA

A camada Gold está preparada para alimentar, já com o enriquecimento
socioeconômico (IDHM) disponível como feature:

- **Modelos preditivos de alfabetização por município** — prever municípios em risco de não atingir a meta 2030, usando IDHM e seus 3 componentes (educação, longevidade, renda) como features socioeconômicas ao lado do histórico do indicador
- **Clusters de vulnerabilidade educacional** — segmentação de municípios (k-means/hierárquico) cruzando `faixa_risco`, `gap_meta_municipal` e IDHM para priorização de políticas públicas
- **Análise de desigualdade educacional** — correlação entre `taxa_alfabetizacao` e IDHM por região/UF, identificando se o gap educacional acompanha o gap de desenvolvimento humano ou é um problema à parte
- **Políticas públicas baseadas em dados** — simulação de cenários de investimento e impacto no indicador, segmentando por faixa de IDHM para direcionar recursos onde o retorno esperado é maior

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

### 3. Executar a Simulação de Streaming (opcional)

```bash
python pipeline/streaming/04_simulacao_streaming.py 30    # roda por 30s
```

Cada execução **acrescenta** eventos (não substitui) — é um log append-only.
Rode Silver e Gold de novo depois para que `indicador_streaming`,
`meta_streaming` e `monitoramento_streaming` reflitam os novos eventos:

```bash
python pipeline/batch/02_processamento_silver.py
python pipeline/batch/03_agregacao_gold.py
```

Sem esse passo, a pipeline roda normalmente — essas 3 tabelas simplesmente
não existem, e tudo o resto (a análise real) fica intacto.

### 4. Validação de Qualidade

```bash
python quality/validacao_dados.py
```

Valida 23 tabelas batch sempre (incluindo o enriquecimento IDHM), +5 de streaming se o passo 3 já rodou.
Alertas de qualidade e falhas de execução ficam persistidos em `data/monitoramento/alertas.jsonl`
(ver `pipeline/monitoring/alertas.py`), além do log de console.

### 5. Rodar os Testes

```bash
python -m pytest tests/ -v
```

24 testes cobrindo qualidade de dados, alertas e transformações da Silver — não dependem de `data/raw/` nem de execução prévia da pipeline.

### 6. Explorar via Notebooks (ordem recomendada)

```bash
jupyter lab
```

1. [`notebooks/01_exploracao_dados.ipynb`](notebooks/01_exploracao_dados.ipynb)
2. [`notebooks/02_pipeline_bronze_silver.ipynb`](notebooks/02_pipeline_bronze_silver.ipynb)
3. [`notebooks/03_camada_gold_analytics.ipynb`](notebooks/03_camada_gold_analytics.ipynb)

### 7. Gerar o Dashboard Analítico

```bash
python reports/gerar_dashboard_dados.py
```

Abra [`reports/dashboard.html`](reports/dashboard.html) direto no navegador — ver seção "Dashboard Analítico" abaixo.

---

## Dashboard Analítico

[`reports/dashboard.html`](reports/dashboard.html) — painel de acompanhamento em HTML
autocontido (sem servidor, sem dependências externas — abre direto no navegador),
gerado a partir da camada Gold. Quatro visões, cada uma com histórico, maiores/piores
e quem mais melhorou/piorou (delta):

- **Brasil** — indicador nacional vs. trajetória oficial da meta (2023–2030)
- **UF** — histórico das 5 melhores/piores UFs, ranking do ano mais recente,
  variação UF a UF entre os dois anos disponíveis
- **Município** — quantos atingiram a própria meta municipal (contagem e %),
  10 melhores/piores municípios (com faixa de risco e IDHM), 10 que mais
  subiram/caíram no indicador
- **Aluno** — participação, % alfabetizados e proficiência média histórica,
  distribuição de proficiência (com a linha do ponto de corte 743), recorte
  por rede de ensino, e ranking/delta por UF calculado direto dos microdados
  (complementar ao ranking oficial da aba UF, que usa o indicador agregado)

O botão **"Qualidade dos Dados"** no canto superior direito abre uma página
separada (com botão para voltar aos indicadores) com:
- Resultado real da última validação (`quality/validacao_dados.py`), por
  camada — tabelas verificadas, OK, com alerta
- A limitação do dicionário oficial indisponível (ver seção "Limitação de
  dados" acima) — quais colunas tiveram o significado inferido, e a evidência
  usada para cada inferência
- As demais limitações conhecidas (vintage do IDHM, streaming sintético,
  alertas sem canal externo)

Para regenerar após rodar a pipeline:
```bash
python reports/gerar_dashboard_dados.py
```
Lê Gold (+ `alunos` da Silver, só para agregados — nenhum microdado individual
é exposto no HTML) e roda a validação de qualidade de dados, escrevendo
`reports/dashboard_data.json` + `reports/dashboard.html` (o JSON é injetado em
`reports/dashboard_template.html`, que contém a estrutura/estilo/JS do painel).

## Monitoramento da Pipeline

Mecanismos de observabilidade implementados:
- Logs estruturados por etapa, com timestamp e nível (Bronze / Silver / Gold),
  `pipeline/batch/utils.py`
- Resumo de execução por tabela (sucesso/erro/pulado/registros) ao final de
  cada camada — distingue erro real de tabela opcional ausente
- Métricas de qualidade por tabela a cada ingestão/transformação (nulos,
  duplicatas, registros) via `log_qualidade()`
- Tempo de execução por função (decorator `@timer`) — visível nos logs
- **Volume e latência da ingestão de streaming** — `monitoramento_streaming`
  na Gold: total de eventos, cobertura territorial distinta, janela temporal
  (primeiro/último evento) e eventos/segundo por tópico, derivados do que foi
  de fato persistido (não é só um log efêmero)
- **Alertas de erro persistidos** — [`pipeline/monitoring/alertas.py`](pipeline/monitoring/alertas.py):
  `disparar_alerta(nivel, origem, mensagem, contexto)` loga no nível
  correspondente **e** grava uma linha JSON em `data/monitoramento/alertas.jsonl`
  (log append-only, consultável depois que a execução termina — diferente do
  log de console, que se perde ao fechar o terminal). Disparado automaticamente em:
  - toda falha de ingestão/processamento/agregação (Bronze, Silver, Gold) — `erro="ERROR"` se a tabela é obrigatória, `"WARNING"` se opcional;
  - toda tabela com alerta de qualidade de dados (`quality/validacao_dados.py`);
  - taxa de eventos de streaming inválidos acima de zero ao final da execução.
- **Eventos de streaming inválidos são auditáveis, não descartados** — o
  consumer grava o payload bruto e o motivo da rejeição em
  `data/bronze/streaming_rejeitados/` (Parquet append-only) em vez de só
  logar e perder o dado, permitindo investigar depois por que um produtor
  enviou um evento incompleto.

**Não implementado nesta entrega** (item opcional do desafio): integração
com um canal de alerta externo real (e-mail/Slack/PagerDuty) e um dashboard
de **observabilidade operacional** (que visualizasse `alertas.jsonl` e
`monitoramento_streaming` como painel de saúde da pipeline) — hoje esses dados
ficam persistidos e consultáveis, mas só via log/arquivo, sem visualização
dedicada. O [dashboard analítico](reports/dashboard.html) (seção abaixo) é um
painel diferente: cobre o **resultado educacional** (indicador, rankings,
metas) a partir da Gold, não a saúde operacional da pipeline.
