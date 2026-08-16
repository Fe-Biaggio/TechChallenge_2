# Arquitetura da Pipeline

Diagramas de referência para o Tech Challenge — Pipeline Híbrido para Análise
da Alfabetização no Brasil. Renderizam nativamente no GitHub (blocos ` ```mermaid `).

## 1. Arquitetura geral (Medalhão + ingestão híbrida)

```mermaid
flowchart TD
    subgraph Fontes["Fontes externas — Base dos Dados / INEP"]
        BQ[("BigQuery\nbr_inep_avaliacao_alfabetizacao\nbr_bd_diretorios_brasil")]
        RAW["CSVs exportados\ndata/raw/*.csv"]
    end

    subgraph Batch["Ingestão Batch"]
        ING["01_ingestao_bronze.py\nBigQuery se configurado,\nfallback automático para raw"]
    end

    subgraph Streaming["Ingestão Streaming (simulada)"]
        PROD["producer.py\neventos sintéticos"]
        CONS["consumer.py\nmicro-lotes"]
    end

    subgraph BronzeL["Bronze — dados brutos"]
        BZ[("8 tabelas Parquet\nparticionadas por ano")]
    end

    subgraph SilverL["Silver — limpeza e integração"]
        SV["02_processamento_silver.py"]
        SVT[("indicador_*, meta_*,\ndiretorio_*, alunos,\nalfabetizacao_integrado")]
    end

    subgraph GoldL["Gold — camada analítica"]
        GD["03_agregacao_gold.py"]
        GDT[("indicador_alfabetizacao_municipio\ncomparacao_metas_resultados\nevolucao_temporal")]
    end

    subgraph Qualidade["Qualidade (transversal)"]
        QA["quality/validacao_dados.py\ncompletude · unicidade ·\nintegridade referencial · domínio"]
    end

    subgraph Consumo["Consumo"]
        DASH["Dashboards"]
        STAT["Análises estatísticas"]
        ML["Modelos de ML"]
    end

    BQ -->|"SELECT * (sem transformação)"| ING
    RAW -->|"fallback se BigQuery indisponível"| ING
    ING --> BZ
    PROD --> CONS --> BZ
    BZ --> SV --> SVT
    SVT --> GD --> GDT
    GDT --> DASH & STAT & ML

    QA -.valida.-> BZ
    QA -.valida.-> SVT
    QA -.valida.-> GDT
```

**Por que ingestão híbrida com fallback, não só BigQuery:** o BigQuery exige um
projeto GCP com faturamento ativo e credenciais válidas — indisponível neste
ambiente de desenvolvimento. `01_ingestao_bronze.py` tenta o BigQuery primeiro
(produção) e cai automaticamente para os CSVs de `data/raw/` (o mesmo dado,
exportado manualmente uma vez) sem exigir intervenção manual — o restante da
pipeline (Silver, Gold, qualidade) roda idêntico em ambos os caminhos.

## 2. Fluxo de dados — de que tabela vem cada join da Gold

```mermaid
flowchart LR
    subgraph Resultado["Resultado (indicador)"]
        IM["indicador_municipio\n(ano, id_municipio, rede)"]
        IU["indicador_uf\n(ano, sigla_uf, rede)"]
    end

    subgraph Meta["Meta (formato long, pós-melt)"]
        MB["meta_brasil\n(ano_meta)"]
        MU["meta_uf\n(sigla_uf, ano_meta)"]
        MM["meta_municipio\n(id_municipio, ano_meta)"]
    end

    subgraph Diretorios["Diretórios de referência"]
        DM["diretorio_municipio\nid_municipio → nome, sigla_uf"]
        DU["diretorio_uf\nsigla_uf → nome, regiao"]
    end

    IM -- "join id_municipio" --> DM
    IM -- "join id_municipio + ano\n(rede = Municipal)" --> MM
    DM -- "sigla_uf" --> DU
    IU -- "join sigla_uf + ano\n(rede = Pública)" --> MU

    IM --> INTEGRADO["alfabetizacao_integrado\n1 linha por (ano, município)"]
    DM --> INTEGRADO
    MM --> INTEGRADO

    INTEGRADO --> G1["Gold: indicador_alfabetizacao_municipio"]
    IU --> G2["Gold: comparacao_metas_resultados"]
    MU --> G2
    IM --> G3["Gold: evolucao_temporal"]
    MB -.resultado nacional derivado.-> G3
```

**Decisão de modelagem que este diagrama evidencia:** `indicador_municipio` e
`indicador_uf` trazem mais de uma linha por (ano, entidade) — uma por rede de
ensino (Municipal, Estadual, Pública=agregado, ...). A Silver escolhe, para
cada município/UF, o resultado "headline" pela rede de maior prioridade
disponível (Pública > Municipal > Estadual > Privada > Total), mas a
**comparação com a meta usa especificamente a rede em que a meta foi
definida** — Municipal para `meta_municipio`, Pública para `meta_uf`/`meta_brasil`
— para não misturar escopos diferentes no cálculo do gap. Não existe uma tabela
de resultado nacional própria no dataset de origem; a série Brasil é derivada
da coluna `taxa_alfabetizacao` já presente em `meta_brasil` (o resultado
observado no ano de referência de cada vintage da meta).

Ver `notebooks/02_pipeline_bronze_silver.ipynb` para um exemplo executado com
dado real dessa decisão (caso "Americana/SP").
