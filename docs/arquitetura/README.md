# Arquitetura da Pipeline

Diagramas de referência para o Tech Challenge — Pipeline Híbrido para Análise
da Alfabetização no Brasil. Renderizam nativamente no GitHub (blocos ` ```mermaid `).

## 1. Arquitetura geral (Medalhão + ingestão híbrida)

```mermaid
flowchart TD
    subgraph Fontes["Fontes externas — Base dos Dados / INEP / IPEA-PNUD-FJP"]
        BQ[("BigQuery\nbr_inep_avaliacao_alfabetizacao\nbr_bd_diretorios_brasil")]
        RAW["CSVs exportados\ndata/raw/*.csv"]
        IDHM["Atlas do Desenv. Humano\ndata/raw/atlas_..._municipio.csv\n(enriquecimento externo)"]
    end

    subgraph Batch["Ingestão Batch"]
        ING["01_ingestao_bronze.py\nBigQuery se configurado,\nfallback automático para raw"]
    end

    subgraph Streaming["Ingestão Streaming (simulada)"]
        PROD["producer.py\neventos sintéticos"]
        CONS["consumer.py\nmicro-lotes"]
    end

    subgraph BronzeL["Bronze — dados brutos"]
        BZ[("9 tabelas batch\nparticionadas por ano")]
        BZS[("2 tabelas streaming\nappend-only")]
        BZR[("streaming_rejeitados\naudita eventos inválidos")]
    end

    subgraph SilverL["Silver — limpeza e integração"]
        SV["02_processamento_silver.py"]
        SVT[("11 tabelas batch tratadas +\nalfabetizacao_integrado (com IDHM)")]
        SVS[("indicador_streaming\nmeta_streaming\n(linhagem separada)")]
    end

    subgraph GoldL["Gold — camada analítica"]
        GD["03_agregacao_gold.py"]
        GDT[("indicador_alfabetizacao_municipio (+IDHM)\ncomparacao_metas_resultados\nevolucao_temporal")]
        GDS[("monitoramento_streaming")]
    end

    subgraph Qualidade["Qualidade + Alertas (transversal)"]
        QA["quality/validacao_dados.py\ncompletude · unicidade ·\nintegridade referencial · domínio"]
        AL["pipeline/monitoring/alertas.py\ndata/monitoramento/alertas.jsonl"]
    end

    subgraph Consumo["Consumo"]
        DASH["Dashboard analítico\n(reports/dashboard.html)"]
        STAT["Análises estatísticas"]
        ML["Modelos de ML"]
    end

    BQ -->|"SELECT * (sem transformação)"| ING
    RAW -->|"fallback se BigQuery indisponível"| ING
    IDHM -->|"ingestão direta (sem BigQuery configurado)"| ING
    ING --> BZ
    PROD --> CONS --> BZS
    CONS -.evento inválido.-> BZR
    BZ --> SV --> SVT
    BZS --> SV --> SVS
    SVT --> GD --> GDT
    SVS --> GD --> GDS
    GDT --> DASH & STAT & ML

    QA -.valida.-> BZ
    QA -.valida.-> SVT
    QA -.valida.-> GDT
    QA -.dispara em falha.-> AL
    ING -.dispara em falha.-> AL
    CONS -.dispara se houver rejeitados.-> AL
```

**Por que ingestão híbrida com fallback, não só BigQuery:** o BigQuery exige um
projeto GCP com faturamento ativo e credenciais válidas — indisponível neste
ambiente de desenvolvimento. `01_ingestao_bronze.py` tenta o BigQuery primeiro
(produção) e cai automaticamente para os CSVs de `data/raw/` (o mesmo dado,
exportado manualmente uma vez) sem exigir intervenção manual — o restante da
pipeline (Silver, Gold, qualidade) roda idêntico em ambos os caminhos.

**Por que o streaming vira uma linhagem separada, não se junta ao batch:** os
eventos de streaming são sintéticos (gerados por `producer.py` para demonstrar
o padrão de ingestão incremental), não o resultado oficial do INEP. Misturá-los
em `alfabetizacao_integrado` contaminaria a análise real com números fabricados
— por isso seguem até a Gold só como `monitoramento_streaming` (volume,
cobertura, latência), nunca como dado analítico.

**Por que eventos de streaming inválidos são auditados, não descartados:**
antes, um evento que falhasse na validação de schema só aparecia no log da
execução ao vivo — sem persistência, não dava para investigar depois. Agora
`consumer.py` grava o payload bruto + motivo em `streaming_rejeitados`
(Bronze), e dispara um alerta (`pipeline/monitoring/alertas.py`) ao final da
execução se houve algum. O alerta fica em `data/monitoramento/alertas.jsonl`,
junto com os disparados por falhas de ingestão/processamento/agregação e por
alertas de qualidade de dados — um log append-only consultável depois que a
execução termina, sem depender de um canal externo (e-mail/Slack) configurado.

**Por que o IDHM entra como enriquecimento estático, não por ano:** o Índice
de Desenvolvimento Humano Municipal depende do Censo Demográfico do IBGE
(decenal) — a vintage mais recente com apuração oficial é 2010. Diferente do
indicador de alfabetização (anual), o IDHM não tem uma linha por ano; por isso
entra como LEFT JOIN por `id_municipio` (sempre a vintage 2010) em
`alfabetizacao_integrado`, cobrindo 99,9% dos municípios avaliados.

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

    subgraph Externo["Enriquecimento externo (estático)"]
        IDHM["idhm_municipio\nid_municipio → IDHM (vintage 2010)"]
    end

    IM -- "join id_municipio" --> DM
    IM -- "join id_municipio + ano\n(rede = Municipal)" --> MM
    DM -- "sigla_uf" --> DU
    IU -- "join sigla_uf + ano\n(rede = Pública)" --> MU

    IM --> INTEGRADO["alfabetizacao_integrado\n1 linha por (ano, município)"]
    DM --> INTEGRADO
    MM --> INTEGRADO
    IDHM -- "join id_municipio\n(sem filtro de ano)" --> INTEGRADO

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
