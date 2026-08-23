"""
Configurações centrais da pipeline.
Todas as constantes e parâmetros são definidos aqui.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ─── Diretórios ───────────────────────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

MONITORAMENTO_DIR = DATA_DIR / "monitoramento"

for _d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, MONITORAMENTO_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ─── Google Cloud / Base dos Dados ────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# NOTA: GCP_PROJECT_ID precisa ser o Project ID do Google Cloud (ex: "meu-projeto-123"),
# usado como billing_project_id nas consultas ao BigQuery. Não confundir com o ID da
# conta de faturamento (Billing Account ID, formato "XXXX-XXXX-XXXX") — este último
# identifica o método de pagamento vinculado à sua conta GCP, mas não é aceito pela
# basedosdados como billing_project_id. É preciso criar/selecionar um projeto GCP e
# vincular essa conta de faturamento a ele; o ID desse projeto é o que vai no .env.

# ─── Base dos Dados — BigQuery ────────────────────────────────────────────────
# Projeto público do Base dos Dados no BigQuery
BDD_PROJECT = "basedosdados"

# Dataset "Avaliação da Alfabetização" (INEP) — indicador + metas
# URL de referência: basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72
BDD_DATASET_ALFABETIZACAO = "br_inep_avaliacao_alfabetizacao"

# Dataset de diretórios de referência (municípios e UFs)
BDD_DATASET_DIRETORIOS = "br_bd_diretorios_brasil"

# ─── Rede de ensino — mapa de códigos ─────────────────────────────────────────
# As tabelas de resultado (indicador_municipio/indicador_uf) trazem "rede" como
# código numérico. O dicionário oficial do BigQuery (br_inep_avaliacao_alfabetizacao
# .dicionario) não está acessível sem credenciais GCP; o mapeamento abaixo foi inferido
# a partir da convenção padrão do INEP e confirmado empiricamente nos dados brutos:
# a tabela "alunos" (microdados, uma rede real por aluno) só contém os códigos 2, 3 e 4,
# enquanto "indicador_municipio/uf" também trazem 0 e 5 (agregados só existem no nível
# agregado, nunca no aluno individual) — consistente com 0/5 sendo somatórios e não
# redes reais. Validar contra o dicionário oficial se/quando houver acesso ao BigQuery.
REDE_MAP = {
    0: "Total",       # todas as redes (pública + privada) combinadas
    1: "Federal",
    2: "Estadual",
    3: "Municipal",
    4: "Privada",
    5: "Pública",      # agregado federal + estadual + municipal
}

# ─── Tabelas da pipeline ──────────────────────────────────────────────────────
# Cada entrada define: tabela de origem no BigQuery, arquivo local de fallback
# (data/raw/), colunas de particionamento na Bronze e obrigatoriedade.
TABELAS = {
    "indicador_municipio": {
        "bq_table": f"{BDD_DATASET_ALFABETIZACAO}.municipio",
        "raw_file": "br_inep_avaliacao_alfabetizacao_municipio.csv",
        "particoes": ["ano"],
        "descricao": "Resultado (taxa de alfabetização) por município, série e rede",
        "obrigatoria": True,
    },
    "indicador_uf": {
        "bq_table": f"{BDD_DATASET_ALFABETIZACAO}.uf",
        "raw_file": "br_inep_avaliacao_alfabetizacao_uf.csv",
        "particoes": ["ano"],
        "descricao": "Resultado (taxa de alfabetização) por UF, série e rede",
        "obrigatoria": True,
    },
    "meta_brasil": {
        "bq_table": f"{BDD_DATASET_ALFABETIZACAO}.meta_alfabetizacao_brasil",
        "raw_file": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
        "particoes": ["ano"],
        "descricao": "Metas nacionais de alfabetização (trajetória 2024-2030)",
        "obrigatoria": True,
    },
    "meta_uf": {
        "bq_table": f"{BDD_DATASET_ALFABETIZACAO}.meta_alfabetizacao_uf",
        "raw_file": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
        "particoes": ["ano"],
        "descricao": "Metas estaduais de alfabetização (trajetória 2024-2030)",
        "obrigatoria": True,
    },
    "meta_municipio": {
        "bq_table": f"{BDD_DATASET_ALFABETIZACAO}.meta_alfabetizacao_municipio",
        "raw_file": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
        "particoes": ["ano"],
        "descricao": "Metas municipais de alfabetização (trajetória 2024-2030)",
        "obrigatoria": True,
    },
    "alunos": {
        "bq_table": f"{BDD_DATASET_ALFABETIZACAO}.alunos",
        "raw_file": "br_inep_avaliacao_alfabetizacao_alunos.csv",
        "particoes": ["ano"],
        "descricao": "Microdados individuais de desempenho dos alunos avaliados",
        "obrigatoria": True,
    },
    "diretorio_municipio": {
        "bq_table": f"{BDD_DATASET_DIRETORIOS}.municipio",
        "raw_file": "br_bd_diretorios_brasil_municipio.csv",
        "particoes": [],
        "descricao": "Diretório de municípios brasileiros (chave de relacionamento)",
        "obrigatoria": True,
    },
    "diretorio_uf": {
        "bq_table": f"{BDD_DATASET_DIRETORIOS}.uf",
        "raw_file": "br_bd_diretorios_brasil_uf.csv",
        "particoes": [],
        "descricao": "Diretório de unidades federativas (chave de relacionamento)",
        "obrigatoria": True,
    },
}

# ─── Enriquecimento externo (opcional) ────────────────────────────────────────
# Atlas do Desenvolvimento Humano no Brasil (IDHM) — IPEA / PNUD / Fundação João
# Pinheiro. Fonte oficial: basedosdados.org/dataset/cbfc7253-089b-44e2-8825-755e1419efc8
# (dataset "Atlas do Desenvolvimento Humano"), sem billing_project_id configurado
# nesta entrega para consultar via BigQuery. O CSV usado aqui é uma redistribuição
# em formato tabular do mesmo dado oficial (censos 1991/2000/2010), mantida em
# https://github.com/mauriciocramos/IDHM — mesmo código IBGE de 7 dígitos
# (Codmun7) usado como id_municipio no restante da pipeline.
#
# Não segue o padrão bq_table/raw_file de TABELAS acima porque não há um
# caminho BigQuery configurado para este dataset nesta entrega — é ingerido
# só pelo fallback CSV (ver ingerir_idhm_municipio() em 01_ingestao_bronze.py).
IDHM_RAW_FILE = "atlas_desenvolvimento_humano_municipio.csv"
# 2010 é o último censo demográfico com apuração oficial do IDHM municipal
# (o índice depende de dados censitários decenais do IBGE).
IDHM_ANO_REFERENCIA = 2010

# ─── Streaming ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_INDICADORES = "alfabetizacao.indicadores"
KAFKA_TOPIC_METAS = "alfabetizacao.metas"
KAFKA_CONSUMER_GROUP = "pipeline-bronze"

# ─── Controle de execução ────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
COMPRESSAO_PARQUET = "snappy"
