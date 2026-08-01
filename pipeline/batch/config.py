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
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

for _d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# ─── Google Cloud / Base dos Dados ────────────────────────────────────────────
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

# ─── Base dos Dados — BigQuery ────────────────────────────────────────────────
# Projeto público do Base dos Dados no BigQuery
BDD_PROJECT = "basedosdados"

# Dataset do Indicador Criança Alfabetizada (INEP)
# URL de referência: basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72
BDD_DATASET_INDICADOR = "br_inep_indicador_crianca_alfabetizada"

# Dataset de diretórios de referência (municípios e UFs)
BDD_DATASET_DIRETORIOS = "br_bd_diretorios_brasil"

# ─── Tabelas da pipeline ──────────────────────────────────────────────────────
# Cada entrada define: dataset, tabela, colunas de particionamento e descrição
TABELAS = {
    "indicador_municipio": {
        "dataset": BDD_DATASET_INDICADOR,
        "table": "municipio",
        "particoes": ["ano", "sigla_uf"],
        "descricao": "Indicador de alfabetização por município e ano",
        "obrigatoria": True,
    },
    "indicador_uf": {
        "dataset": BDD_DATASET_INDICADOR,
        "table": "uf",
        "particoes": ["ano", "sigla_uf"],
        "descricao": "Indicador de alfabetização por UF e ano",
        "obrigatoria": True,
    },
    "indicador_brasil": {
        "dataset": BDD_DATASET_INDICADOR,
        "table": "brasil",
        "particoes": ["ano"],
        "descricao": "Indicador de alfabetização nacional por ano",
        "obrigatoria": True,
    },
    "municipios": {
        "dataset": BDD_DATASET_DIRETORIOS,
        "table": "municipio",
        "particoes": [],
        "descricao": "Diretório de municípios brasileiros (chave de relacionamento)",
        "obrigatoria": False,
    },
    "ufs": {
        "dataset": BDD_DATASET_DIRETORIOS,
        "table": "uf",
        "particoes": [],
        "descricao": "Diretório de unidades federativas (chave de relacionamento)",
        "obrigatoria": False,
    },
}

# ─── Streaming ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_INDICADORES = "alfabetizacao.indicadores"
KAFKA_TOPIC_METAS = "alfabetizacao.metas"
KAFKA_CONSUMER_GROUP = "pipeline-bronze"

# ─── Controle de execução ────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
COMPRESSAO_PARQUET = "snappy"
