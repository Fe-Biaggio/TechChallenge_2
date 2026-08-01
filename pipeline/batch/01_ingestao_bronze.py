"""
Ingestão Batch — Camada Bronze

Consulta as tabelas do Indicador Criança Alfabetizada na plataforma
Base dos Dados (via BigQuery) e persiste os dados brutos na Bronze
em formato Parquet particionado por ano e UF.

Dataset de origem:
    basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72

Pré-requisitos:
    1. Projeto ativo no Google Cloud com BigQuery habilitado
    2. GCP_PROJECT_ID configurado no .env
    3. Autenticação GCP:
       - Opção A: gcloud auth application-default login
       - Opção B: GOOGLE_APPLICATION_CREDENTIALS=/caminho/service_account.json no .env

Uso:
    python pipeline/batch/01_ingestao_bronze.py          # todos os anos
    python pipeline/batch/01_ingestao_bronze.py 2023     # somente 2023
    python pipeline/batch/01_ingestao_bronze.py 2022 2023  # range de anos
"""
import sys
from datetime import datetime

import basedosdados as bd
import pandas as pd

from pipeline.batch.config import (
    GCP_PROJECT_ID,
    BDD_PROJECT,
    BRONZE_DIR,
    TABELAS,
)
from pipeline.batch.utils import (
    get_logger,
    salvar_bronze,
    log_qualidade,
    resumo_execucao,
    timer,
)

logger = get_logger(__name__)


def validar_ambiente() -> bool:
    """Verifica se as dependências mínimas estão configuradas."""
    if not GCP_PROJECT_ID:
        logger.error(
            "GCP_PROJECT_ID não configurado.\n"
            "  1. Copie .env.example para .env\n"
            "  2. Preencha GCP_PROJECT_ID=seu-projeto-gcp\n"
            "  Documentação: cloud.google.com/resource-manager/docs/creating-managing-projects"
        )
        return False
    logger.info(f"Projeto GCP: {GCP_PROJECT_ID}")
    return True


def _construir_query(dataset: str, table: str, anos: list = None) -> str:
    """Monta a query BigQuery com filtro de anos opcional."""
    tabela_bq = f"`{BDD_PROJECT}.{dataset}.{table}`"

    if anos:
        anos_str = ", ".join(str(a) for a in anos)
        filtro = f"WHERE ano IN ({anos_str})"
    else:
        filtro = ""

    return f"SELECT * FROM {tabela_bq} {filtro} ORDER BY 1"


@timer
def ingerir_tabela(
    nome_tabela: str,
    config: dict,
    anos: list = None,
) -> pd.DataFrame:
    """Executa a query no BigQuery e retorna o resultado como DataFrame."""
    dataset = config["dataset"]
    table = config["table"]
    query = _construir_query(dataset, table, anos=anos)

    logger.info(f"Consultando: {BDD_PROJECT}.{dataset}.{table}")
    logger.debug(f"Query:\n{query}")

    df = bd.read_sql(query, billing_project_id=GCP_PROJECT_ID)
    log_qualidade(df, nome_tabela)
    return df


@timer
def executar_ingestao_bronze(anos: list = None) -> dict:
    """
    Executa a ingestão completa de todas as tabelas para a camada Bronze.

    Args:
        anos: Lista de anos para filtrar (ex: [2022, 2023]).
              Se None, ingere todos os anos disponíveis.

    Returns:
        Dicionário com status de cada tabela ingerida.
    """
    resultados = {}
    inicio = datetime.now()

    logger.info("=" * 60)
    logger.info(f"INGESTÃO BRONZE — {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Anos: {anos if anos else 'todos'}")
    logger.info(f"Tabelas: {list(TABELAS.keys())}")
    logger.info("=" * 60)

    for nome_tabela, config in TABELAS.items():
        anos_filtro = anos if "ano" in config.get("particoes", []) else None
        try:
            df = ingerir_tabela(nome_tabela, config, anos=anos_filtro)
            caminho = salvar_bronze(df, nome_tabela, BRONZE_DIR, config["particoes"])
            resultados[nome_tabela] = {
                "status": "sucesso",
                "registros": len(df),
                "caminho": str(caminho),
            }
        except Exception as e:
            nivel = logger.error if config.get("obrigatoria") else logger.warning
            nivel(f"Erro ao ingerir '{nome_tabela}': {e}")
            resultados[nome_tabela] = {"status": "erro", "erro": str(e)}

    fim = datetime.now()
    logger.info(f"INGESTÃO BRONZE CONCLUÍDA em {(fim - inicio).seconds}s")
    resumo_execucao(resultados)
    return resultados


if __name__ == "__main__":
    if not validar_ambiente():
        sys.exit(1)

    # Argumentos: anos opcionais (ex: python 01_ingestao_bronze.py 2022 2023)
    anos_arg = [int(a) for a in sys.argv[1:] if a.isdigit()] or None

    resultado = executar_ingestao_bronze(anos=anos_arg)

    erros = [k for k, v in resultado.items() if v.get("status") == "erro"]
    sys.exit(1 if any(TABELAS[k].get("obrigatoria") for k in erros) else 0)
