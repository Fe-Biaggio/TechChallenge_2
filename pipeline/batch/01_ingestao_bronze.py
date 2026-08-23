"""
Ingestão Batch — Camada Bronze

Ingestão híbrida: tenta consultar as tabelas do dataset "Avaliação da
Alfabetização" (INEP) na plataforma Base dos Dados via BigQuery. Se o
BigQuery não estiver configurado/acessível, cai automaticamente para os
arquivos CSV brutos em data/raw/ (exportados manualmente do BigQuery).

Em ambos os caminhos os dados são persistidos sem transformação na Bronze,
em Parquet particionado por ano — a Bronze reflete a fonte tal como veio,
sem joins, decodificação ou limpeza (isso acontece na Silver).

Dataset de origem:
    basedosdados.org/dataset/073a39d4-89cf-4068-b1e8-34ed0d9c0b72

Pré-requisitos (caminho BigQuery, opcional):
    1. Projeto ativo no Google Cloud com BigQuery habilitado
    2. GCP_PROJECT_ID configurado no .env — é o Project ID do GCP, não o
       Billing Account ID (formato "XXXX-XXXX-XXXX") da conta de faturamento
    3. Autenticação GCP:
       - Opção A: gcloud auth application-default login
       - Opção B: GOOGLE_APPLICATION_CREDENTIALS=/caminho/service_account.json no .env

Caminho alternativo (fallback, usado quando o BigQuery não está disponível):
    Coloque os CSVs exportados em data/raw/ com os nomes configurados em
    pipeline.batch.config.TABELAS (raw_file).

Uso:
    python pipeline/batch/01_ingestao_bronze.py            # todos os anos
    python pipeline/batch/01_ingestao_bronze.py 2023       # somente 2023
    python pipeline/batch/01_ingestao_bronze.py 2022 2023  # múltiplos anos
"""
import sys
from pathlib import Path

import pandas as pd

# Garante que o root do projeto está no path ao rodar como script direto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.batch.config import (
    GCP_PROJECT_ID,
    BDD_PROJECT,
    BRONZE_DIR,
    RAW_DIR,
    TABELAS,
    IDHM_RAW_FILE,
)
from pipeline.batch.utils import (
    get_logger,
    salvar_bronze,
    log_qualidade,
    resumo_execucao,
    timer,
)
from pipeline.monitoring.alertas import disparar_alerta

logger = get_logger(__name__)

# Colunas lidas como texto — preserva formatação (zeros à esquerda, códigos)
# e evita que o pandas infira tipos numéricos incompatíveis entre fontes.
COLUNAS_TEXTO = [
    "id_municipio", "sigla_uf", "sigla", "id_uf", "id_escola", "id_aluno",
    "caderno", "serie", "id_municipio_6", "id_municipio_tse",
    "id_municipio_rf", "id_municipio_bcb",
]


def _bigquery_disponivel() -> bool:
    if not GCP_PROJECT_ID:
        return False
    try:
        import basedosdados  # noqa: F401
        return True
    except ImportError:
        return False


def validar_ambiente() -> bool:
    """Confirma que há pelo menos um caminho de ingestão viável: BigQuery ou raw."""
    if _bigquery_disponivel():
        logger.info(f"Projeto GCP: {GCP_PROJECT_ID} — ingestão via BigQuery")
        return True

    faltando = [
        cfg["raw_file"] for cfg in TABELAS.values()
        if not (RAW_DIR / cfg["raw_file"]).exists()
    ]
    if faltando:
        logger.error(
            "GCP_PROJECT_ID não configurado e faltam arquivos raw em "
            f"{RAW_DIR}:\n  " + "\n  ".join(faltando) +
            "\nColoque os CSVs no diretório acima ou configure GCP_PROJECT_ID "
            "(Project ID, não Billing Account ID) no .env."
        )
        return False

    logger.info(f"GCP_PROJECT_ID não configurado — ingestão via arquivos raw em {RAW_DIR}")
    return True


def _ingerir_bigquery(config: dict, anos: list = None) -> pd.DataFrame:
    import basedosdados as bd

    tabela_bq = f"`{BDD_PROJECT}.{config['bq_table']}`"
    filtro = ""
    if anos and "ano" in config.get("particoes", []):
        anos_str = ", ".join(str(a) for a in anos)
        filtro = f"WHERE ano IN ({anos_str})"

    query = f"SELECT * FROM {tabela_bq} {filtro}"
    logger.info(f"[BigQuery] Consultando: {BDD_PROJECT}.{config['bq_table']}")
    logger.debug(f"Query:\n{query}")
    return bd.read_sql(query, billing_project_id=GCP_PROJECT_ID)


def _ingerir_raw(config: dict, anos: list = None) -> pd.DataFrame:
    caminho = RAW_DIR / config["raw_file"]
    if not caminho.exists():
        raise FileNotFoundError(
            f"Arquivo raw não encontrado: {caminho}\n"
            f"  Exporte a tabela '{config['bq_table']}' do Base dos Dados para este "
            f"caminho, ou configure GCP_PROJECT_ID no .env para ingestão via BigQuery."
        )

    colunas = pd.read_csv(caminho, nrows=0).columns
    dtype_hints = {c: "str" for c in colunas if c in COLUNAS_TEXTO}

    # Os exports do Base dos Dados usados neste projeto vêm em Latin-1/cp1252,
    # não UTF-8. O parser C do pandas substitui bytes inválidos por '�'
    # silenciosamente mesmo com encoding_errors="strict" (não propaga o erro),
    # então a detecção é feita lendo os bytes brutos e tentando decodificar
    # como UTF-8 estrito fora do pandas.
    with open(caminho, "rb") as f:
        bruto = f.read()
    try:
        bruto.decode("utf-8")
        encoding = "utf-8"
    except UnicodeDecodeError:
        encoding = "cp1252"
    del bruto

    logger.info(f"[Raw] Lendo: {caminho.name} (encoding={encoding})")
    df = pd.read_csv(caminho, dtype=dtype_hints, encoding=encoding)

    if anos and "ano" in df.columns:
        df = df[df["ano"].isin(anos)]

    return df


@timer
def ingerir_tabela(nome_tabela: str, config: dict, anos: list = None) -> tuple:
    """
    Ingere uma tabela tentando BigQuery primeiro; em caso de falha ou
    indisponibilidade, cai para o arquivo raw local correspondente.

    Returns:
        (DataFrame, fonte) — fonte é "bigquery" ou "raw"
    """
    if _bigquery_disponivel():
        try:
            df = _ingerir_bigquery(config, anos=anos)
            log_qualidade(df, nome_tabela)
            return df, "bigquery"
        except Exception as e:
            logger.warning(f"[BigQuery] Falhou para '{nome_tabela}' ({e}) — usando fallback raw")

    df = _ingerir_raw(config, anos=anos)
    log_qualidade(df, nome_tabela)
    return df, "raw"


def ingerir_idhm_municipio() -> pd.DataFrame:
    """
    Ingestão do enriquecimento externo opcional (IDHM municipal — ver
    IDHM_RAW_FILE em config.py). Não tem caminho BigQuery configurado nesta
    entrega, só o CSV; se o arquivo não estiver em data/raw/, o chamador
    trata como tabela ausente (mesmo padrão das tabelas de streaming).
    """
    caminho = RAW_DIR / IDHM_RAW_FILE
    if not caminho.exists():
        raise FileNotFoundError(
            f"Arquivo de enriquecimento externo não encontrado: {caminho} "
            "(opcional — a pipeline roda normalmente sem ele)"
        )
    df = pd.read_csv(caminho, dtype={"Codmun7": "str"})
    log_qualidade(df, "idhm_municipio")
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

    logger.info("=" * 60)
    logger.info("INGESTÃO BRONZE")
    logger.info(f"Anos: {anos if anos else 'todos'}")
    logger.info(f"Tabelas: {list(TABELAS.keys())}")
    logger.info("=" * 60)

    for nome_tabela, config in TABELAS.items():
        try:
            df, fonte = ingerir_tabela(nome_tabela, config, anos=anos)
            caminho = salvar_bronze(df, nome_tabela, BRONZE_DIR, config["particoes"])
            resultados[nome_tabela] = {
                "status": "sucesso",
                "fonte": fonte,
                "registros": len(df),
                "caminho": str(caminho),
            }
        except Exception as e:
            nivel = logger.error if config.get("obrigatoria") else logger.warning
            nivel(f"Erro ao ingerir '{nome_tabela}': {e}")
            resultados[nome_tabela] = {"status": "erro", "erro": str(e)}
            disparar_alerta(
                nivel="ERROR" if config.get("obrigatoria") else "WARNING",
                origem=f"bronze.{nome_tabela}",
                mensagem=f"Falha na ingestão: {e}",
                contexto={"obrigatoria": config.get("obrigatoria", False)},
            )

    # Enriquecimento externo (opcional) — ver ingerir_idhm_municipio() acima
    try:
        df_idhm = ingerir_idhm_municipio()
        caminho = salvar_bronze(df_idhm, "idhm_municipio", BRONZE_DIR, particoes=[])
        resultados["idhm_municipio"] = {
            "status": "sucesso",
            "fonte": "raw_externo",
            "registros": len(df_idhm),
            "caminho": str(caminho),
        }
    except FileNotFoundError as e:
        logger.warning(f"[Bronze] Enriquecimento externo 'idhm_municipio' ausente — pulando ({e})")
        resultados["idhm_municipio"] = {"status": "ausente_na_bronze"}

    logger.info("INGESTÃO BRONZE CONCLUÍDA")
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
