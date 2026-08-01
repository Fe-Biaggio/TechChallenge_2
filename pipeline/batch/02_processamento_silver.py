"""
Processamento Silver — Limpeza, Padronização e Integração das Bases

Lê os dados brutos da camada Bronze, aplica as seguintes transformações
e persiste os resultados na camada Silver:

  1. Limpeza e tipagem correta de cada tabela
  2. Tratamento de valores ausentes
  3. Padronização de nomes de colunas e categorias
  4. Normalização de chaves de relacionamento
  5. Validação de consistência entre tabelas
  6. Integração: join das bases formando um dataset analítico unificado

Uso:
    python pipeline/batch/02_processamento_silver.py
    python pipeline/batch/02_processamento_silver.py 2023
"""
import sys
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.batch.config import (
    BRONZE_DIR,
    SILVER_DIR,
    COMPRESSAO_PARQUET,
)
from pipeline.batch.utils import get_logger, ler_bronze, log_qualidade, resumo_execucao, timer

logger = get_logger(__name__)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _salvar_silver(df: pd.DataFrame, nome: str, particoes: list = None) -> None:
    destino = SILVER_DIR / nome
    destino.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)

    if particoes and all(p in df.columns for p in particoes):
        pq.write_to_dataset(
            table,
            root_path=str(destino),
            partition_cols=particoes,
            existing_data_behavior="overwrite_or_ignore",
            compression=COMPRESSAO_PARQUET,
        )
    else:
        pq.write_table(table, destino / "data.parquet", compression=COMPRESSAO_PARQUET)

    mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    logger.info(f"[Silver] {nome}: {len(df):,} registros | {mb:.1f} MB → {destino.relative_to(SILVER_DIR.parent)}")


def _padronizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Remove espaços, converte para snake_case minúsculo."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("ascii")
    )
    return df


def _remover_duplicatas(df: pd.DataFrame, chaves: list, nome: str) -> pd.DataFrame:
    antes = len(df)
    df = df.drop_duplicates(subset=chaves, keep="last")
    removidos = antes - len(df)
    if removidos:
        logger.warning(f"[Silver] {nome}: {removidos} duplicatas removidas (chave: {chaves})")
    return df


def _tratar_ausentes(df: pd.DataFrame, nome: str) -> pd.DataFrame:
    """Preenche nulos com estratégia por tipo de coluna."""
    antes = df.isnull().sum().sum()

    for col in df.select_dtypes(include="number").columns:
        if df[col].isnull().any():
            mediana = df[col].median()
            df[col] = df[col].fillna(mediana)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("NAO_INFORMADO").str.strip()

    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].fillna(False)

    depois = df.isnull().sum().sum()
    if antes > 0:
        logger.info(f"[Silver] {nome}: {antes} nulos tratados → {depois} restantes")
    return df


def _garantir_tipos(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Converte colunas para os tipos esperados, ignorando as ausentes."""
    for col, tipo in schema.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(tipo)
            except (ValueError, TypeError) as e:
                logger.warning(f"  Não foi possível converter '{col}' para {tipo}: {e}")
    return df


# ─── Transformações por tabela ────────────────────────────────────────────────

@timer
def transformar_indicador_municipio(anos: list = None) -> pd.DataFrame:
    """Limpa e padroniza a tabela principal de indicadores municipais."""
    filtros = None
    df = ler_bronze("indicador_municipio", BRONZE_DIR, filtros=filtros)
    df = _padronizar_colunas(df)

    if anos:
        df = df[df["ano"].isin(anos)]
        logger.info(f"[Silver] indicador_municipio: filtrado para anos {anos} → {len(df):,} registros")

    df = _garantir_tipos(df, {
        "ano": "int16",
        "id_municipio": "str",
        "sigla_uf": "str",
    })

    # Normaliza identificadores
    df["id_municipio"] = df["id_municipio"].str.strip().str.zfill(7)
    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()

    # Identifica e sinaliza indicador fora do intervalo válido [0, 100]
    col_ind = next((c for c in df.columns if "indicador" in c or "percentual" in c or "pct" in c), None)
    if col_ind:
        invalidos = (df[col_ind] < 0) | (df[col_ind] > 100)
        if invalidos.any():
            logger.warning(f"[Silver] indicador_municipio: {invalidos.sum()} registros com '{col_ind}' fora de [0,100] — marcados")
            df["flag_indicador_invalido"] = invalidos
        else:
            df["flag_indicador_invalido"] = False

    df = _remover_duplicatas(df, ["ano", "id_municipio"], "indicador_municipio")
    df = _tratar_ausentes(df, "indicador_municipio")
    df["dt_processamento"] = datetime.utcnow().isoformat()

    log_qualidade(df, "indicador_municipio_silver")
    return df


@timer
def transformar_indicador_uf(anos: list = None) -> pd.DataFrame:
    """Limpa e padroniza os indicadores por UF."""
    df = ler_bronze("indicador_uf", BRONZE_DIR)
    df = _padronizar_colunas(df)

    if anos:
        df = df[df["ano"].isin(anos)]

    df = _garantir_tipos(df, {"ano": "int16", "sigla_uf": "str"})
    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()
    df = _remover_duplicatas(df, ["ano", "sigla_uf"], "indicador_uf")
    df = _tratar_ausentes(df, "indicador_uf")
    df["dt_processamento"] = datetime.utcnow().isoformat()

    log_qualidade(df, "indicador_uf_silver")
    return df


@timer
def transformar_indicador_brasil(anos: list = None) -> pd.DataFrame:
    """Limpa e padroniza os indicadores nacionais."""
    df = ler_bronze("indicador_brasil", BRONZE_DIR)
    df = _padronizar_colunas(df)

    if anos:
        df = df[df["ano"].isin(anos)]

    df = _garantir_tipos(df, {"ano": "int16"})
    df = _remover_duplicatas(df, ["ano"], "indicador_brasil")
    df = _tratar_ausentes(df, "indicador_brasil")
    df["dt_processamento"] = datetime.utcnow().isoformat()

    log_qualidade(df, "indicador_brasil_silver")
    return df


@timer
def transformar_municipios() -> pd.DataFrame:
    """Limpa e padroniza o diretório de municípios (tabela de referência)."""
    df = ler_bronze("municipios", BRONZE_DIR)
    df = _padronizar_colunas(df)

    df = _garantir_tipos(df, {"id_municipio": "str", "sigla_uf": "str"})
    df["id_municipio"] = df["id_municipio"].str.strip().str.zfill(7)
    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()

    if "nome" in df.columns:
        df["nome"] = df["nome"].str.strip().str.title()

    df = _remover_duplicatas(df, ["id_municipio"], "municipios")
    df = _tratar_ausentes(df, "municipios")

    log_qualidade(df, "municipios_silver")
    return df


@timer
def transformar_ufs() -> pd.DataFrame:
    """Limpa e padroniza o diretório de UFs (tabela de referência)."""
    df = ler_bronze("ufs", BRONZE_DIR)
    df = _padronizar_colunas(df)

    df = _garantir_tipos(df, {"sigla_uf": "str"})
    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()

    df = _remover_duplicatas(df, ["sigla_uf"], "ufs")
    df = _tratar_ausentes(df, "ufs")

    log_qualidade(df, "ufs_silver")
    return df


# ─── Integração das bases ─────────────────────────────────────────────────────

@timer
def integrar_bases(
    df_municipio: pd.DataFrame,
    df_municipios_ref: pd.DataFrame,
    df_uf: pd.DataFrame,
    df_brasil: pd.DataFrame,
) -> pd.DataFrame:
    """
    Integra as tabelas de indicadores com as de referência (municípios e UFs).

    Resultado: dataset unificado com indicadores municipais enriquecidos
    com nome do município, nome da UF, região e contexto nacional.
    """
    logger.info("[Silver] Iniciando integração das bases...")
    antes = len(df_municipio)

    # Join indicador_municipio ← municipios (nome, região, etc.)
    colunas_municipio = [c for c in df_municipios_ref.columns if c not in df_municipio.columns or c == "id_municipio"]
    df = df_municipio.merge(
        df_municipios_ref[colunas_municipio],
        on="id_municipio",
        how="left",
        suffixes=("", "_ref"),
    )

    # Join ← UFs (nome da UF, região)
    if "sigla_uf" in df.columns and "sigla_uf" in df_uf.columns:
        colunas_uf = [c for c in df_uf.columns if c not in df.columns or c == "sigla_uf"]
        df = df.merge(
            df_uf[colunas_uf],
            on="sigla_uf",
            how="left",
            suffixes=("", "_uf"),
        )

    # Enriquece com indicador nacional do mesmo ano (contexto comparativo)
    col_ind_br = next((c for c in df_brasil.columns if "indicador" in c or "percentual" in c), None)
    if col_ind_br and "ano" in df_brasil.columns:
        df_brasil_reduzido = df_brasil[["ano", col_ind_br]].rename(
            columns={col_ind_br: "indicador_nacional"}
        )
        df = df.merge(df_brasil_reduzido, on="ano", how="left")

    depois = len(df)
    if antes != depois:
        logger.warning(f"[Silver] Integração: {antes} → {depois} registros (diferença por join)")
    else:
        logger.info(f"[Silver] Integração concluída: {depois:,} registros")

    log_qualidade(df, "alfabetizacao_integrado")
    return df


# ─── Orquestrador principal ───────────────────────────────────────────────────

@timer
def executar_processamento_silver(anos: list = None) -> dict:
    """Executa todo o processamento da camada Silver."""
    resultados = {}
    inicio = datetime.now()

    logger.info("=" * 60)
    logger.info(f"PROCESSAMENTO SILVER — {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Anos: {anos if anos else 'todos'}")
    logger.info("=" * 60)

    # ── Passo 1: Transforma cada tabela individualmente ──
    tabelas_individuais = {
        "indicador_municipio": (transformar_indicador_municipio, ["ano", "sigla_uf"]),
        "indicador_uf": (transformar_indicador_uf, ["ano", "sigla_uf"]),
        "indicador_brasil": (transformar_indicador_brasil, ["ano"]),
        "municipios": (lambda: transformar_municipios(), []),
        "ufs": (lambda: transformar_ufs(), []),
    }

    dfs = {}
    for nome, (fn, particoes) in tabelas_individuais.items():
        try:
            df = fn(anos) if nome.startswith("indicador") else fn()
            _salvar_silver(df, nome, particoes)
            dfs[nome] = df
            resultados[nome] = {"status": "sucesso", "registros": len(df)}
        except FileNotFoundError:
            logger.warning(f"[Silver] '{nome}' não encontrado na Bronze — pulando")
            resultados[nome] = {"status": "ausente_na_bronze"}
        except Exception as e:
            logger.error(f"[Silver] Erro ao processar '{nome}': {e}")
            resultados[nome] = {"status": "erro", "erro": str(e)}

    # ── Passo 2: Integração das bases ──
    tabelas_necessarias = ["indicador_municipio", "municipios", "indicador_uf", "indicador_brasil"]
    if all(t in dfs for t in tabelas_necessarias):
        try:
            df_integrado = integrar_bases(
                df_municipio=dfs["indicador_municipio"],
                df_municipios_ref=dfs["municipios"],
                df_uf=dfs.get("ufs", pd.DataFrame()),
                df_brasil=dfs["indicador_brasil"],
            )
            _salvar_silver(df_integrado, "alfabetizacao_integrado", ["ano", "sigla_uf"])
            resultados["alfabetizacao_integrado"] = {
                "status": "sucesso",
                "registros": len(df_integrado),
            }
        except Exception as e:
            logger.error(f"[Silver] Erro na integração: {e}")
            resultados["alfabetizacao_integrado"] = {"status": "erro", "erro": str(e)}
    else:
        ausentes = [t for t in tabelas_necessarias if t not in dfs]
        logger.warning(f"[Silver] Integração pulada — tabelas ausentes: {ausentes}")
        resultados["alfabetizacao_integrado"] = {"status": "pulado", "motivo": f"ausentes: {ausentes}"}

    fim = datetime.now()
    logger.info(f"PROCESSAMENTO SILVER CONCLUÍDO em {(fim - inicio).seconds}s")
    resumo_execucao(resultados)
    return resultados


if __name__ == "__main__":
    anos_arg = [int(a) for a in sys.argv[1:] if a.isdigit()] or None
    resultado = executar_processamento_silver(anos=anos_arg)

    erros = [k for k, v in resultado.items() if v.get("status") == "erro"]
    sys.exit(1 if erros else 0)
