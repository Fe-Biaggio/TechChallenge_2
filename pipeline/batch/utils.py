"""
Funções utilitárias compartilhadas pela pipeline.
"""
import logging
import time
from datetime import datetime
from functools import wraps
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.batch.config import LOG_LEVEL, COMPRESSAO_PARQUET


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(name)


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = get_logger(func.__module__)
        inicio = time.time()
        resultado = func(*args, **kwargs)
        elapsed = time.time() - inicio
        logger.info(f"{func.__name__} concluído em {elapsed:.2f}s")
        return resultado
    return wrapper


def salvar_bronze(
    df: pd.DataFrame,
    nome_tabela: str,
    bronze_dir: Path,
    particoes: list,
) -> Path:
    """Persiste DataFrame na camada Bronze em Parquet particionado."""
    logger = get_logger(__name__)
    destino = bronze_dir / nome_tabela
    destino.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df, preserve_index=False)

    if particoes and all(p in df.columns for p in particoes):
        pq.write_to_dataset(
            table,
            root_path=str(destino),
            partition_cols=particoes,
            existing_data_behavior="delete_matching",
            compression=COMPRESSAO_PARQUET,
        )
    else:
        pq.write_table(table, destino / "data.parquet", compression=COMPRESSAO_PARQUET)

    total_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    logger.info(
        f"[Bronze] {nome_tabela}: {len(df):,} registros | "
        f"{total_mb:.1f} MB → {destino.relative_to(bronze_dir.parent.parent)}"
    )
    return destino


def ler_bronze(
    nome_tabela: str,
    bronze_dir: Path,
    filtros: dict = None,
) -> pd.DataFrame:
    """Lê tabela da camada Bronze com filtros opcionais de partição."""
    caminho = bronze_dir / nome_tabela
    if not caminho.exists():
        raise FileNotFoundError(f"Tabela não encontrada na Bronze: {caminho}")

    dataset = pq.ParquetDataset(str(caminho))
    df = dataset.read().to_pandas()

    if filtros:
        for col, val in filtros.items():
            if col in df.columns:
                df = df[df[col] == val]

    return df


def log_qualidade(df: pd.DataFrame, nome_tabela: str) -> dict:
    """Loga e retorna métricas básicas de qualidade do DataFrame."""
    logger = get_logger(__name__)

    nulos = int(df.isnull().sum().sum())
    total_celulas = len(df) * len(df.columns) if len(df) > 0 else 1
    pct_nulos = round(nulos / total_celulas * 100, 2)
    duplicatas = int(df.duplicated().sum())

    metricas = {
        "tabela": nome_tabela,
        "registros": len(df),
        "colunas": len(df.columns),
        "nulos_total": nulos,
        "pct_nulos": pct_nulos,
        "duplicatas": duplicatas,
        "timestamp_verificacao": datetime.utcnow().isoformat(),
    }

    logger.info(
        f"[Qualidade] {nome_tabela}: {metricas['registros']:,} registros | "
        f"{nulos:,} nulos ({pct_nulos}%) | {duplicatas:,} duplicatas"
    )
    return metricas


_ROTULOS_STATUS = {
    "sucesso": "OK  ",
    "erro": "ERRO",
    "ausente_na_bronze": "-   ",
    "pulado": "-   ",
}


def resumo_execucao(resultados: dict) -> None:
    """Imprime resumo tabular da execução da pipeline."""
    logger = get_logger(__name__)
    sucesso = [k for k, v in resultados.items() if v.get("status") == "sucesso"]
    erro = [k for k, v in resultados.items() if v.get("status") == "erro"]
    outros = [k for k, v in resultados.items() if v.get("status") not in ("sucesso", "erro")]

    logger.info("─" * 60)
    for tabela, info in resultados.items():
        status_str = _ROTULOS_STATUS.get(info.get("status"), "ERRO")
        registros = f"{info.get('registros', '-'):>10,}" if isinstance(info.get("registros"), int) else f"{'':>10}"
        logger.info(f"  [{status_str}] {tabela:<35} {registros} registros")
    logger.info("─" * 60)
    logger.info(f"  Total: {len(sucesso)} sucesso | {len(erro)} erro | {len(outros)} pulado/ausente")
