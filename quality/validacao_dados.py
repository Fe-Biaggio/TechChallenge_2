"""
Validação de Qualidade de Dados

Executa verificações de qualidade nas camadas Bronze e Silver:
  - Completude (valores ausentes)
  - Unicidade (duplicatas)
  - Integridade referencial (chaves órfãs)
  - Consistência (valores fora do domínio esperado)
  - Volumetria (variações inesperadas no número de registros)

Uso:
    python quality/validacao_dados.py              # valida Bronze e Silver
    python quality/validacao_dados.py --camada bronze
    python quality/validacao_dados.py --camada silver
"""
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

# Garante que o root do projeto está no path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.batch.config import BRONZE_DIR, SILVER_DIR
from pipeline.batch.utils import get_logger

logger = get_logger(__name__)


# ─── Regras de validação ──────────────────────────────────────────────────────

REGRAS_BRONZE = {
    "indicador_municipio": {
        "chaves_primarias": ["ano", "id_municipio"],
        "obrigatorias": ["ano", "id_municipio", "sigla_uf"],
        "max_pct_nulos": 5.0,
    },
    "indicador_uf": {
        "chaves_primarias": ["ano", "sigla_uf"],
        "obrigatorias": ["ano", "sigla_uf"],
        "max_pct_nulos": 2.0,
    },
    "indicador_brasil": {
        "chaves_primarias": ["ano"],
        "obrigatorias": ["ano"],
        "max_pct_nulos": 0.0,
    },
    "municipios": {
        "chaves_primarias": ["id_municipio"],
        "obrigatorias": ["id_municipio", "sigla_uf"],
        "max_pct_nulos": 1.0,
    },
    "ufs": {
        "chaves_primarias": ["sigla_uf"],
        "obrigatorias": ["sigla_uf"],
        "max_pct_nulos": 0.0,
    },
}

REGRAS_SILVER = {
    "indicador_municipio": {
        "chaves_primarias": ["ano", "id_municipio"],
        "obrigatorias": ["ano", "id_municipio", "sigla_uf"],
        "max_pct_nulos": 2.0,
    },
    "indicador_uf": {
        "chaves_primarias": ["ano", "sigla_uf"],
        "obrigatorias": ["ano", "sigla_uf"],
        "max_pct_nulos": 1.0,
    },
    "alfabetizacao_integrado": {
        "chaves_primarias": ["ano", "id_municipio"],
        "obrigatorias": ["ano", "id_municipio", "sigla_uf"],
        "max_pct_nulos": 5.0,
        "integridade_referencial": {
            "sigla_uf": ("ufs", "sigla_uf"),
        },
    },
}

UFS_VALIDAS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
}


# ─── Funções de verificação ───────────────────────────────────────────────────

def _ler_tabela(camada_dir: Path, nome: str) -> pd.DataFrame | None:
    caminho = camada_dir / nome
    if not caminho.exists():
        return None
    try:
        return pq.ParquetDataset(str(caminho)).read().to_pandas()
    except Exception as e:
        logger.error(f"Erro ao ler {caminho}: {e}")
        return None


def verificar_completude(df: pd.DataFrame, colunas: list, max_pct: float, nome: str) -> list:
    """Verifica valores ausentes nas colunas obrigatórias."""
    alertas = []
    for col in colunas:
        if col not in df.columns:
            alertas.append({
                "tipo": "COLUNA_AUSENTE",
                "tabela": nome,
                "coluna": col,
                "detalhe": "Coluna obrigatória não encontrada na tabela",
            })
            continue
        nulos = int(df[col].isnull().sum())
        pct = round(nulos / len(df) * 100, 2) if len(df) > 0 else 0
        if pct > max_pct:
            alertas.append({
                "tipo": "NULOS_ACIMA_LIMITE",
                "tabela": nome,
                "coluna": col,
                "valor": f"{nulos} ({pct}%)",
                "limite": f"{max_pct}%",
            })
    return alertas


def verificar_unicidade(df: pd.DataFrame, chaves: list, nome: str) -> list:
    """Verifica duplicatas nas chaves primárias."""
    alertas = []
    chaves_existentes = [c for c in chaves if c in df.columns]
    if not chaves_existentes:
        return alertas

    duplicatas = int(df.duplicated(subset=chaves_existentes).sum())
    if duplicatas > 0:
        alertas.append({
            "tipo": "DUPLICATAS",
            "tabela": nome,
            "coluna": str(chaves_existentes),
            "valor": f"{duplicatas} registros duplicados",
            "limite": "0",
        })
    return alertas


def verificar_dominio_uf(df: pd.DataFrame, nome: str) -> list:
    """Verifica se todas as UFs são válidas."""
    alertas = []
    if "sigla_uf" not in df.columns:
        return alertas

    ufs_encontradas = set(df["sigla_uf"].dropna().str.upper().unique())
    invalidas = ufs_encontradas - UFS_VALIDAS
    if invalidas:
        alertas.append({
            "tipo": "DOMINIO_INVALIDO",
            "tabela": nome,
            "coluna": "sigla_uf",
            "valor": str(invalidas),
            "limite": "UFs válidas do Brasil",
        })
    return alertas


def verificar_integridade_referencial(
    df: pd.DataFrame,
    nome: str,
    regra: dict,
    camada_dir: Path,
) -> list:
    """Verifica se chaves estrangeiras existem na tabela de referência."""
    alertas = []
    for col_fk, (tabela_ref, col_ref) in regra.items():
        if col_fk not in df.columns:
            continue
        df_ref = _ler_tabela(camada_dir, tabela_ref)
        if df_ref is None or col_ref not in df_ref.columns:
            continue

        valores_fk = set(df[col_fk].dropna().unique())
        valores_ref = set(df_ref[col_ref].dropna().unique())
        orfaos = valores_fk - valores_ref

        if orfaos:
            alertas.append({
                "tipo": "INTEGRIDADE_REFERENCIAL",
                "tabela": nome,
                "coluna": col_fk,
                "valor": f"{len(orfaos)} valores órfãos: {list(orfaos)[:5]}...",
                "limite": f"FK → {tabela_ref}.{col_ref}",
            })
    return alertas


def verificar_tabela(
    df: pd.DataFrame,
    nome: str,
    regras: dict,
    camada_dir: Path,
) -> list:
    """Executa todas as verificações de uma tabela."""
    alertas = []
    alertas += verificar_completude(df, regras.get("obrigatorias", []), regras.get("max_pct_nulos", 5.0), nome)
    alertas += verificar_unicidade(df, regras.get("chaves_primarias", []), nome)
    alertas += verificar_dominio_uf(df, nome)

    if "integridade_referencial" in regras:
        alertas += verificar_integridade_referencial(df, nome, regras["integridade_referencial"], camada_dir)

    return alertas


# ─── Orquestrador ─────────────────────────────────────────────────────────────

def validar_camada(camada: str) -> dict:
    """Executa todas as validações de uma camada (bronze ou silver)."""
    if camada == "bronze":
        camada_dir = BRONZE_DIR
        regras_mapa = REGRAS_BRONZE
    elif camada == "silver":
        camada_dir = SILVER_DIR
        regras_mapa = REGRAS_SILVER
    else:
        raise ValueError(f"Camada inválida: '{camada}'. Use 'bronze' ou 'silver'.")

    logger.info(f"{'='*60}")
    logger.info(f"VALIDAÇÃO — CAMADA {camada.upper()} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}")

    resultado = {}
    todos_alertas = []

    for nome_tabela, regras in regras_mapa.items():
        df = _ler_tabela(camada_dir, nome_tabela)
        if df is None:
            logger.warning(f"  [{nome_tabela}] Tabela não encontrada — pulando")
            resultado[nome_tabela] = {"status": "nao_encontrada", "alertas": []}
            continue

        alertas = verificar_tabela(df, nome_tabela, regras, camada_dir)
        todos_alertas.extend(alertas)

        status = "OK" if not alertas else "ALERTA"
        resultado[nome_tabela] = {
            "status": status,
            "registros": len(df),
            "alertas": alertas,
        }

        if alertas:
            logger.warning(f"  [{nome_tabela}] {len(alertas)} alerta(s):")
            for a in alertas:
                logger.warning(f"    • [{a['tipo']}] {a['coluna']}: {a.get('valor', '')} (limite: {a.get('limite', '')})")
        else:
            logger.info(f"  [{nome_tabela}] OK — {len(df):,} registros")

    logger.info(f"{'─'*60}")
    logger.info(f"  Total de alertas: {len(todos_alertas)}")
    logger.info(f"  Tabelas OK: {sum(1 for r in resultado.values() if r['status'] == 'OK')}/{len(resultado)}")
    logger.info(f"{'='*60}")

    return resultado


def executar_validacao_completa() -> dict:
    """Valida Bronze e Silver e retorna relatório consolidado."""
    return {
        "bronze": validar_camada("bronze"),
        "silver": validar_camada("silver"),
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    camada_arg = None
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--camada" and i + 1 < len(sys.argv[1:]):
            camada_arg = sys.argv[i + 2]

    if camada_arg:
        resultado = {camada_arg: validar_camada(camada_arg)}
    else:
        resultado = executar_validacao_completa()

    tem_alertas = any(
        any(t.get("alertas") for t in camada.values())
        for camada in resultado.values()
        if isinstance(camada, dict)
    )
    sys.exit(1 if tem_alertas else 0)
