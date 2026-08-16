"""
Agregação Gold — Camada Analítica

Lê os dados tratados e integrados da camada Silver e produz datasets
analíticos prontos para consumo (dashboards, análises estatísticas e
treinamento de modelos de ML):

  1. indicador_alfabetizacao_municipio — indicador por município e ano,
     com comparação à meta municipal, ao indicador nacional e classificação
     de risco
  2. comparacao_metas_resultados — metas vs. resultados por UF/ano (escopo
     "Pública", o mesmo da meta estadual), com rollup de quantos municípios
     da UF atingiram sua própria meta municipal
  3. evolucao_temporal — série histórica do indicador (Brasil e UF), com
     variação ano a ano
  4. monitoramento_streaming — volume, cobertura e janela temporal dos
     eventos de streaming processados (se pipeline/streaming/ já rodou);
     não entra nos datasets analíticos acima, que usam só dado real do INEP

Uso:
    python pipeline/batch/03_agregacao_gold.py
    python pipeline/batch/03_agregacao_gold.py 2023
"""
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Garante que o root do projeto está no path ao rodar como script direto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.batch.config import GOLD_DIR, SILVER_DIR, COMPRESSAO_PARQUET
from pipeline.batch.utils import get_logger, resumo_execucao, timer

logger = get_logger(__name__)

# Escopo de rede usado para a comparação de meta no nível UF/Brasil — é o
# mesmo escopo em que meta_uf/meta_brasil definem seus alvos.
ESCOPO_META_UF = "Pública"

FAIXAS_RISCO = [
    (0, 50, "Crítico"),
    (50, 70, "Atenção"),
    (70, 90, "Adequado"),
    (90, 100.01, "Meta próxima/atingida"),
]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _salvar_gold(df: pd.DataFrame, nome: str, particoes: list = None) -> None:
    destino = GOLD_DIR / nome
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

    mb = df.memory_usage(deep=True).sum() / 1024 / 1024
    logger.info(f"[Gold] {nome}: {len(df):,} registros | {mb:.1f} MB → {destino.relative_to(GOLD_DIR.parent)}")


def _ler_silver(nome: str) -> pd.DataFrame:
    caminho = SILVER_DIR / nome
    if not caminho.exists():
        raise FileNotFoundError(f"Tabela não encontrada na Silver: {caminho}")
    return pq.ParquetDataset(str(caminho)).read().to_pandas()


def _classificar_risco(indicador: pd.Series) -> pd.Series:
    """Classifica o indicador em faixas de risco educacional."""
    faixa = pd.Series("Não classificado", index=indicador.index, dtype="object")
    for minimo, maximo, label in FAIXAS_RISCO:
        mascara = (indicador >= minimo) & (indicador < maximo)
        faixa[mascara] = label
    return faixa


# ─── Datasets Gold ─────────────────────────────────────────────────────────────

@timer
def construir_indicador_municipio(anos: list = None) -> pd.DataFrame:
    """
    Dataset analítico principal: indicador de alfabetização por município/ano
    (rede "headline" — a melhor disponível), com comparação à meta municipal,
    ao indicador nacional do mesmo ano e classificação de risco.
    """
    df = _ler_silver("alfabetizacao_integrado")
    if anos:
        df = df[df["ano"].isin(anos)]

    df_brasil = _ler_silver("indicador_brasil")[["ano", "taxa_alfabetizacao"]].rename(
        columns={"taxa_alfabetizacao": "indicador_nacional"}
    )
    df = df.merge(df_brasil, on="ano", how="left")
    df["gap_indicador_nacional"] = df["taxa_alfabetizacao"] - df["indicador_nacional"]
    df["faixa_risco"] = _classificar_risco(df["taxa_alfabetizacao"])

    colunas = [
        "ano", "id_municipio", "nome_municipio", "sigla_uf", "nome_uf", "nome_regiao",
        "rede_label", "taxa_alfabetizacao", "taxa_alfabetizacao_municipal",
        "meta_alfabetizacao_municipal", "gap_meta_municipal", "atingiu_meta_municipal",
        "indicador_nacional", "gap_indicador_nacional", "faixa_risco",
    ]
    gold = df[[c for c in colunas if c in df.columns]].rename(columns={"rede_label": "escopo_rede_headline"})
    gold["dt_processamento"] = datetime.utcnow().isoformat()

    logger.info(f"[Gold] indicador_alfabetizacao_municipio: {len(gold):,} registros")
    return gold


@timer
def construir_comparacao_metas(anos: list = None) -> pd.DataFrame:
    """
    Compara resultado x meta por UF/ano no escopo "Pública" (o mesmo em que
    meta_uf define seus alvos), enriquecido com o rollup de quantos
    municípios da UF atingiram sua própria meta municipal.
    """
    df_uf = _ler_silver("indicador_uf")
    df_uf = df_uf[df_uf["rede_label"] == ESCOPO_META_UF][["ano", "sigla_uf", "taxa_alfabetizacao"]]

    meta_uf = _ler_silver("meta_uf")
    meta_uf = meta_uf[meta_uf["rede"] == ESCOPO_META_UF][["sigla_uf", "ano_meta", "valor_meta"]].rename(
        columns={"ano_meta": "ano", "valor_meta": "meta_alfabetizacao"}
    )

    comparacao = df_uf.merge(meta_uf, on=["sigla_uf", "ano"], how="inner")
    if anos:
        comparacao = comparacao[comparacao["ano"].isin(anos)]
    comparacao["gap"] = round(comparacao["taxa_alfabetizacao"] - comparacao["meta_alfabetizacao"], 2)
    comparacao["atingiu_meta"] = comparacao["gap"] >= 0

    integrado = _ler_silver("alfabetizacao_integrado")
    rollup = integrado.groupby(["ano", "sigla_uf"], observed=True).agg(
        total_municipios=("id_municipio", "nunique"),
        municipios_avaliados_meta=("atingiu_meta_municipal", lambda s: int(s.notna().sum())),
        municipios_meta_atingida=("atingiu_meta_municipal", lambda s: int((s == True).sum())),  # noqa: E712
    ).reset_index()
    divisor = rollup["municipios_avaliados_meta"].astype(float).replace(0, np.nan)
    rollup["pct_municipios_meta_atingida"] = round(rollup["municipios_meta_atingida"] / divisor * 100, 2)

    resultado = comparacao.merge(rollup, on=["ano", "sigla_uf"], how="left")
    resultado["dt_processamento"] = datetime.utcnow().isoformat()

    logger.info(f"[Gold] comparacao_metas_resultados: {len(resultado):,} registros (UF x ano)")
    return resultado


@timer
def construir_evolucao_temporal(anos: list = None) -> pd.DataFrame:
    """
    Série histórica do indicador em dois níveis (Brasil e UF, escopo
    "Pública"), com variação ano a ano — pronta para gráficos de evolução.
    """
    linhas = []

    df_brasil = _ler_silver("indicador_brasil")[["ano", "taxa_alfabetizacao"]].rename(
        columns={"taxa_alfabetizacao": "indicador"}
    )
    df_brasil["nivel"] = "brasil"
    df_brasil["referencia"] = "BR"
    linhas.append(df_brasil)

    df_uf = _ler_silver("indicador_uf")
    df_uf = df_uf[df_uf["rede_label"] == ESCOPO_META_UF][["ano", "sigla_uf", "taxa_alfabetizacao"]].rename(
        columns={"taxa_alfabetizacao": "indicador", "sigla_uf": "referencia"}
    )
    df_uf["nivel"] = "uf"
    linhas.append(df_uf)

    evolucao = pd.concat(linhas, ignore_index=True)
    if anos:
        evolucao = evolucao[evolucao["ano"].isin(anos)]

    evolucao = evolucao.sort_values(["nivel", "referencia", "ano"])
    evolucao["variacao_yoy"] = evolucao.groupby(["nivel", "referencia"], observed=True)["indicador"].diff().round(2)
    evolucao["dt_processamento"] = datetime.utcnow().isoformat()

    logger.info(f"[Gold] evolucao_temporal: {len(evolucao):,} registros (brasil + uf)")
    return evolucao


@timer
def construir_monitoramento_streaming() -> pd.DataFrame:
    """
    Monitoramento operacional da ingestão de streaming: volume, cobertura
    territorial e janela temporal por tópico. Não mede eventos inválidos —
    o consumer descarta esses sem persistir (ver pipeline/streaming/consumer.py),
    então essa métrica só existe no log da execução ao vivo, não no dado
    persistido. Datasets vazios (Silver sem indicador_streaming/meta_streaming,
    ou seja, streaming nunca rodou) resultam em tabela vazia — não é erro.
    """
    colunas = [
        "topico", "total_eventos", "cobertura_distinta", "primeiro_evento",
        "ultimo_evento", "janela_segundos", "eventos_por_segundo", "dt_processamento",
    ]
    fontes = [("indicadores", "indicador_streaming", "id_municipio"), ("metas", "meta_streaming", "sigla_uf")]

    linhas = []
    for topico, nome_tabela, col_cobertura in fontes:
        try:
            df = _ler_silver(nome_tabela)
        except FileNotFoundError:
            continue
        if df.empty:
            continue
        linhas.append({
            "topico": topico,
            "total_eventos": len(df),
            "cobertura_distinta": int(df[col_cobertura].nunique()),
            "primeiro_evento": df["timestamp"].min(),
            "ultimo_evento": df["timestamp"].max(),
        })

    if not linhas:
        logger.warning("[Gold] monitoramento_streaming: nenhum evento de streaming encontrado na Silver — pulando")
        return pd.DataFrame(columns=colunas)

    resultado = pd.DataFrame(linhas)
    resultado["janela_segundos"] = (resultado["ultimo_evento"] - resultado["primeiro_evento"]).dt.total_seconds()
    divisor = resultado["janela_segundos"].replace(0, np.nan)
    resultado["eventos_por_segundo"] = round(resultado["total_eventos"] / divisor, 3)
    resultado["dt_processamento"] = datetime.utcnow().isoformat()

    logger.info(f"[Gold] monitoramento_streaming: {len(resultado)} tópico(s), {resultado['total_eventos'].sum():,} eventos totais")
    return resultado[colunas]


# ─── Orquestrador principal ───────────────────────────────────────────────────

@timer
def executar_agregacao_gold(anos: list = None) -> dict:
    """Executa toda a construção da camada Gold."""
    resultados = {}

    logger.info("=" * 60)
    logger.info("AGREGAÇÃO GOLD")
    logger.info(f"Anos: {anos if anos else 'todos'}")
    logger.info("=" * 60)

    try:
        df_municipio = construir_indicador_municipio(anos)
        _salvar_gold(df_municipio, "indicador_alfabetizacao_municipio", ["ano"])
        resultados["indicador_alfabetizacao_municipio"] = {"status": "sucesso", "registros": len(df_municipio)}
    except Exception as e:
        logger.error(f"[Gold] Erro ao construir 'indicador_alfabetizacao_municipio': {e}")
        resultados["indicador_alfabetizacao_municipio"] = {"status": "erro", "erro": str(e)}

    try:
        df_metas = construir_comparacao_metas(anos)
        _salvar_gold(df_metas, "comparacao_metas_resultados", ["ano"])
        resultados["comparacao_metas_resultados"] = {"status": "sucesso", "registros": len(df_metas)}
    except Exception as e:
        logger.error(f"[Gold] Erro ao construir 'comparacao_metas_resultados': {e}")
        resultados["comparacao_metas_resultados"] = {"status": "erro", "erro": str(e)}

    try:
        df_evolucao = construir_evolucao_temporal(anos)
        _salvar_gold(df_evolucao, "evolucao_temporal", ["nivel"])
        resultados["evolucao_temporal"] = {"status": "sucesso", "registros": len(df_evolucao)}
    except Exception as e:
        logger.error(f"[Gold] Erro ao construir 'evolucao_temporal': {e}")
        resultados["evolucao_temporal"] = {"status": "erro", "erro": str(e)}

    try:
        df_monitoramento = construir_monitoramento_streaming()
        if not df_monitoramento.empty:
            _salvar_gold(df_monitoramento, "monitoramento_streaming", ["topico"])
            resultados["monitoramento_streaming"] = {"status": "sucesso", "registros": len(df_monitoramento)}
        else:
            resultados["monitoramento_streaming"] = {"status": "pulado", "motivo": "streaming nunca rodou"}
    except Exception as e:
        logger.error(f"[Gold] Erro ao construir 'monitoramento_streaming': {e}")
        resultados["monitoramento_streaming"] = {"status": "erro", "erro": str(e)}

    logger.info("AGREGAÇÃO GOLD CONCLUÍDA")
    resumo_execucao(resultados)
    return resultados


if __name__ == "__main__":
    anos_arg = [int(a) for a in sys.argv[1:] if a.isdigit()] or None
    resultado = executar_agregacao_gold(anos=anos_arg)

    erros = [k for k, v in resultado.items() if v.get("status") == "erro"]
    sys.exit(1 if erros else 0)
