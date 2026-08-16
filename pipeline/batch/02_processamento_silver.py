"""
Processamento Silver — Limpeza, Padronização e Integração das Bases

Lê os dados brutos da camada Bronze, aplica as seguintes transformações
e persiste os resultados na camada Silver:

  1. Limpeza e tipagem correta de cada tabela
  2. Tratamento de valores ausentes (apenas onde o nulo é de fato um problema
     de qualidade — proporcao_aluno_nivel_* ausente em 2023 e proficiência
     ausente para aluno faltante são estruturais, não são "sujeira")
  3. Padronização de nomes de colunas e decodificação de códigos (rede)
  4. Normalização de chaves de relacionamento (id_municipio, sigla_uf)
  5. "Despivotagem" das tabelas de meta (meta_alfabetizacao_2024..2030,
     formato largo) para long (ano_meta, valor_meta), mantendo por ano-alvo
     apenas a vintage mais recente publicada
  6. Integração: uma linha por (ano, id_município) juntando o resultado
     "headline" (melhor rede disponível), o diretório de municípios/UF e a
     meta municipal

Decisões de modelagem importantes (ver comentários no código):
  - "rede" nas tabelas de resultado é um código numérico (REDE_MAP em
    config.py); infere-se município → município via prioridade
    Pública > Municipal > Estadual > Privada > Total como resultado
    "headline" do município/UF.
  - As metas municipais (meta_municipio) só existem para a rede "Municipal"
    — por isso a comparação meta x resultado usa especificamente o
    resultado da rede Municipal, não o headline, para não misturar escopos.
  - Não existe tabela de resultado nacional no dataset de origem; a série
    Brasil é derivada da coluna taxa_alfabetizacao já presente em
    meta_brasil (resultado observado no ano de referência de cada vintage).

Uso:
    python pipeline/batch/02_processamento_silver.py
    python pipeline/batch/02_processamento_silver.py 2023
"""
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Garante que o root do projeto está no path ao rodar como script direto
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from pipeline.batch.config import (
    BRONZE_DIR,
    SILVER_DIR,
    COMPRESSAO_PARQUET,
    REDE_MAP,
)
from pipeline.batch.utils import get_logger, ler_bronze, log_qualidade, resumo_execucao, timer

logger = get_logger(__name__)

# Escopo de rede usado para comparar resultado x meta em cada nível
ESCOPO_META_MUNICIPIO = "Municipal"

# Prioridade de rede para o indicador "headline" (a que melhor representa o
# município/UF como um todo) quando mais de uma rede está disponível
PRIORIDADE_REDE_HEADLINE = ["Pública", "Municipal", "Estadual", "Privada", "Total", "Federal"]


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
            existing_data_behavior="delete_matching",
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


def _garantir_tipos(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """Converte colunas para os tipos esperados, ignorando as ausentes."""
    for col, tipo in schema.items():
        if col in df.columns:
            try:
                df[col] = df[col].astype(tipo)
            except (ValueError, TypeError) as e:
                logger.warning(f"  Não foi possível converter '{col}' para {tipo}: {e}")
    return df


def _decodificar_rede(df: pd.DataFrame, nome: str) -> pd.DataFrame:
    """Traduz o código numérico de 'rede' para rótulo (ver REDE_MAP em config.py)."""
    df["rede_label"] = df["rede"].map(REDE_MAP)
    nao_mapeados = df.loc[df["rede_label"].isna() & df["rede"].notna(), "rede"].unique()
    if len(nao_mapeados):
        logger.warning(f"[Silver] {nome}: códigos de rede não mapeados em REDE_MAP: {list(nao_mapeados)}")
    return df


def _melt_metas(df: pd.DataFrame, chave_negocio: list, nome: str) -> pd.DataFrame:
    """
    Converte as colunas meta_alfabetizacao_2024..2030 (formato largo, uma
    coluna por ano-alvo) em formato long (ano_meta, valor_meta).

    Cada linha de origem representa uma "vintage" (ano_referencia) em que a
    trajetória de metas foi publicada/revisada. Como o mesmo ano-alvo pode
    aparecer em mais de uma vintage com valores diferentes (revisão), mantém-se
    apenas a publicação mais recente por (chave_negocio, ano_meta).
    """
    value_vars = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]
    id_vars = [c for c in df.columns if c not in value_vars]

    melted = df.melt(id_vars=id_vars, value_vars=value_vars, var_name="_col", value_name="valor_meta")
    melted["ano_meta"] = melted["_col"].str.replace("meta_alfabetizacao_", "", regex=False).astype(int)
    melted = melted.drop(columns="_col").dropna(subset=["valor_meta"])
    melted = melted.rename(columns={"ano": "ano_referencia"})

    chave_dedup = chave_negocio + ["ano_meta"]
    antes = len(melted)
    melted = melted.sort_values("ano_referencia").drop_duplicates(subset=chave_dedup, keep="last")
    logger.info(f"[Silver] {nome}: {antes:,} → {len(melted):,} metas (long, vintage mais recente por {chave_dedup})")
    return melted


# ─── Transformações por tabela ────────────────────────────────────────────────

@timer
def transformar_indicador_municipio(anos: list = None) -> pd.DataFrame:
    """Limpa e padroniza o resultado (taxa de alfabetização) por município."""
    df = ler_bronze("indicador_municipio", BRONZE_DIR)
    df = _padronizar_colunas(df)

    if anos:
        df = df[df["ano"].isin(anos)]

    df = _garantir_tipos(df, {"ano": "int16", "id_municipio": "str", "rede": "Int8"})
    df["id_municipio"] = df["id_municipio"].str.strip()
    df = _decodificar_rede(df, "indicador_municipio")
    df = _remover_duplicatas(df, ["ano", "id_municipio", "serie", "rede"], "indicador_municipio")

    invalidos = (df["taxa_alfabetizacao"] < 0) | (df["taxa_alfabetizacao"] > 100)
    df["flag_taxa_invalida"] = invalidos.fillna(False)
    if invalidos.sum():
        logger.warning(f"[Silver] indicador_municipio: {int(invalidos.sum())} taxa_alfabetizacao fora de [0,100]")

    df["dt_processamento"] = datetime.utcnow().isoformat()
    log_qualidade(df, "indicador_municipio_silver")
    return df


@timer
def transformar_indicador_uf(anos: list = None) -> pd.DataFrame:
    """Limpa e padroniza o resultado (taxa de alfabetização) por UF."""
    df = ler_bronze("indicador_uf", BRONZE_DIR)
    df = _padronizar_colunas(df)

    if anos:
        df = df[df["ano"].isin(anos)]

    df = _garantir_tipos(df, {"ano": "int16", "sigla_uf": "str", "rede": "Int8"})
    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()
    df = _decodificar_rede(df, "indicador_uf")
    df = _remover_duplicatas(df, ["ano", "sigla_uf", "serie", "rede"], "indicador_uf")

    invalidos = (df["taxa_alfabetizacao"] < 0) | (df["taxa_alfabetizacao"] > 100)
    df["flag_taxa_invalida"] = invalidos.fillna(False)

    df["dt_processamento"] = datetime.utcnow().isoformat()
    log_qualidade(df, "indicador_uf_silver")
    return df


@timer
def transformar_indicador_brasil(anos: list = None) -> pd.DataFrame:
    """
    Deriva a série de resultado nacional a partir de meta_brasil: a coluna
    taxa_alfabetizacao dessa tabela já é o resultado observado no ano de
    referência de cada vintage — o dataset de origem não publica uma tabela
    de resultado nacional separada da tabela de metas.
    """
    df = ler_bronze("meta_brasil", BRONZE_DIR)
    df = _padronizar_colunas(df)

    if anos:
        df = df[df["ano"].isin(anos)]

    df = df[["ano", "rede", "taxa_alfabetizacao", "percentual_participacao"]].copy()
    df = _garantir_tipos(df, {"ano": "int16"})
    df = _remover_duplicatas(df, ["ano"], "indicador_brasil")
    df["dt_processamento"] = datetime.utcnow().isoformat()

    log_qualidade(df, "indicador_brasil_silver")
    return df


@timer
def transformar_meta_brasil() -> pd.DataFrame:
    df = ler_bronze("meta_brasil", BRONZE_DIR)
    df = _padronizar_colunas(df)
    melted = _melt_metas(df, chave_negocio=[], nome="meta_brasil")
    melted["dt_processamento"] = datetime.utcnow().isoformat()
    log_qualidade(melted, "meta_brasil_silver")
    return melted


@timer
def transformar_meta_uf() -> pd.DataFrame:
    df = ler_bronze("meta_uf", BRONZE_DIR)
    df = _padronizar_colunas(df)
    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()
    melted = _melt_metas(df, chave_negocio=["sigla_uf"], nome="meta_uf")
    melted["dt_processamento"] = datetime.utcnow().isoformat()
    log_qualidade(melted, "meta_uf_silver")
    return melted


@timer
def transformar_meta_municipio() -> pd.DataFrame:
    df = ler_bronze("meta_municipio", BRONZE_DIR)
    df = _padronizar_colunas(df)
    df = _garantir_tipos(df, {"id_municipio": "str"})
    df["id_municipio"] = df["id_municipio"].str.strip()
    melted = _melt_metas(df, chave_negocio=["id_municipio"], nome="meta_municipio")
    melted["dt_processamento"] = datetime.utcnow().isoformat()
    log_qualidade(melted, "meta_municipio_silver")
    return melted


@timer
def transformar_diretorio_municipio() -> pd.DataFrame:
    """Limpa e padroniza o diretório de municípios (tabela de referência)."""
    df = ler_bronze("diretorio_municipio", BRONZE_DIR)
    df = _padronizar_colunas(df)

    colunas = ["id_municipio", "nome", "sigla_uf", "nome_uf", "nome_regiao", "amazonia_legal", "capital_uf", "ddd"]
    df = df[[c for c in colunas if c in df.columns]].rename(columns={"nome": "nome_municipio"})

    df = _garantir_tipos(df, {"id_municipio": "str"})
    df["id_municipio"] = df["id_municipio"].str.strip()
    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()
    df["nome_municipio"] = df["nome_municipio"].str.strip()

    df = _remover_duplicatas(df, ["id_municipio"], "diretorio_municipio")
    log_qualidade(df, "diretorio_municipio_silver")
    return df


@timer
def transformar_diretorio_uf() -> pd.DataFrame:
    """Limpa e padroniza o diretório de UFs (tabela de referência)."""
    df = ler_bronze("diretorio_uf", BRONZE_DIR)
    df = _padronizar_colunas(df)
    df = df.rename(columns={"sigla": "sigla_uf", "nome": "nome_uf"})

    df["sigla_uf"] = df["sigla_uf"].str.strip().str.upper()
    df["nome_uf"] = df["nome_uf"].str.strip()

    df = _remover_duplicatas(df, ["sigla_uf"], "diretorio_uf")
    log_qualidade(df, "diretorio_uf_silver")
    return df


@timer
def transformar_alunos(anos: list = None) -> pd.DataFrame:
    """
    Limpa e padroniza os microdados de alunos. proficiencia/peso_aluno nulos
    são esperados para alunos com presenca=False (não avaliados) e não são
    imputados — imputar mediana aqui distorceria a distribuição real.
    """
    df = ler_bronze("alunos", BRONZE_DIR)
    df = _padronizar_colunas(df)

    if anos:
        df = df[df["ano"].isin(anos)]

    df = _garantir_tipos(df, {"ano": "int16", "id_municipio": "str", "rede": "Int8"})
    df["id_municipio"] = df["id_municipio"].str.strip()
    df = _decodificar_rede(df, "alunos")

    for col in ("presenca", "preenchimento_caderno", "alfabetizado"):
        if col in df.columns:
            df[col] = df[col].astype("boolean")

    df = _remover_duplicatas(df, ["ano", "id_municipio", "id_escola", "id_aluno"], "alunos")
    df["dt_processamento"] = datetime.utcnow().isoformat()

    log_qualidade(df, "alunos_silver")
    return df


# ─── Integração das bases ─────────────────────────────────────────────────────

def _selecionar_rede_headline(
    df: pd.DataFrame,
    prioridade: list = PRIORIDADE_REDE_HEADLINE,
    chave: tuple = ("ano", "id_municipio"),
) -> pd.DataFrame:
    """Para cada (ano, id_município), mantém apenas a linha da rede de maior prioridade disponível."""
    df = df.copy()
    ordem = {r: i for i, r in enumerate(prioridade)}
    df["_prioridade"] = df["rede_label"].map(ordem).fillna(len(prioridade))
    df = df.sort_values("_prioridade").drop_duplicates(subset=list(chave), keep="first")
    return df.drop(columns="_prioridade")


@timer
def integrar_bases(
    df_indicador_municipio: pd.DataFrame,
    df_diretorio_municipio: pd.DataFrame,
    df_meta_municipio: pd.DataFrame,
) -> pd.DataFrame:
    """
    Integra o resultado municipal com o diretório de municípios e a meta
    municipal, produzindo uma linha por (ano, id_município).

    O resultado "headline" usa a melhor rede disponível (Pública > Municipal
    > ...), mas a comparação com a meta usa especificamente a rede Municipal
    — meta_municipio só define metas para essa rede, então comparar contra
    o headline (que pode ser 'Pública', um agregado maior) misturaria escopos.
    """
    logger.info("[Silver] Iniciando integração das bases...")

    headline = _selecionar_rede_headline(df_indicador_municipio)
    integrado = headline.merge(df_diretorio_municipio, on="id_municipio", how="left")

    municipal = df_indicador_municipio[df_indicador_municipio["rede_label"] == ESCOPO_META_MUNICIPIO][
        ["ano", "id_municipio", "taxa_alfabetizacao"]
    ].rename(columns={"taxa_alfabetizacao": "taxa_alfabetizacao_municipal"})

    meta = df_meta_municipio[df_meta_municipio["rede"] == ESCOPO_META_MUNICIPIO][
        ["id_municipio", "ano_meta", "valor_meta"]
    ].rename(columns={"ano_meta": "ano", "valor_meta": "meta_alfabetizacao_municipal"})

    comparacao = municipal.merge(meta, on=["id_municipio", "ano"], how="inner")
    comparacao["gap_meta_municipal"] = comparacao["taxa_alfabetizacao_municipal"] - comparacao["meta_alfabetizacao_municipal"]
    comparacao["atingiu_meta_municipal"] = comparacao["gap_meta_municipal"] >= 0

    integrado = integrado.merge(comparacao, on=["ano", "id_municipio"], how="left")

    com_meta = int(integrado["atingiu_meta_municipal"].notna().sum())
    logger.info(f"[Silver] Integração concluída: {len(integrado):,} registros | {com_meta:,} com comparação de meta municipal")

    log_qualidade(integrado, "alfabetizacao_integrado")
    return integrado


# ─── Orquestrador principal ───────────────────────────────────────────────────

@timer
def executar_processamento_silver(anos: list = None) -> dict:
    """Executa todo o processamento da camada Silver."""
    resultados = {}

    logger.info("=" * 60)
    logger.info("PROCESSAMENTO SILVER")
    logger.info(f"Anos: {anos if anos else 'todos'}")
    logger.info("=" * 60)

    tabelas = {
        "indicador_municipio": (lambda: transformar_indicador_municipio(anos), ["ano"]),
        "indicador_uf": (lambda: transformar_indicador_uf(anos), ["ano"]),
        "indicador_brasil": (lambda: transformar_indicador_brasil(anos), ["ano"]),
        "meta_brasil": (transformar_meta_brasil, ["ano_meta"]),
        "meta_uf": (transformar_meta_uf, ["ano_meta"]),
        "meta_municipio": (transformar_meta_municipio, ["ano_meta"]),
        "diretorio_municipio": (transformar_diretorio_municipio, []),
        "diretorio_uf": (transformar_diretorio_uf, []),
        "alunos": (lambda: transformar_alunos(anos), ["ano"]),
    }

    dfs = {}
    for nome, (fn, particoes) in tabelas.items():
        try:
            df = fn()
            _salvar_silver(df, nome, particoes)
            dfs[nome] = df
            resultados[nome] = {"status": "sucesso", "registros": len(df)}
        except FileNotFoundError:
            logger.warning(f"[Silver] '{nome}' não encontrado na Bronze — pulando")
            resultados[nome] = {"status": "ausente_na_bronze"}
        except Exception as e:
            logger.error(f"[Silver] Erro ao processar '{nome}': {e}")
            resultados[nome] = {"status": "erro", "erro": str(e)}

    tabelas_necessarias = ["indicador_municipio", "diretorio_municipio", "meta_municipio"]
    if all(t in dfs for t in tabelas_necessarias):
        try:
            df_integrado = integrar_bases(
                df_indicador_municipio=dfs["indicador_municipio"],
                df_diretorio_municipio=dfs["diretorio_municipio"],
                df_meta_municipio=dfs["meta_municipio"],
            )
            _salvar_silver(df_integrado, "alfabetizacao_integrado", ["ano"])
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

    logger.info("PROCESSAMENTO SILVER CONCLUÍDO")
    resumo_execucao(resultados)
    return resultados


if __name__ == "__main__":
    anos_arg = [int(a) for a in sys.argv[1:] if a.isdigit()] or None
    resultado = executar_processamento_silver(anos=anos_arg)

    erros = [k for k, v in resultado.items() if v.get("status") == "erro"]
    sys.exit(1 if erros else 0)
