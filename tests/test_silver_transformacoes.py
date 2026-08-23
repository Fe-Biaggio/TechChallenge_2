"""
Testes unitários para as transformações puras de pipeline/batch/02_processamento_silver.py.

O módulo é carregado via importlib (ver tests/conftest.py) porque o nome do
arquivo começa com dígito e não é importável com a sintaxe normal de import.
"""
import pandas as pd
import pytest

from conftest import importar_modulo

silver = importar_modulo("pipeline/batch/02_processamento_silver.py", "silver_mod")


def test_padronizar_colunas_normaliza_nomes():
    df = pd.DataFrame({" Nome Coluna ": [1], "Município": [2], "JÁ_OK": [3]})
    resultado = silver._padronizar_colunas(df)
    assert list(resultado.columns) == ["nome_coluna", "municipio", "ja_ok"]


def test_decodificar_rede_mapeia_codigos_conhecidos():
    df = pd.DataFrame({"rede": [3, 5, 2]})
    resultado = silver._decodificar_rede(df, nome="teste")
    assert list(resultado["rede_label"]) == ["Municipal", "Pública", "Estadual"]


def test_decodificar_rede_codigo_desconhecido_vira_nulo(caplog):
    df = pd.DataFrame({"rede": [99]})
    resultado = silver._decodificar_rede(df, nome="teste")
    assert resultado["rede_label"].isna().all()


def test_melt_metas_converte_formato_largo_para_long():
    df = pd.DataFrame({
        "ano": [2023, 2024],
        "id_municipio": ["1100015", "1100015"],
        "meta_alfabetizacao_2024": [80.0, 85.0],
        "meta_alfabetizacao_2025": [82.0, 87.0],
    })
    resultado = silver._melt_metas(df, chave_negocio=["id_municipio"], nome="teste")

    assert set(resultado.columns) >= {"id_municipio", "ano_meta", "valor_meta", "ano_referencia"}
    # Para ano_meta=2024, a vintage mais recente (ano_referencia=2024) deve prevalecer
    linha_2024 = resultado[(resultado["id_municipio"] == "1100015") & (resultado["ano_meta"] == 2024)]
    assert len(linha_2024) == 1
    assert linha_2024["valor_meta"].iloc[0] == 85.0


def test_melt_metas_remove_valores_nulos():
    df = pd.DataFrame({
        "ano": [2023],
        "meta_alfabetizacao_2024": [None],
        "meta_alfabetizacao_2025": [90.0],
    })
    resultado = silver._melt_metas(df, chave_negocio=[], nome="teste")
    assert len(resultado) == 1
    assert resultado["ano_meta"].iloc[0] == 2025


def test_selecionar_rede_headline_prioriza_rede_publica():
    df = pd.DataFrame({
        "ano": [2024, 2024],
        "id_municipio": ["1100015", "1100015"],
        "rede_label": ["Municipal", "Pública"],
        "taxa_alfabetizacao": [70.0, 90.0],
    })
    resultado = silver._selecionar_rede_headline(df)
    assert len(resultado) == 1
    assert resultado["rede_label"].iloc[0] == "Pública"
    assert resultado["taxa_alfabetizacao"].iloc[0] == 90.0


def test_selecionar_rede_headline_usa_rede_disponivel_quando_nao_ha_prioritaria():
    df = pd.DataFrame({
        "ano": [2024],
        "id_municipio": ["1100015"],
        "rede_label": ["Privada"],
        "taxa_alfabetizacao": [60.0],
    })
    resultado = silver._selecionar_rede_headline(df)
    assert len(resultado) == 1
    assert resultado["rede_label"].iloc[0] == "Privada"


def test_remover_duplicatas_mantem_ultimo_registro():
    df = pd.DataFrame({"id_municipio": ["1", "1", "2"], "valor": [10, 20, 30]})
    resultado = silver._remover_duplicatas(df, chaves=["id_municipio"], nome="teste")
    assert len(resultado) == 2
    assert resultado.loc[resultado["id_municipio"] == "1", "valor"].iloc[0] == 20
