"""Testes unitários para quality/validacao_dados.py — regras puras de qualidade de dados."""
import pandas as pd
import pytest

from quality.validacao_dados import (
    verificar_completude,
    verificar_unicidade,
    verificar_dominio_uf,
    verificar_dominio_numerico,
    verificar_integridade_referencial,
)


def test_verificar_completude_sem_alerta_quando_dentro_do_limite():
    df = pd.DataFrame({"a": [1, 2, None, 4]})
    alertas = verificar_completude(df, colunas=["a"], max_pct=30.0, nome="teste")
    assert alertas == []


def test_verificar_completude_alerta_quando_acima_do_limite():
    df = pd.DataFrame({"a": [1, None, None, None]})
    alertas = verificar_completude(df, colunas=["a"], max_pct=10.0, nome="teste")
    assert len(alertas) == 1
    assert alertas[0]["tipo"] == "NULOS_ACIMA_LIMITE"


def test_verificar_completude_coluna_ausente():
    df = pd.DataFrame({"a": [1, 2]})
    alertas = verificar_completude(df, colunas=["b"], max_pct=10.0, nome="teste")
    assert alertas[0]["tipo"] == "COLUNA_AUSENTE"


def test_verificar_unicidade_sem_duplicatas():
    df = pd.DataFrame({"id": [1, 2, 3]})
    assert verificar_unicidade(df, chaves=["id"], nome="teste") == []


def test_verificar_unicidade_detecta_duplicatas():
    df = pd.DataFrame({"id": [1, 1, 2]})
    alertas = verificar_unicidade(df, chaves=["id"], nome="teste")
    assert len(alertas) == 1
    assert alertas[0]["tipo"] == "DUPLICATAS"


def test_verificar_dominio_uf_valida():
    df = pd.DataFrame({"sigla_uf": ["SP", "RJ", "MG"]})
    assert verificar_dominio_uf(df, nome="teste") == []


def test_verificar_dominio_uf_invalida():
    df = pd.DataFrame({"sigla_uf": ["SP", "XX"]})
    alertas = verificar_dominio_uf(df, nome="teste")
    assert len(alertas) == 1
    assert "XX" in alertas[0]["valor"]


def test_verificar_dominio_numerico_dentro_do_intervalo():
    df = pd.DataFrame({"idhm": [0.5, 0.7, 0.9]})
    alertas = verificar_dominio_numerico(df, regra={"idhm": (0.0, 1.0)}, nome="teste")
    assert alertas == []


def test_verificar_dominio_numerico_fora_do_intervalo():
    df = pd.DataFrame({"taxa_alfabetizacao": [50.0, 105.0, -3.0]})
    alertas = verificar_dominio_numerico(df, regra={"taxa_alfabetizacao": (0.0, 100.0)}, nome="teste")
    assert len(alertas) == 1
    assert "2 registros" in alertas[0]["valor"]


def test_verificar_dominio_numerico_ignora_coluna_ausente():
    df = pd.DataFrame({"outra": [1, 2]})
    assert verificar_dominio_numerico(df, regra={"idhm": (0.0, 1.0)}, nome="teste") == []


def test_verificar_integridade_referencial_sem_orfaos(tmp_path):
    df_ref = pd.DataFrame({"sigla_uf": ["SP", "RJ"]})
    (tmp_path / "diretorio_uf").mkdir()
    df_ref.to_parquet(tmp_path / "diretorio_uf" / "data.parquet", index=False)

    df = pd.DataFrame({"sigla_uf": ["SP", "RJ"]})
    alertas = verificar_integridade_referencial(
        df, nome="teste", regra={"sigla_uf": ("diretorio_uf", "sigla_uf")}, camada_dir=tmp_path
    )
    assert alertas == []


def test_verificar_integridade_referencial_detecta_orfaos(tmp_path):
    df_ref = pd.DataFrame({"sigla_uf": ["SP", "RJ"]})
    (tmp_path / "diretorio_uf").mkdir()
    df_ref.to_parquet(tmp_path / "diretorio_uf" / "data.parquet", index=False)

    df = pd.DataFrame({"sigla_uf": ["SP", "ZZ"]})
    alertas = verificar_integridade_referencial(
        df, nome="teste", regra={"sigla_uf": ("diretorio_uf", "sigla_uf")}, camada_dir=tmp_path
    )
    assert len(alertas) == 1
    assert alertas[0]["tipo"] == "INTEGRIDADE_REFERENCIAL"
