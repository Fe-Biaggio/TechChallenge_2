"""Testes unitários para pipeline/monitoring/alertas.py."""
import json

import pipeline.monitoring.alertas as alertas_mod


def test_disparar_alerta_persiste_jsonl(tmp_path, monkeypatch):
    arquivo = tmp_path / "alertas.jsonl"
    monkeypatch.setattr(alertas_mod, "MONITORAMENTO_DIR", tmp_path)
    monkeypatch.setattr(alertas_mod, "ARQUIVO_ALERTAS", arquivo)

    resultado = alertas_mod.disparar_alerta(
        nivel="error", origem="teste.origem", mensagem="algo falhou", contexto={"tabela": "x"}
    )

    assert resultado["nivel"] == "ERROR"
    assert arquivo.exists()

    linhas = arquivo.read_text(encoding="utf-8").strip().split("\n")
    assert len(linhas) == 1
    registrado = json.loads(linhas[0])
    assert registrado["origem"] == "teste.origem"
    assert registrado["mensagem"] == "algo falhou"
    assert registrado["contexto"] == {"tabela": "x"}


def test_disparar_alerta_acumula_multiplas_linhas(tmp_path, monkeypatch):
    arquivo = tmp_path / "alertas.jsonl"
    monkeypatch.setattr(alertas_mod, "MONITORAMENTO_DIR", tmp_path)
    monkeypatch.setattr(alertas_mod, "ARQUIVO_ALERTAS", arquivo)

    alertas_mod.disparar_alerta(nivel="WARNING", origem="a", mensagem="1")
    alertas_mod.disparar_alerta(nivel="ERROR", origem="b", mensagem="2")

    linhas = arquivo.read_text(encoding="utf-8").strip().split("\n")
    assert len(linhas) == 2


def test_ler_alertas_retorna_lista_vazia_se_arquivo_nao_existe(tmp_path, monkeypatch):
    monkeypatch.setattr(alertas_mod, "ARQUIVO_ALERTAS", tmp_path / "nao_existe.jsonl")
    assert alertas_mod.ler_alertas() == []


def test_ler_alertas_le_de_volta_o_que_foi_escrito(tmp_path, monkeypatch):
    arquivo = tmp_path / "alertas.jsonl"
    monkeypatch.setattr(alertas_mod, "MONITORAMENTO_DIR", tmp_path)
    monkeypatch.setattr(alertas_mod, "ARQUIVO_ALERTAS", arquivo)

    alertas_mod.disparar_alerta(nivel="CRITICAL", origem="x", mensagem="falha grave")
    lidos = alertas_mod.ler_alertas()

    assert len(lidos) == 1
    assert lidos[0]["nivel"] == "CRITICAL"
