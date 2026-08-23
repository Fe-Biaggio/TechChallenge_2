"""
Alertas de erro — mecanismo simples de observabilidade.

Não depende de infraestrutura externa (e-mail/Slack/PagerDuty): cada alerta é
logado no nível apropriado E persistido como uma linha em
data/monitoramento/alertas.jsonl (log append-only, formato JSON Lines), para
que fiquem consultáveis depois de a execução terminar — diferente do log de
console, que se perde ao fechar o terminal.

Uso:
    from pipeline.monitoring.alertas import disparar_alerta

    disparar_alerta(
        nivel="ERROR",
        origem="bronze.alunos",
        mensagem="Falha ao ingerir tabela",
        contexto={"tabela": "alunos"},
    )
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from pipeline.batch.config import MONITORAMENTO_DIR
from pipeline.batch.utils import get_logger

logger = get_logger("pipeline.monitoring.alertas")

ARQUIVO_ALERTAS = MONITORAMENTO_DIR / "alertas.jsonl"

_NIVEIS_LOG = {
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def disparar_alerta(
    nivel: str,
    origem: str,
    mensagem: str,
    contexto: Optional[dict] = None,
) -> dict:
    """
    Registra um alerta: loga no nível correspondente e persiste em
    data/monitoramento/alertas.jsonl para consulta posterior.

    Args:
        nivel: "INFO", "WARNING", "ERROR" ou "CRITICAL".
        origem: componente que disparou o alerta (ex: "bronze.alunos",
            "qualidade.silver", "streaming.consumer").
        mensagem: descrição legível do problema.
        contexto: dados adicionais opcionais (serializáveis em JSON).

    Returns:
        O alerta registrado, como dict.
    """
    nivel = nivel.upper()
    alerta = {
        "timestamp": datetime.utcnow().isoformat(),
        "nivel": nivel,
        "origem": origem,
        "mensagem": mensagem,
        "contexto": contexto or {},
    }

    log_level = _NIVEIS_LOG.get(nivel, logging.WARNING)
    logger.log(log_level, f"[ALERTA][{origem}] {mensagem} | contexto={contexto or {}}")

    MONITORAMENTO_DIR.mkdir(parents=True, exist_ok=True)
    with open(ARQUIVO_ALERTAS, "a", encoding="utf-8") as f:
        f.write(json.dumps(alerta, ensure_ascii=False, default=str) + "\n")

    return alerta


def ler_alertas(desde: Optional[datetime] = None) -> list:
    """Lê os alertas persistidos, opcionalmente filtrando por data mínima."""
    if not ARQUIVO_ALERTAS.exists():
        return []

    alertas = []
    with open(ARQUIVO_ALERTAS, "r", encoding="utf-8") as f:
        for linha in f:
            linha = linha.strip()
            if not linha:
                continue
            alerta = json.loads(linha)
            if desde is None or datetime.fromisoformat(alerta["timestamp"]) >= desde:
                alertas.append(alerta)
    return alertas
