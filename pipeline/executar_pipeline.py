"""
Orquestrador — roda Bronze → Silver → Gold em sequência e só considera a
Gold "publicada" (disponível em data/gold/ para consumo por dashboards,
notebooks e análises) se a validação de qualidade de cada camada passar.

Por que isso existe: antes, quality/validacao_dados.py era um script
separado, rodado manualmente depois que 01/02/03 já tinham terminado —
nada impedia um alerta de qualidade de ficar disponível em data/gold/ para
quem fosse consumir. Este orquestrador fecha esse gap:

  1. Roda Bronze. Se falhar (tabela obrigatória), para.
  2. Valida Bronze. Se houver alerta em qualquer tabela, para ANTES de
     construir Silver em cima de dado já sinalizado.
  3. Roda Silver. Se falhar, para.
  4. Valida Silver. Mesma lógica — não constrói Gold sobre Silver reprovada.
  5. Roda Gold.
  6. Valida Gold. Se houver alerta, move tudo que acabou de ser gerado em
     data/gold/ para data/gold_rejeitada/<timestamp>/ e encerra com
     código de saída != 0 — data/gold/ nunca fica com dado que não passou
     no gate. Se data/gold/ já tinha uma versão publicada anteriormente
     (de uma execução que passou), ela permanece lá; só o que acabou de
     ser escrito nesta execução é que é rejeitado.

Streaming fica de fora deste gate por decisão — é opcional e append-only
(ver README > Arquitetura da Solução); rodar 04_simulacao_streaming.py e
reprocessar Silver/Gold continua um passo manual separado.

Uso:
    python pipeline/executar_pipeline.py
    python pipeline/executar_pipeline.py --anos 2023 2024
"""
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline.batch.config import GOLD_DIR, DATA_DIR  # noqa: E402
from pipeline.batch.utils import get_logger  # noqa: E402
from pipeline.monitoring.alertas import disparar_alerta  # noqa: E402

logger = get_logger(__name__)

SCRIPTS = {
    "bronze": ROOT / "pipeline" / "batch" / "01_ingestao_bronze.py",
    "silver": ROOT / "pipeline" / "batch" / "02_processamento_silver.py",
    "gold": ROOT / "pipeline" / "batch" / "03_agregacao_gold.py",
}


def _rodar_script(camada: str, args_anos: list) -> bool:
    """Executa o script da camada como subprocesso (mesmo caminho de 'Como Reproduzir')."""
    cmd = [sys.executable, str(SCRIPTS[camada]), *args_anos]
    logger.info(f"[Orquestrador] $ {' '.join(str(c) for c in cmd)}")
    resultado = subprocess.run(cmd)
    if resultado.returncode != 0:
        logger.error(f"[Orquestrador] '{camada}' falhou (código {resultado.returncode}) — pipeline interrompida")
        disparar_alerta(
            nivel="CRITICAL",
            origem=f"orquestrador.{camada}",
            mensagem=f"Script de {camada} encerrou com código {resultado.returncode}",
        )
        return False
    return True


def _gate_qualidade(camada: str) -> bool:
    """Roda a validação de qualidade da camada; reprova se qualquer tabela tiver alerta."""
    from quality.validacao_dados import validar_camada

    resultado = validar_camada(camada)
    tabelas_com_alerta = [t for t, r in resultado.items() if r["status"] == "ALERTA"]

    if tabelas_com_alerta:
        logger.error(f"[Gate] Camada '{camada}' REPROVADA — alerta em: {tabelas_com_alerta}")
        disparar_alerta(
            nivel="CRITICAL",
            origem=f"gate.{camada}",
            mensagem=f"Camada '{camada}' não passou no gate de qualidade — pipeline interrompida",
            contexto={"tabelas_com_alerta": tabelas_com_alerta},
        )
        return False

    logger.info(f"[Gate] Camada '{camada}' aprovada — {len(resultado)} tabela(s) no gate")
    return True


def _rejeitar_gold() -> None:
    """Move o conteúdo de data/gold/ para data/gold_rejeitada/<timestamp>/ — nada fica publicado."""
    if not GOLD_DIR.exists() or not any(GOLD_DIR.iterdir()):
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    destino = DATA_DIR / "gold_rejeitada" / timestamp
    destino.mkdir(parents=True, exist_ok=True)

    for item in GOLD_DIR.iterdir():
        shutil.move(str(item), str(destino / item.name))

    logger.warning(f"[Gate] Gold reprovada movida para {destino} — data/gold/ não contém dado não validado")


def executar(anos: list = None) -> bool:
    """Roda a pipeline completa com gate de qualidade entre camadas. Retorna True se publicou a Gold."""
    args_anos = [str(a) for a in anos] if anos else []

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETA (Bronze -> Silver -> Gold, com gate de qualidade)")
    logger.info("=" * 60)

    if not _rodar_script("bronze", args_anos):
        return False
    if not _gate_qualidade("bronze"):
        return False

    if not _rodar_script("silver", args_anos):
        return False
    if not _gate_qualidade("silver"):
        return False

    if not _rodar_script("gold", args_anos):
        return False
    if not _gate_qualidade("gold"):
        _rejeitar_gold()
        return False

    logger.info("=" * 60)
    logger.info("PIPELINE CONCLUÍDA — Gold publicada em data/gold/, todas as camadas passaram no gate")
    logger.info("=" * 60)
    return True


if __name__ == "__main__":
    argv = sys.argv[1:]
    anos_arg = None
    if "--anos" in argv:
        idx = argv.index("--anos")
        anos_arg = [int(a) for a in argv[idx + 1:] if a.isdigit()]

    sucesso = executar(anos=anos_arg)
    sys.exit(0 if sucesso else 1)
