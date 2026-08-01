"""
Simulação de Ingestão Streaming — Camada Bronze

Orquestra o produtor e o consumidor para simular um fluxo de eventos
de atualização do Indicador Criança Alfabetizada em tempo quase real.

Por padrão usa fila em memória (sem infraestrutura). Para usar Kafka real,
passe --kafka e garanta que KAFKA_BOOTSTRAP_SERVERS esteja no .env.

Uso:
    python pipeline/streaming/04_simulacao_streaming.py
    python pipeline/streaming/04_simulacao_streaming.py 120        # 120 segundos
    python pipeline/streaming/04_simulacao_streaming.py 60 --kafka # Kafka real
"""
import signal
import sys
import time
from queue import Queue

from pipeline.batch.config import (
    KAFKA_TOPIC_INDICADORES,
    KAFKA_TOPIC_METAS,
)
from pipeline.batch.utils import get_logger
from pipeline.streaming.consumer import ConsumidorKafka, ConsumidorSimulado
from pipeline.streaming.producer import ProdutorKafka, ProdutorSimulado

logger = get_logger(__name__)

_ENCERRAR = False


def _configurar_sinais():
    def _handler(sig, frame):
        global _ENCERRAR
        logger.info("Sinal de encerramento recebido — finalizando...")
        _ENCERRAR = True

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


def executar_modo_simulado(duracao_segundos: int = 60) -> dict:
    """
    Executa o pipeline streaming completo usando fila em memória.
    Não requer Kafka nem nenhuma infraestrutura externa.
    """
    filas = {
        KAFKA_TOPIC_INDICADORES: Queue(),
        KAFKA_TOPIC_METAS: Queue(),
    }

    produtor = ProdutorSimulado(filas)
    consumidor = ConsumidorSimulado(filas, batch_size=10, timeout_s=3.0)

    consumidor.iniciar()
    produtor.iniciar(intervalo_segundos=1.5)

    inicio = time.time()
    try:
        while not _ENCERRAR and (time.time() - inicio) < duracao_segundos:
            restante = int(duracao_segundos - (time.time() - inicio))
            logger.info(
                f"[Streaming] Rodando... {restante:>3}s restantes | "
                f"processados: {consumidor.total_processados}"
            )
            time.sleep(10)
    finally:
        produtor.parar()
        time.sleep(2)  # Aguarda consumer drenar o buffer restante
        consumidor.parar()

    return {
        "modo": "simulado",
        "duracao_s": round(time.time() - inicio, 1),
        "total_enviado": produtor.total_enviado,
        "total_processados": consumidor.total_processados,
        "total_invalidos": consumidor.total_invalidos,
    }


def executar_modo_kafka(duracao_segundos: int = 60) -> dict:
    """
    Executa o pipeline streaming com Kafka real.
    Produtor e consumidor correm em threads separadas.
    """
    try:
        produtor = ProdutorKafka()
    except ImportError as e:
        logger.error(str(e))
        sys.exit(1)

    total_enviado = 0
    inicio = time.time()

    import random
    from pipeline.streaming.producer import _gerar_evento_indicador, _gerar_evento_meta

    try:
        while not _ENCERRAR and (time.time() - inicio) < duracao_segundos:
            if random.random() > 0.2:
                evento = _gerar_evento_indicador()
                topico = KAFKA_TOPIC_INDICADORES
            else:
                evento = _gerar_evento_meta()
                topico = KAFKA_TOPIC_METAS
            produtor.enviar(topico, evento)
            total_enviado += 1
            time.sleep(1.0)
    finally:
        produtor.fechar()

    return {
        "modo": "kafka",
        "duracao_s": round(time.time() - inicio, 1),
        "total_enviado": total_enviado,
    }


if __name__ == "__main__":
    _configurar_sinais()

    usar_kafka = "--kafka" in sys.argv
    args_num = [a for a in sys.argv[1:] if a.isdigit()]
    duracao = int(args_num[0]) if args_num else 60

    logger.info("=" * 60)
    logger.info("PIPELINE STREAMING — INDICADOR CRIANÇA ALFABETIZADA")
    logger.info(f"Backend : {'Kafka' if usar_kafka else 'Simulado (fila em memória)'}")
    logger.info(f"Duração : {duracao}s")
    logger.info(f"Destino : data/bronze/streaming_indicadores | data/bronze/streaming_metas")
    logger.info("=" * 60)

    if usar_kafka:
        resultado = executar_modo_kafka(duracao_segundos=duracao)
    else:
        resultado = executar_modo_simulado(duracao_segundos=duracao)

    logger.info("=" * 60)
    logger.info("STREAMING CONCLUÍDO")
    for k, v in resultado.items():
        logger.info(f"  {k:<25}: {v}")
    logger.info("=" * 60)
