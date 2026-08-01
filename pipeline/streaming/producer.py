"""
Produtor de eventos de streaming.

Gera eventos sintéticos de atualização do Indicador Criança Alfabetizada,
simulando um sistema que envia novos resultados em tempo quase real.

Backends disponíveis:
  - ProdutorSimulado: usa threading.Queue (sem dependências externas)
  - ProdutorKafka:    usa Kafka real via kafka-python (requer broker ativo)
"""
import json
import random
import time
import threading
from datetime import datetime
from queue import Queue
from typing import Optional

try:
    from kafka import KafkaProducer as _KafkaProducer
    KAFKA_DISPONIVEL = True
except ImportError:
    KAFKA_DISPONIVEL = False

from pipeline.batch.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INDICADORES,
    KAFKA_TOPIC_METAS,
)
from pipeline.batch.utils import get_logger

logger = get_logger(__name__)

UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO",
    "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI",
    "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]

# IDs de município sintéticos (códigos IBGE válidos por UF)
_ID_MUNICIPIO_BASE = {
    "AC": 1200013, "AL": 2700102, "AP": 1600014, "AM": 1300029,
    "BA": 2900108, "CE": 2300101, "DF": 5300108, "ES": 3200102,
    "GO": 5200050, "MA": 2100055, "MT": 5100102, "MS": 5000203,
    "MG": 3100104, "PA": 1500107, "PB": 2500106, "PR": 4100103,
    "PE": 2600054, "PI": 2200053, "RJ": 3300100, "RN": 2400109,
    "RS": 4300034, "RO": 1100015, "RR": 1400027, "SC": 4200051,
    "SP": 3500105, "SE": 2800100, "TO": 1700251,
}


def _gerar_evento_indicador(ano: int = 2024) -> dict:
    """Evento de atualização do indicador municipal de alfabetização."""
    sigla_uf = random.choice(UFS)
    id_base = _ID_MUNICIPIO_BASE[sigla_uf]
    return {
        "evento": "atualizacao_indicador",
        "timestamp": datetime.utcnow().isoformat(),
        "id_municipio": str(id_base + random.randint(0, 50)),
        "sigla_uf": sigla_uf,
        "ano": ano,
        "indicador_alfabetizacao": round(random.uniform(38.0, 99.5), 2),
        "total_alunos_avaliados": random.randint(30, 8000),
        "ponto_corte_saeb": 743,
        "fonte": "streaming_simulado",
    }


def _gerar_evento_meta(ano: int = 2024) -> dict:
    """Evento de atualização ou revisão de meta municipal."""
    return {
        "evento": "atualizacao_meta",
        "timestamp": datetime.utcnow().isoformat(),
        "sigla_uf": random.choice(UFS),
        "ano": ano,
        "meta_percentual": round(random.uniform(75.0, 100.0), 2),
        "revisao": random.random() > 0.7,
        "fonte": "streaming_simulado",
    }


class ProdutorSimulado:
    """
    Produtor de eventos usando threading.Queue como broker em memória.
    Funciona sem nenhuma dependência de infraestrutura externa.
    """

    def __init__(self, filas: dict):
        self.filas = filas
        self._rodando = False
        self._thread: Optional[threading.Thread] = None
        self.total_enviado = 0

    def enviar(self, topico: str, evento: dict) -> None:
        if topico in self.filas:
            self.filas[topico].put(evento)
            self.total_enviado += 1
            logger.debug(f"[Produtor] → {topico}: {evento['evento']}")

    def iniciar(self, intervalo_segundos: float = 1.5) -> None:
        self._rodando = True
        self._thread = threading.Thread(
            target=self._loop,
            args=(intervalo_segundos,),
            daemon=True,
            name="produtor-simulado",
        )
        self._thread.start()
        logger.info(f"[Produtor] Iniciado — intervalo: {intervalo_segundos}s")

    def parar(self) -> None:
        self._rodando = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info(f"[Produtor] Parado — {self.total_enviado} eventos enviados")

    def _loop(self, intervalo: float) -> None:
        while self._rodando:
            # 80% indicadores, 20% metas
            if random.random() > 0.2:
                evento = _gerar_evento_indicador()
                topico = KAFKA_TOPIC_INDICADORES
            else:
                evento = _gerar_evento_meta()
                topico = KAFKA_TOPIC_METAS
            self.enviar(topico, evento)
            time.sleep(intervalo)


class ProdutorKafka:
    """
    Produtor de eventos usando Kafka real.
    Requer broker Kafka acessível em KAFKA_BOOTSTRAP_SERVERS.
    """

    def __init__(self):
        if not KAFKA_DISPONIVEL:
            raise ImportError("kafka-python não instalado. Execute: pip install kafka-python")
        self._producer = _KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
        logger.info(f"[Kafka] Produtor conectado em {KAFKA_BOOTSTRAP_SERVERS}")

    def enviar(self, topico: str, evento: dict, chave: str = None) -> None:
        self._producer.send(topico, key=chave, value=evento)
        self._producer.flush()
        logger.debug(f"[Kafka] → {topico}: {evento.get('evento')}")

    def fechar(self) -> None:
        self._producer.close()
        logger.info("[Kafka] Produtor encerrado")
