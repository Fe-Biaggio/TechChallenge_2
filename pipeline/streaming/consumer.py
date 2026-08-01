"""
Consumidor de eventos de streaming.

Processa eventos recebidos (fila em memória ou Kafka) em micro-lotes
e os persiste na camada Bronze como arquivos Parquet particionados por data.
"""
import json
import time
import threading
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from kafka import KafkaConsumer as _KafkaConsumer
    KAFKA_DISPONIVEL = True
except ImportError:
    KAFKA_DISPONIVEL = False

from pipeline.batch.config import (
    BRONZE_DIR,
    COMPRESSAO_PARQUET,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC_INDICADORES,
    KAFKA_TOPIC_METAS,
)
from pipeline.batch.utils import get_logger

logger = get_logger(__name__)

DESTINOS_BRONZE = {
    KAFKA_TOPIC_INDICADORES: BRONZE_DIR / "streaming_indicadores",
    KAFKA_TOPIC_METAS: BRONZE_DIR / "streaming_metas",
}

SCHEMAS_ESPERADOS = {
    KAFKA_TOPIC_INDICADORES: [
        "evento", "timestamp", "id_municipio", "sigla_uf",
        "ano", "indicador_alfabetizacao", "total_alunos_avaliados",
        "ponto_corte_saeb", "fonte",
    ],
    KAFKA_TOPIC_METAS: [
        "evento", "timestamp", "sigla_uf", "ano",
        "meta_percentual", "revisao", "fonte",
    ],
}


def _validar_evento(evento: dict, topico: str) -> bool:
    """Verifica se o evento contém os campos obrigatórios do schema."""
    campos_obrigatorios = SCHEMAS_ESPERADOS.get(topico, [])
    ausentes = [c for c in campos_obrigatorios if c not in evento]
    if ausentes:
        logger.warning(f"[Consumer] Evento inválido em {topico} — campos ausentes: {ausentes}")
        return False
    return True


def _persistir_lote(topico: str, eventos: list) -> Optional[Path]:
    """Persiste um lote de eventos na camada Bronze."""
    destino = DESTINOS_BRONZE.get(topico)
    if not destino or not eventos:
        return None

    destino.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(eventos)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    caminho = destino / f"batch_{ts}.parquet"

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        caminho,
        compression=COMPRESSAO_PARQUET,
    )
    logger.info(f"[Consumer] {topico}: {len(eventos)} eventos → {caminho.name}")
    return caminho


class ConsumidorSimulado:
    """
    Consumidor de eventos usando threading.Queue como broker em memória.
    Processa eventos em micro-lotes e os persiste na Bronze.
    """

    def __init__(
        self,
        filas: dict,
        batch_size: int = 10,
        timeout_s: float = 3.0,
    ):
        self.filas = filas
        self.batch_size = batch_size
        self.timeout_s = timeout_s
        self._rodando = False
        self._threads: list[threading.Thread] = []
        self.total_processados = 0
        self.total_invalidos = 0
        self._lock = threading.Lock()

    def iniciar(self) -> None:
        self._rodando = True
        for topico, fila in self.filas.items():
            t = threading.Thread(
                target=self._processar_fila,
                args=(topico, fila),
                daemon=True,
                name=f"consumer-{topico}",
            )
            t.start()
            self._threads.append(t)
        logger.info(f"[Consumer] Iniciado — tópicos: {list(self.filas.keys())}")

    def parar(self) -> None:
        self._rodando = False
        for t in self._threads:
            t.join(timeout=8)
        logger.info(
            f"[Consumer] Parado — "
            f"{self.total_processados} processados | "
            f"{self.total_invalidos} inválidos"
        )

    def _processar_fila(self, topico: str, fila: Queue) -> None:
        buffer = []
        while self._rodando:
            try:
                evento = fila.get(timeout=self.timeout_s)
                if _validar_evento(evento, topico):
                    buffer.append(evento)
                else:
                    with self._lock:
                        self.total_invalidos += 1

                if len(buffer) >= self.batch_size:
                    _persistir_lote(topico, buffer)
                    with self._lock:
                        self.total_processados += len(buffer)
                    buffer = []

            except Empty:
                if buffer:
                    _persistir_lote(topico, buffer)
                    with self._lock:
                        self.total_processados += len(buffer)
                    buffer = []

        # Drena o restante ao encerrar
        while not fila.empty():
            try:
                buffer.append(fila.get_nowait())
            except Empty:
                break
        if buffer:
            _persistir_lote(topico, buffer)
            with self._lock:
                self.total_processados += len(buffer)


class ConsumidorKafka:
    """
    Consumidor de eventos usando Kafka real.
    Requer broker Kafka acessível em KAFKA_BOOTSTRAP_SERVERS.
    """

    def __init__(self, topicos: list):
        if not KAFKA_DISPONIVEL:
            raise ImportError("kafka-python não instalado. Execute: pip install kafka-python")
        self._consumer = _KafkaConsumer(
            *topicos,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self.total_processados = 0
        logger.info(f"[Kafka] Consumidor conectado — tópicos: {topicos}")

    def consumir(self, duracao_segundos: int = 60) -> None:
        inicio = time.time()
        buffer: dict[str, list] = {}

        for msg in self._consumer:
            topico = msg.topic
            evento = msg.value
            if _validar_evento(evento, topico):
                buffer.setdefault(topico, []).append(evento)
                if len(buffer[topico]) >= 10:
                    _persistir_lote(topico, buffer[topico])
                    self.total_processados += len(buffer[topico])
                    buffer[topico] = []

            if time.time() - inicio >= duracao_segundos:
                break

        # Persiste restante
        for topico, eventos in buffer.items():
            if eventos:
                _persistir_lote(topico, eventos)
                self.total_processados += len(eventos)

    def fechar(self) -> None:
        self._consumer.close()
        logger.info(f"[Kafka] Consumidor encerrado — {self.total_processados} processados")
