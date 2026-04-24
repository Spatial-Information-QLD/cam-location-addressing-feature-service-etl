import socket
from collections.abc import Mapping
from typing import Any, Protocol

from address_etl.settings import settings

_producer: "ProducerLike | None" = None


class ProducerLike(Protocol):
    def produce(self, *args: Any, **kwargs: Any) -> Any: ...

    def poll(self, timeout: float | None = None) -> int: ...

    def flush(self, timeout: float | None = None) -> int: ...


def get_producer() -> ProducerLike:
    global _producer

    if _producer is not None:
        return _producer

    from confluent_kafka import Producer

    _producer = Producer(settings.kafka_client_config(client_id=socket.gethostname()))
    return _producer


def publish_presigned_url(
    presigned_url: str,
    headers: Mapping[str, str],
    *,
    producer: ProducerLike | None = None,
) -> None:
    kafka_producer = producer or get_producer()
    delivery_error: RuntimeError | None = None

    def on_delivery(err, _msg) -> None:
        nonlocal delivery_error
        if err is not None:
            delivery_error = RuntimeError(f"Failed to deliver Kafka message: {err}")

    kafka_producer.produce(
        settings.kafka_topic,
        value=presigned_url,
        headers=dict(headers),
        callback=on_delivery,
    )
    kafka_producer.poll(1.0)
    kafka_producer.flush()

    if delivery_error is not None:
        raise delivery_error
