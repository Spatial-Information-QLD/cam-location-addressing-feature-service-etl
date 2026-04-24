import pytest

from address_etl.kafka import publish_presigned_url


class FakeProducer:
    def __init__(self, error=None):
        self.error = error
        self.calls = []
        self.poll_calls = []
        self.flush_calls = 0

    def produce(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        callback = kwargs.get("callback")
        if callback is not None:
            callback(self.error, None)

    def poll(self, timeout: float | None = None) -> int:
        self.poll_calls.append(timeout)
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self.flush_calls += 1
        return 0


def test_publish_presigned_url_publishes_value_and_headers(monkeypatch: pytest.MonkeyPatch):
    fake_producer = FakeProducer()
    monkeypatch.setattr("address_etl.kafka.settings.kafka_topic", "pls.artifact-url.v1")

    headers = {
        "etl-name": "pls",
        "etl-started-at": "2026-04-23T02:00:00+00:00",
    }

    publish_presigned_url(
        "https://example.com/presigned",
        headers,
        producer=fake_producer,
    )

    assert len(fake_producer.calls) == 1
    _args, kwargs = fake_producer.calls[0]
    assert kwargs["value"] == "https://example.com/presigned"
    assert kwargs["headers"] == headers
    assert kwargs["callback"] is not None
    assert fake_producer.poll_calls == [1.0]
    assert fake_producer.flush_calls == 1


def test_publish_presigned_url_uses_configured_topic(monkeypatch: pytest.MonkeyPatch):
    fake_producer = FakeProducer()
    monkeypatch.setattr("address_etl.kafka.settings.kafka_topic", "pls.artifact-url.v1")

    publish_presigned_url("https://example.com/presigned", {}, producer=fake_producer)

    _args, kwargs = fake_producer.calls[0]
    assert _args == ("pls.artifact-url.v1",)
    assert kwargs["value"] == "https://example.com/presigned"


def test_publish_presigned_url_raises_on_delivery_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    fake_producer = FakeProducer(error="kafka down")
    monkeypatch.setattr("address_etl.kafka.settings.kafka_topic", "pls.artifact-url.v1")

    with pytest.raises(RuntimeError, match="Failed to deliver Kafka message: kafka down"):
        publish_presigned_url(
            "https://example.com/presigned",
            {},
            producer=fake_producer,
        )
