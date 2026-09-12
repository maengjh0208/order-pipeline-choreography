import pytest
from pipeline_kafka import Envelope, from_kafka, to_kafka
from pydantic import ValidationError


def test_new_sets_defaults():
    envelope = Envelope.new("order.placed", "order_1", "order-service", {"x": 1})

    assert envelope.schema_version == 1
    assert envelope.occurred_at.tzinfo is not None


def test_event_id_is_unique_per_instance():
    envelope_1 = Envelope.new("t", "c", "p", {})
    envelope_2 = Envelope.new("t", "c", "p", {})

    assert envelope_1.event_id != envelope_2.event_id


def test_envelope_is_frozen():
    envelope = Envelope.new("t", "c", "p", {})

    with pytest.raises(ValidationError):
        # 필드 재할당시 에러 발생
        envelope.event_type = "changed"


def test_to_kafka_shape():
    envelope = Envelope.new("order.placed", "order_1", "order-service", {"sku": "A"})
    record = to_kafka(envelope)

    assert record.key == "order_1"
    assert isinstance(record.value, bytes)

    headers = dict(record.headers)
    assert headers["message-id"] == str(envelope.event_id).encode("utf-8")
    assert headers["correlation-id"] == envelope.correlation_id.encode("utf-8")
    assert headers["event-type"] == envelope.event_type.encode("utf-8")


def test_round_trip():
    envelope = Envelope.new(
        "order.placed", "order_1", "order-service", {"sku": "A", "qty": 2}
    )
    assert from_kafka(to_kafka(envelope).value) == envelope
