from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pipeline_kafka import Envelope, KafkaDeliveryError, KafkaPublisher
from pipeline_kafka.publisher import FLUSH_TIMEOUT_SECONDS


def test_publish_calls_producer_produce_envelope_data():
    # 이 테스트는 스팬 내용 자체는 안보니까 exporter 연결 없이 최소로.
    provider = TracerProvider()
    tracer = provider.get_tracer(__name__)

    # confluent_kafka.Producer 대역. 네트워크 호출 없음. 호출 기록만 남김.
    fake_producer = MagicMock()
    publisher = KafkaPublisher(fake_producer, tracer)

    envelope = Envelope.new(
        event_type="order.placed",
        correlation_id="order_1",
        producer="order-service",
        payload={"sku": "A"},
    )
    publisher.publish("order.events", envelope)

    fake_producer.produce.assert_called_once()  # produce()가 정확히 한 번 불렸는지 확인
    _, kwargs = fake_producer.produce.call_args  # 마지막 호출의 (args, kwargs)를 꺼냄

    assert kwargs["topic"] == "order.events"
    assert kwargs["key"] == envelope.correlation_id
    headers = dict(kwargs["headers"])
    assert headers["message-id"] == str(envelope.event_id).encode("utf-8")


def test_publish_starts_produce_span_and_injects_traceparent():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(__name__)  # 격리된 tracer. 전역 상태 안건드림.

    fake_producer = MagicMock()
    publisher = KafkaPublisher(fake_producer, tracer)

    envelope = Envelope.new(
        event_type="order.placed",
        correlation_id="order_1",
        producer="order-service",
        payload={"sku": "A"},
    )
    publisher.publish("order.events", envelope)

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "produce order.placed"  # span 이름 : "produce {event_type}"

    _, kwargs = fake_producer.produce.call_args
    headers = dict(kwargs["headers"])
    assert "traceparent" in headers  # inject_traceparent가 실제로 헤더에 넣었는지 확인


def test_flush_delegates_to_producer():
    fake_producer = MagicMock()
    fake_producer.flush.return_value = 0

    publisher = KafkaPublisher(fake_producer, TracerProvider().get_tracer(__name__))

    publisher.flush()

    fake_producer.flush.assert_called_once_with(timeout=FLUSH_TIMEOUT_SECONDS)


def test_flush_raises_when_messages_remain_after_timeout():
    fake_producer = MagicMock()
    # 1개가 timeout 안에 못 끝남을 흉내낸것 (return_value로 반환값을 정할 수 있음)
    fake_producer.flush.return_value = 1
    publisher = KafkaPublisher(fake_producer, TracerProvider().get_tracer(__name__))

    with pytest.raises(KafkaDeliveryError):
        publisher.flush()
