from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pipeline_kafka import KafkaDeliveryError, KafkaPublisher
from pipeline_kafka.publisher import FLUSH_TIMEOUT_SECONDS

from tests.helpers import make_envelope


def fail_delivery_on_flush(fake_producer: MagicMock) -> None:
    """flush() 도중 on_delivery로 전달 실패를 알리는 producer로 만든다. (flush 반환값은 0)"""

    def failing_flush(timeout: float) -> int:
        # 진짜 librdkafka 흉내 : flush 도중 on_delivery 콜백으로 실패 알리고, 메시지는 끝났으니 큐는 비어서 0을 반환함
        on_delivery = fake_producer.produce.call_args.kwargs["on_delivery"]
        on_delivery("MSG_SIZE_TOO_LARGE", None)
        return 0

    fake_producer.flush.side_effect = failing_flush


def test_publish_calls_producer_produce_envelope_data():
    # 이 테스트는 스팬 내용 자체는 안보니까 exporter 연결 없이 최소로.
    provider = TracerProvider()
    tracer = provider.get_tracer(__name__)

    # confluent_kafka.Producer 대역. 네트워크 호출 없음. 호출 기록만 남김.
    fake_producer = MagicMock()
    publisher = KafkaPublisher(fake_producer, tracer)

    envelope = make_envelope()
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

    publisher.publish("order.events", make_envelope())

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


def test_flush_raises_when_delivery_failed():
    fake_producer = MagicMock()

    fail_delivery_on_flush(fake_producer)

    publisher = KafkaPublisher(fake_producer, TracerProvider().get_tracer(__name__))
    publisher.publish("order.events", make_envelope())

    with pytest.raises(KafkaDeliveryError):
        publisher.flush()


def test_flush_succeeds_after_previous_delivery_failure():
    fake_producer = MagicMock()

    fail_delivery_on_flush(fake_producer)

    publisher = KafkaPublisher(fake_producer, TracerProvider().get_tracer(__name__))
    publisher.publish("order.events", make_envelope())

    with pytest.raises(KafkaDeliveryError):
        publisher.flush()

    # 2번째 배치: Kafka 정상. 새 에러 없음
    fake_producer.flush.side_effect = None
    fake_producer.flush.return_value = 0

    # 이전 실패가 남아 있지 않다면 raise 하지 않아야 함.
    publisher.flush()
