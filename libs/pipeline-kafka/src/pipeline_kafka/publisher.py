from opentelemetry.trace import Tracer

from pipeline_kafka.envelope import Envelope, to_kafka
from pipeline_kafka.telemetry import inject_traceparent


class KafkaPublisher:
    def __init__(self, producer, tracer: Tracer):
        # 실서비스: confluent_kafka.Producer / 테스트: MagicMock
        self._producer = producer
        # 실서비스: 전역 provider에서 꺼낸 tracer / 테스트: 격리된 tracer
        self._tracer = tracer

    def publish(self, topic: str, envelope: Envelope) -> None:
        record = to_kafka(envelope)
        with self._tracer.start_as_current_span(f"produce {envelope.event_type}"):
            # 스팬이 '활성'인 동안 inject해야 그 스팬의 trace_id/span_id가 traceparent에 실림.
            headers = inject_traceparent(record.headers)

            self._producer.produce(
                topic=topic,
                key=record.key,
                value=record.value,
                headers=headers,
            )
            # flush는 여기서 안함 - 배치 끝에서 poller가 한번에 처리
