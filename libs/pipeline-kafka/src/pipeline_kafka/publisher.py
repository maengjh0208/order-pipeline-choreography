from opentelemetry.trace import Tracer

from pipeline_kafka.envelope import Envelope, to_kafka
from pipeline_kafka.telemetry import inject_traceparent

FLUSH_TIMEOUT_SECONDS = 10.0


class KafkaDeliveryError(Exception):
    """Kafka 브로커로 메시지 전달이 확인되지 않음 (실패 또는 timeout)"""


class KafkaPublisher:
    def __init__(self, producer, tracer: Tracer):
        # 실서비스: confluent_kafka.Producer / 테스트: MagicMock
        self._producer = producer
        # 실서비스: 전역 provider에서 꺼낸 tracer / 테스트: 격리된 tracer
        self._tracer = tracer
        # on_delivery 콜백이 모아두는 전달 실패 목록 (flush 때 확인하고 비움)
        self._errors = []

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
                on_delivery=self._on_delivery,
            )

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self._errors.append(err)

    def flush(self) -> None:
        """
        - publish()는 librdkafka 로컬 큐에 넣기만 함.
        - flush()는 그 큐의 메시지가 브로커까지 전달될때까지 블로킹 대기.
        - flush() 자체에는 시간 제한이 없음. 큐가 빌 때까지 기다림. 그런데 큐는 결국 비긴 함. 이유는 메시지마다 기본 5분짜리 수명이 있기 때문.
        - 그래서 FLUSH_TIMEOUT_SECONDS로 상한을 둠. 상한 안에 못 끝나면 반환값 > 0.
        - 메시지는 성공하면 큐에서 빠지고, 시간 내에 성공 못 하면 실패로 확정되고 큐에서 빠짐.
        """
        remaining = self._producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)
        errors, self._errors = self._errors, []

        if remaining > 0 or errors:
            raise KafkaDeliveryError(
                f"{remaining} message(s) not delivered in time, {len(errors)} failed: {errors}"
            )
