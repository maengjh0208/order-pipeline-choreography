from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, NamedTuple
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class Envelope(BaseModel):
    model_config = ConfigDict(frozen=True)  # 필드 재할당 금지.

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: str
    producer: str
    schema_version: int = 1
    payload: dict[str, Any]

    @classmethod
    def new(
        cls,
        event_type: str,
        correlation_id: str,
        producer: str,
        payload: dict[str, Any],
    ) -> Envelope:  # from __future__ import annotations 덕분에 모든 annotaion을 지연 평가로 바꿔서 자기 참조 가능.
        return cls(
            event_type=event_type,
            correlation_id=correlation_id,
            producer=producer,
            payload=payload,
        )


class KafkaRecord(NamedTuple):  # collections의 namedtuple과 같은 효과
    key: str
    value: bytes
    headers: list[tuple[str, bytes]]


def to_kafka(envelope: Envelope) -> KafkaRecord:
    return KafkaRecord(
        key=envelope.correlation_id,
        value=envelope.model_dump_json().encode("utf-8"),
        headers=[  # headers가 따로 필요한 이유 : value를 JSON 파싱하기 전에 먼저 headers를 보고 판단할거라서.
            ("message-id", str(envelope.event_id).encode("utf-8")),
            ("correlation-id", envelope.correlation_id.encode("utf-8")),
            ("event-type", envelope.event_type.encode("utf-8")),
        ],
    )


def from_kafka(value: bytes) -> Envelope:
    return Envelope.model_validate_json(value)
