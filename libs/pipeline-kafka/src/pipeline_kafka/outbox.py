import asyncio
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String, func, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pipeline_kafka.envelope import Envelope, to_kafka


class Base(DeclarativeBase):
    pass


class OutboxMessage(Base):
    __tablename__ = "outbox_message"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    topic: Mapped[str] = mapped_column(String, nullable=False)
    key: Mapped[str] = mapped_column(String, nullable=False)
    headers: Mapped[list] = mapped_column(JSONB, nullable=False)  # 조회용 스냅샷
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)  # Envelope 전체 (재구성용 원본)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


async def enqueue(session: AsyncSession, topic: str, envelope: Envelope) -> None:
    record = to_kafka(envelope)

    """
    payload=envelope.model_dump(mode="json") 하는 이유:
    JSONB 타입의 컬럼에 넣을 값은 dict 이어야 함. SQLAlchemy가 이걸 받고, 알아서 JSON으로 인코딩해서 DB에 내보냄.
    그런데 UUID, datetime 처럼 기본 model_dump()로는 JSON이 못 삼키는 Python 전용 타입이 들어있을때, 그대로 넘기면 SQLAlchemy가 실제 INSERT 시점에 얘네들을 JSON으로 못바꿔서 에러가 발생함.
    그래서 JSON에 담을 수 있는 값들로만 이뤄진 dict을 돌려주려고 한 것임.
    """
    session.add(
        OutboxMessage(
            topic=topic,
            key=record.key,
            headers=[[name, value.decode("utf-8")] for name, value in record.headers],
            payload=envelope.model_dump(mode="json"),
        )
    )


class OutboxPoller:
    def __init__(self, session_factory, publisher, batch: int = 100):
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch = batch

    async def poll_once(self) -> None:
        async with self._session_factory() as session:
            result = await session.execute(
                select(OutboxMessage).where(OutboxMessage.published_at.is_(None)).limit(self._batch)
            )
            rows = result.scalars().all()

            # 순서: kafka publish -> kafka flush -> db mark -> db commit (at-least-once)
            for row in rows:
                # JSONB에서 꺼낸 dict을 Envelope로 복원 (문자열 UUID/datetime -> 원래 타입으로)
                envelope = Envelope.model_validate(row.payload)
                self._publisher.publish(topic=row.topic, envelope=envelope)
                row.published_at = func.now()

            # flush() 실패 시 예외 -> commit X (published_at 롤백)
            # flush는 동기 블로킹(최대 FLUSH_TIMEOUT_SECONDS) -> 별도 스레드에서 실행해서 이벤트루프(같은 루프의 API 요청 처리)를 막지 않도록.
            await asyncio.to_thread(self._publisher.flush)
            await session.commit()
