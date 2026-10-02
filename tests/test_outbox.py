from unittest.mock import ANY, MagicMock, call

from pipeline_kafka import Envelope
from pipeline_kafka.outbox import OutboxMessage, OutboxPoller, enqueue
from sqlalchemy import select


def make_envelope() -> Envelope:
    return Envelope.new(
        event_type="order.placed",
        correlation_id="order_1",
        producer="order-service",
        payload={"sku": "A"},
    )


async def test_enqueue_inserts_a_row(session_factory):
    async with session_factory() as session:
        await enqueue(session=session, topic="order.events", envelope=make_envelope())
        await session.commit()

        result = await session.execute(select(OutboxMessage))
        row = result.scalar_one()

    assert row.topic == "order.events"
    assert row.key == "order_1"
    assert row.published_at is None
    assert row.payload["event_type"] == "order.placed"


async def test_poll_once_does_nothing_when_outbox_is_empty(session_factory):
    fake_publisher = MagicMock()
    poller = OutboxPoller(session_factory, fake_publisher)

    await poller.poll_once()

    fake_publisher.publish.assert_not_called()


async def test_poll_once_publishes_unpublished_row(session_factory):
    # 1. OutboxMessage 테이블에 발행해야 할 데이터 하나 넣어두고,
    envelope = make_envelope()
    async with session_factory() as session:
        await enqueue(session=session, topic="order.events", envelope=envelope)
        await session.commit()

    # 2. Poller가 발행해야 할 메시지를 꺼내서 발행한다.
    fake_publisher = MagicMock()
    poller = OutboxPoller(session_factory, fake_publisher)
    await poller.poll_once()

    fake_publisher.publish.assert_called_once_with(topic="order.events", envelope=envelope)


async def test_poll_once_marks_row_as_published(session_factory):
    # 1. OutboxMessage 테이블에 발행해야 할 데이터 하나 넣어두고,
    async with session_factory() as session:
        await enqueue(session=session, topic="order.events", envelope=make_envelope())
        await session.commit()

    # 2. Poller가 발행해야 할 메시지를 꺼내서 발행한다.
    fake_publisher = MagicMock()
    poller = OutboxPoller(session_factory, fake_publisher)
    await poller.poll_once()

    # 3. Poller가 커밋까지 했는지 확인한다.
    async with session_factory() as session:
        row = (await session.execute(select(OutboxMessage))).scalar_one()

    assert row.published_at is not None


async def test_poll_once_flushes_once_after_publishing_batch(session_factory):
    async with session_factory() as session:
        await enqueue(session=session, topic="order.events", envelope=make_envelope())
        await enqueue(session=session, topic="order.events", envelope=make_envelope())
        await session.commit()

    fake_publisher = MagicMock()
    poller = OutboxPoller(session_factory, fake_publisher)
    await poller.poll_once()

    # 호출 순서 검증
    assert fake_publisher.mock_calls == [
        call.publish(topic="order.events", envelope=ANY),
        call.publish(topic="order.events", envelope=ANY),
        call.flush(),
    ]
