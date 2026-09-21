from pipeline_kafka import Envelope
from pipeline_kafka.outbox import OutboxMessage, enqueue
from sqlalchemy import select


async def test_enqueue_inserts_a_row(db_session):
    envelope = Envelope.new(
        event_type="order.placed",
        correlation_id="order_1",
        producer="order-service",
        payload={"sku": "A"},
    )

    await enqueue(session=db_session, topic="order.events", envelope=envelope)
    await db_session.commit()

    result = await db_session.execute(select(OutboxMessage))
    row = result.scalar_one()
    assert row.topic == "order.events"
    assert row.key == "order_1"
    assert row.published_at is None
    assert row.payload["event_type"] == "order.placed"
