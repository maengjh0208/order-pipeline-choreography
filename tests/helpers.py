from pipeline_kafka import Envelope


def make_envelope() -> Envelope:
    return Envelope.new(
        event_type="order.placed",
        correlation_id="order_1",
        producer="order-service",
        payload={"sku": "A"},
    )
