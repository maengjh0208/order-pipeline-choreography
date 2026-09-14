from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pipeline_kafka.telemetry import extract_context, inject_traceparent


def test_inject_extract_links_parent_and_child_spans():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(__name__)

    with tracer.start_as_current_span("produce order.placed"):
        headers = inject_traceparent([("message-id", b"abc")])

    assert any(k == "traceparent" for k, _ in headers)

    ctx = extract_context(headers)
    with tracer.start_as_current_span("consume order.placed", context=ctx):
        pass

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["consume order.placed"].context.trace_id == spans["produce order.placed"].context.trace_id
    assert spans["consume order.placed"].parent.span_id == spans["produce order.placed"].context.span_id
