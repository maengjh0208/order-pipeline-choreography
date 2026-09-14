from opentelemetry import context, propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def setup_telemetry(service_name: str, otlp_endpoint: str) -> None:
    """트레이스를 만들거나 넘기는 일과는 무관. 그냥 시작할 때 한 번 하는 준비로, 어디 서버로 보고할지 등록한다."""
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)  # 이 서비스의 트레이싱 루트 객체
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint)

    # 스팬이 끝나면 배치 프로세서에 넘긴다. 배치 프로세스는 모아서 exporter로 보낸다.
    provider.add_span_processor(BatchSpanProcessor(exporter))

    # 전역 등록. 이 함수는 스팬 생성 코드보다 먼저 호출해야 한다. (서비스 시작 시 호출)
    trace.set_tracer_provider(provider)


def inject_traceparent(headers: list[tuple[str, bytes]]) -> list[tuple[str, bytes]]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)  # 현재 활성 스팬의 컨텍스트를 carrier에 씀. (예: carrier['traceparent'] = "1234-A1")
    injected = [(k, v.encode("utf-8")) for k, v in carrier.items()]
    return [*headers, *injected]


def extract_context(headers: list[tuple[str, bytes]]) -> context.Context:
    carrier = {k: v.decode("utf-8") for k, v in headers}
    return propagate.extract(carrier)
