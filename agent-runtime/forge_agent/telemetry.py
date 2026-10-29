"""Local, bounded OTLP traces. Prompts, actions and source are never attributes."""

import ipaddress
import os
import re
from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any

from opentelemetry import trace
from opentelemetry.propagate import set_global_textmap
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider, SpanLimits
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

PARENT = re.compile(r"^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$")


def setup() -> None:
    if os.environ.get("TELEMETRY_ENABLED") != "true":
        return
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4317")
    endpoint = endpoint.removeprefix("http://")
    host, separator, port = endpoint.rpartition(":")
    try:
        local = ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        local = (
            host == "host.docker.internal"
            and os.environ.get("AGENT_INSECURE_LOCAL") == "true"
        ) or (
            endpoint == "collector:4317"
            and os.environ.get("TELEMETRY_DOCKER_LOCAL") == "true"
        )
    if not local or not separator or not port.isdigit() or not 1 <= int(port) <= 65535:
        return
    try:
        ratio = float(os.environ.get("TELEMETRY_SAMPLE_RATIO", "0.25"))
    except ValueError:
        ratio = 0.25
    if not 0 <= ratio <= 1:
        ratio = 0.25
    provider = TracerProvider(
        resource=Resource({"service.name": "forgeswarm-python-runtime"}),
        sampler=TraceIdRatioBased(ratio),
        span_limits=SpanLimits(
            max_attributes=16, max_events=0, max_links=4, max_attribute_length=128
        ),
    )
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=endpoint,
                insecure=True,
                timeout=0.5,
                headers=(("forgeswarm-local", "1"),),
            ),
            max_queue_size=256,
            max_export_batch_size=32,
            schedule_delay_millis=200,
            export_timeout_millis=500,
        )
    )
    trace.set_tracer_provider(provider)
    set_global_textmap(TraceContextTextMapPropagator())


@contextmanager
def provider_span(metadata: Any) -> Iterator[trace.Span]:
    values = [v for k, v in metadata if k == "traceparent"]
    carrier = (
        {"traceparent": values[0]}
        if len(values) == 1 and PARENT.fullmatch(values[0])
        else {}
    )
    parent = TraceContextTextMapPropagator().extract(carrier)
    with trace.get_tracer("forgeswarm").start_as_current_span(
        "python.provider",
        context=parent,
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        try:
            yield span
        except BaseException:
            span.set_status(
                trace.Status(trace.StatusCode.ERROR, "provider_interrupted")
            )
            raise
