"""Tracing is the daemon's only output. piloop does not use ``logging``.

``ConsoleProcessor`` prints one line on stderr for every span end and every span event. When
``OTEL_EXPORTER_OTLP_ENDPOINT`` is set, the same spans are also exported over OTLP.
"""

import contextlib
from collections.abc import Generator, Mapping
from datetime import datetime

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from rich.console import Console

from piloop.settings import settings

Attr = str | int | float | bool
Attrs = dict[str, Attr]


def _clean(attrs: dict[str, Attr | None]) -> Attrs:
    return {k: v for k, v in attrs.items() if v is not None}


class ConsoleProcessor(SpanProcessor):
    """Prints one stderr line for each span end (with its duration) and each span event."""

    def __init__(self) -> None:
        self._console = Console(stderr=True, markup=False, highlight=False)

    def on_end(self, span: ReadableSpan) -> None:
        attrs: dict[str, object] = dict(span.attributes or {})
        if span.status.status_code is trace.StatusCode.ERROR:
            attrs['error'] = span.status.description or 'error'
        seconds = ((span.end_time or 0) - (span.start_time or 0)) / 1e9
        self.line(f'{span.name} {seconds:.1f}s', attrs)

    def on_event(self, name: str, attrs: Mapping[str, object]) -> None:
        """Print an event when it happens, not when its span ends."""
        self.line(name, attrs)

    def line(self, head: str, attrs: Mapping[str, object]) -> None:
        """Print ``HH:MM:SS head k=v k=v``."""
        parts = [datetime.now().strftime('%H:%M:%S'), head, *(f'{k}={v}' for k, v in attrs.items())]
        self._console.print(' '.join(parts), soft_wrap=True)


class Span:
    """A span that records attributes, events and errors. Events also go to the console."""

    def __init__(self, span: trace.Span, console: ConsoleProcessor) -> None:
        self._span = span
        self._console = console

    def set(self, **attrs: Attr | None) -> None:
        """Set the attributes that are not None."""
        self._span.set_attributes(_clean(attrs))

    def event(self, name: str, **attrs: Attr | None) -> None:
        """Add an event. Events are the daemon's log lines."""
        clean = _clean(attrs)
        self._span.add_event(name, clean)
        self._console.on_event(name, clean)

    def error(self, exc: BaseException) -> None:
        """Record the exception and mark the span as failed."""
        self._span.record_exception(exc)
        self._span.set_status(trace.StatusCode.ERROR, f'{type(exc).__name__}: {exc}')


class Tracer:
    """Sends spans to the console, and over OTLP when an endpoint is set. Tests pass an ``exporter`` instead of OTLP."""

    def __init__(self, endpoint: str | None, exporter: SpanExporter | None = None) -> None:
        self._console = ConsoleProcessor()
        self._provider = TracerProvider(resource=Resource.create({'service.name': 'piloop'}))
        self._provider.add_span_processor(self._console)
        if exporter is not None:
            self._provider.add_span_processor(SimpleSpanProcessor(exporter))
        elif endpoint is not None:
            self._provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=f'{endpoint}/v1/traces')))
        self._tracer = self._provider.get_tracer('piloop')

    @contextlib.contextmanager
    def span(self, name: str, **attrs: Attr | None) -> Generator[Span]:
        """Start a child span of the current context."""
        with self._tracer.start_as_current_span(name, attributes=_clean(attrs)) as s:
            yield Span(s, self._console)

    def event(self, name: str, **attrs: Attr | None) -> None:
        """Add an event to the current span, or record it as a zero-length span when no span is open."""
        current = trace.get_current_span()
        if current.is_recording():
            Span(current, self._console).event(name, **attrs)
        else:
            with self.span(name, **attrs):
                pass

    def shutdown(self) -> None:
        """Flush and stop the exporters."""
        self._provider.shutdown()


def tracer() -> Tracer:
    """A tracer configured from the settings."""
    return Tracer(settings.otel_exporter_otlp_endpoint)
