package setup

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"slices"
)

// Libraries can attach raw RPC error descriptions. The export boundary removes
// them and all events, even when upstream instrumentation changes its defaults.
type redactingExporter struct{ sdktrace.SpanExporter }
type redactedSpan struct{ sdktrace.ReadOnlySpan }

func (s redactedSpan) Status() sdktrace.Status {
	v := s.ReadOnlySpan.Status()
	v.Description = ""
	return v
}
func (s redactedSpan) Events() []sdktrace.Event { return nil }
func (s redactedSpan) Attributes() []attribute.KeyValue {
	allowed := []string{"workflow.id", "task.id", "attempt.id", "attempt.generation", "outcome", "tool", "rpc.system", "rpc.system.name", "rpc.service", "rpc.method", "rpc.grpc.status_code", "rpc.response.status_code", "server.address", "server.port", "error.type"}
	values := []attribute.KeyValue{}
	for _, v := range s.ReadOnlySpan.Attributes() {
		if slices.Contains(allowed, string(v.Key)) {
			values = append(values, v)
		}
	}
	return values
}
func (e redactingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	clean := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, s := range spans {
		clean[i] = redactedSpan{s}
	}
	return e.SpanExporter.ExportSpans(ctx, clean)
}
