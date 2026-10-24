package setup

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"testing"
)

func TestRawRPCDescriptionsEventsAndArbitraryAttributesNeverExport(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(redactingExporter{exporter}), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	_, span := provider.Tracer("test").Start(context.Background(), "rpc.call")
	span.SetStatus(codes.Error, "Bearer SECRET and repository source")
	span.SetAttributes(attribute.String("prompt", "private source"), attribute.String("rpc.method", "ClaimTask"))
	span.AddEvent("untrusted event")
	span.End()
	values := exporter.GetSpans()
	if len(values) != 1 || values[0].Status.Description != "" || len(values[0].Events) != 0 || len(values[0].Attributes) != 1 {
		t.Fatal("raw library data escaped redaction")
	}
}
