package telemetry

import (
	"context"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"strings"
	"testing"
	"time"
)

func TestUntrustedContextAndBoundedRetryLinks(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previous)
	ctx, parent := Start(context.Background(), "http.request")
	carrier := propagation.MapCarrier{"baggage": "credential=SECRET", "tracestate": "vendor=SECRET"}
	Propagator{}.Inject(ctx, carrier)
	extracted := Propagator{}.Extract(context.Background(), carrier)
	sanitized := propagation.MapCarrier{}
	Propagator{}.Inject(extracted, sanitized)
	if len(sanitized) != 1 || sanitized["traceparent"] == "" {
		t.Fatal("untrusted baggage/tracestate escaped")
	}
	for _, bad := range []string{"Bearer SECRET", strings.Repeat("a", 10000), "00-" + strings.Repeat("0", 32) + "-" + strings.Repeat("0", 16) + "-01"} {
		if Parent(Extract(context.Background(), bad)) != "" {
			t.Fatal("invalid context accepted")
		}
	}
	first, one := Linked(context.Background(), "attempt.run", Parent(ctx))
	one.End()
	second, two := Linked(context.Background(), "attempt.run", Parent(ctx))
	two.End()
	parent.End()
	if TraceID(first) == TraceID(second) || TraceID(first) == TraceID(ctx) {
		t.Fatal("retry traces were conflated")
	}
	spans := exporter.GetSpans()
	if len(spans) != 3 || len(spans[0].Links) != 1 || len(spans[1].Links) != 1 {
		t.Fatal("missing bounded attempt links")
	}
}

func TestCompletedOutcomesHaveExactCountersHistogramsAndSpanStatus(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	metrics := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	exporter := tracetest.NewInMemoryExporter()
	traces := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previousMetric, previousTrace := otel.GetMeterProvider(), otel.GetTracerProvider()
	otel.SetMeterProvider(metrics)
	otel.SetTracerProvider(traces)
	defer otel.SetMeterProvider(previousMetric)
	defer otel.SetTracerProvider(previousTrace)
	outcomes := []string{"ok", "failed", "cancelled", "incomplete", "passed"}
	for _, outcome := range outcomes {
		ctx, span := Start(context.Background(), "attempt.run")
		Finish(ctx, span, "attempt.run", time.Now().Add(-time.Millisecond), outcome)
	}
	Delivery(context.Background(), false)
	Delivery(context.Background(), false)
	Delivery(context.Background(), true)
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	counters, histograms := 0, 0
	deliveries := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			switch value := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range value.DataPoints {
					if measurement.Name == "forgeswarm.operations" {
						counters++
						if point.Value != 1 {
							t.Fatal("outcome counter lost/duplicated", point)
						}
					} else if measurement.Name == "forgeswarm.broker.deliveries" {
						for _, attr := range point.Attributes.ToSlice() {
							if attr.Key == "delivery" {
								deliveries[attr.Value.AsString()] = point.Value
							}
						}
					}
				}
			case metricdata.Histogram[float64]:
				if measurement.Name == "forgeswarm.operation.duration" {
					for _, point := range value.DataPoints {
						histograms++
						if point.Count != 1 || point.Sum <= 0 {
							t.Fatal("duration sample lost/duplicated", point)
						}
					}
				}
			}
		}
	}
	if counters != len(outcomes) || histograms != len(outcomes) || deliveries["first"] != 2 || deliveries["redelivery"] != 1 {
		t.Fatal("measurement denominators", counters, histograms, deliveries)
	}
	spans := exporter.GetSpans()
	if len(spans) != len(outcomes) {
		t.Fatal("missing outcome spans")
	}
	for index, outcome := range outcomes {
		want := codes.Error
		if outcome == "ok" || outcome == "passed" {
			want = codes.Unset
		}
		if spans[index].Status.Code != want {
			t.Fatal("incorrect span status", outcome, spans[index].Status)
		}
	}
}
func TestMetricCardinalityIgnoresThousandsOfUniqueIDs(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithCardinalityLimit(256))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer otel.SetMeterProvider(previous)
	for range 3000 {
		ctx, span := Start(context.Background(), "attempt.run", IDs(uuid.NewString(), uuid.NewString(), uuid.NewString())...)
		Finish(ctx, span, "attempt.run", time.Now(), "ok")
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	points := 0
	for _, scope := range data.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			switch value := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				for _, p := range value.DataPoints {
					points++
					for _, a := range p.Attributes.ToSlice() {
						if a.Key != "operation" && a.Key != "outcome" {
							t.Fatal("unbounded counter label", a.Key)
						}
					}
				}
			case metricdata.Histogram[float64]:
				for _, p := range value.DataPoints {
					points++
					for _, a := range p.Attributes.ToSlice() {
						if a.Key != "operation" && a.Key != "outcome" {
							t.Fatal("unbounded histogram label", a.Key)
						}
					}
				}
			}
		}
	}
	if points != 2 {
		t.Fatalf("unique IDs allocated metric series: %d", points)
	}
}
