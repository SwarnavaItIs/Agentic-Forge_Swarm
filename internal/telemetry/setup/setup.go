// Package setup owns exporters. The execution/sandbox packages depend only on
// the OpenTelemetry API, so the immutable helper has no exporter or vendor client.
package setup

import (
	"context"
	"go.opentelemetry.io/otel/metric"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"forgeswarm/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Init is optional and never blocks admission on an exporter connection. Invalid
// telemetry configuration disables export; workflow configuration stays separate.
func Init(service string) func() {
	noop := func() {}
	if os.Getenv("TELEMETRY_ENABLED") != "true" {
		return noop
	}
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "127.0.0.1:4317"
	}
	endpoint = strings.TrimPrefix(endpoint, "http://")
	if !telemetry.CollectorEndpoint(endpoint, os.Getenv("TELEMETRY_DOCKER_LOCAL") == "true") {
		slog.Warn("telemetry_disabled", "error_class", "local_endpoint_required")
		return noop
	}
	_ = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://"+endpoint)
	if value := os.Getenv("TELEMETRY_SERVICE_NAME"); slices.Contains([]string{"api", "scheduler", "coordinator", "relay", "worker_a", "worker_b", "publisher", "evaluation"}, value) {
		service = value
	}
	ratio := .25
	if value := os.Getenv("TELEMETRY_SAMPLE_RATIO"); value != "" {
		if v, e := strconv.ParseFloat(value, 64); e == nil && v >= 0 && v <= 1 {
			ratio = v
		}
	}
	res := resource.NewSchemaless(attribute.String("service.name", "forgeswarm-"+service))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	tx, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithHeaders(map[string]string{"forgeswarm-local": "1"}), otlptracegrpc.WithInsecure(), otlptracegrpc.WithTimeout(500*time.Millisecond), otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{Enabled: false}))
	if err != nil {
		slog.Warn("telemetry_disabled", "error_class", "exporter_unavailable")
		return noop
	}
	mx, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithEndpoint(endpoint), otlpmetricgrpc.WithHeaders(map[string]string{"forgeswarm-local": "1"}), otlpmetricgrpc.WithInsecure(), otlpmetricgrpc.WithTimeout(500*time.Millisecond), otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{Enabled: false}))
	if err != nil {
		_ = tx.Shutdown(ctx)
		return noop
	}
	// Remote trace flags never force sampling. No blocking-on-full processor.
	tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.TraceIDRatioBased(ratio)), sdktrace.WithSpanLimits(sdktrace.SpanLimits{AttributeValueLengthLimit: 128, AttributeCountLimit: 16, EventCountLimit: 8, AttributePerEventCountLimit: 8, LinkCountLimit: 4, AttributePerLinkCountLimit: 4}), sdktrace.WithBatcher(redactingExporter{tx}, sdktrace.WithMaxQueueSize(256), sdktrace.WithMaxExportBatchSize(32), sdktrace.WithBatchTimeout(200*time.Millisecond), sdktrace.WithExportTimeout(500*time.Millisecond)))
	allowedKeys := []string{"operation", "outcome", "provider", "direction", "state", "measurement", "quantile", "delivery", "rpc.method", "rpc.service", "rpc.system", "rpc.system.name", "rpc.grpc.status_code", "rpc.response.status_code"}
	view := sdkmetric.NewView(sdkmetric.Instrument{Name: "*"}, sdkmetric.Stream{AttributeFilter: func(v attribute.KeyValue) bool { return slices.Contains(allowedKeys, string(v.Key)) }})
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithView(view), sdkmetric.WithCardinalityLimit(256), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mx, sdkmetric.WithInterval(2*time.Second), sdkmetric.WithTimeout(750*time.Millisecond))))
	var failures atomic.Int64
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) { failures.Add(1) })) // never emit exporter errors containing payloads/URLs
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(telemetry.Propagator{})
	meter := otel.Meter("forgeswarm")
	failureMetric, _ := meter.Int64ObservableCounter("forgeswarm.telemetry.export_errors")
	_, _ = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		o.ObserveInt64(failureMetric, failures.Load())
		return nil
	}, failureMetric)
	return func() {
		stop, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_ = tp.Shutdown(stop)
		_ = mp.Shutdown(stop)
	}
}
