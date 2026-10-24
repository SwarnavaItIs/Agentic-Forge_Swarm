package setup

import (
	"context"
	"forgeswarm/internal/telemetry"
	"go.opentelemetry.io/otel"
	"net"
	"testing"
	"time"
)

func TestUnavailableCollectorIsNonBlockingAndShutdownBounded(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	t.Setenv("TELEMETRY_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", address)
	t.Setenv("TELEMETRY_SAMPLE_RATIO", "1")
	previousTrace, previousMetric := otel.GetTracerProvider(), otel.GetMeterProvider()
	defer otel.SetTracerProvider(previousTrace)
	defer otel.SetMeterProvider(previousMetric)
	started := time.Now()
	stop := Init("evaluation")
	for range 1000 {
		ctx, span := telemetry.Start(context.Background(), "attempt.run")
		telemetry.Finish(ctx, span, "attempt.run", time.Now(), "ok")
	}
	stop()
	if time.Since(started) > 5*time.Second {
		t.Fatal("unavailable exporter blocked operational work")
	}
}
