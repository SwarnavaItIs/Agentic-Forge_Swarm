package dbtrace

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"strings"
	"testing"
	"time"
)

func TestDatabaseTimingNeverRecordsSQLOrArguments(t *testing.T) {
	tracer := New()
	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{SQL: "PRIVATE SQL SECRET", Args: []any{"PRIVATE ARGUMENT"}})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: errors.New("PRIVATE ERROR")})
	acquire := tracer.TraceAcquireStart(context.Background(), nil, pgxpool.TraceAcquireStartData{})
	tracer.TraceAcquireEnd(acquire, nil, pgxpool.TraceAcquireEndData{})
	value := DatabaseSnapshot()
	if value["query"].Count < 1 || value["query"].Failures < 1 || value["acquire"].Count < 1 {
		t.Fatal("timing samples lost")
	}
	body, _ := json.Marshal(value)
	if strings.Contains(string(body), "PRIVATE") {
		t.Fatal("private SQL escaped into observations")
	}
}

func TestQueryTimingInsideObservableCallbackDoesNotDeadlock(t *testing.T) {
	r := sdkmetric.NewManualReader()
	p := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(p)
	defer otel.SetMeterProvider(old)
	defer p.Shutdown(context.Background())
	tracer := New()
	m := p.Meter("test")
	g, _ := m.Int64ObservableGauge("test.snapshot")
	reg, err := m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		call := tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{})
		tracer.TraceQueryEnd(call, nil, pgx.TraceQueryEndData{})
		o.ObserveInt64(g, 1)
		return nil
	}, g)
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Unregister()
	done := make(chan error, 1)
	go func() { done <- r.Collect(context.Background(), &metricdata.ResourceMetrics{}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("database instrumentation deadlocked metric collection")
	}
}
