package dbtrace

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"sync/atomic"
	"time"
)

var DatabaseBuckets = []float64{.001, .005, .02, .1, .5, 1, 3, 10, 30}

type dbAggregate struct {
	count, failed, nanos atomic.Uint64
	buckets              [10]atomic.Uint64
}

var queries, acquires dbAggregate

type DBTiming struct {
	Count    uint64    `json:"count"`
	Failures uint64    `json:"failures"`
	Seconds  float64   `json:"seconds"`
	Bounds   []float64 `json:"bounds_seconds"`
	Buckets  []uint64  `json:"bucket_counts"`
}

func snapshotDB(value *dbAggregate) DBTiming {
	counts := make([]uint64, len(value.buckets))
	for i := range counts {
		counts[i] = value.buckets[i].Load()
	}
	return DBTiming{value.count.Load(), value.failed.Load(), float64(value.nanos.Load()) / 1e9, append([]float64(nil), DatabaseBuckets...), counts}
}
func DatabaseSnapshot() map[string]DBTiming {
	return map[string]DBTiming{"query": snapshotDB(&queries), "acquire": snapshotDB(&acquires)}
}

type dbKey int
type DBTracer struct{ query, acquire metric.Float64Histogram }

// Instruments must exist before observable callbacks query the database.
// Registering an instrument from a callback re-enters the SDK pipeline lock.
func New() DBTracer {
	m := otel.Meter("forgeswarm")
	q, _ := m.Float64Histogram("forgeswarm.database.query.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(DatabaseBuckets...))
	a, _ := m.Float64Histogram("forgeswarm.database.acquire.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(DatabaseBuckets...))
	return DBTracer{query: q, acquire: a}
}

func (DBTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, dbKey(0), time.Now())
}
func (t DBTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	recordDB(ctx, dbKey(0), t.query, &queries, data.Err)
}
func (DBTracer) TraceAcquireStart(ctx context.Context, _ *pgxpool.Pool, _ pgxpool.TraceAcquireStartData) context.Context {
	return context.WithValue(ctx, dbKey(1), time.Now())
}
func (t DBTracer) TraceAcquireEnd(ctx context.Context, _ *pgxpool.Pool, data pgxpool.TraceAcquireEndData) {
	recordDB(ctx, dbKey(1), t.acquire, &acquires, data.Err)
}
func recordDB(ctx context.Context, key dbKey, h metric.Float64Histogram, total *dbAggregate, err error) {
	started, ok := ctx.Value(key).(time.Time)
	if !ok {
		return
	}
	duration := time.Since(started)
	total.count.Add(1)
	total.nanos.Add(uint64(duration.Nanoseconds()))
	outcome := "ok"
	if err != nil {
		total.failed.Add(1)
		outcome = "error"
	}
	index := len(DatabaseBuckets)
	for i, upper := range DatabaseBuckets {
		if duration.Seconds() <= upper {
			index = i
			break
		}
	}
	total.buckets[index].Add(1)
	if h != nil {
		h.Record(ctx, duration.Seconds(), metric.WithAttributes(attribute.String("outcome", outcome)))
	}
}
