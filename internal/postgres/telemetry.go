package postgres

import (
	"context"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"time"
)

// OperationalPoint contains no identifiers or untrusted strings. Historical
// projections cover the last 24 hours. They are gauges, not reset-prone counters.
type OperationalPoint struct {
	Name, Label string
	Value       float64
}
type DurationPoint struct {
	Measurement       string
	Count             int64
	P50, P95, Maximum float64
}
type OperationalMetrics struct {
	Points    []OperationalPoint
	Durations []DurationPoint
}

// OperationalSnapshot uses a separate short read-only MVCC transaction, never
// admission locks or workflow row locks. Export failure cannot roll back work.
func (s *Store) OperationalSnapshot(ctx context.Context) (OperationalMetrics, error) {
	var result OperationalMetrics
	ctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, "SET LOCAL statement_timeout='350ms'"); err != nil {
		return result, err
	}
	rows, err := tx.Query(ctx, `
 WITH fresh AS (SELECT * FROM workers WHERE heartbeat_at>transaction_timestamp()-$1*interval '1 millisecond'),
 running AS (SELECT * FROM task_attempts WHERE state='RUNNING'),
 points AS (
 SELECT 'workers' AS name,'active' AS label,count(*)::float8 AS value FROM fresh WHERE NOT draining
 UNION ALL SELECT 'workers','available',count(*)::float8 FROM fresh w WHERE NOT draining AND w.capacity>(SELECT count(*) FROM running a WHERE a.worker_id=w.id AND a.worker_instance_id=w.instance_id)
 UNION ALL SELECT 'leases','active',count(*)::float8 FROM running WHERE lease_expires_at>transaction_timestamp()
 UNION ALL SELECT 'leases','expired',count(*)::float8 FROM running WHERE lease_expires_at<=transaction_timestamp()
 UNION ALL SELECT 'outbox','pending',count(*)::float8 FROM outbox WHERE state='PENDING'
 UNION ALL SELECT 'outbox_oldest_seconds','pending',COALESCE(extract(epoch FROM transaction_timestamp()-min(available_at)),0)::float8 FROM outbox WHERE state='PENDING'
 UNION ALL SELECT 'tasks',status,count(*)::float8 FROM tasks WHERE created_at>transaction_timestamp()-interval '24 hours' GROUP BY status
 UNION ALL SELECT 'attempts',state,count(*)::float8 FROM task_attempts WHERE started_at>transaction_timestamp()-interval '24 hours' GROUP BY state
 UNION ALL SELECT 'workflows',status,count(*)::float8 FROM workflows WHERE created_at>transaction_timestamp()-interval '24 hours' GROUP BY status
 UNION ALL SELECT 'retries','infrastructure',count(*)::float8 FROM workflow_events WHERE event_type='task.retry_wait' AND occurred_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'expired_attempts','worker_lost',count(*)::float8 FROM task_attempts WHERE state='LOST' AND started_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'broker_republications','same_generation',COALESCE(sum(publish_revision),0)::float8 FROM outbox WHERE available_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'repair_rounds','model_correction',COALESCE(sum(round),0)::float8 FROM collab_projects WHERE created_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'approvals',status,count(*)::float8 FROM approvals WHERE created_at>transaction_timestamp()-interval '24 hours' GROUP BY status
 UNION ALL SELECT 'publications',status,count(*)::float8 FROM external_operations WHERE created_at>transaction_timestamp()-interval '24 hours' GROUP BY status
 UNION ALL SELECT 'reserved_cost_usd','estimate',COALESCE(sum(estimated_microusd),0)::float8/1000000 FROM agent_sessions s JOIN task_attempts a ON a.id=s.attempt_id WHERE a.started_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'unknown_usage_calls','unknown',count(*)::float8 FROM model_calls b LEFT JOIN model_calls e ON e.attempt_id=b.attempt_id AND e.request_id=b.request_id AND e.phase='end' WHERE b.phase='begin' AND b.recorded_at>transaction_timestamp()-interval '24 hours' AND (e.request_id IS NULL OR e.metadata->>'usage'='unknown_retained_reservation')
 ) SELECT name,label,GREATEST(0,value) FROM points`, s.policy.WorkerFreshness.Milliseconds())
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var p OperationalPoint
		if err = rows.Scan(&p.Name, &p.Label, &p.Value); err != nil {
			rows.Close()
			return result, err
		}
		result.Points = append(result.Points, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	rows, err = tx.Query(ctx, `
 WITH recent_events AS (SELECT * FROM workflow_events WHERE occurred_at>transaction_timestamp()-interval '24 hours'),
 samples AS (
 SELECT 'admission_to_ready' AS measurement,extract(epoch FROM min(e.occurred_at)-t.created_at)::float8 AS seconds FROM tasks t JOIN recent_events e ON e.workflow_id=t.workflow_id AND e.event_type='task.ready' AND e.payload->>'task_id'=t.id::text GROUP BY t.id,t.created_at
 UNION ALL SELECT 'ready_to_claim',extract(epoch FROM a.started_at-max(e.occurred_at))::float8 FROM task_attempts a JOIN recent_events e ON e.workflow_id=a.workflow_id AND e.event_type='task.ready' AND e.payload->>'task_id'=a.task_id::text AND e.payload->>'generation'=a.generation::text GROUP BY a.id,a.started_at
 UNION ALL SELECT 'attempt',extract(epoch FROM finished_at-started_at)::float8 FROM task_attempts WHERE finished_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'task',extract(epoch FROM updated_at-created_at)::float8 FROM tasks WHERE status IN ('SUCCEEDED','FAILED','CANCELLED','BLOCKED') AND updated_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'workflow',extract(epoch FROM min(e.occurred_at)-w.created_at)::float8 FROM workflows w JOIN recent_events e ON e.workflow_id=w.id AND e.event_type IN ('workflow.succeeded','workflow.failed','workflow.cancelled') GROUP BY w.id,w.created_at
 UNION ALL SELECT 'approval_wait',extract(epoch FROM decided_at-created_at)::float8 FROM approvals WHERE decided_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'publication',extract(epoch FROM updated_at-created_at)::float8 FROM external_operations WHERE status IN ('confirmed','failed','needs_review','cancelled','manual_reconciliation') AND updated_at>transaction_timestamp()-interval '24 hours'
 UNION ALL SELECT 'model_reported',COALESCE((metadata->>'latency_ms')::float8,0)/1000 FROM model_calls WHERE phase='end' AND recorded_at>transaction_timestamp()-interval '24 hours' AND metadata ? 'latency_ms'
 ) SELECT measurement,count(*),percentile_cont(0.5) WITHIN GROUP(ORDER BY GREATEST(0,seconds)),percentile_cont(0.95) WITHIN GROUP(ORDER BY GREATEST(0,seconds)),max(GREATEST(0,seconds)) FROM samples WHERE seconds IS NOT NULL GROUP BY measurement`)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var d DurationPoint
		if err = rows.Scan(&d.Measurement, &d.Count, &d.P50, &d.P95, &d.Maximum); err != nil {
			rows.Close()
			return result, err
		}
		result.Durations = append(result.Durations, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return result, err
	}
	return result, tx.Commit(ctx)
}

// RegisterOperationalMetrics is called only by the API, avoiding duplicate DB
// projections from each role. Missing samples are omitted, never fabricated zero.
func (s *Store) RegisterOperationalMetrics() (metric.Registration, error) {
	m := otel.Meter("forgeswarm")
	up, err := m.Float64ObservableGauge("forgeswarm.database.up")
	if err != nil {
		return nil, err
	}
	values := map[string]metric.Float64ObservableGauge{}
	observables := []metric.Observable{up}
	for _, name := range []string{"workers", "leases", "outbox", "outbox_oldest_seconds", "tasks", "attempts", "workflows", "retries", "expired_attempts", "broker_republications", "repair_rounds", "approvals", "publications", "reserved_cost_usd", "unknown_usage_calls"} {
		unit := ""
		if name == "outbox_oldest_seconds" {
			unit = "s"
		}
		if name == "reserved_cost_usd" {
			unit = "USD"
		}
		value, e := m.Float64ObservableGauge("forgeswarm.state."+name, metric.WithUnit(unit), metric.WithDescription("Database projection; current gauges or 24h retained records; see definitions.json."))
		if e != nil {
			return nil, e
		}
		values[name] = value
		observables = append(observables, value)
	}
	quantiles, err := m.Float64ObservableGauge("forgeswarm.recorded.duration_quantile", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	counts, err := m.Int64ObservableGauge("forgeswarm.recorded.duration_count")
	if err != nil {
		return nil, err
	}
	observables = append(observables, quantiles, counts)
	return m.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		data, e := s.OperationalSnapshot(ctx)
		if e != nil {
			o.ObserveFloat64(up, 0)
			return nil
		}
		o.ObserveFloat64(up, 1)
		for _, p := range data.Points {
			if instrument, ok := values[p.Name]; ok {
				o.ObserveFloat64(instrument, p.Value, metric.WithAttributes(attribute.String("state", p.Label)))
			}
		}
		for _, d := range data.Durations {
			o.ObserveInt64(counts, d.Count, metric.WithAttributes(attribute.String("measurement", d.Measurement)))
			for i, q := range []string{"p50", "p95", "max"} {
				o.ObserveFloat64(quantiles, []float64{d.P50, d.P95, d.Maximum}[i], metric.WithAttributes(attribute.String("measurement", d.Measurement), attribute.String("quantile", q)))
			}
		}
		return nil
	}, observables...)
}
