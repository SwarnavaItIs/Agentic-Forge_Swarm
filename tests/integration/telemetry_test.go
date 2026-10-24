//go:build integration && stage03 && stage04

package integration

import (
	"context"
	"encoding/json"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/relay"
	"forgeswarm/internal/telemetry"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"os"
	"testing"
	"time"
)

func point(t *testing.T, values postgres.OperationalMetrics, name, label string) float64 {
	t.Helper()
	for _, p := range values.Points {
		if p.Name == name && p.Label == label {
			return p.Value
		}
	}
	return 0
}
func TestTelemetryAsyncRPCWorkerLossRetryCancelAndDurableMeasurements(t *testing.T) {
	f := setup(t)
	b := testBroker(t, f)
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer otel.SetTracerProvider(previous)
	ctx, admissionSpan := telemetry.Start(f.ctx, "http.request")
	receipt, err := f.s1.Create(ctx, "telemetry-worker-loss", workflow.CreateInput{Name: "telemetry", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop", MaxAttempts: 2}}})
	admissionSpan.End()
	if err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, receipt.ID)
	if _, err = relay.Step(f.ctx, f.s1, b, uuid.New(), 10, time.Second); err != nil {
		t.Fatal(err)
	}
	msg := message(t, b)
	parent := msg.Headers().Get("traceparent")
	if !telemetry.ParentPattern.MatchString(parent) {
		t.Fatal("durable outbox -> real broker trace context lost")
	}
	var storedParent string
	if err = f.p1.QueryRow(f.ctx, "SELECT traceparent FROM workflows WHERE id=$1", receipt.ID).Scan(&storedParent); err != nil || storedParent != telemetry.Parent(ctx) {
		t.Fatal("admission context not persisted")
	}
	deliveryCtx, deliverySpan := telemetry.Linked(f.ctx, "broker.consume", parent)
	r := rpcServer(t, f, "")
	instance := uuid.NewString()
	call, done := context.WithTimeout(deliveryCtx, time.Second)
	_, err = r.ca.RegisterWorker(call, &workerv1.RegisterRequest{InstanceId: instance, Capabilities: []string{"noop"}, Capacity: 1})
	done()
	if err != nil {
		t.Fatal(err)
	}
	task := snapshot(t, f, receipt.ID).Nodes[0].ID
	call, done = context.WithTimeout(deliveryCtx, time.Second)
	owned, err := r.ca.ClaimTask(call, &workerv1.ClaimRequest{InstanceId: instance, TaskId: task.String(), DispatchGeneration: 1, RequestId: uuid.NewString()})
	done()
	if err != nil {
		t.Fatal(err)
	}
	_ = msg.DoubleAck(f.ctx)
	deliverySpan.End()
	before, err := f.s1.OperationalSnapshot(f.ctx)
	if err != nil {
		t.Fatal("actual metric query", err)
	}
	if point(t, before, "leases", "active") != 1 || point(t, before, "attempts", "RUNNING") != 1 || point(t, before, "workers", "available") != 0 {
		t.Fatal("active attempt measurements", before)
	}
	lost := execution.Attempt{ID: uuid.MustParse(owned.Id)}
	expire(t, f, lost)
	reconcile(t, f, receipt.ID)
	after, err := f.s1.OperationalSnapshot(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if point(t, after, "expired_attempts", "worker_lost") != 1 || point(t, after, "attempts", "LOST") != 1 || point(t, after, "retries", "infrastructure") != 1 || point(t, after, "leases", "active") != 0 {
		t.Fatal("loss/retry measures do not distinguish tasks and attempts", after)
	}
	awaitRetryReady(t, f, receipt.ID, task)
	replacement := claimTask(t, f, "worker_a", uuid.MustParse(instance), task, 2)
	if _, err = f.s1.FinishAttempt(f.ctx, "worker_a", uuid.MustParse(instance), replacement.ID, 2, true, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, receipt.ID)
	cancelled, err := f.s1.Create(f.ctx, "telemetry-cancel", workflow.CreateInput{Name: "cancel", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s1.Cancel(f.ctx, cancelled.ID); err != nil {
		t.Fatal(err)
	}
	final, err := f.s1.OperationalSnapshot(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if point(t, final, "tasks", "SUCCEEDED") != 1 || point(t, final, "attempts", "SUCCEEDED") != 1 || point(t, final, "attempts", "LOST") != 1 || point(t, final, "tasks", "CANCELLED") != 1 {
		t.Fatal("logical task and execution attempt denominators", final)
	}
	measurements := map[string]int64{}
	for _, d := range final.Durations {
		measurements[d.Measurement] = d.Count
		if d.P50 < 0 || d.P95 < d.P50 {
			t.Fatal("invalid seconds distribution")
		}
	}
	if measurements["admission_to_ready"] != 1 || measurements["ready_to_claim"] != 2 || measurements["attempt"] != 2 || measurements["workflow"] != 2 {
		t.Fatal("latency measurement points", measurements)
	}
	rpcSeen := false
	for _, span := range exporter.GetSpans() {
		if span.SpanContext.TraceID().String() == telemetry.TraceID(deliveryCtx) && span.Name != "broker.consume" {
			rpcSeen = true
		}
	}
	if !rpcSeen {
		t.Fatal("trace context lost across actual gRPC handoff")
	}
	if output := os.Getenv("TEST_OBSERVABILITY_EVIDENCE"); output != "" {
		proof := map[string]any{"verified_at": time.Now().UTC(), "failure": "controlled_worker_lease_loss", "workflow_id": receipt.ID, "lost_attempt": owned.Id, "replacement_attempt": replacement.ID, "admission_trace_id": telemetry.TraceID(ctx), "delivery_trace_id": telemetry.TraceID(deliveryCtx), "persisted_retry_events": 1, "logical_successes": 1, "execution_attempts": 2, "lost_attempts": 1, "measurements": measurements, "rpc_context_survived": rpcSeen}
		body, _ := json.MarshalIndent(proof, "", "  ")
		if err = os.WriteFile(output, body, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
