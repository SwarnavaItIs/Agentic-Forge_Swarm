// Package telemetry exports only allowlisted operational metadata. Trace context
// is correlation data: it never supplies identities, ownership or permissions.
package telemetry

import (
	"context"
	"net"
	"regexp"
	"slices"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var ParentPattern = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$`)
var idPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// No baggage, tracestate, arbitrary headers or remote-selected sampler policy.
type Propagator struct{}

func (Propagator) Fields() []string { return []string{"traceparent"} }
func (Propagator) Inject(ctx context.Context, c propagation.TextMapCarrier) {
	clean := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, clean)
	if value := clean.Get("traceparent"); ParentPattern.MatchString(value) {
		c.Set("traceparent", value)
	}
}
func (Propagator) Extract(ctx context.Context, c propagation.TextMapCarrier) context.Context {
	value := c.Get("traceparent")
	if !ParentPattern.MatchString(value) {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": value})
}
func Parent(ctx context.Context) string {
	c := propagation.MapCarrier{}
	Propagator{}.Inject(ctx, c)
	return c.Get("traceparent")
}
func Extract(ctx context.Context, value string) context.Context {
	return Propagator{}.Extract(ctx, propagation.MapCarrier{"traceparent": value})
}
func LocalEndpoint(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	ip := net.ParseIP(host)
	return err == nil && ip != nil && ip.IsLoopback() && port != "" && len(addr) <= 100
}

var operations = []string{"http.request", "scheduler.scan", "outbox.publish", "broker.consume", "attempt.run", "attempt.claim", "attempt.finish", "model.call", "agent.tool", "tool.authorize", "sandbox.run", "approval.decide", "publication.run", "evaluation.run", "evaluation.test"}
var outcomes = []string{"ok", "error", "cancelled", "timeout", "denied", "failed", "accepted", "budget_exhausted", "provider_error", "conflict", "rejected", "incomplete", "2xx", "4xx", "5xx", "passed", "task_failure", "infrastructure_failure", "provider_failure", "policy_violation", "spending_cap_exhausted", "review_rejected", "model_rejected"}

func Outcome(value string) string {
	if slices.Contains(outcomes, value) {
		return value
	}
	return "error"
}
func Provider(value string) string {
	if slices.Contains([]string{"openai", "scripted_TEST_DEMO", "disabled"}, value) {
		return value
	}
	return "unknown"
}
func Kind(value string) string {
	if slices.Contains([]string{"noop", "delay", "sandbox_go", "agent_go"}, value) {
		return value
	}
	return "unknown"
}
func Tool(value string) string {
	if slices.Contains([]string{"list_tree", "read_file", "search_text", "apply_patch", "run_test_profile", "status", "diff", "final", "final_plan", "final_review"}, value) {
		return value
	}
	return "unknown"
}

func IDs(workflow, task, attempt string) []attribute.KeyValue {
	v := []attribute.KeyValue{}
	for i, value := range []string{workflow, task, attempt} {
		if idPattern.MatchString(value) {
			v = append(v, attribute.String([]string{"workflow.id", "task.id", "attempt.id"}[i], value))
		}
	}
	return v
}
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !slices.Contains(operations, name) {
		name = "agent.tool"
	}
	return otel.Tracer("forgeswarm").Start(ctx, name, trace.WithAttributes(attrs...))
}

// A new bounded trace per delivery/attempt links back to durable admission.
// Retries are distinguishable without one open workflow-lifetime span.
func Linked(ctx context.Context, name, parent string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	linked := trace.SpanContextFromContext(Extract(context.Background(), parent))
	opts := []trace.SpanStartOption{trace.WithNewRoot(), trace.WithAttributes(attrs...)}
	if linked.IsValid() {
		opts = append(opts, trace.WithLinks(trace.Link{SpanContext: linked}))
	}
	return otel.Tracer("forgeswarm").Start(ctx, name, opts...)
}
func Finish(ctx context.Context, span trace.Span, name string, started time.Time, outcome string) {
	if !slices.Contains(operations, name) {
		name = "agent.tool"
	}
	outcome = Outcome(outcome)
	span.SetAttributes(attribute.String("outcome", outcome))
	if outcome != "ok" && outcome != "accepted" && outcome != "2xx" && outcome != "passed" {
		span.SetStatus(codes.Error, outcome)
	}
	span.End()
	attrs := metric.WithAttributes(attribute.String("operation", name), attribute.String("outcome", outcome))
	meter := otel.Meter("forgeswarm")
	n, _ := meter.Int64Counter("forgeswarm.operations", metric.WithDescription("Process-local completed operations, including failures and cancellations."))
	h, _ := meter.Float64Histogram("forgeswarm.operation.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(.005, .02, .1, .5, 1, 3, 10, 30, 90, 300, 900))
	n.Add(ctx, 1, attrs)
	h.Record(ctx, time.Since(started).Seconds(), attrs)
}
func End(ctx context.Context, span trace.Span, name string, started time.Time, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
		if ctx.Err() == context.Canceled {
			outcome = "cancelled"
		}
		if ctx.Err() == context.DeadlineExceeded {
			outcome = "timeout"
		}
	}
	Finish(ctx, span, name, started, outcome)
}
func TraceID(ctx context.Context) string {
	s := trace.SpanContextFromContext(ctx)
	if s.IsValid() {
		return s.TraceID().String()
	}
	return ""
}
func Model(ctx context.Context, provider string, input, output *int, latency time.Duration) {
	m := otel.Meter("forgeswarm")
	opts := metric.WithAttributes(attribute.String("provider", Provider(provider)))
	h, _ := m.Float64Histogram("forgeswarm.model.latency", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(.02, .1, .5, 1, 3, 5, 10, 30))
	h.Record(ctx, latency.Seconds(), opts)
	if input == nil || output == nil {
		n, _ := m.Int64Counter("forgeswarm.model.usage_unknown")
		n.Add(ctx, 1, opts)
		return
	}
	n, _ := m.Int64Counter("forgeswarm.model.tokens", metric.WithUnit("{token}"))
	if *input >= 0 && *input <= 200000 {
		n.Add(ctx, int64(*input), metric.WithAttributes(attribute.String("provider", Provider(provider)), attribute.String("direction", "input")))
	}
	if *output >= 0 && *output <= 200000 {
		n.Add(ctx, int64(*output), metric.WithAttributes(attribute.String("provider", Provider(provider)), attribute.String("direction", "output")))
	}
}

func Delivery(ctx context.Context, redelivery bool) {
	kind := "first"
	if redelivery {
		kind = "redelivery"
	}
	n, _ := otel.Meter("forgeswarm").Int64Counter("forgeswarm.broker.deliveries")
	n.Add(ctx, 1, metric.WithAttributes(attribute.String("delivery", kind)))
}
