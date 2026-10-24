package agent

import (
	"context"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"time"
)

// Per-attempt, bounded bookkeeping only. No text, arguments, prompts, result
// bodies or repository paths are attached to telemetry.
func instrumentGateway(g Gateway) (Gateway, func()) {
	type pending struct {
		ctx     context.Context
		span    trace.Span
		started time.Time
	}
	tools := map[string]pending{}
	var active trace.Span
	contextual := func(ctx context.Context) context.Context {
		if active != nil {
			return trace.ContextWithSpan(ctx, active)
		}
		return ctx
	}
	original := g
	if original.Next != nil {
		g.Next = func(ctx context.Context, n Next) (Reply, error) {
			ctx, span := telemetry.Start(ctx, "model.call")
			start := time.Now()
			reply, err := original.Next(ctx, n)
			telemetry.Model(ctx, reply.Provider, reply.InputTokens, reply.OutputTokens, time.Since(start))
			outcome := "ok"
			if err != nil || reply.Status != "ok" {
				outcome = "provider_error"
			}
			telemetry.Finish(ctx, span, "model.call", start, outcome)
			return reply, err
		}
	}
	if original.Authorize != nil {
		g.Authorize = func(ctx context.Context) error {
			ctx, span := telemetry.Start(contextual(ctx), "tool.authorize")
			start := time.Now()
			err := original.Authorize(ctx)
			telemetry.End(ctx, span, "tool.authorize", start, err)
			return err
		}
	}
	if original.Test != nil {
		g.Test = func(ctx context.Context, s sandbox.Snapshot) (sandbox.Result, error) {
			return original.Test(contextual(ctx), s)
		}
	}
	if original.Journal != nil {
		g.Journal = func(ctx context.Context, e Event) error {
			err := original.Journal(contextual(ctx), e)
			if err != nil {
				return err
			}
			if e.Kind == "tool" {
				if e.Phase == "begin" && len(tools) < MaxTools {
					child, span := telemetry.Start(ctx, "agent.tool", attribute.String("tool", telemetry.Tool(e.Name)))
					tools[e.ID] = pending{child, span, time.Now()}
					active = span
				} else if e.Phase == "end" {
					if p, ok := tools[e.ID]; ok {
						status := "ok"
						if e.Status == "denied" || e.Status == "conflict" || e.Status == "failed" {
							status = e.Status
						}
						telemetry.Finish(p.ctx, p.span, "agent.tool", p.started, status)
						delete(tools, e.ID)
					}
					active = nil
				}
			}
			return nil
		}
	}
	return g, func() {
		for _, p := range tools {
			telemetry.Finish(p.ctx, p.span, "agent.tool", p.started, "incomplete")
		}
	}
}
