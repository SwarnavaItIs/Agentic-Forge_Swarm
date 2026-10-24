// Package worker executes bounded diagnostics and registered sandbox fixtures. Notifications
// are acknowledged after durable claim; completion recovery belongs to leases.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/config"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/telemetry"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log/slog"
	"sync"
	"time"
)

func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func claim(ctx context.Context, client workerv1.CoordinatorClient, c config.Distributed, r *workerv1.ClaimRequest) (*workerv1.Attempt, error) {
	var a *workerv1.Attempt
	var err error
	for i := 0; i < 3; i++ {
		call, done := context.WithTimeout(ctx, c.RPCTimeout)
		a, err = client.ClaimTask(call, r)
		done()
		if err == nil || status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			return a, err
		}
		if !pause(ctx, c.Poll) {
			break
		}
	}
	return a, err
}
func Handle(ctx context.Context, client workerv1.CoordinatorClient, b *broker.Broker, c config.Distributed, instance uuid.UUID, msg jetstream.Msg, logger *slog.Logger) {
	HandleWithSandbox(ctx, client, b, c, instance, msg, logger, nil)
}
func HandleWithSandbox(ctx context.Context, client workerv1.CoordinatorClient, b *broker.Broker, c config.Distributed, instance uuid.UUID, msg jetstream.Msg, logger *slog.Logger, driver *sandbox.Docker) {
	handle(ctx, client, b, c, instance, msg, logger, driver, nil)
}
func handle(ctx context.Context, client workerv1.CoordinatorClient, b *broker.Broker, c config.Distributed, instance uuid.UUID, msg jetstream.Msg, logger *slog.Logger, driver *sandbox.Docker, runtime *AgentRuntime) {
	ctx, deliverySpan := telemetry.Linked(ctx, "broker.consume", msg.Headers().Get("traceparent"))
	deliveryStarted := time.Now()
	if metadata, e := msg.Metadata(); e == nil {
		telemetry.Delivery(ctx, metadata.NumDelivered > 1)
	}
	defer func() { telemetry.Finish(ctx, deliverySpan, "broker.consume", deliveryStarted, "ok") }()
	n, err := execution.DecodeNotification(msg.Data())
	if err != nil {
		call, done := context.WithTimeout(ctx, c.RPCTimeout)
		defer done()
		if b.QuarantineMessage(call, msg) != nil {
			_ = msg.NakWithDelay(time.Second)
		}
		return
	}
	request := uuid.NewSHA1(instance, []byte(fmt.Sprintf("%s:%d", n.TaskID, n.Generation)))
	a, err := claim(ctx, client, c, &workerv1.ClaimRequest{InstanceId: instance.String(), TaskId: n.TaskID.String(), DispatchGeneration: n.Generation, RequestId: request.String()})
	if err != nil {
		if status.Code(err) == codes.AlreadyExists || status.Code(err) == codes.FailedPrecondition {
			call, done := context.WithTimeout(ctx, c.RPCTimeout)
			_ = msg.DoubleAck(call)
			done()
		} else {
			delay := time.Second
			if status.Code(err) == codes.Unimplemented {
				delay = 5 * time.Second
			}
			_ = msg.NakWithDelay(delay)
			logger.Warn("worker_claim_deferred", "error_class", status.Code(err).String())
		}
		return
	}
	call, done := context.WithTimeout(ctx, c.RPCTimeout)
	_ = msg.DoubleAck(call)
	done()
	owned := &workerv1.AttemptRequest{InstanceId: instance.String(), AttemptId: a.Id, Generation: a.Generation}
	ctx, attemptSpan := telemetry.Start(ctx, "attempt.run", telemetry.IDs(a.WorkflowId, a.TaskId, a.Id)...)
	attemptStarted := time.Now()
	attemptOutcome := "incomplete"
	defer func() { telemetry.Finish(ctx, attemptSpan, "attempt.run", attemptStarted, attemptOutcome) }()
	stopped := func() {
		stopCtx, stopDone := context.WithTimeout(context.Background(), c.RPCTimeout)
		defer stopDone()
		_, _ = client.AcknowledgeStop(stopCtx, owned)
	}
	call, done = context.WithTimeout(ctx, c.RPCTimeout)
	_, err = client.AuthorizeTool(call, owned)
	done()
	if err != nil {
		stopped()
		return
	}
	// Even an ambiguous broker ack does not revoke an already-durable claim.
	logger.Info("diagnostic_started", "attempt_id", a.Id, "task_id", a.TaskId, "generation", a.Generation, "trace_id", telemetry.TraceID(ctx))
	execCtx, cancelExec := context.WithDeadline(ctx, a.DeadlineAt.AsTime())
	defer cancelExec()
	renewCtx, stopRenew := context.WithCancel(ctx)
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		for pause(renewCtx, c.Limits.Lease/3) {
			call, done := context.WithTimeout(renewCtx, c.RPCTimeout)
			renewed, err := client.RenewLease(call, owned)
			done()
			if err != nil && renewCtx.Err() != nil {
				return
			} // normal completion stopped the renewal loop
			if err != nil || renewed.StopRequested {
				cancelExec()
				return
			}
		}
	}()
	success := true
	var sandboxResult *sandbox.Result
	var agentResult *agent.Outcome
	cleanupConfirmed := true
	switch a.Kind {
	case "agent_go":
		cleanupConfirmed = false
		if driver != nil && runtime != nil {
			result, e := ExecuteAgent(execCtx, client, c, driver, runtime, a)
			if e == nil {
				agentResult = &result
				cleanupConfirmed = true
				success = result.Success()
			} else {
				logger.Error("agent_execution_interrupted", "attempt_id", a.Id, "error_class", "restart_from_pinned_base", "rpc_code", status.Code(e).String(), "policy_denied", errors.Is(e, sandbox.ErrPolicy))
			}
		}
	case "noop":
	case "delay":
		success = pause(execCtx, time.Duration(a.DelayMs)*time.Millisecond)
	case "sandbox_go":
		cleanupConfirmed = false
		if driver != nil {
			result, e := ExecuteSandbox(execCtx, client, c, driver, a)
			cleanupConfirmed = result.CleanupConfirmed
			if e == nil {
				sandboxResult = &result
				success = result.Status == "exited" && result.ExitCode == 0
			} else {
				logger.Error("sandbox_execution_failed", "attempt_id", a.Id, "cleanup_confirmed", cleanupConfirmed, "error_class", "sandbox_execution_or_cleanup_failure")
			}
		}
	default:
		success = false
	}
	stopRenew()
	<-renewDone
	if (a.Kind == "sandbox_go" || a.Kind == "agent_go") && (!cleanupConfirmed || sandboxResult == nil && agentResult == nil) {
		if cleanupConfirmed {
			stopped()
		}
		return
	}
	if execCtx.Err() != nil || ctx.Err() != nil {
		attemptOutcome = "cancelled"
		stopped()
		logger.Info("diagnostic_stopped", "attempt_id", a.Id, "reason", "lease_cancel_or_deadline")
		return
	} // ownership/cancellation loss: no late result
	body, _ := json.Marshal(map[string]any{"kind": a.Kind, "delay_ms": a.DelayMs})
	r := &workerv1.OutcomeRequest{InstanceId: instance.String(), AttemptId: a.Id, Generation: a.Generation, ResultJson: body}
	if !success {
		r.FailureClass = execution.Unsupported
	}
	for i := 0; i < 3; i++ {
		call, done := context.WithTimeout(ctx, c.RPCTimeout)
		if agentResult != nil {
			wire, _ := json.Marshal(agentResult)
			_, err = client.CompleteAgent(call, &workerv1.AgentOutcome{Attempt: owned, OutcomeJson: wire})
		} else if sandboxResult != nil {
			wire, _ := json.Marshal(sandboxResult)
			_, err = client.CompleteSandbox(call, &workerv1.SandboxOutcome{Request: &workerv1.SandboxRequest{Attempt: owned, RequestId: sandbox.RequestID(uuid.MustParse(a.Id)).String()}, CommandResultJson: wire})
		} else if success {
			_, err = client.CompleteTask(call, r)
		} else {
			_, err = client.FailTask(call, r)
		}
		done()
		if err == nil {
			attemptOutcome = "failed"
			if success {
				attemptOutcome = "ok"
			}
			logger.Info("diagnostic_finished", "attempt_id", a.Id, "generation", a.Generation)
			return
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			logger.Warn("diagnostic_outcome_rejected", "attempt_id", a.Id, "rpc_code", status.Code(err).String())
			stopped()
			return
		}
		if !pause(ctx, c.Poll) {
			return
		}
	}
}
func Run(stop context.Context, client workerv1.CoordinatorClient, b *broker.Broker, c config.Distributed, logger *slog.Logger) error {
	return RunWithSandbox(stop, client, b, c, logger, nil)
}
func RunWithSandbox(stop context.Context, client workerv1.CoordinatorClient, b *broker.Broker, c config.Distributed, logger *slog.Logger, driver *sandbox.Docker) error {
	return RunWithAgent(stop, client, b, c, logger, driver, nil)
}
func RunWithAgent(stop context.Context, client workerv1.CoordinatorClient, b *broker.Broker, c config.Distributed, logger *slog.Logger, driver *sandbox.Docker, runtime *AgentRuntime) error {
	instance := uuid.New()
	work, cancel := context.WithCancel(context.Background())
	defer cancel()
	capabilities := []string{"noop", "delay"}
	if driver != nil {
		capabilities = append(capabilities, "sandbox_go")
	}
	if driver != nil && runtime != nil {
		capabilities = append(capabilities, "agent_go")
	}
	workerID := ""
	for {
		call, done := context.WithTimeout(stop, c.RPCTimeout)
		registered, err := client.RegisterWorker(call, &workerv1.RegisterRequest{InstanceId: instance.String(), Capabilities: capabilities, Capacity: int32(c.Capacity)})
		done()
		if err == nil {
			workerID = registered.WorkerId
			break
		}
		if !pause(stop, c.Poll) {
			return nil
		}
		if status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.InvalidArgument {
			return err
		}
	}
	if driver != nil {
		for {
			if err := reconcileSandbox(stop, client, c, driver, workerID, instance); err != nil {
				driver.Suspend()
				logger.Error("sandbox_cleanup_retry_required", "error_class", "runtime_or_coordination_unavailable")
				if !pause(stop, 500*time.Millisecond) {
					return nil
				}
				continue
			}
			break
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for pause(work, 500*time.Millisecond) {
			call, done := context.WithTimeout(work, c.RPCTimeout)
			_, err := client.HeartbeatWorker(call, &workerv1.InstanceRequest{InstanceId: instance.String()})
			done()
			if err != nil {
				logger.Warn("worker_heartbeat_failed", "error_class", "rpc_unavailable")
			}
		}
	}()
	if driver != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pause(work, 2*time.Second) {
				if err := reconcileSandbox(work, client, c, driver, workerID, instance); err != nil {
					driver.Suspend()
					logger.Error("sandbox_cleanup_retry_required", "error_class", "runtime_or_coordination_unavailable")
				}
			}
		}()
	}
	slots := make(chan struct{}, c.Capacity)
	var jobs sync.WaitGroup
	for stop.Err() == nil {
		if driver != nil && !driver.CanStart() {
			if !pause(stop, c.Poll) {
				break
			}
			continue
		}
		available := c.Capacity - len(slots)
		if available == 0 {
			if !pause(stop, c.Poll) {
				break
			}
			continue
		}
		// Short pulls disable heartbeats by default; FetchHeartbeat(0) is an
		// invalid option in the pinned client and would prevent every delivery.
		batch, err := b.Consumer.Fetch(available, jetstream.FetchMaxWait(300*time.Millisecond))
		if err != nil {
			logger.Warn("worker_fetch_failed", "error_class", "broker_unavailable")
			ensureCtx, ensureDone := context.WithTimeout(stop, c.RPCTimeout)
			_ = b.Ensure(ensureCtx)
			ensureDone()
			if !pause(stop, c.Poll) {
				break
			}
			continue
		}
		for msg := range batch.Messages() {
			if stop.Err() != nil {
				_ = msg.NakWithDelay(time.Second)
				continue
			}
			slots <- struct{}{}
			jobs.Add(1)
			go func(m jetstream.Msg) {
				defer jobs.Done()
				defer func() { <-slots }()
				if stop.Err() != nil {
					_ = m.NakWithDelay(time.Second)
					return
				}
				handle(work, client, b, c, instance, m, logger, driver, runtime)
			}(msg)
		}
		if batch.Error() != nil {
			logger.Warn("worker_fetch_failed", "error_class", "broker_unavailable")
			ensureCtx, ensureDone := context.WithTimeout(stop, c.RPCTimeout)
			_ = b.Ensure(ensureCtx)
			ensureDone()
			if !pause(stop, c.Poll) {
				break
			}
		}
	}
	call, done := context.WithTimeout(context.Background(), c.RPCTimeout)
	_, _ = client.SetDraining(call, &workerv1.DrainRequest{InstanceId: instance.String(), Draining: true})
	done()
	finished := make(chan struct{})
	go func() { jobs.Wait(); close(finished) }()
	timer := time.NewTimer(c.Shutdown)
	select {
	case <-finished:
		timer.Stop()
	case <-timer.C:
		cancel()
		<-finished
	}
	cancel()
	wg.Wait()
	logger.Info("worker_drained")
	return nil
}
