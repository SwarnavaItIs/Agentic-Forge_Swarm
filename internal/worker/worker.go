// Package worker executes only bounded noop/delay diagnostics. Notifications
// are acknowledged after durable claim; completion recovery belongs to leases.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/config"
	"forgeswarm/internal/execution"
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
	logger.Info("diagnostic_started", "attempt_id", a.Id, "task_id", a.TaskId, "generation", a.Generation)
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
	switch a.Kind {
	case "noop":
	case "delay":
		success = pause(execCtx, time.Duration(a.DelayMs)*time.Millisecond)
	default:
		success = false
	}
	stopRenew()
	<-renewDone
	if execCtx.Err() != nil || ctx.Err() != nil {
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
		if success {
			_, err = client.CompleteTask(call, r)
		} else {
			_, err = client.FailTask(call, r)
		}
		done()
		if err == nil {
			logger.Info("diagnostic_finished", "attempt_id", a.Id, "generation", a.Generation)
			return
		}
		if status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			stopped()
			return
		}
		if !pause(ctx, c.Poll) {
			return
		}
	}
}
func Run(stop context.Context, client workerv1.CoordinatorClient, b *broker.Broker, c config.Distributed, logger *slog.Logger) error {
	instance := uuid.New()
	work, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		call, done := context.WithTimeout(stop, c.RPCTimeout)
		_, err := client.RegisterWorker(call, &workerv1.RegisterRequest{InstanceId: instance.String(), Capabilities: []string{"noop", "delay"}, Capacity: int32(c.Capacity)})
		done()
		if err == nil {
			break
		}
		if !pause(stop, c.Poll) {
			return nil
		}
		if status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.InvalidArgument {
			return err
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
	slots := make(chan struct{}, c.Capacity)
	var jobs sync.WaitGroup
	for stop.Err() == nil {
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
				Handle(work, client, b, c, instance, m, logger)
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
