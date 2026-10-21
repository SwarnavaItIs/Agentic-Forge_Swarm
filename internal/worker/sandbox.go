package worker

import (
	"context"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/config"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"time"
)

func binding(driver *sandbox.Docker, a *workerv1.Attempt) (sandbox.Binding, error) {
	b := sandbox.Binding{Installation: driver.Installation, Generation: a.Generation, Worker: a.WorkerId}
	var err error
	b.Workflow, err = uuid.Parse(a.WorkflowId)
	if err != nil {
		return b, err
	}
	b.Task, err = uuid.Parse(a.TaskId)
	if err != nil {
		return b, err
	}
	b.Attempt, err = uuid.Parse(a.Id)
	if err != nil {
		return b, err
	}
	b.Instance, err = uuid.Parse(a.InstanceId)
	if err != nil {
		return b, err
	}
	b.Request = sandbox.RequestID(b.Attempt)
	if !b.Valid() {
		return b, sandbox.ErrPolicy
	}
	return b, nil
}
func ExecuteSandbox(ctx context.Context, client workerv1.CoordinatorClient, c config.Distributed, driver *sandbox.Docker, a *workerv1.Attempt) (sandbox.Result, error) {
	b, err := binding(driver, a)
	if err != nil {
		return sandbox.Result{}, err
	}
	var in sandbox.Input
	if sandbox.Decode(a.SandboxInputJson, &in, 1024) != nil {
		return sandbox.Result{}, sandbox.ErrPolicy
	}
	request := &workerv1.SandboxRequest{Attempt: &workerv1.AttemptRequest{InstanceId: b.Instance.String(), AttemptId: b.Attempt.String(), Generation: b.Generation}, RequestId: b.Request.String()}
	authorize := func(parent context.Context) error {
		call, done := context.WithTimeout(parent, c.RPCTimeout)
		defer done()
		_, e := client.AuthorizeSandbox(call, request)
		return e
	}
	return driver.RunCommand(ctx, driver.Request(b, in, sandbox.DefaultPolicy()), authorize)
}
func reconcileSandbox(ctx context.Context, client workerv1.CoordinatorClient, c config.Distributed, driver *sandbox.Docker, worker string, instance uuid.UUID) error {
	call, done := context.WithTimeout(ctx, 15*time.Second)
	defer done()
	ack := func(parent context.Context, b sandbox.Binding) error {
		rpc, finish := context.WithTimeout(parent, c.RPCTimeout)
		defer finish()
		_, err := client.AcknowledgeStop(rpc, &workerv1.AttemptRequest{InstanceId: b.Instance.String(), AttemptId: b.Attempt.String(), Generation: b.Generation})
		return err
	}
	if err := driver.Reconcile(call, worker, ack); err != nil {
		return err
	}
	rpc, finish := context.WithTimeout(call, c.RPCTimeout)
	page, err := client.SandboxCleanup(rpc, &workerv1.InstanceRequest{InstanceId: instance.String()})
	finish()
	if err != nil {
		return err
	}
	for _, a := range page.Attempts {
		b, e := binding(driver, a)
		if e != nil {
			return e
		}
		if e = driver.ReconcileAttempt(call, b); e != nil {
			return e
		}
		if e = ack(call, b); e != nil {
			return e
		}
	}
	return nil
}
