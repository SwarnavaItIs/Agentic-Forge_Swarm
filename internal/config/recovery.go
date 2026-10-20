package config

import (
	"errors"
	"forgeswarm/internal/execution"
	"strconv"
	"time"
)

func RecoveryParse(env func(string) (string, bool)) (execution.Policy, execution.Limits, error) {
	p, l := execution.DefaultPolicy(), execution.DefaultLimits()
	for key, dst := range map[string]*int{"MAX_WORKFLOWS": &p.MaxWorkflows, "MAX_TASKS_PER_WORKFLOW": &p.MaxTasks, "MAX_QUEUED_TASKS": &p.MaxQueued, "MAX_READY_TASKS": &p.MaxReady, "TOTAL_CAPACITY": &l.TotalCapacity, "WORKER_MAX_CAPACITY": &l.WorkerCapacity, "TASK_MAX_ATTEMPTS": &l.MaxAttempts} {
		if v, ok := env(key); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return p, l, errors.New("invalid recovery integer setting")
			}
			*dst = n
		}
	}
	for key, dst := range map[string]*time.Duration{"WORKFLOW_TIMEOUT": &p.WorkflowTimeout, "TASK_TIMEOUT": &p.TaskTimeout, "RETRY_BACKOFF_BASE": &p.BackoffBase, "RETRY_BACKOFF_CAP": &p.BackoffCap, "READY_RESEND_INTERVAL": &p.ReadyResend, "CAPABILITY_WAIT": &p.CapabilityWait, "WORKER_FRESHNESS": &p.WorkerFreshness, "ATTEMPT_LEASE": &l.Lease} {
		if v, ok := env(key); ok {
			d, err := time.ParseDuration(v)
			if err != nil {
				return p, l, errors.New("invalid recovery duration setting")
			}
			*dst = d
		}
	}
	if !p.Valid() || !l.Valid() {
		return p, l, errors.New("invalid recovery bounds")
	}
	return p, l, nil
}
