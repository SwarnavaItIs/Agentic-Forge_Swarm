package execution

import "time"

const (
	Transient      = "transient_infrastructure"
	WorkerLost     = "worker_lost"
	Permission     = "permission_denied"
	Configuration  = "invalid_configuration"
	Unsupported    = "unsupported_task"
	Diagnostic     = "diagnostic_failed"
	Deadline       = "deadline_exceeded"
	ExplicitCancel = "explicit_cancel"
)

func FailureClass(class string) bool {
	switch class {
	case Transient, WorkerLost, Permission, Configuration, Unsupported, Diagnostic:
		return true
	}
	return false
}
func Retryable(class string) bool { return class == Transient || class == WorkerLost }

// Policy bounds platform admission and recovery. All API/scheduler/coordinator
// instances for a database must use the same operator configuration. Per-task
// budgets and deadlines are snapshots persisted at creation, never reset later.
type Policy struct {
	MaxWorkflows, MaxTasks, MaxQueued, MaxReady           int
	WorkflowTimeout, TaskTimeout, BackoffBase, BackoffCap time.Duration
	ReadyResend, CapabilityWait, WorkerFreshness          time.Duration
}

func DefaultPolicy() Policy {
	return Policy{100, 100, 10000, 1000, 10 * time.Minute, 2 * time.Minute, 250 * time.Millisecond, 5 * time.Second, 5 * time.Second, 30 * time.Second, 5 * time.Second}
}
func (p Policy) Valid() bool {
	return p.MaxWorkflows >= 1 && p.MaxWorkflows <= 10000 && p.MaxTasks >= 1 && p.MaxTasks <= 100 && p.MaxQueued >= 1 && p.MaxQueued <= 100000 && p.MaxReady >= 1 && p.MaxReady <= p.MaxQueued &&
		p.WorkflowTimeout >= 100*time.Millisecond && p.WorkflowTimeout <= 24*time.Hour && p.TaskTimeout >= 100*time.Millisecond && p.TaskTimeout <= 24*time.Hour &&
		p.BackoffBase >= 50*time.Millisecond && p.BackoffCap >= p.BackoffBase && p.BackoffCap <= time.Hour && p.ReadyResend >= time.Second && p.ReadyResend <= time.Hour &&
		p.CapabilityWait >= 100*time.Millisecond && p.CapabilityWait <= time.Hour && p.WorkerFreshness >= time.Second && p.WorkerFreshness <= time.Minute
}

// RetryAt uses equal jitter in [cap/2,cap]. The caller supplies database time
// and an RNG sample (injectable in tests), and persists the choice atomically.
// No application clock, goroutine or timer owns a durable retry.
func (p Policy) RetryAt(now time.Time, attempt int, sample float64) time.Time {
	d := p.BackoffBase
	for i := 1; i < attempt && d < p.BackoffCap; i++ {
		d = min(d*2, p.BackoffCap)
	}
	if sample < 0 {
		sample = 0
	}
	if sample > 1 {
		sample = 1
	}
	return now.Add(d/2 + time.Duration(float64(d-d/2)*sample))
}

type TaskInspection struct {
	ID             string     `json:"id"`
	WorkflowID     string     `json:"workflow_id"`
	Status         string     `json:"status"`
	Kind           string     `json:"kind"`
	DelayMS        int        `json:"delay_ms"`
	Generation     int64      `json:"dispatch_generation"`
	AttemptCount   int        `json:"attempt_count"`
	MaxAttempts    int        `json:"max_attempts"`
	CurrentAttempt *string    `json:"current_attempt_id"`
	NextAttemptAt  *time.Time `json:"next_attempt_at"`
	DeadlineAt     time.Time  `json:"deadline_at"`
	LastError      string     `json:"last_error_class"`
	ManualRedrive  string     `json:"manual_redrive"`
}
type AttemptPage struct {
	Attempts  []Attempt `json:"attempts"`
	NextAfter int64     `json:"next_after"`
}
