package execution

import (
	"testing"
	"time"
)

func TestRetryPolicyInjectedClockAndJitter(t *testing.T) {
	p := DefaultPolicy()
	now := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	if !p.Valid() {
		t.Fatal("invalid defaults")
	}
	for attempt := 1; attempt <= 10; attempt++ {
		cap := min(p.BackoffBase*time.Duration(1<<uint(attempt-1)), p.BackoffCap)
		if got := p.RetryAt(now, attempt, 0); !got.Equal(now.Add(cap / 2)) {
			t.Fatal("lower jitter bound", attempt, got)
		}
		if got := p.RetryAt(now, attempt, 1); !got.Equal(now.Add(cap)) {
			t.Fatal("upper jitter bound", attempt, got)
		}
		if !p.RetryAt(now, attempt, 0.5).After(now) {
			t.Fatal("hot retry")
		}
	}
	for _, class := range []string{Permission, Configuration, Unsupported, Diagnostic, Deadline, ExplicitCancel} {
		if Retryable(class) {
			t.Fatal("permanent/cancel/deadline retries", class)
		}
	}
	if !Retryable(Transient) || !Retryable(WorkerLost) || FailureClass("arbitrary") {
		t.Fatal("classification")
	}
}
