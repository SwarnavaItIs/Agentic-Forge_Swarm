package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

// Controlled command responses prove reconciliation logic, not Docker kernel
// enforcement; the existing real-Docker acceptance gate remains mandatory.
func TestReconcileNormalTeardownDoesNotSuspendOtherAttempts(t *testing.T) {
	for _, priorFailure := range []bool{false, true} {
		b := testBinding()
		id := strings.Repeat("b", 64)
		d := &Docker{Installation: b.Installation}
		if priorFailure {
			d.Suspend()
		}
		d.active.Store(b.Attempt, false)
		d.command = func(_ context.Context, _ io.Reader, _ int, args ...string) ([]byte, error) {
			if args[0] == "ps" {
				return []byte(id), nil
			}
			if args[0] == "container" {
				i := Inspection{ID: id}
				i.Config.Labels = labels(b)
				body, _ := json.Marshal([]Inspection{i})
				return body, nil
			}
			t.Fatalf("normal owned teardown was interrupted: %v", args)
			return nil, nil
		}
		if e := d.Reconcile(context.Background(), b.Worker, nil); e != nil {
			t.Errorf("normal teardown is not a cleanup failure: %v", e)
		}
		if d.CanStart() == priorFailure {
			t.Errorf("admission changed incorrectly: prior failure=%v can start=%v", priorFailure, d.CanStart())
		}
		// Prior uncertainty must remain until a later scan proves cleanup complete.
		d.active.Delete(b.Attempt)
		d.command = func(context.Context, io.Reader, int, ...string) ([]byte, error) { return nil, nil }
		if e := d.Reconcile(context.Background(), b.Worker, nil); e != nil || !d.CanStart() {
			t.Fatal("confirmed absence did not reconcile prior uncertainty", e)
		}
	}
}
func TestReconcileVanishedScanEntryRequiresPositiveAbsenceProof(t *testing.T) {
	for _, mode := range []string{"gone", "daemon_unavailable", "still_present"} {
		t.Run(mode, func(t *testing.T) {
			b := testBinding()
			id := strings.Repeat("b", 64)
			d := &Docker{Installation: b.Installation}
			d.command = func(_ context.Context, _ io.Reader, _ int, args ...string) ([]byte, error) {
				if args[0] == "container" {
					return nil, errors.New("inspect no longer available")
				}
				if strings.HasPrefix(args[len(args)-1], "id=") {
					if mode == "daemon_unavailable" {
						return nil, errors.New("daemon unavailable")
					}
					if mode == "still_present" {
						return []byte(id), nil
					}
					return nil, nil
				}
				return []byte(id), nil
			}
			e := d.Reconcile(context.Background(), b.Worker, nil)
			if mode == "gone" {
				if e != nil || !d.CanStart() {
					t.Fatal("normal remove/scan race suspended admission", e)
				}
			} else if e == nil || d.CanStart() {
				t.Fatal("uncertainty reopened admission")
			}
		})
	}
}
