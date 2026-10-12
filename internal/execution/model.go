// Package execution defines diagnostic execution and ownership contracts. A
// broker notification is never execution authorization; a durable claim is.
package execution

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"io"
	"time"
)

var (
	ErrClaimed  = errors.New("already_claimed")
	ErrObsolete = errors.New("obsolete_notification")
	ErrCapacity = errors.New("capacity_exceeded")
	ErrDraining = errors.New("worker_draining")
	ErrWorker   = errors.New("worker_instance_invalid")
	ErrStale    = errors.New("stale_or_expired_attempt")
	ErrOutcome  = errors.New("conflicting_outcome")
)

type Limits struct {
	WorkerCapacity, TotalCapacity, MaxAttempts int
	Lease                                      time.Duration
}

func DefaultLimits() Limits { return Limits{2, 4, 3, 3 * time.Second} }
func (l Limits) Valid() bool {
	return l.WorkerCapacity >= 1 && l.WorkerCapacity <= 100 && l.TotalCapacity >= l.WorkerCapacity && l.TotalCapacity <= 1000 && l.MaxAttempts >= 1 && l.MaxAttempts <= 10 && l.Lease >= 500*time.Millisecond && l.Lease <= time.Minute
}

type Worker struct {
	ID           string
	Instance     uuid.UUID
	Capabilities []string
	Capacity     int
	Heartbeat    time.Time
	Draining     bool
}
type Attempt struct {
	ID, TaskID, Instance      uuid.UUID
	WorkerID                  string
	Number                    int
	Generation                int64
	State, Kind               string
	DelayMS                   int
	StartedAt, LeaseExpiresAt time.Time
	FinishedAt                *time.Time
	Digest, Result            []byte
}
type Notification struct {
	SchemaVersion int       `json:"schema_version"`
	TaskID        uuid.UUID `json:"task_id"`
	Generation    int64     `json:"dispatch_generation"`
}

func DecodeNotification(body []byte) (Notification, error) {
	var n Notification
	if len(body) > 4096 {
		return n, errors.New("message_too_large")
	}
	// Reject ambiguous duplicate keys before the typed decoder can overwrite
	// them. Reuse the bounded JSON walker used by outcome metadata.
	check := json.NewDecoder(bytes.NewReader(body))
	check.UseNumber()
	if _, err := readJSON(check, 0); err != nil {
		return n, errors.New("malformed_notification")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if err := d.Decode(&n); err != nil {
		return n, errors.New("malformed_notification")
	}
	if d.Decode(new(any)) != io.EOF || n.SchemaVersion != 1 || n.TaskID == uuid.Nil || n.Generation < 1 {
		return n, errors.New("unsupported_notification")
	}
	return n, nil
}

// Canonical metadata permits logical retries independent of object key order.
// Counts/numbers retain their JSON representation; bytes or canonical JSON must
// match the accepted outcome. Duplicate object keys are rejected before hashing.
func Outcome(body []byte) ([]byte, [32]byte, error) {
	if len(body) > 4096 {
		return nil, [32]byte{}, errors.New("result_too_large")
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	value, err := readJSON(d, 0)
	if err != nil {
		return nil, [32]byte{}, err
	}
	if _, ok := value.(map[string]any); !ok || d.Decode(new(any)) != io.EOF {
		return nil, [32]byte{}, errors.New("result_object_required")
	}
	canonical, err := json.Marshal(value)
	if err != nil || len(canonical) > 4000 {
		return nil, [32]byte{}, errors.New("result_too_large")
	}
	return canonical, sha256.Sum256(canonical), nil
}
func readJSON(d *json.Decoder, depth int) (any, error) {
	if depth > 16 {
		return nil, errors.New("result_nesting_limit")
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t {
	case json.Delim('{'):
		m := map[string]any{}
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := k.(string)
			if !ok {
				return nil, errors.New("invalid_key")
			}
			if _, ok = m[key]; ok {
				return nil, errors.New("duplicate_key")
			}
			v, err := readJSON(d, depth+1)
			if err != nil {
				return nil, err
			}
			m[key] = v
		}
		_, err = d.Token()
		return m, err
	case json.Delim('['):
		a := []any{}
		for d.More() {
			v, err := readJSON(d, depth+1)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		_, err = d.Token()
		return a, err
	default:
		return t, nil
	}
}
