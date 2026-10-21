// Package workflow defines the durable state machine and versioned creation input.
package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
)

const (
	Pending              = "PENDING"
	Cancelled            = "CANCELLED"
	RequestSchemaVersion = 5
	Cancelling           = "CANCELLING"
	RetryWait            = "RETRY_WAIT"
)

var (
	ErrInvalid         = errors.New("invalid request")
	ErrNotFound        = errors.New("workflow not found")
	ErrConflict        = errors.New("idempotency conflict")
	ErrUnavailable     = errors.New("database unavailable")
	ErrState           = errors.New("unsupported persisted state")
	ErrSaturated       = errors.New("admission_saturated")
	keyPattern         = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	idempotencyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)

type TaskDefinition struct {
	Key         string         `json:"key"`
	Kind        string         `json:"kind"`
	DependsOn   []string       `json:"depends_on,omitempty"`
	DelayMS     int            `json:"delay_ms,omitempty"`
	TimeoutMS   int64          `json:"timeout_ms,omitempty"`
	MaxAttempts int            `json:"max_attempts,omitempty"`
	Sandbox     *sandbox.Input `json:"sandbox,omitempty"`
}

type CreateInput struct {
	Name      string           `json:"name"`
	Tasks     []TaskDefinition `json:"tasks"`
	TimeoutMS int64            `json:"timeout_ms,omitempty"`
}

func ValidCreationKey(key string) bool { return idempotencyPattern.MatchString(key) }

func Normalize(in CreateInput) (CreateInput, error) {
	if !utf8.ValidString(in.Name) {
		return CreateInput{}, ErrInvalid
	}
	in.Name = strings.TrimSpace(in.Name)
	n := utf8.RuneCountInString(in.Name)
	if n < 1 || n > 128 || strings.ContainsRune(in.Name, '\x00') || len(in.Tasks) < 1 || len(in.Tasks) > 100 || in.TimeoutMS < 0 || in.TimeoutMS > 86400000 || (in.TimeoutMS > 0 && in.TimeoutMS < 100) {
		return CreateInput{}, ErrInvalid
	}
	seen := make(map[string]struct{}, len(in.Tasks))
	for _, task := range in.Tasks {
		if task.MaxAttempts < 0 || task.MaxAttempts > 10 || task.TimeoutMS < 0 || task.TimeoutMS > 86400000 || (task.TimeoutMS > 0 && task.TimeoutMS < 100) {
			return CreateInput{}, ErrInvalid
		}
		if !keyPattern.MatchString(task.Key) || !((task.Kind == "noop" && task.DelayMS == 0 && task.Sandbox == nil) || (task.Kind == "delay" && task.DelayMS >= 1 && task.DelayMS <= 30000 && task.Sandbox == nil) || (task.Kind == "sandbox_go" && task.DelayMS == 0 && task.Sandbox != nil && task.Sandbox.Valid())) {
			return CreateInput{}, ErrInvalid
		}
		if _, ok := seen[task.Key]; ok {
			return CreateInput{}, ErrInvalid
		}
		seen[task.Key] = struct{}{}
	}
	in.Tasks = append([]TaskDefinition(nil), in.Tasks...)
	if _, err := ValidateGraph(in.Tasks); err != nil {
		return CreateInput{}, err
	}
	for i := range in.Tasks {
		in.Tasks[i].DependsOn = append([]string(nil), in.Tasks[i].DependsOn...)
		if in.Tasks[i].Sandbox != nil {
			value := *in.Tasks[i].Sandbox
			in.Tasks[i].Sandbox = &value
		}
		slices.Sort(in.Tasks[i].DependsOn)
	}
	return in, nil
}

// HashForVersion never silently projects newly supplied fields onto an old input.
// Future input fields require an explicit compatibility rule and a new version.
// Version 1 uses this fixed typed representation; changing it changes old hashes.
func HashForVersion(in CreateInput, version int) ([32]byte, error) {
	if version != 1 && version != 2 && version != 3 && version != 4 && version != 5 {
		return [32]byte{}, ErrState
	}
	normalized, err := Normalize(in)
	if err != nil {
		return [32]byte{}, err
	}
	if version < 4 {
		if normalized.TimeoutMS != 0 {
			return [32]byte{}, ErrConflict
		}
		for _, task := range normalized.Tasks {
			if task.TimeoutMS != 0 || task.MaxAttempts != 0 {
				return [32]byte{}, ErrConflict
			}
		}
	}
	if version < 5 {
		for _, task := range normalized.Tasks {
			if task.Sandbox != nil || task.Kind == "sandbox_go" {
				return [32]byte{}, ErrConflict
			}
		}
	}
	if version < 3 {
		for _, task := range normalized.Tasks {
			if task.Kind != "noop" || task.DelayMS != 0 {
				return [32]byte{}, ErrConflict
			}
		}
	}
	var representation any
	if version == 1 {
		// Frozen stage-01 encoding, including field and display order. Explicit
		// empty dependencies are equivalent to missing; new edges conflict.
		type legacyTask struct {
			Key  string `json:"key"`
			Kind string `json:"kind"`
		}
		old := struct {
			Name  string       `json:"name"`
			Tasks []legacyTask `json:"tasks"`
		}{Name: normalized.Name}
		for _, task := range normalized.Tasks {
			if len(task.DependsOn) != 0 {
				return [32]byte{}, ErrConflict
			}
			old.Tasks = append(old.Tasks, legacyTask{task.Key, task.Kind})
		}
		representation = old
	} else {
		representation = struct {
			Version int         `json:"normalization_version"`
			Input   CreateInput `json:"input"`
		}{version, normalized}
	}
	encoded, err := json.Marshal(representation)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

// Version counts committed logical mutations, independently from event sequence.
// Terminal outcomes are immutable; cancelling them is an idempotent no-op.
// Late cleanup attestations may append audit history after logical termination.
func Cancellation(status string, version int64) (bool, error) {
	if version < 1 {
		return false, ErrState
	}
	switch status {
	case Cancelling:
		return false, nil
	case Pending:
		return true, nil
	case Running:
		if version < 2 {
			return false, ErrState
		}
		return true, nil
	case Cancelled, Succeeded, Failed:
		if version < 2 {
			return false, ErrState
		}
		return false, nil
	default:
		return false, ErrState
	}
}

type Receipt struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

type Task struct {
	ID        uuid.UUID `json:"id"`
	Key       string    `json:"key"`
	Position  int       `json:"position"`
	Kind      string    `json:"kind"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Details struct {
	DeadlineAt     time.Time `json:"deadline_at"`
	TerminalReason string    `json:"terminal_reason,omitempty"`
	ID             uuid.UUID `json:"id"`
	Name           string    `json:"name"`
	Status         string    `json:"status"`
	Version        int64     `json:"version"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	Tasks          []Task    `json:"tasks"`
}

type CancellationResult struct {
	ID      uuid.UUID `json:"id"`
	Status  string    `json:"status"`
	Version int64     `json:"version"`
}

type Event struct {
	Sequence   int64           `json:"sequence"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload"`
	OccurredAt time.Time       `json:"occurred_at"`
}

type EventPage struct {
	Events    []Event `json:"events"`
	NextAfter int64   `json:"next_after"`
}

type Service interface {
	Create(context.Context, string, CreateInput) (Receipt, error)
	Details(context.Context, uuid.UUID) (Details, error)
	Cancel(context.Context, uuid.UUID) (CancellationResult, error)
	Events(context.Context, uuid.UUID, int64, int) (EventPage, error)
	Graph(context.Context, uuid.UUID) (Graph, error)
	Ready(context.Context) error
	InspectTask(context.Context, uuid.UUID, uuid.UUID) (execution.TaskInspection, error)
	Attempts(context.Context, uuid.UUID, uuid.UUID, int64, int) (execution.AttemptPage, error)
}
