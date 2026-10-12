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

	"github.com/google/uuid"
)

const (
	Pending              = "PENDING"
	Cancelled            = "CANCELLED"
	RequestSchemaVersion = 3
)

var (
	ErrInvalid         = errors.New("invalid request")
	ErrNotFound        = errors.New("workflow not found")
	ErrConflict        = errors.New("idempotency conflict")
	ErrUnavailable     = errors.New("database unavailable")
	ErrState           = errors.New("unsupported persisted state")
	keyPattern         = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	idempotencyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
)

type TaskDefinition struct {
	Key       string   `json:"key"`
	Kind      string   `json:"kind"`
	DependsOn []string `json:"depends_on,omitempty"`
	DelayMS   int      `json:"delay_ms,omitempty"`
}

type CreateInput struct {
	Name  string           `json:"name"`
	Tasks []TaskDefinition `json:"tasks"`
}

func ValidCreationKey(key string) bool { return idempotencyPattern.MatchString(key) }

func Normalize(in CreateInput) (CreateInput, error) {
	if !utf8.ValidString(in.Name) {
		return CreateInput{}, ErrInvalid
	}
	in.Name = strings.TrimSpace(in.Name)
	n := utf8.RuneCountInString(in.Name)
	if n < 1 || n > 128 || strings.ContainsRune(in.Name, '\x00') || len(in.Tasks) < 1 || len(in.Tasks) > 100 {
		return CreateInput{}, ErrInvalid
	}
	seen := make(map[string]struct{}, len(in.Tasks))
	for _, task := range in.Tasks {
		if !keyPattern.MatchString(task.Key) || !((task.Kind == "noop" && task.DelayMS == 0) || (task.Kind == "delay" && task.DelayMS >= 1 && task.DelayMS <= 30000)) {
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
		slices.Sort(in.Tasks[i].DependsOn)
	}
	return in, nil
}

// HashForVersion never silently projects newly supplied fields onto an old input.
// Future input fields require an explicit compatibility rule and a new version.
// Version 1 uses this fixed typed representation; changing it changes old hashes.
func HashForVersion(in CreateInput, version int) ([32]byte, error) {
	if version != 1 && version != 2 && version != 3 {
		return [32]byte{}, ErrState
	}
	normalized, err := Normalize(in)
	if err != nil {
		return [32]byte{}, err
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
// Terminal workflows are immutable; cancelling them is an idempotent no-op.
func Cancellation(status string, version int64) (bool, error) {
	if version < 1 {
		return false, ErrState
	}
	switch status {
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
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Tasks     []Task    `json:"tasks"`
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
}
