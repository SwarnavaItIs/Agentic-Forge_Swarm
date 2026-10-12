package workflow

import "github.com/google/uuid"

const (
	Ready     = "READY"
	Running   = "RUNNING" // future claiming only; stage 02 never emits this transition
	Succeeded = "SUCCEEDED"
	Failed    = "FAILED"
	Blocked   = "BLOCKED"
	MaxTasks  = 100
	MaxEdges  = 1000
)

// ValidateGraph is Kahn's algorithm: O(V+E), separate from deterministic hash
// normalization. Dependencies are sets; task display order is significant.
// The queue and successor adjacency preserve display order for stable decisions.
func ValidateGraph(tasks []TaskDefinition) ([]int, error) {
	if len(tasks) < 1 || len(tasks) > MaxTasks {
		return nil, ErrInvalid
	}
	indexes := make(map[string]int, len(tasks))
	for i, task := range tasks {
		if _, exists := indexes[task.Key]; exists {
			return nil, ErrInvalid
		}
		indexes[task.Key] = i
	}
	indegree := make([]int, len(tasks))
	successors := make([][]int, len(tasks))
	edges := 0
	for i, task := range tasks {
		seen := make(map[string]bool, len(task.DependsOn))
		for _, key := range task.DependsOn {
			predecessor, exists := indexes[key]
			if !exists || predecessor == i || seen[key] {
				return nil, ErrInvalid
			}
			seen[key] = true
			edges++
			if edges > MaxEdges {
				return nil, ErrInvalid
			}
			indegree[i]++
			successors[predecessor] = append(successors[predecessor], i)
		}
	}
	queue := make([]int, 0, len(tasks))
	for i, degree := range indegree {
		if degree == 0 {
			queue = append(queue, i)
		}
	}
	for head := 0; head < len(queue); head++ {
		for _, next := range successors[queue[head]] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if len(queue) != len(tasks) {
		return nil, ErrInvalid
	}
	return queue, nil
}

type Edge struct {
	TaskID          uuid.UUID `json:"task_id"`
	DependsOnTaskID uuid.UUID `json:"depends_on_task_id"`
}

type Graph struct {
	ID      uuid.UUID `json:"id"`
	Status  string    `json:"status"`
	Version int64     `json:"version"`
	Nodes   []Task    `json:"nodes"`
	Edges   []Edge    `json:"edges"`
}

type Transition struct {
	TaskID   uuid.UUID
	From, To string
}
type Decision struct {
	Transitions []Transition
	Status      string
}

// TaskTransition defines the future RUNNING boundary without implementing a
// claim/executor. Only PENDING/READY -> READY/BLOCKED are used by this scheduler.
func TaskTransition(from, to string) bool {
	switch from {
	case Pending:
		return to == Ready || to == Blocked || to == Cancelled
	case Ready:
		return to == Running || to == Blocked || to == Cancelled
	case Running:
		return to == Succeeded || to == Failed || to == Cancelled
	default:
		return false
	}
}

func Reconcile(graph Graph) (Decision, error) {
	decision := Decision{Status: graph.Status, Transitions: make([]Transition, 0)}
	if graph.Status == Cancelled || graph.Status == Succeeded || graph.Status == Failed {
		return decision, nil
	}
	if graph.Status != Pending {
		return Decision{}, ErrState
	}
	definitions := make([]TaskDefinition, len(graph.Nodes))
	byID := make(map[uuid.UUID]int, len(graph.Nodes))
	states := make([]string, len(graph.Nodes))
	for i, task := range graph.Nodes {
		if _, exists := byID[task.ID]; exists {
			return Decision{}, ErrState
		}
		byID[task.ID] = i
		definitions[i] = TaskDefinition{Key: task.Key, Kind: task.Kind}
		switch task.Status {
		case Pending, Ready, Running, Succeeded, Failed, Blocked, Cancelled:
		default:
			return Decision{}, ErrState
		}
		states[i] = task.Status
	}
	for _, edge := range graph.Edges {
		task, exists := byID[edge.TaskID]
		predecessor, predecessorExists := byID[edge.DependsOnTaskID]
		if !exists || !predecessorExists {
			return Decision{}, ErrState
		}
		definitions[task].DependsOn = append(definitions[task].DependsOn, graph.Nodes[predecessor].Key)
	}
	order, err := ValidateGraph(definitions)
	if err != nil {
		return Decision{}, ErrState
	}
	indexes := make(map[string]int, len(definitions))
	for i, task := range definitions {
		indexes[task.Key] = i
	}
	for _, i := range order {
		if states[i] != Pending && states[i] != Ready {
			continue
		}
		allSucceeded, failed := true, false
		for _, key := range definitions[i].DependsOn {
			state := states[indexes[key]]
			allSucceeded = allSucceeded && state == Succeeded
			failed = failed || state == Failed || state == Blocked || state == Cancelled
		}
		next := states[i]
		if failed {
			next = Blocked
		} else if states[i] == Pending && allSucceeded {
			next = Ready
		}
		if next != states[i] {
			if !TaskTransition(states[i], next) {
				return Decision{}, ErrState
			}
			decision.Transitions = append(decision.Transitions, Transition{graph.Nodes[i].ID, states[i], next})
			states[i] = next
		}
	}
	allSucceeded, unfinished := true, false
	for _, state := range states {
		allSucceeded = allSucceeded && state == Succeeded
		unfinished = unfinished || state == Pending || state == Ready || state == Running
	}
	if allSucceeded {
		decision.Status = Succeeded
	} else if !unfinished {
		decision.Status = Failed
	}
	return decision, nil
}
