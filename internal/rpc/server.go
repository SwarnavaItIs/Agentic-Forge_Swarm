package rpc

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type Store interface {
	RegisterWorker(context.Context, string, uuid.UUID, []string, int) (execution.Worker, error)
	HeartbeatWorker(context.Context, string, uuid.UUID, *bool) (execution.Worker, error)
	ClaimTask(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, int64) (execution.Attempt, error)
	RenewLease(context.Context, string, uuid.UUID, uuid.UUID, int64) (execution.Attempt, error)
	FinishAttempt(context.Context, string, uuid.UUID, uuid.UUID, int64, bool, []byte) (execution.Attempt, error)
	FailAttempt(context.Context, string, uuid.UUID, uuid.UUID, int64, string, []byte) (execution.Attempt, error)
	AuthorizeTool(context.Context, string, uuid.UUID, uuid.UUID, int64) (execution.Attempt, error)
	AcknowledgeStop(context.Context, string, uuid.UUID, uuid.UUID, int64) (execution.Attempt, error)
}
type identityKey struct{}

func Identity(ctx context.Context) string { v, _ := ctx.Value(identityKey{}).(string); return v }
func Auth(keys map[string]string, timeout time.Duration) grpc.UnaryServerInterceptor {
	hashes := map[string][32]byte{}
	for id, key := range keys {
		hashes[id] = sha256.Sum256([]byte("Bearer " + key))
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 || len(values[0]) > 300 {
			return nil, status.Error(codes.Unauthenticated, "worker_auth_required")
		}
		actual := sha256.Sum256([]byte(values[0]))
		id := ""
		for principal, want := range hashes {
			if subtle.ConstantTimeCompare(actual[:], want[:]) == 1 {
				id = principal
			}
		}
		if id == "" {
			return nil, status.Error(codes.Unauthenticated, "worker_auth_required")
		}
		ctx = context.WithValue(ctx, identityKey{}, id)
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return handler(ctx, req)
	}
}

type Server struct {
	workerv1.UnimplementedCoordinatorServer
	store Store
}

func New(store Store) *Server { return &Server{store: store} }
func safe(err error) error {
	if err == nil {
		return nil
	}
	code, msg := codes.Internal, "internal_error"
	switch {
	case errors.Is(err, workflow.ErrInvalid):
		code, msg = codes.InvalidArgument, "invalid_request"
	case errors.Is(err, workflow.ErrNotFound):
		code, msg = codes.NotFound, "not_found"
	case errors.Is(err, workflow.ErrUnavailable), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		code, msg = codes.Unavailable, "temporarily_unavailable"
	case errors.Is(err, execution.ErrClaimed):
		code, msg = codes.AlreadyExists, "already_claimed"
	case errors.Is(err, execution.ErrObsolete):
		code, msg = codes.FailedPrecondition, "obsolete_notification"
	case errors.Is(err, execution.ErrStale):
		code, msg = codes.FailedPrecondition, "stale_or_expired_attempt"
	case errors.Is(err, execution.ErrCapacity), errors.Is(err, execution.ErrDraining):
		code, msg = codes.ResourceExhausted, "capacity_or_draining"
	case errors.Is(err, execution.ErrWorker):
		code, msg = codes.PermissionDenied, "worker_instance_invalid"
	case errors.Is(err, execution.ErrCapability):
		code, msg = codes.Unimplemented, "capability_mismatch"
	case errors.Is(err, execution.ErrInvariant):
		code, msg = codes.FailedPrecondition, "ownership_consistency_violation"
	case errors.Is(err, execution.ErrOutcome):
		code, msg = codes.AlreadyExists, "conflicting_request_or_outcome"
	}
	return status.Error(code, msg)
}
func worker(w execution.Worker) *workerv1.Worker {
	return &workerv1.Worker{WorkerId: w.ID, InstanceId: w.Instance.String(), Capacity: int32(w.Capacity), Draining: w.Draining, HeartbeatAt: timestamppb.New(w.Heartbeat)}
}
func attempt(a execution.Attempt) *workerv1.Attempt {
	out := &workerv1.Attempt{Id: a.ID.String(), TaskId: a.TaskID.String(), AttemptNumber: int32(a.Number), Generation: a.Generation, State: a.State, Kind: a.Kind, DelayMs: int32(a.DelayMS), StartedAt: timestamppb.New(a.StartedAt), LeaseExpiresAt: timestamppb.New(a.LeaseExpiresAt), OutcomeDigest: a.Digest, ResultJson: a.Result}
	if a.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*a.FinishedAt)
	}
	out.DeadlineAt = timestamppb.New(a.DeadlineAt)
	out.StopRequested = a.StopRequested
	out.StopReason = a.StopReason
	out.CleanupRequired = a.CleanupRequired
	out.LastErrorClass = a.LastError
	out.ExecutionMetadataJson = a.SandboxExecution
	out.WorkflowId = a.WorkflowID.String()
	out.WorkerId = a.WorkerID
	out.InstanceId = a.Instance.String()
	if a.Sandbox != nil {
		out.SandboxInputJson, _ = json.Marshal(a.Sandbox)
	}
	return out
}
func id(text string) (uuid.UUID, error) {
	v, err := uuid.Parse(text)
	if err != nil || len(text) != 36 || v == uuid.Nil {
		return uuid.Nil, workflow.ErrInvalid
	}
	return v, nil
}
func (s *Server) RegisterWorker(ctx context.Context, r *workerv1.RegisterRequest) (*workerv1.Worker, error) {
	i, err := id(r.InstanceId)
	if err != nil {
		return nil, safe(err)
	}
	w, err := s.store.RegisterWorker(ctx, Identity(ctx), i, r.Capabilities, int(r.Capacity))
	if err != nil {
		return nil, safe(err)
	}
	return worker(w), nil
}
func (s *Server) HeartbeatWorker(ctx context.Context, r *workerv1.InstanceRequest) (*workerv1.Worker, error) {
	i, err := id(r.InstanceId)
	if err != nil {
		return nil, safe(err)
	}
	w, err := s.store.HeartbeatWorker(ctx, Identity(ctx), i, nil)
	if err != nil {
		return nil, safe(err)
	}
	return worker(w), nil
}
func (s *Server) SetDraining(ctx context.Context, r *workerv1.DrainRequest) (*workerv1.Worker, error) {
	i, err := id(r.InstanceId)
	if err != nil {
		return nil, safe(err)
	}
	w, err := s.store.HeartbeatWorker(ctx, Identity(ctx), i, &r.Draining)
	if err != nil {
		return nil, safe(err)
	}
	return worker(w), nil
}
func (s *Server) ClaimTask(ctx context.Context, r *workerv1.ClaimRequest) (*workerv1.Attempt, error) {
	i, err := id(r.InstanceId)
	if err != nil {
		return nil, safe(err)
	}
	t, err := id(r.TaskId)
	if err != nil {
		return nil, safe(err)
	}
	key, err := id(r.RequestId)
	if err != nil {
		return nil, safe(err)
	}
	a, err := s.store.ClaimTask(ctx, Identity(ctx), i, t, key, r.DispatchGeneration)
	if err != nil {
		return nil, safe(err)
	}
	return attempt(a), nil
}
func (s *Server) RenewLease(ctx context.Context, r *workerv1.AttemptRequest) (*workerv1.Attempt, error) {
	i, err := id(r.InstanceId)
	if err != nil {
		return nil, safe(err)
	}
	aID, err := id(r.AttemptId)
	if err != nil {
		return nil, safe(err)
	}
	a, err := s.store.RenewLease(ctx, Identity(ctx), i, aID, r.Generation)
	if err != nil {
		return nil, safe(err)
	}
	return attempt(a), nil
}
func (s *Server) finish(ctx context.Context, r *workerv1.OutcomeRequest, success bool) (*workerv1.Attempt, error) {
	i, err := id(r.InstanceId)
	if err != nil {
		return nil, safe(err)
	}
	aID, err := id(r.AttemptId)
	if err != nil {
		return nil, safe(err)
	}
	var a execution.Attempt
	if success {
		if r.FailureClass != "" {
			return nil, safe(workflow.ErrInvalid)
		}
		a, err = s.store.FinishAttempt(ctx, Identity(ctx), i, aID, r.Generation, true, r.ResultJson)
	} else {
		a, err = s.store.FailAttempt(ctx, Identity(ctx), i, aID, r.Generation, r.FailureClass, r.ResultJson)
	}
	if err != nil {
		return nil, safe(err)
	}
	return attempt(a), nil
}
func (s *Server) CompleteTask(ctx context.Context, r *workerv1.OutcomeRequest) (*workerv1.Attempt, error) {
	return s.finish(ctx, r, true)
}
func (s *Server) FailTask(ctx context.Context, r *workerv1.OutcomeRequest) (*workerv1.Attempt, error) {
	return s.finish(ctx, r, false)
}
func (s *Server) ownedAction(ctx context.Context, r *workerv1.AttemptRequest, stop bool) (*workerv1.Attempt, error) {
	i, err := id(r.InstanceId)
	if err != nil {
		return nil, safe(err)
	}
	aID, err := id(r.AttemptId)
	if err != nil {
		return nil, safe(err)
	}
	var a execution.Attempt
	if stop {
		a, err = s.store.AcknowledgeStop(ctx, Identity(ctx), i, aID, r.Generation)
	} else {
		a, err = s.store.AuthorizeTool(ctx, Identity(ctx), i, aID, r.Generation)
	}
	if err != nil {
		return nil, safe(err)
	}
	return attempt(a), nil
}
func (s *Server) AuthorizeTool(ctx context.Context, r *workerv1.AttemptRequest) (*workerv1.Attempt, error) {
	return s.ownedAction(ctx, r, false)
}
func (s *Server) AcknowledgeStop(ctx context.Context, r *workerv1.AttemptRequest) (*workerv1.Attempt, error) {
	return s.ownedAction(ctx, r, true)
}

type sandboxStore interface {
	AuthorizeSandbox(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, int64) (execution.Attempt, error)
	FinishSandbox(context.Context, string, uuid.UUID, uuid.UUID, uuid.UUID, int64, sandbox.Result) (execution.Attempt, error)
	SandboxCleanup(context.Context, string, uuid.UUID) ([]execution.Attempt, error)
}

func sandboxIDs(r *workerv1.SandboxRequest) (uuid.UUID, uuid.UUID, uuid.UUID, error) {
	if r == nil || r.Attempt == nil {
		return uuid.Nil, uuid.Nil, uuid.Nil, workflow.ErrInvalid
	}
	i, e := id(r.Attempt.InstanceId)
	if e != nil {
		return i, uuid.Nil, uuid.Nil, e
	}
	a, e := id(r.Attempt.AttemptId)
	if e != nil {
		return i, a, uuid.Nil, e
	}
	key, e := id(r.RequestId)
	return i, a, key, e
}
func (s *Server) AuthorizeSandbox(ctx context.Context, r *workerv1.SandboxRequest) (*workerv1.Attempt, error) {
	i, a, key, e := sandboxIDs(r)
	if e != nil {
		return nil, safe(e)
	}
	store, ok := s.store.(sandboxStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "sandbox_unavailable")
	}
	out, e := store.AuthorizeSandbox(ctx, Identity(ctx), i, a, key, r.Attempt.Generation)
	if e != nil {
		return nil, safe(e)
	}
	return attempt(out), nil
}
func (s *Server) CompleteSandbox(ctx context.Context, r *workerv1.SandboxOutcome) (*workerv1.Attempt, error) {
	i, a, key, e := sandboxIDs(r.Request)
	if e != nil {
		return nil, safe(e)
	}
	var result sandbox.Result
	if sandbox.Decode(r.CommandResultJson, &result, sandbox.MaxWire) != nil {
		return nil, safe(workflow.ErrInvalid)
	}
	store, ok := s.store.(sandboxStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "sandbox_unavailable")
	}
	out, e := store.FinishSandbox(ctx, Identity(ctx), i, a, key, r.Request.Attempt.Generation, result)
	if e != nil {
		return nil, safe(e)
	}
	return attempt(out), nil
}
func (s *Server) SandboxCleanup(ctx context.Context, r *workerv1.InstanceRequest) (*workerv1.CleanupPage, error) {
	i, e := id(r.InstanceId)
	if e != nil {
		return nil, safe(e)
	}
	store, ok := s.store.(sandboxStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "sandbox_unavailable")
	}
	items, e := store.SandboxCleanup(ctx, Identity(ctx), i)
	if e != nil {
		return nil, safe(e)
	}
	page := &workerv1.CleanupPage{}
	for _, a := range items {
		page.Attempts = append(page.Attempts, attempt(a))
	}
	return page, nil
}
