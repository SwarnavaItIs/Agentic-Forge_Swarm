package agent

import (
	"context"
	"encoding/json"
	"forgeswarm/api/agent/v1"
	"forgeswarm/internal/sandbox"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"net"
)

// Initial deployment is a local, separately confined runtime container. The
// authenticated plaintext listener is published only on host loopback. Remote
// runtimes require a future verified TLS deployment rather than accepting URLs.
func DialRuntime(addr, token string, insecureLocal bool) (*grpc.ClientConn, error) {
	if !insecureLocal || !loopback(addr) || len(token) < 32 || len(token) > 256 {
		return nil, sandbox.ErrPolicy
	}
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(32<<10), grpc.MaxCallSendMsgSize(32<<10)), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), method, req, reply, cc, opts...)
	}))
}
func NextAction(client agentv1.RuntimeClient) func(context.Context, Next) (Reply, error) {
	return func(ctx context.Context, n Next) (Reply, error) {
		var r Reply
		b, _ := json.Marshal(n)
		wire, err := client.NextAction(ctx, &agentv1.NextRequest{ContextJson: b})
		if err != nil {
			return r, err
		}
		if sandbox.Decode(wire.ResponseJson, &r, 32<<10) != nil || !ValidReply(r) {
			return r, sandbox.ErrPolicy
		}
		return r, nil
	}
}

func loopback(addr string) bool {
	host, _, e := net.SplitHostPort(addr)
	ip := net.ParseIP(host)
	return e == nil && ip != nil && ip.IsLoopback()
}
