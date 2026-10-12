package rpc

import (
	"context"
	"forgeswarm/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"strings"
	"testing"
	"time"
)

func TestAuthenticationIdentityAndPrivateTransport(t *testing.T) {
	token := strings.Repeat("a", 32)
	intercept := Auth(map[string]string{"worker_a": token}, time.Second)
	for _, values := range [][]string{nil, {"Bearer wrong"}, {"Bearer " + token, "Bearer " + token}} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs())
		if values != nil {
			ctx = metadata.NewIncomingContext(ctx, metadata.MD{"authorization": values})
		}
		_, err := intercept(ctx, nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { t.Fatal("unauthenticated handler called"); return nil, nil })
		if status.Code(err) != codes.Unauthenticated {
			t.Fatal(err)
		}
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	_, err := intercept(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		if Identity(ctx) != "worker_a" {
			t.Fatal("identity not from auth")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("RPC lacks deadline")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Distributed{RPCAddr: "0.0.0.0:9090", InsecureLocal: true, WorkerToken: token}
	if _, err := ServerOptions(cfg, map[string]string{"worker_a": token}); err == nil {
		t.Fatal("public plaintext server allowed")
	}
	if _, err := Dial(cfg); err == nil {
		t.Fatal("public plaintext client allowed")
	}
	cfg.InsecureLocal = false
	if _, err := ServerOptions(cfg, map[string]string{"worker_a": token}); err == nil {
		t.Fatal("nonlocal TLS configuration not required")
	}
}
