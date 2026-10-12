package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"forgeswarm/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"os"
)

func ServerOptions(c config.Distributed, keys map[string]string) ([]grpc.ServerOption, error) {
	opts := []grpc.ServerOption{grpc.UnaryInterceptor(Auth(keys, c.RPCTimeout)), grpc.MaxRecvMsgSize(64 << 10), grpc.MaxSendMsgSize(64 << 10), grpc.MaxConcurrentStreams(100)}
	if c.InsecureLocal {
		if !config.Loopback(c.RPCAddr) {
			return nil, errors.New("plaintext RPC must bind loopback")
		}
		return opts, nil
	}
	pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, errors.New("RPC TLS certificate/key required")
	}
	return append(opts, grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{pair}}))), nil
}
func Dial(c config.Distributed) (*grpc.ClientConn, error) {
	if len(c.WorkerToken) < 32 || len(c.WorkerToken) > 256 {
		return nil, errors.New("configured worker credential required")
	}
	var transport credentials.TransportCredentials
	if c.InsecureLocal {
		if !config.Loopback(c.RPCAddr) {
			return nil, errors.New("plaintext RPC must use loopback")
		}
		transport = insecure.NewCredentials()
	} else {
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if c.CAFile != "" {
			pem, err := os.ReadFile(c.CAFile)
			if err != nil || !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("invalid RPC root certificate")
			}
		}
		transport = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots})
	}
	attach := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoke(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.WorkerToken), method, req, reply, cc, opts...)
	}
	return grpc.NewClient(c.RPCAddr, grpc.WithTransportCredentials(transport), grpc.WithUnaryInterceptor(attach), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10), grpc.MaxCallSendMsgSize(64<<10)))
}
