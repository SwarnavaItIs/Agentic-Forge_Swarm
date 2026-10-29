// launch loads only explicitly mounted service secrets, supervises one trusted
// binary and implements bounded dependency probes. Repository code cannot call
// it: the immutable execution image contains only the sandbox helper.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

var roles = []string{"api", "scheduler", "coordinator", "relay", "worker", "publisher", "migrate", "github-test-server", "seed-assets", "operator-init", "tlsgen"}

func loadSecrets() error {
	for _, key := range []string{"DATABASE_URL", "WORKER_TOKEN", "NATS_TOKEN", "AGENT_RUNTIME_TOKEN"} {
		path := os.Getenv(key + "_FILE")
		if path == "" {
			continue
		}
		if os.Getenv(key) != "" {
			return errors.New("ambiguous secret configuration")
		}
		file, err := os.Open(path)
		if err != nil {
			return errors.New("secret unavailable")
		}
		data, err := io.ReadAll(io.LimitReader(file, 4097))
		file.Close()
		value := strings.TrimSuffix(string(data), "\n")
		if err != nil || len(data) > 4096 || value == "" || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("invalid mounted secret")
		}
		os.Setenv(key, value)
	}
	return nil
}
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "trusted service or dependency probe failed")
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("role required")
	}
	if err := loadSecrets(); err != nil {
		return err
	}
	if args[0] == "health" {
		if len(args) != 2 {
			return errors.New("probe required")
		}
		return health(args[1])
	}
	if !slices.Contains(roles, args[0]) {
		return errors.New("unsupported role")
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(filepath.Join(filepath.Dir(exe), args[0]), args[1:]...)
	child.Env = os.Environ()
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.Stdin = os.Stdin
	if err = child.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case err = <-done:
		return err
	case sig := <-signals:
		_ = child.Process.Signal(sig)
	}
	select {
	case err = <-done:
		return err
	case <-time.After(20 * time.Second):
		_ = child.Process.Kill()
		return <-done
	}
}
func roots(path string) (*x509.CertPool, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(body) {
		return nil, errors.New("invalid CA")
	}
	return pool, nil
}
func health(kind string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	switch kind {
	case "database":
		conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
		if err != nil {
			return err
		}
		defer conn.Close(ctx)
		return conn.Ping(ctx)
	case "http":
		addr := os.Getenv("HTTP_ADDR")
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}
		endpoint := "http://127.0.0.1:" + port + "/health/ready"
		transport := &http.Transport{}
		if os.Getenv("HTTP_TLS_CERT") != "" {
			ca, e := roots(os.Getenv("HTTP_TLS_CA"))
			if e != nil {
				return e
			}
			transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca}
			endpoint = "https://127.0.0.1:" + port + "/health/ready"
		}
		defer transport.CloseIdleConnections()
		request, _ := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
		if origin := os.Getenv("UI_ORIGIN"); origin != "" {
			parsed, e := url.Parse(origin)
			if e != nil {
				return e
			}
			request.Host = parsed.Host
		}
		response, e := (&http.Client{Transport: transport, Timeout: 2 * time.Second}).Do(request)
		if e != nil {
			return e
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return errors.New("not ready")
		}
		return nil
	case "rpc":
		ca, err := roots(os.Getenv("RPC_TLS_CA"))
		if err != nil {
			return err
		}
		dialer := tls.Dialer{Config: &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca}}
		conn, err := dialer.DialContext(ctx, "tcp", os.Getenv("RPC_ADDR"))
		if err != nil {
			return err
		}
		return conn.Close()
	}
	return errors.New("unsupported probe")
}
