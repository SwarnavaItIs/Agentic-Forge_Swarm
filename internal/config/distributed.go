package config

import (
	"encoding/json"
	"errors"
	"forgeswarm/internal/execution"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"
)

type Distributed struct {
	RPCAddr, BrokerURL, WorkerToken, KeysFile, CertFile, KeyFile, CAFile, NATSCA, NATSToken string
	InsecureLocal                                                                           bool
	Capacity, RelayBatch                                                                    int
	RPCTimeout, Shutdown, RelayLease, Poll                                                  time.Duration
	Limits                                                                                  execution.Limits
}

func DistributedLoad() (Distributed, error) { return DistributedParse(os.LookupEnv) }
func DistributedParse(env func(string) (string, bool)) (Distributed, error) {
	c := Distributed{RPCAddr: "127.0.0.1:9090", BrokerURL: "tls://127.0.0.1:4222", Capacity: 2, RelayBatch: 10, RPCTimeout: time.Second, Shutdown: 10 * time.Second, RelayLease: 5 * time.Second, Poll: 100 * time.Millisecond, Limits: execution.DefaultLimits()}
	for key, dst := range map[string]*string{"RPC_ADDR": &c.RPCAddr, "NATS_URL": &c.BrokerURL, "WORKER_TOKEN": &c.WorkerToken, "RPC_KEYS_FILE": &c.KeysFile, "RPC_TLS_CERT": &c.CertFile, "RPC_TLS_KEY": &c.KeyFile, "RPC_TLS_CA": &c.CAFile, "NATS_TLS_CA": &c.NATSCA, "NATS_TOKEN": &c.NATSToken} {
		if v, ok := env(key); ok {
			*dst = v
		}
	}
	if v, ok := env("INSECURE_LOCAL"); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return c, errors.New("INSECURE_LOCAL must be boolean")
		}
		c.InsecureLocal = b
	}
	for key, dst := range map[string]*int{"WORKER_CAPACITY": &c.Capacity, "TOTAL_CAPACITY": &c.Limits.TotalCapacity, "WORKER_MAX_CAPACITY": &c.Limits.WorkerCapacity, "RELAY_BATCH": &c.RelayBatch} {
		if v, ok := env(key); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return c, errors.New("invalid distributed integer setting")
			}
			*dst = n
		}
	}
	for key, dst := range map[string]*time.Duration{"ATTEMPT_LEASE": &c.Limits.Lease, "RPC_TIMEOUT": &c.RPCTimeout, "SHUTDOWN_TIMEOUT": &c.Shutdown, "RELAY_LEASE": &c.RelayLease, "DISPATCH_POLL": &c.Poll} {
		if v, ok := env(key); ok {
			n, err := time.ParseDuration(v)
			if err != nil {
				return c, errors.New("invalid distributed duration")
			}
			*dst = n
		}
	}
	if !c.Limits.Valid() || c.Capacity < 1 || c.Capacity > c.Limits.WorkerCapacity || c.RelayBatch < 1 || c.RelayBatch > 100 || c.RPCTimeout < 50*time.Millisecond || c.RPCTimeout > 2*time.Second || c.RPCTimeout >= c.Limits.Lease || c.Shutdown < time.Second || c.Shutdown > time.Minute || c.RelayLease < time.Second || c.RelayLease > time.Minute || c.Poll < 50*time.Millisecond || c.Poll > time.Second {
		return c, errors.New("invalid distributed bounds")
	}
	if _, _, err := net.SplitHostPort(c.RPCAddr); err != nil {
		return c, errors.New("invalid RPC_ADDR")
	}
	u, err := url.Parse(c.BrokerURL)
	if err != nil || u.User != nil || u.Port() == "" || (u.Scheme != "nats" && u.Scheme != "tls") {
		return c, errors.New("invalid NATS_URL; credentials belong in NATS_TOKEN")
	}
	if c.InsecureLocal && (!Loopback(c.RPCAddr) || u.Scheme == "nats" && !Loopback(u.Host)) {
		return c, errors.New("plaintext test mode requires loopback addresses")
	}
	if !c.InsecureLocal && u.Scheme != "tls" {
		return c, errors.New("NATS transport requires TLS or explicit loopback test mode")
	}
	return c, nil
}
func Loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
func (c Distributed) WorkerKeys() (map[string]string, error) {
	if c.KeysFile == "" {
		return nil, errors.New("RPC_KEYS_FILE is required")
	}
	data, err := os.ReadFile(c.KeysFile)
	if err != nil || len(data) > 16384 {
		return nil, errors.New("cannot read bounded worker credentials")
	}
	var keys map[string]string
	if json.Unmarshal(data, &keys) != nil || len(keys) < 1 || len(keys) > 100 {
		return nil, errors.New("invalid worker credential configuration")
	}
	seen := map[string]bool{}
	for id, key := range keys {
		if !regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`).MatchString(id) || len(key) < 32 || len(key) > 256 || seen[key] {
			return nil, errors.New("invalid worker credential configuration")
		}
		seen[key] = true
	}
	return keys, nil
}
