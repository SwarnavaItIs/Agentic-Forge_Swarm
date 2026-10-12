package config

import (
	"testing"
)

func TestDistributedTransportAndLimits(t *testing.T) {
	parse := func(m map[string]string) (Distributed, error) {
		return DistributedParse(func(k string) (string, bool) { v, ok := m[k]; return v, ok })
	}
	if _, err := parse(nil); err != nil {
		t.Fatal("secure defaults", err)
	}
	if _, err := parse(map[string]string{"INSECURE_LOCAL": "true", "NATS_URL": "nats://127.0.0.1:4222"}); err != nil {
		t.Fatal(err)
	}
	for _, m := range []map[string]string{{"INSECURE_LOCAL": "true", "RPC_ADDR": "0.0.0.0:9090"}, {"INSECURE_LOCAL": "true", "NATS_URL": "nats://remote:4222"}, {"NATS_URL": "nats://127.0.0.1:4222"}, {"ATTEMPT_LEASE": "0s"}, {"TOTAL_CAPACITY": "1"}, {"WORKER_CAPACITY": "3"}, {"RELAY_BATCH": "101"}, {"RPC_TIMEOUT": "3s"}, {"NATS_URL": "tls://user:TOPSECRET@host:4222"}} {
		if _, err := parse(m); err == nil {
			t.Fatal("bad settings accepted")
		}
	}
}
