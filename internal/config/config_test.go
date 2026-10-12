package config

import (
	"strings"
	"testing"
)

func TestDefaultsAndValidation(t *testing.T) {
	parse := func(values map[string]string) (Config, error) {
		return Parse(func(key string) (string, bool) { v, ok := values[key]; return v, ok })
	}
	c, err := parse(nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != "127.0.0.1:8080" || !strings.Contains(c.DatabaseURL, "127.0.0.1") {
		t.Fatal("non-loopback default")
	}
	p, err := c.PoolConfig()
	if err != nil || p.MaxConns != 10 || p.MinConns != 0 || p.ConnConfig.ConnectTimeout != c.ConnectTimeout {
		t.Fatal("pool defaults", err)
	}
	for _, values := range []map[string]string{
		{"HTTP_ADDR": ":8080"}, {"HTTP_ADDR": "localhost:0"}, {"HTTP_ADDR": "bad"}, {"DATABASE_URL": "postgres://user:TOPSECRET@localhost:bad/db"},
		{"SCHEDULER_BATCH_SIZE": "0"}, {"SCHEDULER_BATCH_SIZE": "101"}, {"SCHEDULER_INTERVAL": "1ms"}, {"SCHEDULER_SCAN_TIMEOUT": "31s"},
		{"REQUEST_TIMEOUT": "0s"}, {"READY_TIMEOUT": "6s"}, {"READ_HEADER_TIMEOUT": "11s"}, {"WRITE_TIMEOUT": "2s"},
		{"CONNECT_TIMEOUT": "garbage"}, {"IDLE_TIMEOUT": "6m"}, {"DB_MAX_CONNS": "0"}, {"DB_MIN_CONNS": "11"}, {"DB_MAX_CONNS": "200"}, {"DATABASE_URL": ""},
	} {
		_, err := parse(values)
		if err == nil {
			t.Fatalf("accepted %+v", values)
		}
		if strings.Contains(err.Error(), "TOPSECRET") {
			t.Fatal("config error leaked credential")
		}
	}
}
