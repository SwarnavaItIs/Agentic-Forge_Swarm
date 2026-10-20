package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"forgeswarm/internal/execution"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	RecoveryPolicy                                                execution.Policy
	ExecutionLimits                                               execution.Limits
	HTTPAddr                                                      string
	DatabaseURL                                                   string
	RequestTimeout, ReadyTimeout, ConnectTimeout, ShutdownTimeout time.Duration
	ReadHeaderTimeout, ReadTimeout, WriteTimeout, IdleTimeout     time.Duration
	MaxConns, MinConns                                            int32
	SchedulerInterval, SchedulerScanTimeout                       time.Duration
	SchedulerBatchSize                                            int
}

func Load() (Config, error) { return Parse(os.LookupEnv) }

func Parse(env func(string) (string, bool)) (Config, error) {
	c := Config{HTTPAddr: "127.0.0.1:8080", DatabaseURL: "postgres://forgeswarm:forgeswarm_dev@127.0.0.1:54329/forgeswarm?sslmode=disable", MaxConns: 10, SchedulerBatchSize: 16}
	var recoveryErr error
	c.RecoveryPolicy, c.ExecutionLimits, recoveryErr = RecoveryParse(env)
	if recoveryErr != nil {
		return Config{}, recoveryErr
	}
	for key, dest := range map[string]*string{"HTTP_ADDR": &c.HTTPAddr, "DATABASE_URL": &c.DatabaseURL} {
		if value, ok := env(key); ok {
			if value == "" {
				return Config{}, fmt.Errorf("%s must not be empty", key)
			}
			*dest = value
		}
	}
	host, port, err := net.SplitHostPort(c.HTTPAddr)
	if err != nil || host == "" || port == "" {
		return Config{}, errors.New("HTTP_ADDR must contain an explicit host and port")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return Config{}, errors.New("HTTP_ADDR port must be 1-65535")
	}
	defaults := []struct {
		key   string
		dest  *time.Duration
		value time.Duration
	}{
		{"REQUEST_TIMEOUT", &c.RequestTimeout, 5 * time.Second}, {"READY_TIMEOUT", &c.ReadyTimeout, time.Second},
		{"CONNECT_TIMEOUT", &c.ConnectTimeout, 2 * time.Second}, {"SHUTDOWN_TIMEOUT", &c.ShutdownTimeout, 10 * time.Second},
		{"SCHEDULER_INTERVAL", &c.SchedulerInterval, time.Second}, {"SCHEDULER_SCAN_TIMEOUT", &c.SchedulerScanTimeout, 5 * time.Second},
		{"READ_HEADER_TIMEOUT", &c.ReadHeaderTimeout, 2 * time.Second}, {"READ_TIMEOUT", &c.ReadTimeout, 10 * time.Second},
		{"WRITE_TIMEOUT", &c.WriteTimeout, 15 * time.Second}, {"IDLE_TIMEOUT", &c.IdleTimeout, time.Minute},
	}
	for _, entry := range defaults {
		*entry.dest = entry.value
		if value, ok := env(entry.key); ok {
			parsed, err := time.ParseDuration(value)
			if err != nil || parsed <= 0 || parsed > 5*time.Minute {
				return Config{}, fmt.Errorf("%s must be a duration between 0 and 5m", entry.key)
			}
			*entry.dest = parsed
		}
	}
	for key, dest := range map[string]*int32{"DB_MAX_CONNS": &c.MaxConns, "DB_MIN_CONNS": &c.MinConns} {
		if value, ok := env(key); ok {
			n, err := strconv.ParseInt(value, 10, 32)
			if err != nil || n < 0 || n > 100 {
				return Config{}, fmt.Errorf("%s must be an integer 0-100", key)
			}
			*dest = int32(n)
		}
	}
	if value, ok := env("SCHEDULER_BATCH_SIZE"); ok {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 100 {
			return Config{}, errors.New("SCHEDULER_BATCH_SIZE must be 1-100")
		}
		c.SchedulerBatchSize = n
	}
	if c.SchedulerInterval < 50*time.Millisecond || c.SchedulerInterval > time.Minute || c.SchedulerScanTimeout < 50*time.Millisecond || c.SchedulerScanTimeout > 30*time.Second {
		return Config{}, errors.New("scheduler interval must be 50ms-1m and scan timeout 50ms-30s")
	}
	if c.MaxConns < 1 || c.MinConns > c.MaxConns {
		return Config{}, errors.New("pool requires 0 <= min <= max and max >= 1")
	}
	if c.WriteTimeout <= c.RequestTimeout || c.ReadHeaderTimeout > c.ReadTimeout || c.ReadyTimeout > c.RequestTimeout {
		return Config{}, errors.New("timeouts require write > request >= ready and read >= read-header")
	}
	if _, err := c.PoolConfig(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) PoolConfig() (*pgxpool.Config, error) {
	pool, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, errors.New("DATABASE_URL is invalid")
	}
	pool.MaxConns, pool.MinConns = c.MaxConns, c.MinConns
	pool.MinIdleConns = 0
	pool.ConnConfig.ConnectTimeout = c.ConnectTimeout
	pool.HealthCheckPeriod = 5 * time.Second
	pool.PingTimeout = c.ReadyTimeout
	pool.MaxConnLifetime = 30 * time.Minute
	pool.MaxConnIdleTime = 5 * time.Minute
	return pool, nil
}
