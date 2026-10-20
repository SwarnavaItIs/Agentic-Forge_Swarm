package config

import "testing"

func TestRecoveryConfigurationBounds(t *testing.T) {
	for _, values := range []map[string]string{{"MAX_WORKFLOWS": "0"}, {"MAX_TASKS_PER_WORKFLOW": "101"}, {"MAX_QUEUED_TASKS": "0"}, {"MAX_READY_TASKS": "10001"}, {"WORKFLOW_TIMEOUT": "0s"}, {"TASK_TIMEOUT": "25h"}, {"RETRY_BACKOFF_BASE": "0s"}, {"RETRY_BACKOFF_CAP": "1ms"}, {"CAPABILITY_WAIT": "0s"}, {"TASK_MAX_ATTEMPTS": "11"}, {"READY_RESEND_INTERVAL": "1ms"}} {
		_, _, err := RecoveryParse(func(k string) (string, bool) { v, ok := values[k]; return v, ok })
		if err == nil {
			t.Fatal("bad setting accepted", values)
		}
	}
}
