//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"forgeswarm/internal/agent"
	"forgeswarm/internal/workflow"
	"forgeswarm/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/stdlib"
)

// Every previously supported schema is upgraded without rewriting an old
// receipt, normalization hash or terminal state. Fresh install uses setup().
func TestStage10AllSupportedUpgrades(t *testing.T) {
	for version := int64(1); version < migrations.Version; version++ {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			f := setupAt(t, version)
			in := input("legacy receipt")
			hash, err := workflow.HashForVersion(in, 1)
			if err != nil {
				t.Fatal(err)
			}
			id := uuid.New()
			var original workflow.Receipt
			err = f.p1.QueryRow(f.ctx, `INSERT INTO workflows(id,name,status,version,creation_key,request_hash,request_schema_version) VALUES($1,$2,'CANCELLED',2,'stage10-upgrade',$3,1) RETURNING id,created_at`, id, in.Name, hash[:]).Scan(&original.ID, &original.CreatedAt)
			if err != nil {
				t.Fatal(err)
			}
			for i, task := range in.Tasks {
				exec(t, f, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,$3,$4,'noop','CANCELLED')`, uuid.New(), id, task.Key, i)
			}
			exec(t, f, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{"task_count":2}'),($1,2,'workflow.cancelled','{"cancelled_task_count":2}')`, id)
			db := stdlib.OpenDB(*f.cfg2.ConnConfig)
			defer db.Close()
			if err = migrations.Up(f.ctx, db); err != nil {
				t.Fatal(err)
			}
			if err = f.s1.Ready(f.ctx); err != nil {
				t.Fatal(err)
			}
			replay, err := f.s1.Create(f.ctx, "stage10-upgrade", in)
			if err != nil || replay.ID != original.ID || !replay.CreatedAt.Equal(original.CreatedAt) {
				t.Fatal("receipt changed", err)
			}
			var stored []byte
			if err = f.p1.QueryRow(f.ctx, `SELECT request_hash FROM workflows WHERE id=$1`, id).Scan(&stored); err != nil || !bytes.Equal(hash[:], stored) {
				t.Fatal("hash changed", err)
			}
			if _, err = f.s1.Cancel(f.ctx, id); err != nil || len(history(t, f, id)) != 2 {
				t.Fatal("terminal history changed", err)
			}
		})
	}
	f := setup(t)
	if data, err := f.s1.Diagnostics(f.ctx); err != nil || data.Pricing != nil {
		t.Fatal("fresh diagnostics", err)
	}
	pricing := agent.Pricing{SchemaVersion: 1, Provider: "openai", Model: "configured-model", VerifiedAt: "2026-10-25", Source: "PRIVATE OPERATOR SOURCE", Input: 1, Output: 2, Maximum: 500000, Note: "PRIVATE OPERATOR NOTE"}
	if err := f.s1.ConfigureAgent(pricing); err != nil {
		t.Fatal(err)
	}
	data, err := f.s1.Diagnostics(f.ctx)
	if err != nil || data.Pricing["maximum_estimate_microusd"] != int64(500000) {
		t.Fatal("actual project policy unavailable", err)
	}
	body, _ := json.Marshal(data)
	if strings.Contains(string(body), "PRIVATE") {
		t.Fatal("operator strings leaked into diagnostics")
	}
}
