package postgres

import (
	"context"
	"errors"
	"testing"

	"forgeswarm/internal/workflow"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestErrorClassification(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, &pgconn.PgError{Code: "57014"}, &pgconn.PgError{Code: "57P01"}, &pgconn.PgError{Code: "08006"}, &pgconn.PgError{Code: "42P01"}} {
		if !errors.Is(classify(err), workflow.ErrUnavailable) {
			t.Fatal(err)
		}
	}
	if errors.Is(classify(&pgconn.PgError{Code: "23514"}), workflow.ErrUnavailable) {
		t.Fatal("check failure is not a transient outage")
	}
}
