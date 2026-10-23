package migrations

import (
	"context"
	"database/sql"
	"embed"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

const Version int64 = 8

//go:embed *.sql
var Files embed.FS

// Provider is used only by the explicit migration command and disposable tests.
// The API never invokes it. A PostgreSQL session lock serializes migration runners.
func Provider(db *sql.DB) (*goose.Provider, error) {
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 10), lock.WithUnlockTimeout(1, 3))
	if err != nil {
		return nil, err
	}
	return goose.NewProvider(goose.DialectPostgres, db, Files, goose.WithSessionLocker(locker))
}

func Up(ctx context.Context, db *sql.DB) error {
	provider, err := Provider(db)
	if err != nil {
		return err
	}
	_, err = provider.Up(ctx)
	return err
}
