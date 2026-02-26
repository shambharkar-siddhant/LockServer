package storage

import (
	"context"
	"database/sql"
	"fmt"
)

func (d *DB) Migrate(ctx context.Context) error {
	if _, err := d.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  applied_at_ns INTEGER NOT NULL
);
`); err != nil {
		return err
	}

	const latest = 1

	cur, err := currentVersion(ctx, d.DB)
	if err != nil {
		return err
	}
	for v := cur + 1; v <= latest; v++ {
		if err := apply(ctx, d.DB, v); err != nil {
			return err
		}
	}
	return nil
}

func currentVersion(ctx context.Context, db *sql.DB) (int, error) {
	var v sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations;`).Scan(&v); err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, nil
	}
	return int(v.Int64), nil
}

func apply(ctx context.Context, db *sql.DB, version int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	switch version {
	case 1:
		if _, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS tokens (
  lock_name TEXT PRIMARY KEY,
  last_token INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS locks (
  lock_name TEXT PRIMARY KEY,
  owner_id TEXT,
  lease_id TEXT,
  fencing_token INTEGER NOT NULL,
  lease_expiry_ns INTEGER NOT NULL,
  version INTEGER NOT NULL,
  created_at_ns INTEGER NOT NULL,
  updated_at_ns INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_locks_expiry ON locks(lease_expiry_ns);
`); err != nil {
			return fmt.Errorf("migration v1 failed: %w", err)
		}
	default:
		return fmt.Errorf("unknown migration version: %d", version)
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version, applied_at_ns) VALUES(?, strftime('%s','now')*1000000000);`, version); err != nil {
		return err
	}
	return tx.Commit()
}