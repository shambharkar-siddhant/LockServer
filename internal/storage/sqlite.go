package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	*sql.DB
}

type Config struct {
	Path            string
	BusyTimeout     time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("sqlite path is required")
	}
	if cfg.BusyTimeout <= 0 {
		cfg.BusyTimeout = 5 * time.Second
	}
	if cfg.MaxOpenConns <= 0 {
		cfg.MaxOpenConns = 10
	}
	if cfg.MaxIdleConns <= 0 {
		cfg.MaxIdleConns = 10
	}
	if cfg.ConnMaxLifetime <= 0 {
		cfg.ConnMaxLifetime = 30 * time.Minute
	}

	// modernc sqlite supports pragma via DSN.
	// Busy timeout helps under contention.
	dsn := fmt.Sprintf("file:%s?_busy_timeout=%d&_journal_mode=WAL&_synchronous=NORMAL&_foreign_keys=ON",
		cfg.Path,
		int(cfg.BusyTimeout.Milliseconds()),
	)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	// Health check
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	wdb := &DB{DB: db}

	// WAL mode + sane defaults
	if err := wdb.applyPragmas(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := wdb.Migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return wdb, nil
}

func (d *DB) applyPragmas(ctx context.Context) error {
	// WAL improves concurrency (readers don’t block writers as much).
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA temp_store=MEMORY;",
	}
	for _, p := range pragmas {
		if _, err := d.ExecContext(ctx, p); err != nil {
			return fmt.Errorf("apply pragma failed (%s): %w", p, err)
		}
	}
	return nil
}