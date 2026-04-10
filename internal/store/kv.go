package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const dbVersionKey = "db_version"

func (s *Store) ensureKVTable(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
`)
	if err != nil {
		return fmt.Errorf("ensure kv table: %w", err)
	}
	return nil
}

func (s *Store) getKV(ctx context.Context, key string) (string, bool, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, key).Scan(&value)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("get kv %q: %w", key, err)
	}
	return value, true, nil
}

func (s *Store) setSchemaVersion(ctx context.Context, version int) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO kv(key, value, updated_at)
VALUES(?, ?, ?)
ON CONFLICT(key) DO UPDATE SET
    value = excluded.value,
    updated_at = excluded.updated_at
`, dbVersionKey, strconv.Itoa(version), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("set schema version: %w", err)
	}
	return nil
}

func parseSchemaVersion(value string) (int, error) {
	version, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse schema version %q: %w", value, err)
	}
	if version < 0 {
		return 0, fmt.Errorf("invalid schema version %d", version)
	}
	return version, nil
}
