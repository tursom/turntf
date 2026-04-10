package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestInitMigratesLegacyDatabaseWithRoleAndVersion(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(context.Background(), `
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    push_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
INSERT INTO users(username, password_hash, push_id, created_at) VALUES
    ('alice', 'hash1', 'push_alice', '`+now+`'),
    ('bob', 'hash2', 'push_bob', '`+now+`');

CREATE TABLE user_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX idx_user_sessions_expires_at ON user_sessions(expires_at);

CREATE TABLE service_api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL UNIQUE,
    token_prefix TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_used_at TEXT
);

CREATE TABLE notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    push_id TEXT NOT NULL,
    sender TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX idx_notifications_user_id_created_at ON notifications(user_id, created_at DESC);
`); err != nil {
		t.Fatalf("seed legacy schema: %v", err)
	}

	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	hasRole, err := st.columnExists(context.Background(), "users", "role")
	if err != nil {
		t.Fatalf("check role column: %v", err)
	}
	if !hasRole {
		t.Fatalf("expected users.role column to exist after migration")
	}

	alice, err := st.GetUserByUsername(context.Background(), "alice")
	if err != nil {
		t.Fatalf("get alice: %v", err)
	}
	if alice.Role != UserRoleAdmin {
		t.Fatalf("expected alice to be promoted to admin, got %q", alice.Role)
	}

	bob, err := st.GetUserByUsername(context.Background(), "bob")
	if err != nil {
		t.Fatalf("get bob: %v", err)
	}
	if bob.Role != UserRoleUser {
		t.Fatalf("expected bob to remain user, got %q", bob.Role)
	}

	version, ok, err := st.getKV(context.Background(), dbVersionKey)
	if err != nil {
		t.Fatalf("get db version: %v", err)
	}
	if !ok {
		t.Fatalf("expected db version to be stored in kv")
	}
	if version != "2" {
		t.Fatalf("unexpected db version: %s", version)
	}
}
