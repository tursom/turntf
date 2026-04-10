package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

const (
	UserRoleAdmin = "admin"
	UserRoleUser  = "user"

	currentDBVersion = 2
)

var (
	ErrConflict            = errors.New("conflict")
	ErrNotFound            = errors.New("not found")
	ErrInvalidCredentials  = errors.New("invalid credentials")
	ErrUnauthorizedService = errors.New("unauthorized service")
	ErrInvalidRole         = errors.New("invalid role")
	ErrRegistrationClosed  = errors.New("registration closed")
)

type Store struct {
	db *sql.DB
}

type User struct {
	ID        int64     `json:"id"`
	Username  string    `json:"username"`
	PushID    string    `json:"push_id"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

type UserCredentials struct {
	User
	PasswordHash string
}

type Notification struct {
	ID        int64     `json:"id"`
	PushID    string    `json:"push_id"`
	Sender    string    `json:"sender"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	Metadata  string    `json:"metadata,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type ServiceKey struct {
	ID         int64      `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

type resultExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func Open(dbPath string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite3: %w", err)
	}

	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Init(ctx context.Context) error {
	if err := s.ensureKVTable(ctx); err != nil {
		return fmt.Errorf("init kv table: %w", err)
	}

	version, err := s.schemaVersion(ctx)
	if err != nil {
		return fmt.Errorf("load schema version: %w", err)
	}

	for version < currentDBVersion {
		nextVersion := version + 1
		switch nextVersion {
		case 1:
			err = s.migrateToV1(ctx)
		case 2:
			err = s.migrateToV2(ctx)
		default:
			return fmt.Errorf("unsupported migration target version %d", nextVersion)
		}
		if err != nil {
			return fmt.Errorf("migrate to version %d: %w", nextVersion, err)
		}
		if err := s.setSchemaVersion(ctx, nextVersion); err != nil {
			return fmt.Errorf("persist schema version %d: %w", nextVersion, err)
		}
		version = nextVersion
	}

	if err := s.setSchemaVersion(ctx, version); err != nil {
		return fmt.Errorf("finalize schema version %d: %w", version, err)
	}

	return nil
}

func (s *Store) migrateToV1(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    push_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at ON user_sessions(expires_at);

CREATE TABLE IF NOT EXISTS service_api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    token_hash TEXT NOT NULL UNIQUE,
    token_prefix TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_used_at TEXT
);

CREATE TABLE IF NOT EXISTS notifications (
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
CREATE INDEX IF NOT EXISTS idx_notifications_user_id_created_at ON notifications(user_id, created_at DESC);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init v1 schema: %w", err)
	}
	return nil
}

func (s *Store) migrateToV2(ctx context.Context) error {
	hasRole, err := s.columnExists(ctx, "users", "role")
	if err != nil {
		return err
	}
	if !hasRole {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'`); err != nil {
			return fmt.Errorf("add users.role column: %w", err)
		}
	}

	if _, err := s.db.ExecContext(ctx, `UPDATE users SET role = ? WHERE role IS NULL OR TRIM(role) = ''`, UserRoleUser); err != nil {
		return fmt.Errorf("backfill user roles: %w", err)
	}

	adminCount, err := s.CountAdmins(ctx)
	if err != nil {
		return err
	}
	if adminCount > 0 {
		return nil
	}

	totalUsers, err := s.CountUsers(ctx)
	if err != nil {
		return err
	}
	if totalUsers == 0 {
		return nil
	}

	if _, err := s.db.ExecContext(ctx, `
UPDATE users
SET role = ?
WHERE id = (SELECT id FROM users ORDER BY id ASC LIMIT 1)
`, UserRoleAdmin); err != nil {
		return fmt.Errorf("promote bootstrap admin: %w", err)
	}
	return nil
}

func NormalizeUserRole(role string) string {
	return strings.ToLower(strings.TrimSpace(role))
}

func IsValidUserRole(role string) bool {
	switch NormalizeUserRole(role) {
	case UserRoleAdmin, UserRoleUser:
		return true
	default:
		return false
	}
}

func (s *Store) CountUsers(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return 0, fmt.Errorf("count users: %w", err)
	}
	return count, nil
}

func (s *Store) CountAdmins(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE role = ?`, UserRoleAdmin).Scan(&count); err != nil {
		return 0, fmt.Errorf("count admins: %w", err)
	}
	return count, nil
}

func (s *Store) CreateInitialAdmin(ctx context.Context, username, passwordHash string) (User, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, fmt.Errorf("begin bootstrap admin transaction: %w", err)
	}
	defer tx.Rollback()

	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		return User{}, fmt.Errorf("count users for bootstrap admin: %w", err)
	}
	if count > 0 {
		return User{}, ErrRegistrationClosed
	}

	user, err := insertUser(ctx, tx, username, passwordHash, UserRoleAdmin, time.Now().UTC())
	if err != nil {
		return User{}, err
	}
	if err := tx.Commit(); err != nil {
		return User{}, fmt.Errorf("commit bootstrap admin transaction: %w", err)
	}
	return user, nil
}

func (s *Store) CreateUser(ctx context.Context, username, passwordHash, role string) (User, error) {
	return insertUser(ctx, s.db, username, passwordHash, role, time.Now().UTC())
}

func (s *Store) GetUserByUsername(ctx context.Context, username string) (UserCredentials, error) {
	var rec UserCredentials
	var createdAt string
	err := s.db.QueryRowContext(ctx, `
SELECT id, username, push_id, role, password_hash, created_at
FROM users
WHERE username = ?
`, strings.TrimSpace(username)).Scan(&rec.ID, &rec.Username, &rec.PushID, &rec.Role, &rec.PasswordHash, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return UserCredentials{}, ErrNotFound
		}
		return UserCredentials{}, fmt.Errorf("get user by username: %w", err)
	}

	rec.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return UserCredentials{}, fmt.Errorf("parse user created_at: %w", err)
	}
	return rec, nil
}

func (s *Store) GetUserByID(ctx context.Context, userID int64) (User, error) {
	var user User
	var createdAt string
	err := s.db.QueryRowContext(ctx, `
SELECT id, username, push_id, role, created_at
FROM users
WHERE id = ?
`, userID).Scan(&user.ID, &user.Username, &user.PushID, &user.Role, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("get user by id: %w", err)
	}

	user.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return User{}, fmt.Errorf("parse user created_at: %w", err)
	}
	return user, nil
}

func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, username, push_id, role, created_at
FROM users
ORDER BY id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		var createdAt string
		if err := rows.Scan(&user.ID, &user.Username, &user.PushID, &user.Role, &createdAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		user.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse user created_at: %w", err)
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate users: %w", err)
	}
	return users, nil
}

func (s *Store) GetUserByPushID(ctx context.Context, pushID string) (User, error) {
	var user User
	var createdAt string
	err := s.db.QueryRowContext(ctx, `
SELECT id, username, push_id, role, created_at
FROM users
WHERE push_id = ?
`, pushID).Scan(&user.ID, &user.Username, &user.PushID, &user.Role, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("get user by push id: %w", err)
	}

	user.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return User{}, fmt.Errorf("parse user created_at: %w", err)
	}
	return user, nil
}

func (s *Store) UpdateUserRole(ctx context.Context, userID int64, role string) error {
	normalizedRole, err := normalizeUserRole(role)
	if err != nil {
		return err
	}

	result, err := s.db.ExecContext(ctx, `UPDATE users SET role = ? WHERE id = ?`, normalizedRole, userID)
	if err != nil {
		return fmt.Errorf("update user role: %w", err)
	}
	return assertRowsAffected(result, "update user role")
}

func (s *Store) UpdateUserPasswordHash(ctx context.Context, userID int64, passwordHash string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE users SET password_hash = ? WHERE id = ?`, passwordHash, userID)
	if err != nil {
		return fmt.Errorf("update user password hash: %w", err)
	}
	return assertRowsAffected(result, "update user password hash")
}

func (s *Store) DeleteUser(ctx context.Context, userID int64) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM users WHERE id = ?`, userID)
	if err != nil {
		return fmt.Errorf("delete user: %w", err)
	}
	return assertRowsAffected(result, "delete user")
}

func (s *Store) CreateSession(ctx context.Context, userID int64, tokenHash string, expiresAt time.Time) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO user_sessions(user_id, token_hash, expires_at, created_at)
VALUES(?, ?, ?, ?)
`, userID, tokenHash, expiresAt.UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

func (s *Store) GetUserBySessionTokenHash(ctx context.Context, tokenHash string) (User, error) {
	var user User
	var createdAt string
	var expiresAt string
	err := s.db.QueryRowContext(ctx, `
SELECT u.id, u.username, u.push_id, u.role, u.created_at, us.expires_at
FROM user_sessions us
JOIN users u ON u.id = us.user_id
WHERE us.token_hash = ?
`, tokenHash).Scan(&user.ID, &user.Username, &user.PushID, &user.Role, &createdAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, fmt.Errorf("get user by session token: %w", err)
	}

	user.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return User{}, fmt.Errorf("parse user created_at: %w", err)
	}

	expiresTime, err := time.Parse(time.RFC3339Nano, expiresAt)
	if err != nil {
		return User{}, fmt.Errorf("parse session expires_at: %w", err)
	}
	if time.Now().UTC().After(expiresTime) {
		return User{}, ErrNotFound
	}

	return user, nil
}

func (s *Store) DeleteExpiredSessions(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM user_sessions WHERE expires_at <= ?`, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("delete expired sessions: %w", err)
	}
	return nil
}

func (s *Store) CreateServiceKey(ctx context.Context, name, tokenHash, prefix string) (ServiceKey, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
INSERT INTO service_api_keys(name, token_hash, token_prefix, created_at)
VALUES(?, ?, ?, ?)
`, name, tokenHash, prefix, now.Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueErr(err) {
			return ServiceKey{}, ErrConflict
		}
		return ServiceKey{}, fmt.Errorf("create service key: %w", err)
	}

	id, _ := result.LastInsertId()
	return ServiceKey{ID: id, Name: name, Prefix: prefix, CreatedAt: now}, nil
}

func (s *Store) GetServiceKeyByTokenHash(ctx context.Context, tokenHash string) (ServiceKey, error) {
	var key ServiceKey
	var createdAt string
	var lastUsed sql.NullString
	err := s.db.QueryRowContext(ctx, `
SELECT id, name, token_prefix, created_at, last_used_at
FROM service_api_keys
WHERE token_hash = ?
`, tokenHash).Scan(&key.ID, &key.Name, &key.Prefix, &createdAt, &lastUsed)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ServiceKey{}, ErrUnauthorizedService
		}
		return ServiceKey{}, fmt.Errorf("get service key: %w", err)
	}

	key.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return ServiceKey{}, fmt.Errorf("parse service key created_at: %w", err)
	}
	if lastUsed.Valid {
		parsed, err := time.Parse(time.RFC3339Nano, lastUsed.String)
		if err != nil {
			return ServiceKey{}, fmt.Errorf("parse service key last_used_at: %w", err)
		}
		key.LastUsedAt = &parsed
	}
	return key, nil
}

func (s *Store) TouchServiceKeyUsage(ctx context.Context, keyID int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE service_api_keys SET last_used_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339Nano), keyID)
	if err != nil {
		return fmt.Errorf("touch service key usage: %w", err)
	}
	return nil
}

func (s *Store) ListServiceKeys(ctx context.Context) ([]ServiceKey, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, name, token_prefix, created_at, last_used_at
FROM service_api_keys
ORDER BY id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list service keys: %w", err)
	}
	defer rows.Close()

	var keys []ServiceKey
	for rows.Next() {
		var key ServiceKey
		var createdAt string
		var lastUsed sql.NullString
		if err := rows.Scan(&key.ID, &key.Name, &key.Prefix, &createdAt, &lastUsed); err != nil {
			return nil, fmt.Errorf("scan service key: %w", err)
		}
		key.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse service key created_at: %w", err)
		}
		if lastUsed.Valid {
			parsed, err := time.Parse(time.RFC3339Nano, lastUsed.String)
			if err != nil {
				return nil, fmt.Errorf("parse service key last_used_at: %w", err)
			}
			key.LastUsedAt = &parsed
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate service keys: %w", err)
	}
	return keys, nil
}

func (s *Store) CreateNotification(ctx context.Context, userID int64, pushID, sender, title, body, metadata string) (Notification, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
INSERT INTO notifications(user_id, push_id, sender, title, body, metadata, created_at)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, userID, pushID, sender, title, body, nullIfEmpty(metadata), now.Format(time.RFC3339Nano))
	if err != nil {
		return Notification{}, fmt.Errorf("create notification: %w", err)
	}

	id, _ := result.LastInsertId()
	return Notification{
		ID:        id,
		PushID:    pushID,
		Sender:    sender,
		Title:     title,
		Body:      body,
		Metadata:  metadata,
		CreatedAt: now,
	}, nil
}

func (s *Store) ListNotificationsByUser(ctx context.Context, userID int64, limit int) ([]Notification, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT id, push_id, sender, title, body, COALESCE(metadata, ''), created_at
FROM notifications
WHERE user_id = ?
ORDER BY created_at DESC
LIMIT ?
`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("list notifications: %w", err)
	}
	defer rows.Close()

	var notifications []Notification
	for rows.Next() {
		var item Notification
		var createdAt string
		if err := rows.Scan(&item.ID, &item.PushID, &item.Sender, &item.Title, &item.Body, &item.Metadata, &createdAt); err != nil {
			return nil, fmt.Errorf("scan notification: %w", err)
		}
		item.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse notification created_at: %w", err)
		}
		notifications = append(notifications, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate notifications: %w", err)
	}
	return notifications, nil
}

func insertUser(ctx context.Context, execer resultExecer, username, passwordHash, role string, now time.Time) (User, error) {
	normalizedRole, err := normalizeUserRole(role)
	if err != nil {
		return User{}, err
	}

	user := User{
		Username:  strings.TrimSpace(username),
		PushID:    "push_" + uuid.NewString(),
		Role:      normalizedRole,
		CreatedAt: now,
	}

	result, err := execer.ExecContext(ctx, `
INSERT INTO users(username, password_hash, push_id, role, created_at)
VALUES(?, ?, ?, ?, ?)
`, user.Username, passwordHash, user.PushID, user.Role, now.Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueErr(err) {
			return User{}, ErrConflict
		}
		return User{}, fmt.Errorf("create user: %w", err)
	}

	user.ID, _ = result.LastInsertId()
	return user, nil
}

func normalizeUserRole(role string) (string, error) {
	normalized := NormalizeUserRole(role)
	if !IsValidUserRole(normalized) {
		return "", ErrInvalidRole
	}
	return normalized, nil
}

func assertRowsAffected(result sql.Result, action string) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s rows affected: %w", action, err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) schemaVersion(ctx context.Context) (int, error) {
	value, ok, err := s.getKV(ctx, dbVersionKey)
	if err != nil {
		return 0, err
	}
	if ok {
		version, err := parseSchemaVersion(value)
		if err != nil {
			return 0, err
		}
		return version, nil
	}
	return s.detectLegacySchemaVersion(ctx)
}

func (s *Store) detectLegacySchemaVersion(ctx context.Context) (int, error) {
	hasUsersTable, err := s.tableExists(ctx, "users")
	if err != nil {
		return 0, err
	}
	if !hasUsersTable {
		return 0, nil
	}

	hasRole, err := s.columnExists(ctx, "users", "role")
	if err != nil {
		return 0, err
	}
	if hasRole {
		return currentDBVersion, nil
	}
	return 1, nil
}

func (s *Store) tableExists(ctx context.Context, tableName string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM sqlite_master
WHERE type = 'table' AND name = ?
`, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check table %s exists: %w", tableName, err)
	}
	return count > 0, nil
}

func (s *Store) columnExists(ctx context.Context, tableName, columnName string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
	if err != nil {
		return false, fmt.Errorf("query table info for %s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return false, fmt.Errorf("scan table info for %s: %w", tableName, err)
		}
		if name == columnName {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table info for %s: %w", tableName, err)
	}
	return false, nil
}

func isUniqueErr(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "unique")
}

func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}
