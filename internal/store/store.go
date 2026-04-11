package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"notifier/internal/clock"
)

var (
	ErrConflict     = errors.New("conflict")
	ErrNotFound     = errors.New("not found")
	ErrInvalidInput = errors.New("invalid input")
)

const DefaultMessageWindowSize = 500

type Options struct {
	NodeID            string
	NodeSlot          uint16
	MessageWindowSize int
	Clock             *clock.Clock
}

type Store struct {
	db                *sql.DB
	nodeID            string
	clock             *clock.Clock
	ids               *clock.IDGenerator
	nodeSlot          uint16
	messageWindowSize int
}

type User struct {
	ID                  int64            `json:"id"`
	Username            string           `json:"username"`
	PasswordHash        string           `json:"password_hash"`
	Profile             string           `json:"profile"`
	CreatedAt           clock.Timestamp  `json:"created_at"`
	UpdatedAt           clock.Timestamp  `json:"updated_at"`
	DeletedAt           *clock.Timestamp `json:"deleted_at,omitempty"`
	VersionUsername     clock.Timestamp  `json:"version_username"`
	VersionPasswordHash clock.Timestamp  `json:"version_password_hash"`
	VersionProfile      clock.Timestamp  `json:"version_profile"`
	VersionDeleted      *clock.Timestamp `json:"version_deleted,omitempty"`
	OriginNodeID        string           `json:"origin_node_id"`
}

type Message struct {
	ID           int64           `json:"id"`
	UserID       int64           `json:"user_id"`
	Sender       string          `json:"sender"`
	Body         string          `json:"body"`
	Metadata     string          `json:"metadata,omitempty"`
	CreatedAt    clock.Timestamp `json:"created_at"`
	OriginNodeID string          `json:"origin_node_id"`
}

type Event struct {
	Sequence     int64           `json:"sequence"`
	EventID      int64           `json:"event_id"`
	Kind         string          `json:"kind"`
	Aggregate    string          `json:"aggregate"`
	AggregateID  int64           `json:"aggregate_id"`
	HLC          clock.Timestamp `json:"hlc"`
	OriginNodeID string          `json:"origin_node_id"`
	Payload      string          `json:"payload"`
}

type PeerCursor struct {
	PeerNodeID      string          `json:"peer_node_id"`
	AckedSequence   int64           `json:"acked_sequence"`
	AppliedSequence int64           `json:"applied_sequence"`
	UpdatedAt       clock.Timestamp `json:"updated_at"`
}

type CreateUserParams struct {
	Username     string
	PasswordHash string
	Profile      string
}

type UpdateUserParams struct {
	UserID       int64
	Username     *string
	PasswordHash *string
	Profile      *string
}

type CreateMessageParams struct {
	UserID   int64
	Sender   string
	Body     string
	Metadata string
}

func Open(dbPath string, opts Options) (*Store, error) {
	if strings.TrimSpace(opts.NodeID) == "" {
		return nil, fmt.Errorf("node id cannot be empty")
	}
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	ids, err := clock.NewIDGenerator(opts.NodeSlot)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{
		db:                db,
		nodeID:            opts.NodeID,
		nodeSlot:          opts.NodeSlot,
		clock:             resolvedClock(opts),
		ids:               ids,
		messageWindowSize: normalizeMessageWindowSize(opts.MessageWindowSize),
	}, nil
}

func resolvedClock(opts Options) *clock.Clock {
	if opts.Clock != nil {
		return opts.Clock
	}
	return clock.NewClock(opts.NodeSlot)
}

func (s *Store) Clock() *clock.Clock {
	return s.clock
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Init(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    profile TEXT NOT NULL DEFAULT '{}',
    created_at_hlc TEXT NOT NULL,
    updated_at_hlc TEXT NOT NULL,
    deleted_at_hlc TEXT,
    version_username TEXT NOT NULL,
    version_password_hash TEXT NOT NULL,
    version_profile TEXT NOT NULL,
    version_deleted TEXT,
    origin_node_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_username_deleted ON users(username, deleted_at_hlc);

CREATE TABLE IF NOT EXISTS messages (
    message_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    sender TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata TEXT,
    created_at_hlc TEXT NOT NULL,
    origin_node_id TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(user_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_user_created ON messages(user_id, created_at_hlc DESC, message_id DESC);

CREATE TABLE IF NOT EXISTS event_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id INTEGER NOT NULL,
    hlc TEXT NOT NULL,
    origin_node_id TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS peer_cursors (
    peer_node_id TEXT PRIMARY KEY,
    acked_sequence INTEGER NOT NULL DEFAULT 0,
    applied_sequence INTEGER NOT NULL DEFAULT 0,
    updated_at_hlc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_events (
    event_id INTEGER PRIMARY KEY,
    source_node_id TEXT NOT NULL,
    applied_at_hlc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loser_user_id INTEGER NOT NULL,
    winner_user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    detected_at_hlc TEXT NOT NULL,
    resolved_by_event_id INTEGER
);

CREATE TABLE IF NOT EXISTS tombstones (
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    deleted_at_hlc TEXT NOT NULL,
    expires_at_hlc TEXT,
    origin_node_id TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
INSERT INTO schema_meta(key, value)
VALUES('schema_version', '1')
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`); err != nil {
		return fmt.Errorf("store schema version: %w", err)
	}
	return nil
}

func (s *Store) CreateUser(ctx context.Context, params CreateUserParams) (User, Event, error) {
	username := strings.TrimSpace(params.Username)
	if username == "" {
		return User{}, Event{}, fmt.Errorf("%w: username cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(params.PasswordHash) == "" {
		return User{}, Event{}, fmt.Errorf("%w: password hash cannot be empty", ErrInvalidInput)
	}

	profile := strings.TrimSpace(params.Profile)
	if profile == "" {
		profile = "{}"
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("begin create user: %w", err)
	}
	defer tx.Rollback()

	now := s.clock.Now()
	user := User{
		ID:                  s.ids.Next(),
		Username:            username,
		PasswordHash:        params.PasswordHash,
		Profile:             profile,
		CreatedAt:           now,
		UpdatedAt:           now,
		VersionUsername:     now,
		VersionPasswordHash: now,
		VersionProfile:      now,
		OriginNodeID:        s.nodeID,
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    user_id, username, password_hash, profile, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
`, user.ID, user.Username, user.PasswordHash, user.Profile, user.CreatedAt.String(),
		user.UpdatedAt.String(), user.VersionUsername.String(), user.VersionPasswordHash.String(),
		user.VersionProfile.String(), user.OriginNodeID); err != nil {
		return User{}, Event{}, fmt.Errorf("insert user: %w", err)
	}

	payload, err := encodeCreateUserPayload(user)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("marshal create user event: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, "user.created", "user", user.ID, now, string(payload))
	if err != nil {
		return User{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return User{}, Event{}, fmt.Errorf("commit create user: %w", err)
	}
	return user, event, nil
}

func (s *Store) UpdateUser(ctx context.Context, params UpdateUserParams) (User, Event, error) {
	if params.UserID == 0 {
		return User{}, Event{}, fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}
	if params.Username == nil && params.PasswordHash == nil && params.Profile == nil {
		return User{}, Event{}, fmt.Errorf("%w: at least one field must be updated", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("begin update user: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getUserTx(ctx, tx, params.UserID, false)
	if err != nil {
		return User{}, Event{}, err
	}

	now := s.clock.Now()
	changed := false

	if params.Username != nil {
		nextUsername := strings.TrimSpace(*params.Username)
		if nextUsername == "" {
			return User{}, Event{}, fmt.Errorf("%w: username cannot be empty", ErrInvalidInput)
		}
		if nextUsername != current.Username {
			current.Username = nextUsername
			current.VersionUsername = now
			changed = true
		}
	}

	if params.PasswordHash != nil {
		nextHash := strings.TrimSpace(*params.PasswordHash)
		if nextHash == "" {
			return User{}, Event{}, fmt.Errorf("%w: password hash cannot be empty", ErrInvalidInput)
		}
		if nextHash != current.PasswordHash {
			current.PasswordHash = nextHash
			current.VersionPasswordHash = now
			changed = true
		}
	}

	if params.Profile != nil {
		nextProfile := strings.TrimSpace(*params.Profile)
		if nextProfile == "" {
			nextProfile = "{}"
		}
		if nextProfile != current.Profile {
			current.Profile = nextProfile
			current.VersionProfile = now
			changed = true
		}
	}

	if !changed {
		return current, Event{}, fmt.Errorf("%w: no changes detected", ErrInvalidInput)
	}
	current.UpdatedAt = now

	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, updated_at_hlc = ?,
    version_username = ?, version_password_hash = ?, version_profile = ?
WHERE user_id = ? AND deleted_at_hlc IS NULL
`, current.Username, current.PasswordHash, current.Profile, current.UpdatedAt.String(),
		current.VersionUsername.String(), current.VersionPasswordHash.String(),
		current.VersionProfile.String(), current.ID); err != nil {
		return User{}, Event{}, fmt.Errorf("update user: %w", err)
	}

	payload, err := encodeUpdateUserPayload(current)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("marshal update user event: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, "user.updated", "user", current.ID, now, string(payload))
	if err != nil {
		return User{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return User{}, Event{}, fmt.Errorf("commit update user: %w", err)
	}
	return current, event, nil
}

func (s *Store) DeleteUser(ctx context.Context, userID int64) (Event, error) {
	if userID == 0 {
		return Event{}, fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, fmt.Errorf("begin delete user: %w", err)
	}
	defer tx.Rollback()

	if _, err := s.getUserTx(ctx, tx, userID, false); err != nil {
		return Event{}, err
	}

	now := s.clock.Now()
	if err := s.applyUserDeleteTx(ctx, tx, userID, now, s.nodeID, true); err != nil {
		return Event{}, err
	}

	payload, err := encodeDeleteUserPayload(userID, now.String())
	if err != nil {
		return Event{}, fmt.Errorf("marshal delete user event: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, "user.deleted", "user", userID, now, string(payload))
	if err != nil {
		return Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return Event{}, fmt.Errorf("commit delete user: %w", err)
	}
	return event, nil
}

func (s *Store) GetUser(ctx context.Context, userID int64) (User, error) {
	return s.getUser(ctx, userID, false)
}

func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT user_id, username, password_hash, profile, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_deleted, origin_node_id
FROM users
WHERE deleted_at_hlc IS NULL
ORDER BY user_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		user, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate users: %w", err)
	}
	return users, nil
}

func (s *Store) CreateMessage(ctx context.Context, params CreateMessageParams) (Message, Event, error) {
	if params.UserID == 0 {
		return Message{}, Event{}, fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(params.Sender) == "" {
		return Message{}, Event{}, fmt.Errorf("%w: sender cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(params.Body) == "" {
		return Message{}, Event{}, fmt.Errorf("%w: body cannot be empty", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Message{}, Event{}, fmt.Errorf("begin create message: %w", err)
	}
	defer tx.Rollback()

	if _, err := s.getUserTx(ctx, tx, params.UserID, false); err != nil {
		return Message{}, Event{}, err
	}

	now := s.clock.Now()
	message := Message{
		ID:           s.ids.Next(),
		UserID:       params.UserID,
		Sender:       strings.TrimSpace(params.Sender),
		Body:         strings.TrimSpace(params.Body),
		Metadata:     strings.TrimSpace(params.Metadata),
		CreatedAt:    now,
		OriginNodeID: s.nodeID,
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(message_id, user_id, sender, body, metadata, created_at_hlc, origin_node_id)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, message.ID, message.UserID, message.Sender, message.Body, nullIfEmpty(message.Metadata),
		message.CreatedAt.String(), message.OriginNodeID); err != nil {
		return Message{}, Event{}, fmt.Errorf("insert message: %w", err)
	}

	payload, err := encodeCreateMessagePayload(message)
	if err != nil {
		return Message{}, Event{}, fmt.Errorf("marshal create message event: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, "message.created", "message", message.ID, now, string(payload))
	if err != nil {
		return Message{}, Event{}, err
	}

	if err := s.trimMessagesForUserTx(ctx, tx, message.UserID); err != nil {
		return Message{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return Message{}, Event{}, fmt.Errorf("commit create message: %w", err)
	}
	return message, event, nil
}

func (s *Store) ListMessagesByUser(ctx context.Context, userID int64, limit int) ([]Message, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT message_id, user_id, sender, body, COALESCE(metadata, ''), created_at_hlc, origin_node_id
FROM messages
WHERE user_id = ?
ORDER BY created_at_hlc DESC, message_id DESC
LIMIT ?
`, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate messages: %w", err)
	}
	return messages, nil
}

func (s *Store) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT sequence, event_id, kind, aggregate_type, aggregate_id, hlc, origin_node_id, payload
FROM event_log
WHERE sequence > ?
ORDER BY sequence ASC
LIMIT ?
`, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

func (s *Store) getUser(ctx context.Context, userID int64, includeDeleted bool) (User, error) {
	query := `
SELECT user_id, username, password_hash, profile, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_deleted, origin_node_id
FROM users
WHERE user_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at_hlc IS NULL`
	}

	row := s.db.QueryRowContext(ctx, query, userID)
	user, err := scanUser(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	return user, nil
}

func (s *Store) getUserTx(ctx context.Context, tx *sql.Tx, userID int64, includeDeleted bool) (User, error) {
	query := `
SELECT user_id, username, password_hash, profile, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_deleted, origin_node_id
FROM users
WHERE user_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at_hlc IS NULL`
	}

	row := tx.QueryRowContext(ctx, query, userID)
	user, err := scanUser(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return User{}, ErrNotFound
		}
		return User{}, err
	}
	return user, nil
}

func (s *Store) insertEvent(ctx context.Context, tx *sql.Tx, kind, aggregate string, aggregateID int64, hlc clock.Timestamp, payload string) (Event, error) {
	event := Event{
		EventID:      s.ids.Next(),
		Kind:         kind,
		Aggregate:    aggregate,
		AggregateID:  aggregateID,
		HLC:          hlc,
		OriginNodeID: s.nodeID,
		Payload:      payload,
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO event_log(event_id, kind, aggregate_type, aggregate_id, hlc, origin_node_id, payload)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, event.EventID, event.Kind, event.Aggregate, event.AggregateID, event.HLC.String(),
		event.OriginNodeID, event.Payload)
	if err != nil {
		return Event{}, fmt.Errorf("insert event: %w", err)
	}

	event.Sequence, err = result.LastInsertId()
	if err != nil {
		return Event{}, fmt.Errorf("read event sequence: %w", err)
	}
	return event, nil
}

func activeUsernameExists(ctx context.Context, tx *sql.Tx, username string, excludeUserID int64) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM users
WHERE username = ? AND deleted_at_hlc IS NULL AND user_id != ?
`, username, excludeUserID).Scan(&count); err != nil {
		return false, fmt.Errorf("check active username: %w", err)
	}
	return count > 0, nil
}

type tombstoneRecord struct {
	EntityType   string
	EntityID     int64
	DeletedAt    clock.Timestamp
	OriginNodeID string
}

func (s *Store) getTombstoneTx(ctx context.Context, tx *sql.Tx, entityType string, entityID int64) (tombstoneRecord, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT entity_type, entity_id, deleted_at_hlc, origin_node_id
FROM tombstones
WHERE entity_type = ? AND entity_id = ?
`, entityType, entityID)

	var record tombstoneRecord
	var deletedAtRaw string
	if err := row.Scan(&record.EntityType, &record.EntityID, &deletedAtRaw, &record.OriginNodeID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return tombstoneRecord{}, false, nil
		}
		return tombstoneRecord{}, false, fmt.Errorf("get tombstone: %w", err)
	}

	deletedAt, err := clock.ParseTimestamp(deletedAtRaw)
	if err != nil {
		return tombstoneRecord{}, false, fmt.Errorf("parse tombstone deleted_at: %w", err)
	}
	record.DeletedAt = deletedAt
	return record, true, nil
}

func (s *Store) upsertTombstoneTx(ctx context.Context, tx *sql.Tx, entityType string, entityID int64, deletedAt clock.Timestamp, originNodeID string) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tombstones(entity_type, entity_id, deleted_at_hlc, expires_at_hlc, origin_node_id)
VALUES(?, ?, ?, NULL, ?)
ON CONFLICT(entity_type, entity_id) DO UPDATE SET
    deleted_at_hlc = CASE
        WHEN excluded.deleted_at_hlc > tombstones.deleted_at_hlc THEN excluded.deleted_at_hlc
        ELSE tombstones.deleted_at_hlc
    END,
    origin_node_id = CASE
        WHEN excluded.deleted_at_hlc > tombstones.deleted_at_hlc THEN excluded.origin_node_id
        ELSE tombstones.origin_node_id
    END
`, entityType, entityID, deletedAt.String(), originNodeID); err != nil {
		return fmt.Errorf("upsert tombstone: %w", err)
	}
	return nil
}

func (s *Store) applyUserDeleteTx(ctx context.Context, tx *sql.Tx, userID int64, deletedAt clock.Timestamp, originNodeID string, requireActive bool) error {
	user, err := s.getUserByIDTx(ctx, tx, userID, true)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		if requireActive {
			return ErrNotFound
		}
	default:
		return err
	}

	if err == nil {
		if requireActive && user.DeletedAt != nil {
			return ErrNotFound
		}

		shouldUpdateRow := user.DeletedAt == nil
		if !shouldUpdateRow && user.VersionDeleted != nil && deletedAt.Compare(*user.VersionDeleted) > 0 {
			shouldUpdateRow = true
		}

		if shouldUpdateRow {
			updatedAt := user.UpdatedAt
			if deletedAt.Compare(updatedAt) > 0 {
				updatedAt = deletedAt
			}
			if _, err := tx.ExecContext(ctx, `
UPDATE users
SET deleted_at_hlc = ?, updated_at_hlc = ?, version_deleted = ?
WHERE user_id = ?
`, deletedAt.String(), updatedAt.String(), deletedAt.String(), userID); err != nil {
				return fmt.Errorf("delete user: %w", err)
			}
		}
	}

	if err := s.upsertTombstoneTx(ctx, tx, "user", userID, deletedAt, originNodeID); err != nil {
		return err
	}
	return nil
}

func scanUser(scanner interface {
	Scan(dest ...any) error
}) (User, error) {
	var user User
	var createdAtRaw string
	var updatedAtRaw string
	var deletedAtRaw sql.NullString
	var versionUsernameRaw string
	var versionPasswordRaw string
	var versionProfileRaw string
	var versionDeletedRaw sql.NullString

	if err := scanner.Scan(
		&user.ID,
		&user.Username,
		&user.PasswordHash,
		&user.Profile,
		&createdAtRaw,
		&updatedAtRaw,
		&deletedAtRaw,
		&versionUsernameRaw,
		&versionPasswordRaw,
		&versionProfileRaw,
		&versionDeletedRaw,
		&user.OriginNodeID,
	); err != nil {
		return User{}, err
	}

	var err error
	user.CreatedAt, err = clock.ParseTimestamp(createdAtRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user created_at: %w", err)
	}
	user.UpdatedAt, err = clock.ParseTimestamp(updatedAtRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user updated_at: %w", err)
	}
	user.VersionUsername, err = clock.ParseTimestamp(versionUsernameRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_username: %w", err)
	}
	user.VersionPasswordHash, err = clock.ParseTimestamp(versionPasswordRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_password_hash: %w", err)
	}
	user.VersionProfile, err = clock.ParseTimestamp(versionProfileRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_profile: %w", err)
	}
	if deletedAtRaw.Valid {
		parsed, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return User{}, fmt.Errorf("parse user deleted_at: %w", err)
		}
		user.DeletedAt = &parsed
	}
	if versionDeletedRaw.Valid {
		parsed, err := clock.ParseTimestamp(versionDeletedRaw.String)
		if err != nil {
			return User{}, fmt.Errorf("parse user version_deleted: %w", err)
		}
		user.VersionDeleted = &parsed
	}
	return user, nil
}

func scanMessage(scanner interface {
	Scan(dest ...any) error
}) (Message, error) {
	var message Message
	var createdAtRaw string

	if err := scanner.Scan(
		&message.ID,
		&message.UserID,
		&message.Sender,
		&message.Body,
		&message.Metadata,
		&createdAtRaw,
		&message.OriginNodeID,
	); err != nil {
		return Message{}, err
	}

	createdAt, err := clock.ParseTimestamp(createdAtRaw)
	if err != nil {
		return Message{}, fmt.Errorf("parse message created_at: %w", err)
	}
	message.CreatedAt = createdAt
	return message, nil
}

func scanEvent(scanner interface {
	Scan(dest ...any) error
}) (Event, error) {
	var event Event
	var hlcRaw string

	if err := scanner.Scan(
		&event.Sequence,
		&event.EventID,
		&event.Kind,
		&event.Aggregate,
		&event.AggregateID,
		&hlcRaw,
		&event.OriginNodeID,
		&event.Payload,
	); err != nil {
		return Event{}, err
	}

	hlc, err := clock.ParseTimestamp(hlcRaw)
	if err != nil {
		return Event{}, fmt.Errorf("parse event hlc: %w", err)
	}
	event.HLC = hlc
	return event, nil
}

func nullIfEmpty(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func normalizeMessageWindowSize(size int) int {
	if size <= 0 {
		return DefaultMessageWindowSize
	}
	return size
}

func (s *Store) trimMessagesForUserTx(ctx context.Context, tx *sql.Tx, userID int64) error {
	windowSize := normalizeMessageWindowSize(s.messageWindowSize)
	if _, err := tx.ExecContext(ctx, `
DELETE FROM messages
WHERE user_id = ?
  AND message_id IN (
    SELECT message_id
    FROM messages
    WHERE user_id = ?
    ORDER BY created_at_hlc DESC, message_id DESC
    LIMIT -1 OFFSET ?
  )
`, userID, userID, windowSize); err != nil {
		return fmt.Errorf("trim messages for user %d: %w", userID, err)
	}
	return nil
}
