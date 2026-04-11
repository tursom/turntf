package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"notifier/internal/auth"
	"notifier/internal/clock"
)

var (
	ErrConflict     = errors.New("conflict")
	ErrForbidden    = errors.New("forbidden")
	ErrNotFound     = errors.New("not found")
	ErrInvalidInput = errors.New("invalid input")
)

const DefaultMessageWindowSize = 500

const (
	RoleSuperAdmin       = "super_admin"
	RoleAdmin            = "admin"
	RoleUser             = "user"
	BootstrapAdminUserID = int64(1)
	defaultSchemaVersion = "3"
	schemaMetaNodeIDKey  = "node_id"
)

type Options struct {
	// NodeID seeds schema_meta.node_id for deterministic tests; production leaves it empty.
	NodeID            int64
	MessageWindowSize int
	Clock             *clock.Clock
}

type Store struct {
	db                *sql.DB
	nodeID            int64
	clock             *clock.Clock
	ids               *clock.IDGenerator
	nodeSlot          uint16
	initialNodeID     int64
	messageWindowSize int
	bootstrapAdmin    BootstrapAdminConfig
}

type User struct {
	ID                  int64            `json:"id"`
	Username            string           `json:"username"`
	PasswordHash        string           `json:"password_hash"`
	Profile             string           `json:"profile"`
	Role                string           `json:"role"`
	SystemReserved      bool             `json:"system_reserved"`
	CreatedAt           clock.Timestamp  `json:"created_at"`
	UpdatedAt           clock.Timestamp  `json:"updated_at"`
	DeletedAt           *clock.Timestamp `json:"deleted_at,omitempty"`
	VersionUsername     clock.Timestamp  `json:"version_username"`
	VersionPasswordHash clock.Timestamp  `json:"version_password_hash"`
	VersionProfile      clock.Timestamp  `json:"version_profile"`
	VersionRole         clock.Timestamp  `json:"version_role"`
	VersionDeleted      *clock.Timestamp `json:"version_deleted,omitempty"`
	OriginNodeID        int64            `json:"origin_node_id"`
}

type Message struct {
	UserID    int64           `json:"user_id"`
	NodeID    int64           `json:"node_id"`
	Seq       int64           `json:"seq"`
	Sender    string          `json:"sender"`
	Body      string          `json:"body"`
	Metadata  string          `json:"metadata,omitempty"`
	CreatedAt clock.Timestamp `json:"created_at"`
}

type Event struct {
	Sequence     int64           `json:"sequence"`
	EventID      int64           `json:"event_id"`
	Kind         string          `json:"kind"`
	Aggregate    string          `json:"aggregate"`
	AggregateID  int64           `json:"aggregate_id"`
	HLC          clock.Timestamp `json:"hlc"`
	OriginNodeID int64           `json:"origin_node_id"`
	Payload      string          `json:"payload"`
}

type PeerCursor struct {
	PeerNodeID      int64           `json:"peer_node_id"`
	AckedSequence   int64           `json:"acked_sequence"`
	AppliedSequence int64           `json:"applied_sequence"`
	UpdatedAt       clock.Timestamp `json:"updated_at"`
}

type CreateUserParams struct {
	Username     string
	PasswordHash string
	Profile      string
	Role         string
}

type UpdateUserParams struct {
	UserID       int64
	Username     *string
	PasswordHash *string
	Profile      *string
	Role         *string
}

type CreateMessageParams struct {
	UserID   int64
	Sender   string
	Body     string
	Metadata string
}

type BootstrapAdminConfig struct {
	Username     string
	PasswordHash string
}

func Open(dbPath string, opts Options) (*Store, error) {
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

	return &Store{
		db:                db,
		initialNodeID:     opts.NodeID,
		messageWindowSize: normalizeMessageWindowSize(opts.MessageWindowSize),
		clock:             opts.Clock,
	}, nil
}

func (s *Store) Clock() *clock.Clock {
	return s.clock
}

func (s *Store) NodeID() int64 {
	return s.nodeID
}

func (s *Store) NodeSlot() uint16 {
	return s.nodeSlot
}

func (s *Store) MessageWindowSize() int {
	return normalizeMessageWindowSize(s.messageWindowSize)
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
    role TEXT NOT NULL DEFAULT 'user',
    system_reserved INTEGER NOT NULL DEFAULT 0,
    created_at_hlc TEXT NOT NULL,
    updated_at_hlc TEXT NOT NULL,
    deleted_at_hlc TEXT,
    version_username TEXT NOT NULL,
    version_password_hash TEXT NOT NULL,
    version_profile TEXT NOT NULL,
    version_role TEXT NOT NULL,
    version_deleted TEXT,
    origin_node_id INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_username_deleted ON users(username, deleted_at_hlc);

CREATE TABLE IF NOT EXISTS messages (
    user_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    sender TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata TEXT,
    created_at_hlc TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    PRIMARY KEY(user_id, node_id, seq)
);

CREATE TABLE IF NOT EXISTS message_cursors (
    user_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    last_seq INTEGER NOT NULL,
    PRIMARY KEY(user_id, node_id)
);

CREATE TABLE IF NOT EXISTS event_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id INTEGER NOT NULL,
    hlc TEXT NOT NULL,
    origin_node_id INTEGER NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS peer_cursors (
    peer_node_id INTEGER PRIMARY KEY,
    acked_sequence INTEGER NOT NULL DEFAULT 0,
    applied_sequence INTEGER NOT NULL DEFAULT 0,
    updated_at_hlc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_events (
    event_id INTEGER PRIMARY KEY,
    source_node_id INTEGER NOT NULL,
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
    origin_node_id INTEGER NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS message_trim_stats (
    scope TEXT PRIMARY KEY,
    trimmed_total INTEGER NOT NULL DEFAULT 0,
    last_trimmed_at_hlc TEXT
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
CREATE INDEX IF NOT EXISTS idx_messages_user_created ON messages(user_id, created_at_hlc DESC, node_id ASC, seq DESC);
CREATE INDEX IF NOT EXISTS idx_messages_node ON messages(node_id, user_id, seq);
`); err != nil {
		return fmt.Errorf("init message indexes: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
INSERT INTO schema_meta(key, value)
VALUES('schema_version', ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value
`, defaultSchemaVersion); err != nil {
		return fmt.Errorf("store schema version: %w", err)
	}
	if err := s.ensureNodeIdentity(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureNodeIdentity(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin node identity init: %w", err)
	}
	defer tx.Rollback()

	nodeID, err := readNodeIDTx(ctx, tx)
	if err != nil {
		return err
	}
	if nodeID == 0 {
		if s.initialNodeID > 0 {
			nodeID = s.initialNodeID
		} else {
			nodeID, err = clock.GenerateNodeID()
			if err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO schema_meta(key, value)
VALUES(?, ?)
`, schemaMetaNodeIDKey, strconv.FormatInt(nodeID, 10)); err != nil {
			return fmt.Errorf("store node id: %w", err)
		}
	} else if s.initialNodeID > 0 && nodeID != s.initialNodeID {
		return fmt.Errorf("node id %d does not match existing node id %d", s.initialNodeID, nodeID)
	}

	slot, err := clock.NodeSlotFromID(nodeID)
	if err != nil {
		return fmt.Errorf("load node id: %w", err)
	}
	ids, err := clock.NewIDGenerator(slot)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit node identity init: %w", err)
	}

	s.nodeID = nodeID
	s.nodeSlot = slot
	s.ids = ids
	if s.clock == nil {
		s.clock = clock.NewClock(slot)
	}
	return nil
}

func readNodeIDTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `
SELECT value
FROM schema_meta
WHERE key = ?
`, schemaMetaNodeIDKey).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read node id: %w", err)
	}

	nodeID, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse node id %q: %w", raw, err)
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("node id must be positive")
	}
	return nodeID, nil
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
	role, err := normalizeMutableRole(params.Role)
	if err != nil {
		return User{}, Event{}, err
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
		Role:                role,
		SystemReserved:      false,
		CreatedAt:           now,
		UpdatedAt:           now,
		VersionUsername:     now,
		VersionPasswordHash: now,
		VersionProfile:      now,
		VersionRole:         now,
		OriginNodeID:        s.nodeID,
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
		user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
		user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
		user.OriginNodeID); err != nil {
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
	if params.Username == nil && params.PasswordHash == nil && params.Profile == nil && params.Role == nil {
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
		if current.isProtectedBootstrapAdmin() {
			return User{}, Event{}, fmt.Errorf("%w: bootstrap admin username cannot be changed", ErrForbidden)
		}
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
	if params.Role != nil {
		if current.isProtectedBootstrapAdmin() {
			return User{}, Event{}, fmt.Errorf("%w: bootstrap admin role cannot be changed", ErrForbidden)
		}
		nextRole, err := normalizeMutableRole(*params.Role)
		if err != nil {
			return User{}, Event{}, err
		}
		if nextRole != current.Role {
			current.Role = nextRole
			current.VersionRole = now
			changed = true
		}
	}

	if !changed {
		return current, Event{}, fmt.Errorf("%w: no changes detected", ErrInvalidInput)
	}
	current.UpdatedAt = now
	current = s.applyReservedUserInvariants(current)

	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, updated_at_hlc = ?,
    version_username = ?, version_password_hash = ?, version_profile = ?, version_role = ?
WHERE user_id = ? AND deleted_at_hlc IS NULL
`, current.Username, current.PasswordHash, current.Profile, current.Role, boolToInt(current.SystemReserved),
		current.UpdatedAt.String(), current.VersionUsername.String(), current.VersionPasswordHash.String(),
		current.VersionProfile.String(), current.VersionRole.String(), current.ID); err != nil {
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

	user, err := s.getUserTx(ctx, tx, userID, false)
	if err != nil {
		return Event{}, err
	}
	if user.isProtectedBootstrapAdmin() {
		return Event{}, fmt.Errorf("%w: bootstrap admin cannot be deleted", ErrForbidden)
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
SELECT user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
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
	seq, err := s.nextMessageSeqTx(ctx, tx, params.UserID, s.nodeID)
	if err != nil {
		return Message{}, Event{}, err
	}
	message := Message{
		UserID:    params.UserID,
		NodeID:    s.nodeID,
		Seq:       seq,
		Sender:    strings.TrimSpace(params.Sender),
		Body:      strings.TrimSpace(params.Body),
		Metadata:  strings.TrimSpace(params.Metadata),
		CreatedAt: now,
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_id, node_id, seq, sender, body, metadata, created_at_hlc)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, message.UserID, message.NodeID, message.Seq, message.Sender, message.Body, nullIfEmpty(message.Metadata),
		message.CreatedAt.String()); err != nil {
		return Message{}, Event{}, fmt.Errorf("insert message: %w", err)
	}

	payload, err := encodeCreateMessagePayload(message)
	if err != nil {
		return Message{}, Event{}, fmt.Errorf("marshal create message event: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, "message.created", "message", message.Seq, now, string(payload))
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
SELECT user_id, node_id, seq, sender, body, COALESCE(metadata, ''), created_at_hlc
FROM messages
WHERE user_id = ?
ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
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
SELECT user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
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
SELECT user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
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

func (s *Store) nextMessageSeqTx(ctx context.Context, tx *sql.Tx, userID int64, nodeID int64) (int64, error) {
	if userID == 0 || nodeID <= 0 {
		return 0, fmt.Errorf("%w: user id and node id are required for message sequence", ErrInvalidInput)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO message_cursors(user_id, node_id, last_seq)
VALUES(?, ?, 0)
ON CONFLICT(user_id, node_id) DO NOTHING
`, userID, nodeID); err != nil {
		return 0, fmt.Errorf("initialize message cursor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE message_cursors
SET last_seq = last_seq + 1
WHERE user_id = ? AND node_id = ?
`, userID, nodeID); err != nil {
		return 0, fmt.Errorf("advance message cursor: %w", err)
	}

	var seq int64
	if err := tx.QueryRowContext(ctx, `
SELECT last_seq
FROM message_cursors
WHERE user_id = ? AND node_id = ?
`, userID, nodeID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("read message cursor: %w", err)
	}
	return seq, nil
}

func (s *Store) recordMessageSeqTx(ctx context.Context, tx *sql.Tx, userID int64, nodeID int64, seq int64) error {
	if userID == 0 || nodeID <= 0 || seq <= 0 {
		return fmt.Errorf("%w: user id, node id, and seq are required for message cursor", ErrInvalidInput)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO message_cursors(user_id, node_id, last_seq)
VALUES(?, ?, ?)
ON CONFLICT(user_id, node_id) DO UPDATE SET
    last_seq = CASE
        WHEN excluded.last_seq > message_cursors.last_seq THEN excluded.last_seq
        ELSE message_cursors.last_seq
    END
`, userID, nodeID, seq); err != nil {
		return fmt.Errorf("record message cursor: %w", err)
	}
	return nil
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
	OriginNodeID int64
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

func (s *Store) upsertTombstoneTx(ctx context.Context, tx *sql.Tx, entityType string, entityID int64, deletedAt clock.Timestamp, originNodeID int64) error {
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

func (s *Store) applyUserDeleteTx(ctx context.Context, tx *sql.Tx, userID int64, deletedAt clock.Timestamp, originNodeID int64, requireActive bool) error {
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
		if user.isProtectedBootstrapAdmin() {
			if requireActive {
				return fmt.Errorf("%w: bootstrap admin cannot be deleted", ErrForbidden)
			}
			return nil
		}
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
	var systemReserved int
	var createdAtRaw string
	var updatedAtRaw string
	var deletedAtRaw sql.NullString
	var versionUsernameRaw string
	var versionPasswordRaw string
	var versionProfileRaw string
	var versionRoleRaw string
	var versionDeletedRaw sql.NullString

	if err := scanner.Scan(
		&user.ID,
		&user.Username,
		&user.PasswordHash,
		&user.Profile,
		&user.Role,
		&systemReserved,
		&createdAtRaw,
		&updatedAtRaw,
		&deletedAtRaw,
		&versionUsernameRaw,
		&versionPasswordRaw,
		&versionProfileRaw,
		&versionRoleRaw,
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
	user.VersionRole, err = clock.ParseTimestamp(versionRoleRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_role: %w", err)
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
	user.SystemReserved = systemReserved != 0
	return user, nil
}

func scanMessage(scanner interface {
	Scan(dest ...any) error
}) (Message, error) {
	var message Message
	var createdAtRaw string

	if err := scanner.Scan(
		&message.UserID,
		&message.NodeID,
		&message.Seq,
		&message.Sender,
		&message.Body,
		&message.Metadata,
		&createdAtRaw,
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

func (s *Store) AuthenticateUser(ctx context.Context, userID int64, password string) (User, error) {
	if userID == 0 {
		return User{}, fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(password) == "" {
		return User{}, fmt.Errorf("%w: password cannot be empty", ErrInvalidInput)
	}
	user, err := s.GetUser(ctx, userID)
	if err != nil {
		return User{}, err
	}
	if err := auth.VerifyPassword(user.PasswordHash, password); err != nil {
		return User{}, ErrNotFound
	}
	return user, nil
}

func (s *Store) EnsureBootstrapAdmin(ctx context.Context, cfg BootstrapAdminConfig) error {
	username := strings.TrimSpace(cfg.Username)
	passwordHash := strings.TrimSpace(cfg.PasswordHash)
	if username == "" {
		return fmt.Errorf("%w: bootstrap admin username cannot be empty", ErrInvalidInput)
	}
	if passwordHash == "" {
		return fmt.Errorf("%w: bootstrap admin password hash cannot be empty", ErrInvalidInput)
	}
	s.bootstrapAdmin = BootstrapAdminConfig{Username: username, PasswordHash: passwordHash}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ensure bootstrap admin: %w", err)
	}
	defer tx.Rollback()

	now := s.clock.Now()
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_id = ?`, BootstrapAdminUserID); err != nil {
		return fmt.Errorf("delete bootstrap admin tombstone: %w", err)
	}

	current, err := s.getUserTx(ctx, tx, BootstrapAdminUserID, true)
	switch {
	case errors.Is(err, ErrNotFound):
		user := User{
			ID:                  BootstrapAdminUserID,
			Username:            username,
			PasswordHash:        passwordHash,
			Profile:             "{}",
			Role:                RoleSuperAdmin,
			SystemReserved:      true,
			CreatedAt:           now,
			UpdatedAt:           now,
			VersionUsername:     now,
			VersionPasswordHash: now,
			VersionProfile:      now,
			VersionRole:         now,
			OriginNodeID:        s.nodeID,
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
			user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
			user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
			user.OriginNodeID); err != nil {
			return fmt.Errorf("insert bootstrap admin: %w", err)
		}
		payload, err := encodeCreateUserPayload(user)
		if err != nil {
			return fmt.Errorf("marshal bootstrap admin create event: %w", err)
		}
		if _, err := s.insertEvent(ctx, tx, "user.created", "user", user.ID, now, payload); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		updated := current
		changed := false
		if updated.Username != username {
			updated.Username = username
			updated.VersionUsername = now
			changed = true
		}
		if updated.DeletedAt != nil || updated.VersionDeleted != nil {
			updated.DeletedAt = nil
			updated.VersionDeleted = nil
			changed = true
		}
		if updated.Role != RoleSuperAdmin {
			updated.Role = RoleSuperAdmin
			updated.VersionRole = now
			changed = true
		}
		if !updated.SystemReserved {
			updated.SystemReserved = true
			changed = true
		}
		if changed {
			updated.UpdatedAt = latestUserVersion(updated)
			if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, created_at_hlc = ?, updated_at_hlc = ?,
    deleted_at_hlc = NULL, version_username = ?, version_password_hash = ?, version_profile = ?,
    version_role = ?, version_deleted = NULL, origin_node_id = ?
WHERE user_id = ?
`, updated.Username, updated.PasswordHash, updated.Profile, updated.Role, boolToInt(updated.SystemReserved),
				updated.CreatedAt.String(), updated.UpdatedAt.String(), updated.VersionUsername.String(),
				updated.VersionPasswordHash.String(), updated.VersionProfile.String(), updated.VersionRole.String(),
				updated.OriginNodeID, updated.ID); err != nil {
				return fmt.Errorf("repair bootstrap admin: %w", err)
			}
			payload, err := encodeUpdateUserPayload(updated)
			if err != nil {
				return fmt.Errorf("marshal bootstrap admin update event: %w", err)
			}
			if _, err := s.insertEvent(ctx, tx, "user.updated", "user", updated.ID, updated.UpdatedAt, payload); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ensure bootstrap admin: %w", err)
	}
	return nil
}

func normalizeAnyRole(role string) (string, error) {
	normalized := strings.TrimSpace(role)
	if normalized == "" {
		return RoleUser, nil
	}
	switch normalized {
	case RoleSuperAdmin, RoleAdmin, RoleUser:
		return normalized, nil
	default:
		return "", fmt.Errorf("%w: unsupported role %q", ErrInvalidInput, role)
	}
}

func normalizeMutableRole(role string) (string, error) {
	normalized, err := normalizeAnyRole(role)
	if err != nil {
		return "", err
	}
	if normalized == RoleSuperAdmin {
		return "", fmt.Errorf("%w: role %q cannot be assigned through this API", ErrInvalidInput, role)
	}
	return normalized, nil
}

func (s *Store) applyReservedUserInvariants(user User) User {
	if user.ID != BootstrapAdminUserID {
		user.SystemReserved = false
		return user
	}
	user.Role = RoleSuperAdmin
	user.SystemReserved = true
	if configured := strings.TrimSpace(s.bootstrapAdmin.Username); configured != "" {
		user.Username = configured
	}
	return user
}

func (u User) isProtectedBootstrapAdmin() bool {
	return u.ID == BootstrapAdminUserID && u.SystemReserved
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *Store) trimMessagesForUserTx(ctx context.Context, tx *sql.Tx, userID int64) error {
	windowSize := normalizeMessageWindowSize(s.messageWindowSize)
	result, err := tx.ExecContext(ctx, `
DELETE FROM messages
WHERE user_id = ?
  AND (node_id, seq) IN (
    SELECT node_id, seq
    FROM messages
    WHERE user_id = ?
    ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
    LIMIT -1 OFFSET ?
  )
`, userID, userID, windowSize)
	if err != nil {
		return fmt.Errorf("trim messages for user %d: %w", userID, err)
	}
	trimmed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count trimmed messages for user %d: %w", userID, err)
	}
	if trimmed > 0 {
		if err := s.recordMessageTrimTx(ctx, tx, trimmed); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) recordMessageTrimTx(ctx context.Context, tx *sql.Tx, trimmed int64) error {
	if trimmed <= 0 {
		return nil
	}
	now := s.clock.Now().String()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO message_trim_stats(scope, trimmed_total, last_trimmed_at_hlc)
VALUES('global', ?, ?)
ON CONFLICT(scope) DO UPDATE SET
    trimmed_total = message_trim_stats.trimmed_total + excluded.trimmed_total,
    last_trimmed_at_hlc = excluded.last_trimmed_at_hlc
`, trimmed, now); err != nil {
		return fmt.Errorf("record message trim stats: %w", err)
	}
	return nil
}
