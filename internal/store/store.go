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

	"github.com/cockroachdb/pebble"
	_ "github.com/mattn/go-sqlite3"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

var (
	ErrConflict     = errors.New("conflict")
	ErrForbidden    = errors.New("forbidden")
	ErrNotFound     = errors.New("not found")
	ErrInvalidInput = errors.New("invalid input")
)

const DefaultMessageWindowSize = 500

const (
	EngineSQLite = "sqlite"
	EnginePebble = "pebble"
)

const (
	RoleSuperAdmin       = "super_admin"
	RoleAdmin            = "admin"
	RoleUser             = "user"
	RoleChannel          = "channel"
	RoleBroadcast        = "broadcast"
	RoleNode             = "node"
	BootstrapAdminUserID = int64(1)
	BroadcastUserID      = int64(2)
	NodeIngressUserID    = int64(3)
	ReservedUserIDMax    = int64(1024)
	defaultSchemaVersion = "12"
	schemaMetaNodeIDKey  = "node_id"
)

const disabledPasswordHash = "!"

func isSystemReservedUserID(userID int64) bool {
	return userID == BootstrapAdminUserID || userID == BroadcastUserID || userID == NodeIngressUserID
}

type Options struct {
	// NodeID seeds schema_meta.node_id for deterministic tests; production leaves it empty.
	NodeID            int64
	Engine            string
	PebblePath        string
	MessageWindowSize int
	Clock             *clock.Clock
}

type Store struct {
	db                *sql.DB
	pebbleDB          *pebble.DB
	engine            string
	nodeID            int64
	clock             *clock.Clock
	ids               *clock.IDGenerator
	initialNodeID     int64
	messageWindowSize int
	bootstrapAdmin    BootstrapAdminConfig
	eventLog          EventLogRepository
	userRepository    UserRepository
	subscriptions     SubscriptionRepository
	messageTrim       MessageTrimRepository
	messageProjection MessageProjectionRepository
}

type UserKey struct {
	NodeID int64 `json:"node_id"`
	UserID int64 `json:"user_id"`
}

func (k UserKey) Validate() error {
	if k.NodeID <= 0 || k.UserID <= 0 {
		return fmt.Errorf("%w: user node id and user id are required", ErrInvalidInput)
	}
	return nil
}

type User struct {
	NodeID              int64            `json:"node_id"`
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

func (u User) Key() UserKey {
	return UserKey{NodeID: u.NodeID, UserID: u.ID}
}

func (u User) CanLogin() bool {
	return isLoginRole(u.Role)
}

type Message struct {
	Recipient UserKey         `json:"recipient"`
	NodeID    int64           `json:"node_id"`
	Seq       int64           `json:"seq"`
	Sender    UserKey         `json:"sender"`
	Body      []byte          `json:"body"`
	CreatedAt clock.Timestamp `json:"created_at"`
}

func (m Message) UserKey() UserKey {
	return m.Recipient
}

type Subscription struct {
	Subscriber   UserKey          `json:"subscriber"`
	Channel      UserKey          `json:"channel"`
	SubscribedAt clock.Timestamp  `json:"subscribed_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

type Event struct {
	Sequence        int64                   `json:"sequence"`
	EventID         int64                   `json:"event_id"`
	EventType       EventType               `json:"event_type"`
	Aggregate       string                  `json:"aggregate"`
	AggregateNodeID int64                   `json:"aggregate_node_id"`
	AggregateID     int64                   `json:"aggregate_id"`
	HLC             clock.Timestamp         `json:"hlc"`
	OriginNodeID    int64                   `json:"origin_node_id"`
	Body            internalproto.EventBody `json:"-"`
}

type OriginProgress struct {
	OriginNodeID int64 `json:"origin_node_id"`
	LastEventID  int64 `json:"last_event_id"`
}

type PeerAckCursor struct {
	PeerNodeID   int64           `json:"peer_node_id"`
	OriginNodeID int64           `json:"origin_node_id"`
	AckedEventID int64           `json:"acked_event_id"`
	UpdatedAt    clock.Timestamp `json:"updated_at"`
}

type OriginCursor struct {
	OriginNodeID   int64           `json:"origin_node_id"`
	AppliedEventID int64           `json:"applied_event_id"`
	UpdatedAt      clock.Timestamp `json:"updated_at"`
}

type CreateUserParams struct {
	Username     string
	PasswordHash string
	Profile      string
	Role         string
}

type UpdateUserParams struct {
	Key          UserKey
	Username     *string
	PasswordHash *string
	Profile      *string
	Role         *string
}

type CreateMessageParams struct {
	UserKey UserKey
	Sender  UserKey
	Body    []byte
}

type DeliveryMode string

const (
	DeliveryModeBestEffort DeliveryMode = "best_effort"
	DeliveryModeRouteRetry DeliveryMode = "route_retry"
)

type TransientPacket struct {
	PacketID      uint64       `json:"packet_id"`
	SourceNodeID  int64        `json:"source_node_id"`
	TargetNodeID  int64        `json:"target_node_id"`
	Recipient     UserKey      `json:"recipient"`
	Sender        UserKey      `json:"sender"`
	Body          []byte       `json:"body"`
	DeliveryMode  DeliveryMode `json:"delivery_mode"`
	TTLHops       int32        `json:"ttl_hops"`
	RouteRetryTTL int64        `json:"route_retry_ttl_ms,omitempty"`
}

func NormalizeDeliveryMode(raw string) (DeliveryMode, error) {
	switch DeliveryMode(strings.TrimSpace(raw)) {
	case "", DeliveryModeBestEffort:
		return DeliveryModeBestEffort, nil
	case DeliveryModeRouteRetry:
		return DeliveryModeRouteRetry, nil
	default:
		return "", fmt.Errorf("%w: unsupported delivery mode %q", ErrInvalidInput, raw)
	}
}

type ChannelSubscriptionParams struct {
	Subscriber UserKey
	Channel    UserKey
}

type BootstrapAdminConfig struct {
	Username     string
	PasswordHash string
}

func Open(dbPath string, opts Options) (*Store, error) {
	engine := normalizeEngine(opts.Engine)
	if engine == "" {
		return nil, fmt.Errorf("%w: unsupported store engine %q", ErrInvalidInput, opts.Engine)
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

	var pebbleDB *pebble.DB
	if engine == EnginePebble {
		pebblePath := strings.TrimSpace(opts.PebblePath)
		if pebblePath == "" {
			_ = db.Close()
			return nil, fmt.Errorf("%w: pebble path cannot be empty", ErrInvalidInput)
		}
		if err := os.MkdirAll(filepath.Dir(pebblePath), 0o755); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("create pebble dir: %w", err)
		}
		var err error
		pebbleDB, err = pebble.Open(pebblePath, &pebble.Options{})
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open pebble: %w", err)
		}
	}

	st := &Store{
		db:                db,
		pebbleDB:          pebbleDB,
		engine:            engine,
		initialNodeID:     opts.NodeID,
		messageWindowSize: normalizeMessageWindowSize(opts.MessageWindowSize),
		clock:             opts.Clock,
		userRepository:    newCachedUserRepository(&sqliteUserRepository{db: db}),
		subscriptions:     &sqliteSubscriptionRepository{db: db},
	}
	return st, nil
}

func (s *Store) Clock() *clock.Clock {
	return s.clock
}

func (s *Store) NodeID() int64 {
	return s.nodeID
}

func (s *Store) MessageWindowSize() int {
	return normalizeMessageWindowSize(s.messageWindowSize)
}

func (s *Store) Close() error {
	var err error
	if s.pebbleDB != nil {
		err = s.pebbleDB.Close()
	}
	if closeErr := s.db.Close(); err == nil {
		err = closeErr
	}
	return err
}

func (s *Store) Init(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    node_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
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
    origin_node_id INTEGER NOT NULL,
    PRIMARY KEY(node_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_users_username_deleted ON users(username, deleted_at_hlc);

CREATE TABLE IF NOT EXISTS messages (
    user_node_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    sender_node_id INTEGER NOT NULL,
    sender_user_id INTEGER NOT NULL,
    body BLOB NOT NULL,
    created_at_hlc TEXT NOT NULL,
    FOREIGN KEY(user_node_id, user_id) REFERENCES users(node_id, user_id),
    PRIMARY KEY(user_node_id, user_id, node_id, seq)
);

CREATE TABLE IF NOT EXISTS channel_subscriptions (
    subscriber_node_id INTEGER NOT NULL,
    subscriber_user_id INTEGER NOT NULL,
    channel_node_id INTEGER NOT NULL,
    channel_user_id INTEGER NOT NULL,
    subscribed_at_hlc TEXT NOT NULL,
    deleted_at_hlc TEXT,
    origin_node_id INTEGER NOT NULL,
    FOREIGN KEY(subscriber_node_id, subscriber_user_id) REFERENCES users(node_id, user_id),
    FOREIGN KEY(channel_node_id, channel_user_id) REFERENCES users(node_id, user_id),
    PRIMARY KEY(subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id)
);

CREATE TABLE IF NOT EXISTS event_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER NOT NULL,
    origin_node_id INTEGER NOT NULL,
    value BLOB NOT NULL,
    UNIQUE(origin_node_id, event_id)
);

CREATE TABLE IF NOT EXISTS peer_ack_cursors (
    peer_node_id INTEGER NOT NULL,
    origin_node_id INTEGER NOT NULL,
    acked_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at_hlc TEXT NOT NULL,
    PRIMARY KEY(peer_node_id, origin_node_id)
);

CREATE TABLE IF NOT EXISTS origin_cursors (
    origin_node_id INTEGER PRIMARY KEY,
    applied_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at_hlc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_events (
    event_id INTEGER NOT NULL,
    source_node_id INTEGER NOT NULL,
    applied_at_hlc TEXT NOT NULL,
    PRIMARY KEY(source_node_id, event_id)
);

CREATE TABLE IF NOT EXISTS user_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    loser_node_id INTEGER NOT NULL,
    loser_user_id INTEGER NOT NULL,
    winner_node_id INTEGER NOT NULL,
    winner_user_id INTEGER NOT NULL,
    username TEXT NOT NULL,
    detected_at_hlc TEXT NOT NULL,
    resolved_by_origin_node_id INTEGER,
    resolved_by_event_id INTEGER
);

CREATE TABLE IF NOT EXISTS tombstones (
    entity_type TEXT NOT NULL,
    entity_node_id INTEGER NOT NULL,
    entity_id INTEGER NOT NULL,
    deleted_at_hlc TEXT NOT NULL,
    expires_at_hlc TEXT,
    origin_node_id INTEGER NOT NULL,
    PRIMARY KEY(entity_type, entity_node_id, entity_id)
);

CREATE TABLE IF NOT EXISTS message_trim_stats (
    scope TEXT PRIMARY KEY,
    trimmed_total INTEGER NOT NULL DEFAULT 0,
    last_trimmed_at_hlc TEXT
);

CREATE TABLE IF NOT EXISTS message_sequence_counters (
    user_node_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    node_id INTEGER NOT NULL,
    next_seq INTEGER NOT NULL,
    PRIMARY KEY(user_node_id, user_id, node_id)
);

CREATE TABLE IF NOT EXISTS pending_projections (
    origin_node_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_node_id INTEGER NOT NULL,
    aggregate_id INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL,
    first_failed_at_hlc TEXT NOT NULL,
    last_failed_at_hlc TEXT NOT NULL,
    PRIMARY KEY(origin_node_id, event_id)
);
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init schema: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
CREATE INDEX IF NOT EXISTS idx_messages_user_created ON messages(user_node_id, user_id, created_at_hlc DESC, node_id ASC, seq DESC);
CREATE INDEX IF NOT EXISTS idx_messages_node ON messages(node_id, user_node_id, user_id, seq);
CREATE INDEX IF NOT EXISTS idx_subscriptions_subscriber ON channel_subscriptions(subscriber_node_id, subscriber_user_id, deleted_at_hlc);
CREATE INDEX IF NOT EXISTS idx_subscriptions_channel ON channel_subscriptions(channel_node_id, channel_user_id, deleted_at_hlc);
CREATE INDEX IF NOT EXISTS idx_event_log_origin_event ON event_log(origin_node_id, event_id);
CREATE INDEX IF NOT EXISTS idx_pending_projections_failed ON pending_projections(last_failed_at_hlc, origin_node_id, event_id);
`); err != nil {
		return fmt.Errorf("init message indexes: %w", err)
	}

	if err := s.validateSchemaVersion(ctx); err != nil {
		return err
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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit node identity init: %w", err)
	}

	s.nodeID = nodeID
	s.ids = clock.NewIDGenerator()
	if s.clock == nil {
		s.clock = clock.NewClock(nodeID)
	}
	s.messageTrim = &sqliteMessageTrimRepository{db: s.db, clock: s.clock}
	switch s.engine {
	case EngineSQLite:
		s.messageProjection = &sqliteMessageProjectionRepository{
			db:                s.db,
			clock:             s.clock,
			messageWindowSize: s.messageWindowSize,
			userRepository:    s.userRepository,
		}
		s.eventLog = &sqliteEventLogRepository{
			db:     s.db,
			ids:    s.ids,
			nodeID: s.nodeID,
			clock:  s.clock,
		}
	case EnginePebble:
		s.messageProjection = &pebbleMessageProjectionRepository{
			db:                s.pebbleDB,
			messageWindowSize: s.messageWindowSize,
			userRepository:    s.userRepository,
			subscriptions:     s.subscriptions,
			messageTrim:       s.messageTrim,
		}
		s.eventLog = &pebbleEventLogRepository{
			db:     s.pebbleDB,
			ids:    s.ids,
			nodeID: s.nodeID,
			clock:  s.clock,
		}
	default:
		return fmt.Errorf("%w: unsupported store engine %q", ErrInvalidInput, s.engine)
	}
	return nil
}

func normalizeEngine(engine string) string {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "", EngineSQLite:
		return EngineSQLite
	case EnginePebble:
		return EnginePebble
	default:
		return ""
	}
}

func (s *Store) validateSchemaVersion(ctx context.Context) error {
	var raw string
	err := s.db.QueryRowContext(ctx, `
SELECT value
FROM schema_meta
WHERE key = 'schema_version'
`).Scan(&raw)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("read schema version: %w", err)
	}

	version := strings.TrimSpace(raw)
	if version == "" || version == defaultSchemaVersion {
		return nil
	}
	return fmt.Errorf("unsupported schema version %q: rebuild the database with schema version %s", version, defaultSchemaVersion)
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

	profile := strings.TrimSpace(params.Profile)
	if profile == "" {
		profile = "{}"
	}
	role, err := normalizeMutableRole(params.Role)
	if err != nil {
		return User{}, Event{}, err
	}
	passwordHash := strings.TrimSpace(params.PasswordHash)
	if isLoginRole(role) && passwordHash == "" {
		return User{}, Event{}, fmt.Errorf("%w: password hash cannot be empty", ErrInvalidInput)
	}
	if !isLoginRole(role) {
		passwordHash = disabledPasswordHash
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("begin create user: %w", err)
	}
	defer tx.Rollback()

	now := s.clock.Now()
	userID, err := s.nextUserIDTx(ctx, tx, s.nodeID)
	if err != nil {
		return User{}, Event{}, err
	}
	user := User{
		NodeID:              s.nodeID,
		ID:                  userID,
		Username:            username,
		PasswordHash:        passwordHash,
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
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.NodeID, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
		user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
		user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
		user.OriginNodeID); err != nil {
		return User{}, Event{}, fmt.Errorf("insert user: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserCreated,
		Aggregate:       "user",
		AggregateNodeID: user.NodeID,
		AggregateID:     user.ID,
		HLC:             now,
		Body:            userCreatedProtoFromUser(user),
	})
	if err != nil {
		return User{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return User{}, Event{}, fmt.Errorf("commit create user: %w", err)
	}
	s.cacheUser(user)
	return user, event, nil
}

func (s *Store) UpdateUser(ctx context.Context, params UpdateUserParams) (User, Event, error) {
	if err := params.Key.Validate(); err != nil {
		return User{}, Event{}, err
	}
	if params.Username == nil && params.PasswordHash == nil && params.Profile == nil && params.Role == nil {
		return User{}, Event{}, fmt.Errorf("%w: at least one field must be updated", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return User{}, Event{}, fmt.Errorf("begin update user: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getUserTx(ctx, tx, params.Key, false)
	if err != nil {
		return User{}, Event{}, err
	}
	if current.isProtectedBroadcastUser() {
		return User{}, Event{}, fmt.Errorf("%w: broadcast user cannot be updated", ErrForbidden)
	}
	if current.isProtectedNodeIngressUser() {
		return User{}, Event{}, fmt.Errorf("%w: node ingress user cannot be updated", ErrForbidden)
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
		if !current.CanLogin() {
			return User{}, Event{}, fmt.Errorf("%w: channel users cannot set passwords", ErrForbidden)
		}
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
			if isLoginRole(nextRole) && !isLoginRole(current.Role) && strings.TrimSpace(current.PasswordHash) == disabledPasswordHash {
				return User{}, Event{}, fmt.Errorf("%w: password hash is required before enabling login", ErrInvalidInput)
			}
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
WHERE node_id = ? AND user_id = ? AND deleted_at_hlc IS NULL
`, current.Username, current.PasswordHash, current.Profile, current.Role, boolToInt(current.SystemReserved),
		current.UpdatedAt.String(), current.VersionUsername.String(), current.VersionPasswordHash.String(),
		current.VersionProfile.String(), current.VersionRole.String(), current.NodeID, current.ID); err != nil {
		return User{}, Event{}, fmt.Errorf("update user: %w", err)
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserUpdated,
		Aggregate:       "user",
		AggregateNodeID: current.NodeID,
		AggregateID:     current.ID,
		HLC:             now,
		Body:            userUpdatedProtoFromUser(current),
	})
	if err != nil {
		return User{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return User{}, Event{}, fmt.Errorf("commit update user: %w", err)
	}
	s.cacheUser(current)
	return current, event, nil
}

func (s *Store) DeleteUser(ctx context.Context, key UserKey) (Event, error) {
	if err := key.Validate(); err != nil {
		return Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, fmt.Errorf("begin delete user: %w", err)
	}
	defer tx.Rollback()

	user, err := s.getUserTx(ctx, tx, key, false)
	if err != nil {
		return Event{}, err
	}
	if user.isProtectedBootstrapAdmin() {
		return Event{}, fmt.Errorf("%w: bootstrap admin cannot be deleted", ErrForbidden)
	}
	if user.isProtectedBroadcastUser() {
		return Event{}, fmt.Errorf("%w: broadcast user cannot be deleted", ErrForbidden)
	}
	if user.isProtectedNodeIngressUser() {
		return Event{}, fmt.Errorf("%w: node ingress user cannot be deleted", ErrForbidden)
	}

	now := s.clock.Now()
	if err := s.applyUserDeleteTx(ctx, tx, key, now, s.nodeID, true); err != nil {
		return Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserDeleted,
		Aggregate:       "user",
		AggregateNodeID: key.NodeID,
		AggregateID:     key.UserID,
		HLC:             now,
		Body:            userDeletedProtoFromKey(key, now),
	})
	if err != nil {
		return Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return Event{}, fmt.Errorf("commit delete user: %w", err)
	}
	s.invalidateCachedUser(key)
	return event, nil
}

func (s *Store) GetUser(ctx context.Context, key UserKey) (User, error) {
	return s.userRepository.GetUser(ctx, key, false)
}

func (s *Store) ListUsers(ctx context.Context) ([]User, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
FROM users
WHERE deleted_at_hlc IS NULL
ORDER BY node_id ASC, user_id ASC
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
	if err := params.UserKey.Validate(); err != nil {
		return Message{}, Event{}, err
	}
	if err := params.Sender.Validate(); err != nil {
		return Message{}, Event{}, fmt.Errorf("%w: sender cannot be empty", ErrInvalidInput)
	}
	if len(params.Body) == 0 {
		return Message{}, Event{}, fmt.Errorf("%w: body cannot be empty", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Message{}, Event{}, fmt.Errorf("begin create message: %w", err)
	}
	defer tx.Rollback()

	if _, err := s.getUserTx(ctx, tx, params.UserKey, false); err != nil {
		return Message{}, Event{}, err
	}

	now := s.clock.Now()
	seq, err := s.nextMessageSeqTx(ctx, tx, params.UserKey, s.nodeID)
	if err != nil {
		return Message{}, Event{}, err
	}
	message := Message{
		Recipient: params.UserKey,
		NodeID:    s.nodeID,
		Seq:       seq,
		Sender:    params.Sender,
		Body:      append([]byte(nil), params.Body...),
		CreatedAt: now,
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeMessageCreated,
		Aggregate:       "message",
		AggregateNodeID: message.NodeID,
		AggregateID:     message.Seq,
		HLC:             now,
		Body:            messageCreatedProtoFromMessage(message),
	})
	if err != nil {
		return Message{}, Event{}, err
	}

	if err := tx.Commit(); err != nil {
		return Message{}, Event{}, fmt.Errorf("commit create message: %w", err)
	}
	if err := s.projectMessageEvent(ctx, event); err != nil {
		if recordErr := s.recordPendingProjection(ctx, event, err); recordErr != nil {
			return Message{}, Event{}, fmt.Errorf("record deferred message projection: %w", recordErr)
		}
		return message, event, fmt.Errorf("%w: %v", ErrProjectionDeferred, err)
	}
	if err := s.clearPendingProjection(ctx, event.OriginNodeID, event.EventID); err != nil {
		return Message{}, Event{}, err
	}
	return message, event, nil
}

func (s *Store) ListMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	return s.messageProjection.ListMessagesByUser(ctx, key, limit)
}

func (s *Store) SubscribeChannel(ctx context.Context, params ChannelSubscriptionParams) (Subscription, Event, error) {
	if err := params.Subscriber.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}
	if err := params.Channel.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}
	if params.Subscriber == params.Channel {
		return Subscription{}, Event{}, fmt.Errorf("%w: subscriber cannot subscribe to itself", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Subscription{}, Event{}, fmt.Errorf("begin subscribe channel: %w", err)
	}
	defer tx.Rollback()

	if err := s.validateSubscriptionUsersTx(ctx, tx, params.Subscriber, params.Channel); err != nil {
		return Subscription{}, Event{}, err
	}

	now := s.clock.Now()
	subscription := Subscription{
		Subscriber:   params.Subscriber,
		Channel:      params.Channel,
		SubscribedAt: now,
		OriginNodeID: s.nodeID,
	}
	if err := s.upsertSubscriptionTx(ctx, tx, subscription); err != nil {
		return Subscription{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeChannelSubscribed,
		Aggregate:       "subscription",
		AggregateNodeID: params.Subscriber.NodeID,
		AggregateID:     params.Subscriber.UserID,
		HLC:             now,
		Body:            channelSubscribedProtoFromSubscription(subscription),
	})
	if err != nil {
		return Subscription{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return Subscription{}, Event{}, fmt.Errorf("commit subscribe channel: %w", err)
	}
	return subscription, event, nil
}

func (s *Store) UnsubscribeChannel(ctx context.Context, params ChannelSubscriptionParams) (Subscription, Event, error) {
	if err := params.Subscriber.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}
	if err := params.Channel.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Subscription{}, Event{}, fmt.Errorf("begin unsubscribe channel: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getSubscriptionTx(ctx, tx, params.Subscriber, params.Channel)
	if err != nil {
		return Subscription{}, Event{}, err
	}
	if current.DeletedAt != nil {
		return Subscription{}, Event{}, ErrNotFound
	}

	now := s.clock.Now()
	current.DeletedAt = &now
	current.OriginNodeID = s.nodeID
	if err := s.upsertSubscriptionTx(ctx, tx, current); err != nil {
		return Subscription{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeChannelUnsubscribed,
		Aggregate:       "subscription",
		AggregateNodeID: params.Subscriber.NodeID,
		AggregateID:     params.Subscriber.UserID,
		HLC:             now,
		Body:            channelUnsubscribedProtoFromSubscription(current),
	})
	if err != nil {
		return Subscription{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return Subscription{}, Event{}, fmt.Errorf("commit unsubscribe channel: %w", err)
	}
	return current, event, nil
}

func (s *Store) ListChannelSubscriptions(ctx context.Context, subscriber UserKey) ([]Subscription, error) {
	if err := subscriber.Validate(); err != nil {
		return nil, err
	}
	if _, err := s.GetUser(ctx, subscriber); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id, subscribed_at_hlc, deleted_at_hlc, origin_node_id
FROM channel_subscriptions
WHERE subscriber_node_id = ? AND subscriber_user_id = ? AND deleted_at_hlc IS NULL
ORDER BY channel_node_id ASC, channel_user_id ASC
`, subscriber.NodeID, subscriber.UserID)
	if err != nil {
		return nil, fmt.Errorf("list channel subscriptions: %w", err)
	}
	defer rows.Close()

	var subscriptions []Subscription
	for rows.Next() {
		subscription, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel subscriptions: %w", err)
	}
	return subscriptions, nil
}

func (s *Store) IsSubscribedToChannel(ctx context.Context, subscriber, channel UserKey) (bool, error) {
	if err := subscriber.Validate(); err != nil {
		return false, err
	}
	if err := channel.Validate(); err != nil {
		return false, err
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM channel_subscriptions
WHERE subscriber_node_id = ? AND subscriber_user_id = ?
  AND channel_node_id = ? AND channel_user_id = ?
  AND deleted_at_hlc IS NULL
`, subscriber.NodeID, subscriber.UserID, channel.NodeID, channel.UserID).Scan(&count); err != nil {
		return false, fmt.Errorf("check channel subscription: %w", err)
	}
	return count > 0, nil
}

func (s *Store) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	return s.eventLog.ListEvents(ctx, afterSequence, limit)
}

func (s *Store) getUser(ctx context.Context, key UserKey, includeDeleted bool) (User, error) {
	return s.userRepository.GetUser(ctx, key, includeDeleted)
}

func (s *Store) getUserTx(ctx context.Context, tx *sql.Tx, key UserKey, includeDeleted bool) (User, error) {
	return s.userRepository.GetUserTx(ctx, tx, key, includeDeleted)
}

func (s *Store) cacheUser(user User) {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.StoreUser(user)
	}
}

func (s *Store) invalidateCachedUser(key UserKey) {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.InvalidateUser(key)
	}
}

func (s *Store) invalidateUserCache() {
	if cache, ok := s.userRepository.(*cachedUserRepository); ok {
		cache.InvalidateAll()
	}
}

func (s *Store) insertEvent(ctx context.Context, tx *sql.Tx, event Event) (Event, error) {
	if s.engine == EnginePebble {
		return s.eventLog.Append(ctx, event)
	}

	event.EventID = s.ids.Next()
	event.OriginNodeID = s.nodeID

	value, err := eventLogValue(event)
	if err != nil {
		return Event{}, err
	}

	result, err := tx.ExecContext(ctx, `
INSERT INTO event_log(event_id, origin_node_id, value)
VALUES(?, ?, ?)
`, event.EventID, event.OriginNodeID, value)
	if err != nil {
		return Event{}, fmt.Errorf("insert event: %w", err)
	}

	event.Sequence, err = result.LastInsertId()
	if err != nil {
		return Event{}, fmt.Errorf("read event sequence: %w", err)
	}
	if err := s.upsertOriginCursorTx(ctx, tx, event.OriginNodeID, event.EventID, s.clock.Now().String()); err != nil {
		return Event{}, fmt.Errorf("record local origin cursor: %w", err)
	}
	return event, nil
}

func (s *Store) nextUserIDTx(ctx context.Context, tx *sql.Tx, nodeID int64) (int64, error) {
	if nodeID <= 0 {
		return 0, fmt.Errorf("%w: node id is required for user sequence", ErrInvalidInput)
	}
	var userID int64
	if err := tx.QueryRowContext(ctx, `
SELECT CASE
    WHEN COALESCE(MAX(user_id), 0) < ? THEN ?
    ELSE MAX(user_id)
END + 1
FROM users
WHERE node_id = ?
`, ReservedUserIDMax, ReservedUserIDMax, nodeID).Scan(&userID); err != nil {
		return 0, fmt.Errorf("read next user id: %w", err)
	}
	return userID, nil
}

func (s *Store) nextMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	if err := key.Validate(); err != nil {
		return 0, err
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("%w: user id and node id are required for message sequence", ErrInvalidInput)
	}

	var seq int64
	err := tx.QueryRowContext(ctx, `
SELECT next_seq
FROM message_sequence_counters
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`, key.NodeID, key.UserID, nodeID).Scan(&seq)
	switch {
	case err == nil:
	case errors.Is(err, sql.ErrNoRows):
		if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(seq), 0) + 1
FROM messages
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`, key.NodeID, key.UserID, nodeID).Scan(&seq); err != nil {
			return 0, fmt.Errorf("seed next message sequence: %w", err)
		}
	default:
		return 0, fmt.Errorf("read next message sequence: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO message_sequence_counters(user_node_id, user_id, node_id, next_seq)
VALUES(?, ?, ?, ?)
ON CONFLICT(user_node_id, user_id, node_id) DO UPDATE SET next_seq = excluded.next_seq
`, key.NodeID, key.UserID, nodeID, seq+1); err != nil {
		return 0, fmt.Errorf("store next message sequence: %w", err)
	}
	return seq, nil
}

func (s *Store) validateSubscriptionUsersTx(ctx context.Context, tx *sql.Tx, subscriberKey, channelKey UserKey) error {
	subscriber, err := s.getUserByIDTx(ctx, tx, subscriberKey, false)
	if err != nil {
		return err
	}
	if !subscriber.CanLogin() {
		return fmt.Errorf("%w: subscriber must be a login user", ErrInvalidInput)
	}
	channel, err := s.getUserByIDTx(ctx, tx, channelKey, false)
	if err != nil {
		return err
	}
	if channel.Role != RoleChannel {
		return fmt.Errorf("%w: subscription target must be a channel", ErrInvalidInput)
	}
	return nil
}

func (s *Store) getSubscriptionTx(ctx context.Context, tx *sql.Tx, subscriber, channel UserKey) (Subscription, error) {
	row := tx.QueryRowContext(ctx, `
SELECT subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id, subscribed_at_hlc, deleted_at_hlc, origin_node_id
FROM channel_subscriptions
WHERE subscriber_node_id = ? AND subscriber_user_id = ? AND channel_node_id = ? AND channel_user_id = ?
`, subscriber.NodeID, subscriber.UserID, channel.NodeID, channel.UserID)
	subscription, err := scanSubscription(row)
	if err == sql.ErrNoRows {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, err
	}
	return subscription, nil
}

func (s *Store) upsertSubscriptionTx(ctx context.Context, tx *sql.Tx, subscription Subscription) error {
	if err := subscription.Subscriber.Validate(); err != nil {
		return err
	}
	if err := subscription.Channel.Validate(); err != nil {
		return err
	}
	if subscription.SubscribedAt == (clock.Timestamp{}) {
		return fmt.Errorf("%w: subscribed_at is required", ErrInvalidInput)
	}
	if subscription.OriginNodeID <= 0 {
		return fmt.Errorf("%w: subscription origin node id is required", ErrInvalidInput)
	}

	deletedAt := nullableTimestampString(subscription.DeletedAt)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_subscriptions(
    subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id,
    subscribed_at_hlc, deleted_at_hlc, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id) DO UPDATE SET
    subscribed_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.subscribed_at_hlc > channel_subscriptions.deleted_at_hlc
        ) AND excluded.subscribed_at_hlc > channel_subscriptions.subscribed_at_hlc THEN excluded.subscribed_at_hlc
        ELSE channel_subscriptions.subscribed_at_hlc
    END,
    deleted_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.subscribed_at_hlc > channel_subscriptions.deleted_at_hlc
        ) THEN NULL
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > channel_subscriptions.deleted_at_hlc
        ) AND excluded.deleted_at_hlc >= channel_subscriptions.subscribed_at_hlc THEN excluded.deleted_at_hlc
        ELSE channel_subscriptions.deleted_at_hlc
    END,
    origin_node_id = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.subscribed_at_hlc > channel_subscriptions.deleted_at_hlc
        ) AND excluded.subscribed_at_hlc >= channel_subscriptions.subscribed_at_hlc THEN excluded.origin_node_id
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > channel_subscriptions.deleted_at_hlc
        ) THEN excluded.origin_node_id
        ELSE channel_subscriptions.origin_node_id
    END
`, subscription.Subscriber.NodeID, subscription.Subscriber.UserID, subscription.Channel.NodeID, subscription.Channel.UserID,
		subscription.SubscribedAt.String(), deletedAt, subscription.OriginNodeID); err != nil {
		return fmt.Errorf("upsert channel subscription: %w", err)
	}
	return nil
}

func validateMessageIdentity(key UserKey, nodeID int64, seq int64) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if nodeID <= 0 || seq <= 0 {
		return fmt.Errorf("%w: user id, node id, and seq are required for message", ErrInvalidInput)
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
	EntityNodeID int64
	EntityID     int64
	DeletedAt    clock.Timestamp
	OriginNodeID int64
}

func (s *Store) getTombstoneTx(ctx context.Context, tx *sql.Tx, entityType string, key UserKey) (tombstoneRecord, bool, error) {
	if err := key.Validate(); err != nil {
		return tombstoneRecord{}, false, err
	}
	row := tx.QueryRowContext(ctx, `
SELECT entity_type, entity_node_id, entity_id, deleted_at_hlc, origin_node_id
FROM tombstones
WHERE entity_type = ? AND entity_node_id = ? AND entity_id = ?
`, entityType, key.NodeID, key.UserID)

	var record tombstoneRecord
	var deletedAtRaw string
	if err := row.Scan(&record.EntityType, &record.EntityNodeID, &record.EntityID, &deletedAtRaw, &record.OriginNodeID); err != nil {
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

func (s *Store) upsertTombstoneTx(ctx context.Context, tx *sql.Tx, entityType string, key UserKey, deletedAt clock.Timestamp, originNodeID int64) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tombstones(entity_type, entity_node_id, entity_id, deleted_at_hlc, expires_at_hlc, origin_node_id)
VALUES(?, ?, ?, ?, NULL, ?)
ON CONFLICT(entity_type, entity_node_id, entity_id) DO UPDATE SET
    deleted_at_hlc = CASE
        WHEN excluded.deleted_at_hlc > tombstones.deleted_at_hlc THEN excluded.deleted_at_hlc
        ELSE tombstones.deleted_at_hlc
    END,
    origin_node_id = CASE
        WHEN excluded.deleted_at_hlc > tombstones.deleted_at_hlc THEN excluded.origin_node_id
        ELSE tombstones.origin_node_id
    END
`, entityType, key.NodeID, key.UserID, deletedAt.String(), originNodeID); err != nil {
		return fmt.Errorf("upsert tombstone: %w", err)
	}
	return nil
}

func (s *Store) applyUserDeleteTx(ctx context.Context, tx *sql.Tx, key UserKey, deletedAt clock.Timestamp, originNodeID int64, requireActive bool) error {
	user, err := s.getUserByIDTx(ctx, tx, key, true)
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
		if user.isProtectedBroadcastUser() {
			if requireActive {
				return fmt.Errorf("%w: broadcast user cannot be deleted", ErrForbidden)
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
WHERE node_id = ? AND user_id = ?
`, deletedAt.String(), updatedAt.String(), deletedAt.String(), key.NodeID, key.UserID); err != nil {
				return fmt.Errorf("delete user: %w", err)
			}
		}
	}

	if err := s.upsertTombstoneTx(ctx, tx, "user", key, deletedAt, originNodeID); err != nil {
		return err
	}
	if err := s.reconcileBootstrapAdminsTx(ctx, tx); err != nil {
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
		&user.NodeID,
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
		&message.Recipient.NodeID,
		&message.Recipient.UserID,
		&message.NodeID,
		&message.Seq,
		&message.Sender.NodeID,
		&message.Sender.UserID,
		&message.Body,
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

func scanSubscription(scanner interface {
	Scan(dest ...any) error
}) (Subscription, error) {
	var subscription Subscription
	var subscribedAtRaw string
	var deletedAtRaw sql.NullString

	if err := scanner.Scan(
		&subscription.Subscriber.NodeID,
		&subscription.Subscriber.UserID,
		&subscription.Channel.NodeID,
		&subscription.Channel.UserID,
		&subscribedAtRaw,
		&deletedAtRaw,
		&subscription.OriginNodeID,
	); err != nil {
		return Subscription{}, err
	}

	subscribedAt, err := clock.ParseTimestamp(subscribedAtRaw)
	if err != nil {
		return Subscription{}, fmt.Errorf("parse subscription subscribed_at: %w", err)
	}
	subscription.SubscribedAt = subscribedAt
	if deletedAtRaw.Valid {
		deletedAt, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return Subscription{}, fmt.Errorf("parse subscription deleted_at: %w", err)
		}
		subscription.DeletedAt = &deletedAt
	}
	return subscription, nil
}

func scanEvent(scanner interface {
	Scan(dest ...any) error
}) (Event, error) {
	var sequence int64
	var eventID int64
	var originNodeID int64
	var value []byte

	if err := scanner.Scan(
		&sequence,
		&eventID,
		&originNodeID,
		&value,
	); err != nil {
		return Event{}, err
	}

	var replicated internalproto.ReplicatedEvent
	if err := gproto.Unmarshal(value, &replicated); err != nil {
		return Event{}, fmt.Errorf("unmarshal event value: %w", err)
	}

	event, err := eventFromReplicatedEvent(&replicated)
	if err != nil {
		return Event{}, err
	}
	event.Sequence = sequence
	event.EventID = eventID
	event.OriginNodeID = originNodeID
	return event, nil
}

func eventLogValue(event Event) ([]byte, error) {
	replicated := ToReplicatedEvent(event)
	if replicated == nil {
		return nil, fmt.Errorf("%w: event cannot be marshaled", ErrInvalidInput)
	}
	value, err := gproto.Marshal(replicated)
	if err != nil {
		return nil, fmt.Errorf("marshal event value: %w", err)
	}
	return value, nil
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

func (s *Store) AuthenticateUser(ctx context.Context, key UserKey, password string) (User, error) {
	if err := key.Validate(); err != nil {
		return User{}, err
	}
	if strings.TrimSpace(password) == "" {
		return User{}, fmt.Errorf("%w: password cannot be empty", ErrInvalidInput)
	}
	user, err := s.GetUser(ctx, key)
	if err != nil {
		return User{}, err
	}
	if !user.CanLogin() {
		return User{}, ErrNotFound
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
	key := UserKey{NodeID: s.nodeID, UserID: BootstrapAdminUserID}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?`, key.NodeID, key.UserID); err != nil {
		return fmt.Errorf("delete bootstrap admin tombstone: %w", err)
	}

	current, err := s.getUserTx(ctx, tx, key, true)
	switch {
	case errors.Is(err, ErrNotFound):
		user := User{
			NodeID:              s.nodeID,
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
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.NodeID, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
			user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
			user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
			user.OriginNodeID); err != nil {
			return fmt.Errorf("insert bootstrap admin: %w", err)
		}
		if _, err := s.insertEvent(ctx, tx, Event{
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: user.NodeID,
			AggregateID:     user.ID,
			HLC:             now,
			Body:            userCreatedProtoFromUser(user),
		}); err != nil {
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
WHERE node_id = ? AND user_id = ?
`, updated.Username, updated.PasswordHash, updated.Profile, updated.Role, boolToInt(updated.SystemReserved),
				updated.CreatedAt.String(), updated.UpdatedAt.String(), updated.VersionUsername.String(),
				updated.VersionPasswordHash.String(), updated.VersionProfile.String(), updated.VersionRole.String(),
				updated.OriginNodeID, updated.NodeID, updated.ID); err != nil {
				return fmt.Errorf("repair bootstrap admin: %w", err)
			}
			if _, err := s.insertEvent(ctx, tx, Event{
				EventType:       EventTypeUserUpdated,
				Aggregate:       "user",
				AggregateNodeID: updated.NodeID,
				AggregateID:     updated.ID,
				HLC:             updated.UpdatedAt,
				Body:            userUpdatedProtoFromUser(updated),
			}); err != nil {
				return err
			}
		}
	}
	if err := s.reconcileBootstrapAdminsTx(ctx, tx); err != nil {
		return err
	}
	if err := s.ensureBroadcastUserTx(ctx, tx, now); err != nil {
		return err
	}
	if err := s.ensureNodeIngressUserTx(ctx, tx, now); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit ensure bootstrap admin: %w", err)
	}
	s.invalidateUserCache()
	return nil
}

func normalizeAnyRole(role string) (string, error) {
	normalized := strings.TrimSpace(role)
	if normalized == "" {
		return RoleUser, nil
	}
	switch normalized {
	case RoleSuperAdmin, RoleAdmin, RoleUser, RoleChannel, RoleBroadcast, RoleNode:
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
	if normalized == RoleBroadcast {
		return "", fmt.Errorf("%w: role %q cannot be assigned through this API", ErrInvalidInput, role)
	}
	if normalized == RoleNode {
		return "", fmt.Errorf("%w: role %q cannot be assigned through this API", ErrInvalidInput, role)
	}
	return normalized, nil
}

func isLoginRole(role string) bool {
	return role != RoleChannel && role != RoleBroadcast && role != RoleNode
}

func (s *Store) applyReservedUserInvariants(user User) User {
	if user.ID == BroadcastUserID && user.SystemReserved {
		user.Role = RoleBroadcast
		user.SystemReserved = true
		user.PasswordHash = disabledPasswordHash
		return user
	}
	if user.ID == NodeIngressUserID && user.SystemReserved {
		user.Role = RoleNode
		user.SystemReserved = true
		user.PasswordHash = disabledPasswordHash
		return user
	}
	if user.ID != BootstrapAdminUserID || !user.SystemReserved {
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

func (u User) isProtectedBroadcastUser() bool {
	return u.ID == BroadcastUserID && u.SystemReserved && u.Role == RoleBroadcast
}

func (u User) isProtectedNodeIngressUser() bool {
	return u.ID == NodeIngressUserID && u.SystemReserved && u.Role == RoleNode
}

func (s *Store) ensureBroadcastUserTx(ctx context.Context, tx *sql.Tx, now clock.Timestamp) error {
	key := UserKey{NodeID: s.nodeID, UserID: BroadcastUserID}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?`, key.NodeID, key.UserID); err != nil {
		return fmt.Errorf("delete broadcast user tombstone: %w", err)
	}

	current, err := s.getUserTx(ctx, tx, key, true)
	switch {
	case errors.Is(err, ErrNotFound):
		user := User{
			NodeID:              s.nodeID,
			ID:                  BroadcastUserID,
			Username:            "broadcast",
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleBroadcast,
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
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.NodeID, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
			user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
			user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
			user.OriginNodeID); err != nil {
			return fmt.Errorf("insert broadcast user: %w", err)
		}
		if _, err := s.insertEvent(ctx, tx, Event{
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: user.NodeID,
			AggregateID:     user.ID,
			HLC:             now,
			Body:            userCreatedProtoFromUser(user),
		}); err != nil {
			return err
		}
		return nil
	case err != nil:
		return err
	}

	changed := current.Username != "broadcast" ||
		current.PasswordHash != disabledPasswordHash ||
		current.Profile == "" ||
		current.Role != RoleBroadcast ||
		!current.SystemReserved ||
		current.DeletedAt != nil ||
		current.VersionDeleted != nil
	if !changed {
		return nil
	}

	updated := current
	updated.Username = "broadcast"
	updated.PasswordHash = disabledPasswordHash
	updated.Profile = defaultJSON(updated.Profile)
	updated.Role = RoleBroadcast
	updated.SystemReserved = true
	updated.DeletedAt = nil
	updated.VersionDeleted = nil
	updated.UpdatedAt = now
	updated.VersionUsername = now
	updated.VersionPasswordHash = now
	updated.VersionProfile = now
	updated.VersionRole = now
	updated.OriginNodeID = s.nodeID
	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, created_at_hlc = ?, updated_at_hlc = ?,
    deleted_at_hlc = NULL, version_username = ?, version_password_hash = ?, version_profile = ?,
    version_role = ?, version_deleted = NULL, origin_node_id = ?
WHERE node_id = ? AND user_id = ?
`, updated.Username, updated.PasswordHash, updated.Profile, updated.Role, boolToInt(updated.SystemReserved),
		updated.CreatedAt.String(), updated.UpdatedAt.String(), updated.VersionUsername.String(),
		updated.VersionPasswordHash.String(), updated.VersionProfile.String(), updated.VersionRole.String(),
		updated.OriginNodeID, updated.NodeID, updated.ID); err != nil {
		return fmt.Errorf("repair broadcast user: %w", err)
	}
	if _, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserUpdated,
		Aggregate:       "user",
		AggregateNodeID: updated.NodeID,
		AggregateID:     updated.ID,
		HLC:             updated.UpdatedAt,
		Body:            userUpdatedProtoFromUser(updated),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Store) ensureNodeIngressUserTx(ctx context.Context, tx *sql.Tx, now clock.Timestamp) error {
	key := UserKey{NodeID: s.nodeID, UserID: NodeIngressUserID}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tombstones WHERE entity_type = 'user' AND entity_node_id = ? AND entity_id = ?`, key.NodeID, key.UserID); err != nil {
		return fmt.Errorf("delete node ingress user tombstone: %w", err)
	}

	current, err := s.getUserTx(ctx, tx, key, true)
	switch {
	case errors.Is(err, ErrNotFound):
		user := User{
			NodeID:              s.nodeID,
			ID:                  NodeIngressUserID,
			Username:            "node",
			PasswordHash:        disabledPasswordHash,
			Profile:             "{}",
			Role:                RoleNode,
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
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, ?)
`, user.NodeID, user.ID, user.Username, user.PasswordHash, user.Profile, user.Role, boolToInt(user.SystemReserved),
			user.CreatedAt.String(), user.UpdatedAt.String(), user.VersionUsername.String(),
			user.VersionPasswordHash.String(), user.VersionProfile.String(), user.VersionRole.String(),
			user.OriginNodeID); err != nil {
			return fmt.Errorf("insert node ingress user: %w", err)
		}
		if _, err := s.insertEvent(ctx, tx, Event{
			EventType:       EventTypeUserCreated,
			Aggregate:       "user",
			AggregateNodeID: user.NodeID,
			AggregateID:     user.ID,
			HLC:             now,
			Body:            userCreatedProtoFromUser(user),
		}); err != nil {
			return err
		}
		return nil
	case err != nil:
		return err
	}

	changed := current.Username != "node" ||
		current.PasswordHash != disabledPasswordHash ||
		current.Profile == "" ||
		current.Role != RoleNode ||
		!current.SystemReserved ||
		current.DeletedAt != nil ||
		current.VersionDeleted != nil
	if !changed {
		return nil
	}

	updated := current
	updated.Username = "node"
	updated.PasswordHash = disabledPasswordHash
	updated.Profile = defaultJSON(updated.Profile)
	updated.Role = RoleNode
	updated.SystemReserved = true
	updated.DeletedAt = nil
	updated.VersionDeleted = nil
	updated.UpdatedAt = now
	updated.VersionUsername = now
	updated.VersionPasswordHash = now
	updated.VersionProfile = now
	updated.VersionRole = now
	updated.OriginNodeID = s.nodeID
	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, created_at_hlc = ?, updated_at_hlc = ?,
    deleted_at_hlc = NULL, version_username = ?, version_password_hash = ?, version_profile = ?,
    version_role = ?, version_deleted = NULL, origin_node_id = ?
WHERE node_id = ? AND user_id = ?
`, updated.Username, updated.PasswordHash, updated.Profile, updated.Role, boolToInt(updated.SystemReserved),
		updated.CreatedAt.String(), updated.UpdatedAt.String(), updated.VersionUsername.String(),
		updated.VersionPasswordHash.String(), updated.VersionProfile.String(), updated.VersionRole.String(),
		updated.OriginNodeID, updated.NodeID, updated.ID); err != nil {
		return fmt.Errorf("repair node ingress user: %w", err)
	}
	if _, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserUpdated,
		Aggregate:       "user",
		AggregateNodeID: updated.NodeID,
		AggregateID:     updated.ID,
		HLC:             updated.UpdatedAt,
		Body:            userUpdatedProtoFromUser(updated),
	}); err != nil {
		return err
	}
	return nil
}

func (s *Store) reconcileBootstrapAdminsTx(ctx context.Context, tx *sql.Tx) error {
	var minNodeID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT MIN(node_id)
FROM users
WHERE user_id = ? AND deleted_at_hlc IS NULL
`, BootstrapAdminUserID).Scan(&minNodeID); err != nil {
		return fmt.Errorf("find bootstrap admin owner: %w", err)
	}
	if !minNodeID.Valid {
		return nil
	}

	rows, err := tx.QueryContext(ctx, `
SELECT node_id, role, system_reserved, updated_at_hlc, version_role
FROM users
WHERE user_id = ? AND deleted_at_hlc IS NULL
  AND ((node_id = ? AND (role != ? OR system_reserved != 1))
       OR (node_id != ? AND (role = ? OR system_reserved != 0)))
ORDER BY node_id ASC
`, BootstrapAdminUserID, minNodeID.Int64, RoleSuperAdmin, minNodeID.Int64, RoleSuperAdmin)
	if err != nil {
		return fmt.Errorf("query bootstrap admins for reconciliation: %w", err)
	}
	defer rows.Close()

	type bootstrapAdminReconciliation struct {
		nodeID         int64
		role           string
		systemReserved bool
		updatedAt      clock.Timestamp
		versionRole    clock.Timestamp
	}
	reconciliations := make([]bootstrapAdminReconciliation, 0)
	for rows.Next() {
		var item bootstrapAdminReconciliation
		var systemReserved int
		var updatedAtRaw, versionRoleRaw string
		if err := rows.Scan(&item.nodeID, &item.role, &systemReserved, &updatedAtRaw, &versionRoleRaw); err != nil {
			return fmt.Errorf("scan bootstrap admin for reconciliation: %w", err)
		}
		item.systemReserved = systemReserved != 0
		item.updatedAt, err = clock.ParseTimestamp(updatedAtRaw)
		if err != nil {
			return fmt.Errorf("parse bootstrap admin updated_at: %w", err)
		}
		item.versionRole, err = clock.ParseTimestamp(versionRoleRaw)
		if err != nil {
			return fmt.Errorf("parse bootstrap admin version_role: %w", err)
		}
		reconciliations = append(reconciliations, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate bootstrap admins for reconciliation: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close bootstrap admin reconciliation rows: %w", err)
	}

	for _, item := range reconciliations {
		targetRole := item.role
		targetSystemReserved := item.systemReserved
		if item.nodeID == minNodeID.Int64 {
			targetRole = RoleSuperAdmin
			targetSystemReserved = true
		} else {
			if item.role == RoleSuperAdmin {
				targetRole = RoleUser
			}
			targetSystemReserved = false
		}

		roleChanged := targetRole != item.role
		updatedAt := nextUserInvariantTimestamp(item.updatedAt, item.versionRole)
		versionRole := item.versionRole
		if roleChanged {
			versionRole = updatedAt
		}

		if _, err := tx.ExecContext(ctx, `
UPDATE users
SET role = ?, system_reserved = ?, updated_at_hlc = ?, version_role = ?
WHERE node_id = ? AND user_id = ? AND deleted_at_hlc IS NULL
`, targetRole, boolToInt(targetSystemReserved), updatedAt.String(), versionRole.String(), item.nodeID, BootstrapAdminUserID); err != nil {
			return fmt.Errorf("reconcile bootstrap admin %d: %w", item.nodeID, err)
		}
	}
	return nil
}

func nextUserInvariantTimestamp(updatedAt, versionRole clock.Timestamp) clock.Timestamp {
	if versionRole.Compare(updatedAt) > 0 {
		return nextDeterministicTimestamp(versionRole)
	}
	return nextDeterministicTimestamp(updatedAt)
}

func nextDeterministicTimestamp(base clock.Timestamp) clock.Timestamp {
	next := base
	if next.Logical == ^uint16(0) {
		next.WallTimeMs++
		next.Logical = 0
		return next
	}
	next.Logical++
	return next
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (s *Store) trimMessagesForUserTx(ctx context.Context, tx *sql.Tx, key UserKey) error {
	if err := key.Validate(); err != nil {
		return err
	}
	windowSize := normalizeMessageWindowSize(s.messageWindowSize)
	result, err := tx.ExecContext(ctx, `
DELETE FROM messages
WHERE user_node_id = ? AND user_id = ?
  AND (node_id, seq) IN (
    SELECT node_id, seq
    FROM messages
    WHERE user_node_id = ? AND user_id = ?
    ORDER BY created_at_hlc DESC, node_id ASC, seq DESC
    LIMIT -1 OFFSET ?
  )
`, key.NodeID, key.UserID, key.NodeID, key.UserID, windowSize)
	if err != nil {
		return fmt.Errorf("trim messages for user %d:%d: %w", key.NodeID, key.UserID, err)
	}
	trimmed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count trimmed messages for user %d:%d: %w", key.NodeID, key.UserID, err)
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
