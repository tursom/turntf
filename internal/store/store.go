package store

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	sqlite3 "github.com/mattn/go-sqlite3"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

var (
	ErrConflict           = errors.New("conflict")
	ErrForbidden          = errors.New("forbidden")
	ErrNotFound           = errors.New("not found")
	ErrInvalidInput       = errors.New("invalid input")
	ErrBlockedByBlacklist = errors.New("blocked by blacklist")
)

const DefaultMessageWindowSize = 500
const DefaultEventLogMaxEventsPerOrigin = 100000

const (
	sqliteDriverName          = "turntf-sqlite3"
	sqliteMaxOpenConns        = 4
	sqliteMaxIdleConns        = 4
	sqliteBusyTimeoutMillis   = "5000"
	sqliteJournalMode         = "WAL"
	sqliteSynchronousMode     = "NORMAL"
	sqliteTransactionLockMode = "immediate"
	sqliteTempStoreMemoryPrag = "PRAGMA temp_store = MEMORY;"
)

var sqliteDriverOnce sync.Once

const (
	EngineSQLite = "sqlite"
	EnginePebble = "pebble"
)

type PebbleMessageSyncMode string

const (
	PebbleMessageSyncModeDefault   PebbleMessageSyncMode = ""
	PebbleMessageSyncModeNoSync    PebbleMessageSyncMode = "no_sync"
	PebbleMessageSyncModeForceSync PebbleMessageSyncMode = "force_sync"
)

type PebbleProfile string

const (
	PebbleProfileDefault    PebbleProfile = ""
	PebbleProfileBalanced   PebbleProfile = "balanced"
	PebbleProfileThroughput PebbleProfile = "throughput"
)

const (
	RoleSuperAdmin                      = "super_admin"
	RoleAdmin                           = "admin"
	RoleUser                            = "user"
	RoleChannel                         = "channel"
	RoleBroadcast                       = "broadcast"
	RoleNode                            = "node"
	BootstrapAdminUserID                = int64(1)
	BroadcastUserID                     = int64(2)
	NodeIngressUserID                   = int64(3)
	ReservedUserIDMax                   = int64(1024)
	defaultSchemaVersion                = "15"
	schemaMetaNodeIDKey                 = "node_id"
	schemaMetaMeshTopologyGenerationKey = "mesh_topology_generation"
)

const disabledPasswordHash = "!"

func isSystemReservedUserID(userID int64) bool {
	return userID == BootstrapAdminUserID || userID == BroadcastUserID || userID == NodeIngressUserID
}

type Options struct {
	// NodeID seeds schema_meta.node_id for deterministic tests; production leaves it empty.
	NodeID                     int64
	Engine                     string
	PebblePath                 string
	PebbleProfile              PebbleProfile
	PebbleMessageSyncMode      PebbleMessageSyncMode
	MessageWindowSize          int
	EventLogMaxEventsPerOrigin int
	Clock                      *clock.Clock
}

type Store struct {
	db                         *sql.DB
	backend                    storeBackend
	nodeID                     int64
	clock                      *clock.Clock
	ids                        *clock.IDGenerator
	initialNodeID              int64
	messageWindowSize          int
	eventLogMaxEventsPerOrigin int
	pebbleMessageSyncMode      PebbleMessageSyncMode
	bootstrapAdmin             BootstrapAdminConfig
	eventLog                   EventLogRepository
	userRepository             UserRepository
	attachments                AttachmentRepository
	subscriptions              SubscriptionRepository
	blacklists                 BlacklistRepository
	messageTrim                MessageTrimRepository
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

type AttachmentType string

const (
	AttachmentTypeChannelManager      AttachmentType = "channel_manager"
	AttachmentTypeChannelWriter       AttachmentType = "channel_writer"
	AttachmentTypeChannelSubscription AttachmentType = "channel_subscription"
	AttachmentTypeUserBlacklist       AttachmentType = "user_blacklist"
)

type Attachment struct {
	Owner        UserKey          `json:"owner"`
	Subject      UserKey          `json:"subject"`
	Type         AttachmentType   `json:"attachment_type"`
	ConfigJSON   string           `json:"config_json"`
	AttachedAt   clock.Timestamp  `json:"attached_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

type Subscription struct {
	Subscriber   UserKey          `json:"subscriber"`
	Channel      UserKey          `json:"channel"`
	SubscribedAt clock.Timestamp  `json:"subscribed_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

type BlacklistEntry struct {
	Owner        UserKey          `json:"owner"`
	Blocked      UserKey          `json:"blocked"`
	BlockedAt    clock.Timestamp  `json:"blocked_at"`
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
	UserKey               UserKey
	Sender                UserKey
	Body                  []byte
	PebbleMessageSyncMode PebbleMessageSyncMode
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

func NormalizePebbleMessageSyncMode(raw string) (PebbleMessageSyncMode, error) {
	switch PebbleMessageSyncMode(strings.ToLower(strings.TrimSpace(raw))) {
	case PebbleMessageSyncModeDefault:
		return PebbleMessageSyncModeDefault, nil
	case PebbleMessageSyncModeNoSync:
		return PebbleMessageSyncModeNoSync, nil
	case PebbleMessageSyncModeForceSync:
		return PebbleMessageSyncModeForceSync, nil
	default:
		return "", fmt.Errorf("%w: unsupported pebble message sync mode %q", ErrInvalidInput, raw)
	}
}

func NormalizePebbleProfile(raw string) (PebbleProfile, error) {
	switch PebbleProfile(strings.ToLower(strings.TrimSpace(raw))) {
	case PebbleProfileDefault:
		return PebbleProfileDefault, nil
	case PebbleProfileBalanced:
		return PebbleProfileBalanced, nil
	case PebbleProfileThroughput:
		return PebbleProfileThroughput, nil
	default:
		return "", fmt.Errorf("%w: unsupported pebble profile %q", ErrInvalidInput, raw)
	}
}

type ChannelSubscriptionParams struct {
	Subscriber UserKey
	Channel    UserKey
}

type BlacklistParams struct {
	Owner   UserKey
	Blocked UserKey
}

type UpsertAttachmentParams struct {
	Owner      UserKey
	Subject    UserKey
	Type       AttachmentType
	ConfigJSON string
}

type DeleteAttachmentParams struct {
	Owner   UserKey
	Subject UserKey
	Type    AttachmentType
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
	if _, err := NormalizePebbleProfile(string(opts.PebbleProfile)); err != nil {
		return nil, err
	}
	pebbleProfile := normalizePebbleProfileOption(opts.PebbleProfile)
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}

	db, err := sql.Open(sqliteDriverName, sqliteDSN(dbPath))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(sqliteMaxOpenConns)
	db.SetMaxIdleConns(sqliteMaxIdleConns)

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
		pebbleDB, err = openPebbleDB(pebblePath, pebbleProfile)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("open pebble: %w", err)
		}
	}

	backend, err := newStoreBackend(engine, db, pebbleDB, pebbleProfile)
	if err != nil {
		if pebbleDB != nil {
			_ = pebbleDB.Close()
		}
		_ = db.Close()
		return nil, err
	}

	attachmentRepo := &sqliteUserAttachmentRepository{db: db}
	st := &Store{
		db:                         db,
		backend:                    backend,
		initialNodeID:              opts.NodeID,
		messageWindowSize:          normalizeMessageWindowSize(opts.MessageWindowSize),
		eventLogMaxEventsPerOrigin: normalizeEventLogMaxEventsPerOrigin(opts.EventLogMaxEventsPerOrigin),
		pebbleMessageSyncMode:      normalizePebbleMessageSyncModeOption(opts.PebbleMessageSyncMode),
		clock:                      opts.Clock,
		userRepository:             newCachedUserRepository(&sqliteUserRepository{db: db}),
		attachments:                attachmentRepo,
		subscriptions:              &sqliteSubscriptionRepository{attachments: attachmentRepo},
		blacklists:                 &sqliteBlacklistRepository{attachments: attachmentRepo},
	}
	return st, nil
}

func init() {
	registerSQLiteDriver()
}

func registerSQLiteDriver() {
	sqliteDriverOnce.Do(func() {
		sql.Register(sqliteDriverName, &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				if _, err := conn.Exec(sqliteTempStoreMemoryPrag, nil); err != nil {
					return fmt.Errorf("set sqlite temp_store: %w", err)
				}
				return nil
			},
		})
	})
}

func NormalizeAttachmentType(raw string) (AttachmentType, error) {
	switch AttachmentType(strings.TrimSpace(raw)) {
	case AttachmentTypeChannelManager:
		return AttachmentTypeChannelManager, nil
	case AttachmentTypeChannelWriter:
		return AttachmentTypeChannelWriter, nil
	case AttachmentTypeChannelSubscription:
		return AttachmentTypeChannelSubscription, nil
	case AttachmentTypeUserBlacklist:
		return AttachmentTypeUserBlacklist, nil
	default:
		return "", fmt.Errorf("%w: unsupported attachment type %q", ErrInvalidInput, raw)
	}
}

func sqliteDSN(dbPath string) string {
	values := url.Values{}
	values.Set("mode", "rwc")
	values.Set("_busy_timeout", sqliteBusyTimeoutMillis)
	values.Set("_foreign_keys", "1")
	values.Set("_journal_mode", sqliteJournalMode)
	values.Set("_synchronous", sqliteSynchronousMode)
	values.Set("_txlock", sqliteTransactionLockMode)
	return "file:" + dbPath + "?" + values.Encode()
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

func (s *Store) EventLogMaxEventsPerOrigin() int {
	return normalizeEventLogMaxEventsPerOrigin(s.eventLogMaxEventsPerOrigin)
}

func (s *Store) Close() error {
	var err error
	if s.backend != nil {
		err = s.backend.Close()
	}
	if closeErr := s.db.Close(); err == nil {
		err = closeErr
	}
	return err
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

func normalizePebbleMessageSyncModeOption(mode PebbleMessageSyncMode) PebbleMessageSyncMode {
	switch mode {
	case PebbleMessageSyncModeForceSync:
		return PebbleMessageSyncModeForceSync
	case PebbleMessageSyncModeDefault, PebbleMessageSyncModeNoSync:
		return PebbleMessageSyncModeNoSync
	default:
		return PebbleMessageSyncModeNoSync
	}
}

func normalizePebbleProfileOption(profile PebbleProfile) PebbleProfile {
	switch profile {
	case PebbleProfileThroughput:
		return PebbleProfileThroughput
	case PebbleProfileDefault, PebbleProfileBalanced:
		return PebbleProfileBalanced
	default:
		return PebbleProfileBalanced
	}
}

func resolvePebbleMessageSyncMode(mode PebbleMessageSyncMode, fallback PebbleMessageSyncMode) PebbleMessageSyncMode {
	switch mode {
	case PebbleMessageSyncModeForceSync:
		return PebbleMessageSyncModeForceSync
	case PebbleMessageSyncModeNoSync:
		return PebbleMessageSyncModeNoSync
	default:
		return normalizePebbleMessageSyncModeOption(fallback)
	}
}

func normalizeMessageWindowSize(size int) int {
	if size <= 0 {
		return DefaultMessageWindowSize
	}
	return size
}

func normalizeEventLogMaxEventsPerOrigin(size int) int {
	if size <= 0 {
		return DefaultEventLogMaxEventsPerOrigin
	}
	return size
}
