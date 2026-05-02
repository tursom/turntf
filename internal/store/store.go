// Package store 是 turntf 的数据层核心，提供基于事件溯源（Event Sourcing）的双后端存储。
//
// 架构概览：
//   - 双后端抽象：通过 storeBackend 接口支持 SQLite 和 Pebble 两种存储引擎。
//     SQLite 存储关系型数据（用户、附件、元数据、游标），Pebble（可选）用于高吞吐消息场景。
//   - 事件溯源：所有写操作产生事件（Event），事件通过 HLC（Hybrid Logical Clock）时间戳
//     排序，支持多节点复制和 CRDT 风格的冲突解决。
//   - CQRS/Projection：消息读取通过投影（MessageProjection）实现，与事件日志分离。
//   - 集群支持：内置快照构建/应用、peer 游标追踪、节点发现等集群同步机制。
//
// 该包被 api（HTTP/WebSocket 服务层）、cluster（mesh 网络层）和 permission（权限）包消费。
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
	"time"

	"github.com/cockroachdb/pebble"
	sqlite3 "github.com/mattn/go-sqlite3"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

var (
	// ErrConflict 表示并发冲突，通常在 CRDT 合并时发生。
	ErrConflict = errors.New("conflict")
	// ErrForbidden 表示操作被禁止（例如修改受保护的系统用户）。
	ErrForbidden = errors.New("forbidden")
	// ErrNotFound 表示请求的实体不存在。
	ErrNotFound = errors.New("not found")
	// ErrInvalidInput 表示输入参数校验失败。
	ErrInvalidInput = errors.New("invalid input")
	// ErrBlockedByBlacklist 表示发送者被接收者的黑名单阻止。
	ErrBlockedByBlacklist = errors.New("blocked by blacklist")
)

// DefaultMessageWindowSize 是每个用户保留的最大消息数量，可通过 Options.MessageWindowSize 覆盖。
const DefaultMessageWindowSize = 500

// DefaultEventLogMaxEventsPerOrigin 是每个来源节点保留的最大事件数量，可通过 Options.EventLogMaxEventsPerOrigin 覆盖。
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

// EngineSQLite 和 EnginePebble 是 Options.Engine 的有效值。
const (
	EngineSQLite = "sqlite"
	EnginePebble = "pebble"
)

// PebbleMessageSyncMode 控制 Pebble 后端消息写入的同步策略。
type PebbleMessageSyncMode string

const (
	// PebbleMessageSyncModeDefault 等同于 NoSync。
	PebbleMessageSyncModeDefault PebbleMessageSyncMode = ""
	// PebbleMessageSyncModeNoSync 不等待 Pebble 写入同步到磁盘，延迟最低。
	PebbleMessageSyncModeNoSync PebbleMessageSyncMode = "no_sync"
	// PebbleMessageSyncModeForceSync 等待 Pebble 写入同步到磁盘，可靠性最高。
	PebbleMessageSyncModeForceSync PebbleMessageSyncMode = "force_sync"
)

// PebbleProfile 定义 Pebble 性能配置。
type PebbleProfile string

const (
	// PebbleProfileDefault 等同于 Balanced。
	PebbleProfileDefault PebbleProfile = ""
	// PebbleProfileBalanced 是写入吞吐与读延迟之间的平衡配置。
	PebbleProfileBalanced PebbleProfile = "balanced"
	// PebbleProfileThroughput 优化写入吞吐，适合大量消息写入场景。
	PebbleProfileThroughput PebbleProfile = "throughput"
)

const (
	// 系统角色常量。
	RoleSuperAdmin = "super_admin"
	RoleAdmin      = "admin"
	RoleUser       = "user"
	RoleChannel    = "channel"
	RoleBroadcast  = "broadcast"
	RoleNode       = "node"

	// BootstrapAdminUserID 是系统启动时的超级管理员用户 ID。
	BootstrapAdminUserID = int64(1)
	// BroadcastUserID 是广播消息用户 ID，用于系统广播。
	BroadcastUserID = int64(2)
	// NodeIngressUserID 是节点入口用户 ID，用于集群间消息入口。
	NodeIngressUserID = int64(3)
	// ReservedUserIDMax 是预留用户 ID 的上限值，小于此值的 ID 为系统预留。
	ReservedUserIDMax = int64(1024)

	defaultSchemaVersion  = "17"
	previousSchemaVersion = "16"

	schemaMetaNodeIDKey                 = "node_id"
	schemaMetaMeshTopologyGenerationKey = "mesh_topology_generation"
)

// disabledPasswordHash 是禁用密码登录的标记值。
const disabledPasswordHash = "!"

// isSystemReservedUserID 判断给定 userID 是否为系统预留的三个用户 ID 之一。
func isSystemReservedUserID(userID int64) bool {
	return userID == BootstrapAdminUserID || userID == BroadcastUserID || userID == NodeIngressUserID
}

// Options 是打开 Store 的配置参数。
type Options struct {
	// NodeID 用于设定 schema_meta.node_id，生产环境留空（自动生成），测试环境可指定确定性值。
	NodeID                     int64
	Engine                     string
	PebblePath                 string
	PebbleProfile              PebbleProfile
	PebbleMessageSyncMode      PebbleMessageSyncMode
	MessageWindowSize          int
	EventLogMaxEventsPerOrigin int
	Clock                      *clock.Clock
}

// Store 是数据层的核心结构体，管理 SQLite 和 Pebble 双后端、HLC 时钟、ID 生成器以及
// 各种 Repository（事件日志、消息投影、用户、附件、订阅、黑名单等）。
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

// UserKey 是用户的复合主键，由节点 ID 和用户 ID 组成，跨集群唯一标识一个用户。
type UserKey struct {
	NodeID int64 `json:"node_id"`
	UserID int64 `json:"user_id"`
}

// Validate 验证 UserKey 是否合法（NodeID 和 UserID 必须为正数）。
func (k UserKey) Validate() error {
	if k.NodeID <= 0 || k.UserID <= 0 {
		return fmt.Errorf("%w: user node id and user id are required", ErrInvalidInput)
	}
	return nil
}

// User 代表一个用户实体。Version* 字段记录每个可变字段的最后写入 HLC 时间戳，
// 用于跨节点复制时的 CRDT 风格 LWW（Last-Writer-Wins）冲突解决。
// DeletedAt 非 nil 表示用户已被软删除。
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

// Key 从 User 中提取 UserKey。
func (u User) Key() UserKey {
	return UserKey{NodeID: u.NodeID, UserID: u.ID}
}

// CanLogin 返回用户角色是否支持密码登录。
func (u User) CanLogin() bool {
	return isLoginRole(u.Role)
}

// UserLoginName 表示一个登录名绑定，将 login_name 关联到某个用户。
// 每个用户最多只能有一个活跃的绑定。DeletedAt 非 nil 表示绑定已解绑。
type UserLoginName struct {
	LoginName    string           `json:"login_name"`
	User         UserKey          `json:"user"`
	BoundAt      clock.Timestamp  `json:"bound_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

// Message 表示一条聊天消息，包含接收者、发送者、消息体和 HLC 时间戳。
type Message struct {
	Recipient UserKey         `json:"recipient"`
	NodeID    int64           `json:"node_id"`
	Seq       int64           `json:"seq"`
	Sender    UserKey         `json:"sender"`
	Body      []byte          `json:"body"`
	CreatedAt clock.Timestamp `json:"created_at"`
}

// UserKey 返回消息的接收者 UserKey。
func (m Message) UserKey() UserKey {
	return m.Recipient
}

// AttachmentType 表示附件关系类型，是多态关系系统的核心枚举。
// 支持：channel_manager、channel_writer、channel_subscription、user_blacklist 四种关系。
type AttachmentType string

const (
	// AttachmentTypeChannelManager 表示用户是某个 channel 的管理员。
	AttachmentTypeChannelManager AttachmentType = "channel_manager"
	// AttachmentTypeChannelWriter 表示用户是某个 channel 的写入者。
	AttachmentTypeChannelWriter AttachmentType = "channel_writer"
	// AttachmentTypeChannelSubscription 表示用户订阅了某个 channel。
	AttachmentTypeChannelSubscription AttachmentType = "channel_subscription"
	// AttachmentTypeUserBlacklist 表示用户将另一用户加入黑名单。
	AttachmentTypeUserBlacklist AttachmentType = "user_blacklist"
)

// Attachment 是一个通用的多态关系实体，用于表示用户与用户之间的多种关系类型。
// Owner 是关系拥有者，Subject 是关系目标，Type 决定关系的语义。
// Subscription 和 BlacklistEntry 是 Attachment 的语义化视图。
type Attachment struct {
	Owner        UserKey          `json:"owner"`
	Subject      UserKey          `json:"subject"`
	Type         AttachmentType   `json:"attachment_type"`
	ConfigJSON   string           `json:"config_json"`
	AttachedAt   clock.Timestamp  `json:"attached_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

// UserMetadata 表示附加在用户上的键值对元数据，支持可选的 TTL 过期时间。
type UserMetadata struct {
	Owner        UserKey          `json:"owner"`
	Key          string           `json:"key"`
	Value        []byte           `json:"value"`
	UpdatedAt    clock.Timestamp  `json:"updated_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	ExpiresAt    *time.Time       `json:"expires_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

// Subscription 是 Attachment 的语义化视图，表示用户订阅了某个 channel。
// 由 AttachmentTypeChannelSubscription 类型的 Attachment 派生。
type Subscription struct {
	Subscriber   UserKey          `json:"subscriber"`
	Channel      UserKey          `json:"channel"`
	SubscribedAt clock.Timestamp  `json:"subscribed_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

// BlacklistEntry 是 Attachment 的语义化视图，表示用户将另一用户加入黑名单。
// 由 AttachmentTypeUserBlacklist 类型的 Attachment 派生。
type BlacklistEntry struct {
	Owner        UserKey          `json:"owner"`
	Blocked      UserKey          `json:"blocked"`
	BlockedAt    clock.Timestamp  `json:"blocked_at"`
	DeletedAt    *clock.Timestamp `json:"deleted_at,omitempty"`
	OriginNodeID int64            `json:"origin_node_id"`
}

// Event 是事件溯源系统中的核心实体，记录一次状态变更。
// 每个事件有全局唯一的 Sequence，以及与聚合根关联的 Aggregate* 字段。
// Body 包含具体的事件载荷（protobuf）。
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

// OriginProgress 追踪每个来源节点产生的最新事件 ID，用于复制进度监控。
type OriginProgress struct {
	OriginNodeID int64 `json:"origin_node_id"`
	LastEventID  int64 `json:"last_event_id"`
}

// PeerAckCursor 记录某个 peer 对某个来源节点的事件确认（acknowledged）进度。
type PeerAckCursor struct {
	PeerNodeID   int64           `json:"peer_node_id"`
	OriginNodeID int64           `json:"origin_node_id"`
	AckedEventID int64           `json:"acked_event_id"`
	UpdatedAt    clock.Timestamp `json:"updated_at"`
}

// OriginCursor 记录本地节点对某个来源节点的事件应用进度。
type OriginCursor struct {
	OriginNodeID   int64           `json:"origin_node_id"`
	AppliedEventID int64           `json:"applied_event_id"`
	UpdatedAt      clock.Timestamp `json:"updated_at"`
}

// CreateUserParams 是创建用户的参数集。
type CreateUserParams struct {
	Username     string
	LoginName    string
	PasswordHash string
	Profile      string
	Role         string
}

// UpdateUserParams 是更新用户的参数集。
// 指针字段表示部分更新——nil 表示不修改该字段，非 nil 表示更新为新值。
type UpdateUserParams struct {
	Key          UserKey
	Username     *string
	LoginName    *string
	PasswordHash *string
	Profile      *string
	Role         *string
}

// CreateMessageParams 是创建消息的参数集。
type CreateMessageParams struct {
	UserKey               UserKey
	Sender                UserKey
	Body                  []byte
	PebbleMessageSyncMode PebbleMessageSyncMode
}

// SessionRef 是 WebSocket 会话的引用，由 serving_node_id 和 session_id 唯一标识。
type SessionRef struct {
	ServingNodeID int64  `json:"serving_node_id"`
	SessionID     string `json:"session_id"`
}

// Valid 验证 SessionRef 是否有效（ServingNodeID 为正且 SessionID 非空）。
func (r SessionRef) Valid() bool {
	return r.ServingNodeID > 0 && strings.TrimSpace(r.SessionID) != ""
}

// OnlineNodePresence 表示某个用户在当前节点上的在线状态。
type OnlineNodePresence struct {
	User          UserKey `json:"user"`
	ServingNodeID int64   `json:"serving_node_id"`
	SessionCount  int32   `json:"session_count"`
	TransportHint string  `json:"transport_hint,omitempty"`
}

// OnlineSession 表示一个活跃的 WebSocket 会话。
type OnlineSession struct {
	User             UserKey    `json:"user"`
	SessionRef       SessionRef `json:"session_ref"`
	Transport        string     `json:"transport,omitempty"`
	TransientCapable bool       `json:"transient_capable"`
}

// DeliveryMode 定义瞬时消息的投递模式。
type DeliveryMode string

const (
	// DeliveryModeBestEffort 尽力投递，不重试。
	DeliveryModeBestEffort DeliveryMode = "best_effort"
	// DeliveryModeRouteRetry 投递失败时通过路由重试，适用于需要可靠投递的场景。
	DeliveryModeRouteRetry DeliveryMode = "route_retry"
)

// TransientPacket 是瞬时（非持久化）消息包，通过 mesh 网络路由传输。
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
	TargetSession SessionRef   `json:"target_session,omitempty"`
}

// NormalizeDeliveryMode 校验并标准化 DeliveryMode 字符串，无效值时返回错误。
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

// NormalizePebbleMessageSyncMode 校验并标准化 PebbleMessageSyncMode 字符串，无效值时返回错误。
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

// NormalizePebbleProfile 校验并标准化 PebbleProfile 字符串，无效值时返回错误。
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

// ChannelSubscriptionParams 是订阅/取消订阅 channel 的参数集。
type ChannelSubscriptionParams struct {
	Subscriber UserKey
	Channel    UserKey
}

// BlacklistParams 是拉黑/取消拉黑的参数集。
type BlacklistParams struct {
	Owner   UserKey
	Blocked UserKey
}

// UpsertAttachmentParams 是创建/更新附件的参数集。
type UpsertAttachmentParams struct {
	Owner      UserKey
	Subject    UserKey
	Type       AttachmentType
	ConfigJSON string
}

// DeleteAttachmentParams 是删除附件的参数集。
type DeleteAttachmentParams struct {
	Owner   UserKey
	Subject UserKey
	Type    AttachmentType
}

// UpsertUserMetadataParams 是创建/更新用户元数据的参数集。
type UpsertUserMetadataParams struct {
	Owner     UserKey
	Key       string
	Value     []byte
	ExpiresAt *time.Time
}

// DeleteUserMetadataParams 是删除用户元数据的参数集。
type DeleteUserMetadataParams struct {
	Owner UserKey
	Key   string
}

// ScanUserMetadataParams 是分页扫描用户元数据的参数集。
// Prefix 指定键前缀过滤，After 指定游标位置，Limit 为每页数量。
type ScanUserMetadataParams struct {
	Owner  UserKey
	Prefix string
	After  string
	Limit  int
}

// UserMetadataScanResult 是分页扫描用户元数据的结果，NextAfter 为空表示已到末尾。
type UserMetadataScanResult struct {
	Items     []UserMetadata
	NextAfter string
}

// BootstrapAdminConfig 是系统启动超级管理员的初始配置。
type BootstrapAdminConfig struct {
	Username     string
	PasswordHash string
	LoginName    string
}

// Open 打开 Store。根据 opts.Engine 选择 SQLite 或 SQLite+Pebble 后端，
// 初始化 Repository 并返回 *Store。Pebble 路径为空且引擎为 Pebble 时返回错误。
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

// registerSQLiteDriver 注册自定义 SQLite 驱动，设置 temp_store = MEMORY 的 PRAGMA。
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

// NormalizeAttachmentType 校验并标准化 AttachmentType 字符串，无效值时返回错误。
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

// sqliteDSN 构造 SQLite 数据库连接字符串，配置 WAL 模式、busy_timeout 等参数。
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

// Clock 返回 Store 的 HLC 时钟实例，供调用者生成全局有序的时间戳。
func (s *Store) Clock() *clock.Clock {
	return s.clock
}

// NodeID 返回当前节点的 ID。
func (s *Store) NodeID() int64 {
	return s.nodeID
}

// MessageWindowSize 返回配置的消息窗口大小，如果未配置则返回默认值。
func (s *Store) MessageWindowSize() int {
	return normalizeMessageWindowSize(s.messageWindowSize)
}

// EventLogMaxEventsPerOrigin 返回每个来源节点保留的最大事件数，如果未配置则返回默认值。
func (s *Store) EventLogMaxEventsPerOrigin() int {
	return normalizeEventLogMaxEventsPerOrigin(s.eventLogMaxEventsPerOrigin)
}

// Close 关闭 Store。先关闭 Pebble 后端再关闭 SQLite，确保数据完整性。
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

// normalizeEngine 标准化引擎名称，空字符串或无效值时默认返回 SQLite。
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

// normalizePebbleMessageSyncModeOption 标准化 Pebble 消息同步模式，默认或无效时返回 NoSync。
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

// normalizePebbleProfileOption 标准化 Pebble 性能配置，默认或无效时返回 Balanced。
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

// resolvePebbleMessageSyncMode 解析消息同步模式，默认时回退到 fallback。
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

// normalizeMessageWindowSize 标准化消息窗口大小，<=0 时返回默认值。
func normalizeMessageWindowSize(size int) int {
	if size <= 0 {
		return DefaultMessageWindowSize
	}
	return size
}

// normalizeEventLogMaxEventsPerOrigin 标准化每来源最大事件数，<=0 时返回默认值。
func normalizeEventLogMaxEventsPerOrigin(size int) int {
	if size <= 0 {
		return DefaultEventLogMaxEventsPerOrigin
	}
	return size
}
