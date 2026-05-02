package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/tursom/turntf/internal/clock"
	clusterproto "github.com/tursom/turntf/internal/proto"
)

// ErrProjectionDeferred 表示消息事件无法立即投影，已加入重试队列。
var ErrProjectionDeferred = errors.New("projection deferred")

// EventLogRepository 定义事件日志的持久化接口。
// 支持追加本地事件、追加复制事件、按来源/序列号列表查询等操作。
// 由 sqliteEventLogRepository 和 pebbleEventLogRepository 实现。
type EventLogRepository interface {
	Append(context.Context, Event) (Event, error)
	AppendReplicated(context.Context, Event) (Event, bool, error)
	ListEvents(context.Context, int64, int) ([]Event, error)
	ListEventsByOrigin(context.Context, int64, int64, int) ([]Event, error)
	CountEventsByOrigin(context.Context, int64, int64) (int64, error)
	LastEventSequence(context.Context) (int64, error)
	ListOriginProgress(context.Context) ([]OriginProgress, error)
}

// MessageProjectionRepository 定义消息投影的持久化接口。
// 消息投影是事件日志的物化视图，支持按用户查询最近消息。
// 由 sqliteMessageProjectionRepository 和 pebbleMessageProjectionRepository 实现。
type MessageProjectionRepository interface {
	ApplyMessageCreated(context.Context, Message) error
	ListMessagesByUser(context.Context, UserKey, int) ([]Message, error)
	ListMessagesBySession(context.Context, []byte, UserKey, int) ([]Message, error)
	BuildMessageSnapshotRows(context.Context, int64) ([]*clusterproto.SnapshotRow, error)
	ApplyMessageSnapshotRows(context.Context, int64, []*clusterproto.SnapshotRow) error
}

// UserRepository 定义用户读取的接口。
// 支持按 UserKey 查询（含/不含事务）、列出广播用户 Key。
// 由 sqliteUserRepository 实现，通过 cachedUserRepository 装饰器添加缓存。
type UserRepository interface {
	GetUser(context.Context, UserKey, bool) (User, error)
	GetUserTx(context.Context, *sql.Tx, UserKey, bool) (User, error)
	ListBroadcastUserKeys(context.Context) ([]UserKey, error)
}

// AttachmentRepository 定义附件关系查询的接口。
// 支持按所有者列出活跃附件、检查指定附件是否存在。
// 由 sqliteUserAttachmentRepository 实现。
type AttachmentRepository interface {
	ListActiveByOwner(context.Context, UserKey, AttachmentType) ([]Attachment, error)
	HasActive(context.Context, UserKey, UserKey, AttachmentType, *clock.Timestamp) (bool, error)
}

// SubscriptionRepository 定义频道订阅查询的接口。
// Subscription 是 Attachment 的语义化视图（AttachmentTypeChannelSubscription）。
// 由 sqliteSubscriptionRepository 实现。
type SubscriptionRepository interface {
	ListActiveSubscriptions(context.Context, UserKey) ([]Subscription, error)
	ListChannelSubscribers(context.Context, UserKey) ([]Subscription, error)
}

// BlacklistRepository 定义用户黑名单查询的接口。
// BlacklistEntry 是 Attachment 的语义化视图（AttachmentTypeUserBlacklist）。
// 由 sqliteBlacklistRepository 实现。
type BlacklistRepository interface {
	ListActiveBlockedUsers(context.Context, UserKey) ([]BlacklistEntry, error)
	HasActiveBlock(context.Context, UserKey, UserKey, *clock.Timestamp) (bool, error)
}

// MessageTrimRepository 定义消息裁剪记录的接口，由 sqliteMessageTrimRepository 实现。
type MessageTrimRepository interface {
	RecordMessageTrim(context.Context, int64) error
}

// ProjectionStats 包含消息投影的待处理统计信息和最后一次失败的时间戳。
type ProjectionStats struct {
	PendingTotal int64
	LastFailedAt *clock.Timestamp
}
