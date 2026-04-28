package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/tursom/turntf/internal/clock"
	clusterproto "github.com/tursom/turntf/internal/proto"
)

var ErrProjectionDeferred = errors.New("projection deferred")

type EventLogRepository interface {
	Append(context.Context, Event) (Event, error)
	AppendReplicated(context.Context, Event) (Event, bool, error)
	ListEvents(context.Context, int64, int) ([]Event, error)
	ListEventsByOrigin(context.Context, int64, int64, int) ([]Event, error)
	CountEventsByOrigin(context.Context, int64, int64) (int64, error)
	LastEventSequence(context.Context) (int64, error)
	ListOriginProgress(context.Context) ([]OriginProgress, error)
}

type MessageProjectionRepository interface {
	ApplyMessageCreated(context.Context, Message) error
	ListMessagesByUser(context.Context, UserKey, int) ([]Message, error)
	BuildMessageSnapshotRows(context.Context, int64) ([]*clusterproto.SnapshotRow, error)
	ApplyMessageSnapshotRows(context.Context, int64, []*clusterproto.SnapshotRow) error
}

type UserRepository interface {
	GetUser(context.Context, UserKey, bool) (User, error)
	GetUserTx(context.Context, *sql.Tx, UserKey, bool) (User, error)
	ListBroadcastUserKeys(context.Context) ([]UserKey, error)
}

type AttachmentRepository interface {
	ListActiveByOwner(context.Context, UserKey, AttachmentType) ([]Attachment, error)
	HasActive(context.Context, UserKey, UserKey, AttachmentType, *clock.Timestamp) (bool, error)
}

type SubscriptionRepository interface {
	ListActiveSubscriptions(context.Context, UserKey) ([]Subscription, error)
	ListChannelSubscribers(context.Context, UserKey) ([]Subscription, error)
}

type BlacklistRepository interface {
	ListActiveBlockedUsers(context.Context, UserKey) ([]BlacklistEntry, error)
	HasActiveBlock(context.Context, UserKey, UserKey, *clock.Timestamp) (bool, error)
}

type MessageTrimRepository interface {
	RecordMessageTrim(context.Context, int64) error
}

type ProjectionStats struct {
	PendingTotal int64
	LastFailedAt *clock.Timestamp
}
