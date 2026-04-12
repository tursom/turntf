package store

import (
	"context"
	"database/sql"
	"errors"

	"notifier/internal/clock"
	clusterproto "notifier/internal/proto"
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

type SubscriptionRepository interface {
	ListActiveSubscriptions(context.Context, UserKey) ([]Subscription, error)
}

type MessageTrimRepository interface {
	RecordMessageTrim(context.Context, int64) error
}

type ProjectionStats struct {
	PendingTotal int64
	LastFailedAt *clock.Timestamp
}
