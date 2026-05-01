package store

import (
	"context"
	"database/sql"
	"fmt"

	gproto "google.golang.org/protobuf/proto"
)

// LastEventSequence 返回全局最高事件序列号。
func (s *Store) LastEventSequence(ctx context.Context) (int64, error) {
	return s.backend.EventLog().LastEventSequence(ctx)
}

// ListOriginProgress 列出所有来源节点的事件进度（每个来源的最新事件 ID）。
func (s *Store) ListOriginProgress(ctx context.Context) ([]OriginProgress, error) {
	return s.backend.EventLog().ListOriginProgress(ctx)
}

// ListEventsByOrigin 按来源节点列出事件，afterEventID 为游标，limit 为每页数量。
func (s *Store) ListEventsByOrigin(ctx context.Context, originNodeID, afterEventID int64, limit int) ([]Event, error) {
	return s.backend.EventLog().ListEventsByOrigin(ctx, originNodeID, afterEventID, limit)
}

// ListEvents 按全局序列号列出事件，afterSequence 为游标。
func (s *Store) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	return s.backend.EventLog().ListEvents(ctx, afterSequence, limit)
}

// insertEvent 通过后端在事务中插入一条本地事件。
func (s *Store) insertEvent(ctx context.Context, tx *sql.Tx, event Event) (Event, error) {
	return s.backend.InsertLocalEventTx(ctx, tx, event)
}

// eventLogValue 将 Event 序列化为 protobuf 二进制格式，用于事件日志存储。
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
