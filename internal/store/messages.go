package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
)

// MessageSession 计算两个用户之间的 session 标识（32 字节 BLOB）。
// 将两个 UserKey 按 (node_id, user_id) 排序后以大端编码拼接，保证 A↔B 和 B↔A 得到相同的 session。
func MessageSession(a, b UserKey) []byte {
	if a.NodeID > b.NodeID || (a.NodeID == b.NodeID && a.UserID > b.UserID) {
		a, b = b, a
	}
	session := make([]byte, 32)
	binary.BigEndian.PutUint64(session[0:8], uint64(a.NodeID))
	binary.BigEndian.PutUint64(session[8:16], uint64(a.UserID))
	binary.BigEndian.PutUint64(session[16:24], uint64(b.NodeID))
	binary.BigEndian.PutUint64(session[24:32], uint64(b.UserID))
	return session
}

// CreateMessage 创建消息。校验参数后进行黑名单检查，然后通过后端写入事件日志和消息投影。
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
	if _, err := NormalizePebbleMessageSyncMode(string(params.PebbleMessageSyncMode)); err != nil {
		return Message{}, Event{}, err
	}
	if err := ctx.Err(); err != nil {
		return Message{}, Event{}, err
	}
	message, event, err := s.backend.CreateMessage(ctx, s, params)
	if event.Sequence > 0 && event.EventType == EventTypeMessageCreated {
		s.notifyMessageCommitted()
	}
	return message, event, err
}

// nextMessageSeqTx 为给定 (UserKey, nodeID) 对分配下一个消息序列号，委托给后端。
func (s *Store) nextMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	return s.backend.NextMessageSeqTx(ctx, tx, key, nodeID)
}

// ListMessagesByUser 列出用户最近的消息（来自消息投影）。
func (s *Store) ListMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	return s.backend.MessageProjection().ListMessagesByUser(ctx, key, limit)
}

// ListMessagesBySession 列出指定 session（会话双方）之间的消息，按时间降序排列。
func (s *Store) ListMessagesBySession(ctx context.Context, session []byte, requester UserKey, limit int) ([]Message, error) {
	return s.backend.MessageProjection().ListMessagesBySession(ctx, session, requester, limit)
}

// nextSQLiteMessageSeq 在 SQLite 中分配下一个消息序列号。
// 优先使用 message_sequence_counters 表中的预计算值，回退到 messages 表投影计算。
func nextSQLiteMessageSeq(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	if err := key.Validate(); err != nil {
		return 0, err
	}
	if nodeID <= 0 {
		return 0, fmt.Errorf("%w: user id and node id are required for message sequence", ErrInvalidInput)
	}

	seq, ok, err := readStoredMessageCounterNextSeq(ctx, tx, key, nodeID)
	if err != nil {
		return 0, err
	}
	if !ok {
		seq, err = readProjectedMessageNextSeq(ctx, tx, key, nodeID)
		if err != nil {
			return 0, err
		}
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

// sqlQueryRowContext 是 QueryRowContext 的接口窄化，便于测试时替换实现。
type sqlQueryRowContext interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// readStoredMessageCounterNextSeq 从 message_sequence_counters 表读取预存的下一个序列号。
func readStoredMessageCounterNextSeq(ctx context.Context, querier sqlQueryRowContext, key UserKey, nodeID int64) (int64, bool, error) {
	var seq int64
	err := querier.QueryRowContext(ctx, `
SELECT next_seq
FROM message_sequence_counters
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`, key.NodeID, key.UserID, nodeID).Scan(&seq)
	switch {
	case err == nil:
		return seq, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("read next message sequence: %w", err)
	}
}

// readProjectedMessageNextSeq 从 messages 投影表计算下一个序列号（预存计数器不存在时的回退方案）。
func readProjectedMessageNextSeq(ctx context.Context, querier sqlQueryRowContext, key UserKey, nodeID int64) (int64, error) {
	var seq int64
	if err := querier.QueryRowContext(ctx, `
SELECT COALESCE(MAX(seq), 0) + 1
FROM messages
WHERE user_node_id = ? AND user_id = ? AND node_id = ?
`, key.NodeID, key.UserID, nodeID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("seed next message sequence: %w", err)
	}
	return seq, nil
}

// validateMessageIdentity 校验消息标识字段：UserKey、nodeID、seq 必须为正且非零。
func validateMessageIdentity(key UserKey, nodeID int64, seq int64) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if nodeID <= 0 || seq <= 0 {
		return fmt.Errorf("%w: user id, node id, and seq are required for message", ErrInvalidInput)
	}
	return nil
}
