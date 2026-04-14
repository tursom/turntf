package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

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

	recipient, err := s.getUserTx(ctx, tx, params.UserKey, false)
	if err != nil {
		return Message{}, Event{}, err
	}
	sender, err := s.getUserTx(ctx, tx, params.Sender, false)
	switch {
	case err == nil:
		if recipient.CanLogin() && sender.Role == RoleUser {
			blocked, err := s.isBlockedByRecipientTx(ctx, tx, recipient.Key(), sender.Key(), nil)
			if err != nil {
				return Message{}, Event{}, err
			}
			if blocked {
				return Message{}, Event{}, ErrBlockedByBlacklist
			}
		}
	case !errors.Is(err, ErrNotFound):
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

func validateMessageIdentity(key UserKey, nodeID int64, seq int64) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if nodeID <= 0 || seq <= 0 {
		return fmt.Errorf("%w: user id, node id, and seq are required for message", ErrInvalidInput)
	}
	return nil
}
