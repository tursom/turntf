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
	if _, err := NormalizePebbleMessageSyncMode(string(params.PebbleMessageSyncMode)); err != nil {
		return Message{}, Event{}, err
	}
	if err := ctx.Err(); err != nil {
		return Message{}, Event{}, err
	}
	return s.backend.CreateMessage(ctx, s, params)
}

func (s *Store) nextMessageSeqTx(ctx context.Context, tx *sql.Tx, key UserKey, nodeID int64) (int64, error) {
	return s.backend.NextMessageSeqTx(ctx, tx, key, nodeID)
}

func (s *Store) ListMessagesByUser(ctx context.Context, key UserKey, limit int) ([]Message, error) {
	return s.backend.MessageProjection().ListMessagesByUser(ctx, key, limit)
}

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

type sqlQueryRowContext interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

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

func validateMessageIdentity(key UserKey, nodeID int64, seq int64) error {
	if err := key.Validate(); err != nil {
		return err
	}
	if nodeID <= 0 || seq <= 0 {
		return fmt.Errorf("%w: user id, node id, and seq are required for message", ErrInvalidInput)
	}
	return nil
}
