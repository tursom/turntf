package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/tursom/turntf/internal/clock"
)

func (s *Store) BlockUser(ctx context.Context, params BlacklistParams) (BlacklistEntry, Event, error) {
	if err := params.Owner.Validate(); err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	if err := params.Blocked.Validate(); err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	if params.Owner == params.Blocked {
		return BlacklistEntry{}, Event{}, fmt.Errorf("%w: owner cannot block itself", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BlacklistEntry{}, Event{}, fmt.Errorf("begin block user: %w", err)
	}
	defer tx.Rollback()

	if err := s.validateBlacklistUsersTx(ctx, tx, params.Owner, params.Blocked); err != nil {
		return BlacklistEntry{}, Event{}, err
	}

	current, err := s.getBlacklistEntryTx(ctx, tx, params.Owner, params.Blocked)
	switch {
	case err == nil && current.DeletedAt == nil:
		if err := tx.Commit(); err != nil {
			return BlacklistEntry{}, Event{}, fmt.Errorf("commit block user noop: %w", err)
		}
		return current, Event{}, nil
	case err != nil && !errors.Is(err, ErrNotFound):
		return BlacklistEntry{}, Event{}, err
	}

	now := s.clock.Now()
	entry := BlacklistEntry{
		Owner:        params.Owner,
		Blocked:      params.Blocked,
		BlockedAt:    now,
		OriginNodeID: s.nodeID,
	}
	if err == nil {
		entry = current
		entry.BlockedAt = now
		entry.DeletedAt = nil
		entry.OriginNodeID = s.nodeID
	}
	if err := s.upsertBlacklistEntryTx(ctx, tx, entry); err != nil {
		return BlacklistEntry{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserBlocked,
		Aggregate:       "blacklist",
		AggregateNodeID: params.Owner.NodeID,
		AggregateID:     params.Owner.UserID,
		HLC:             now,
		Body:            userBlockedProtoFromBlacklist(entry),
	})
	if err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return BlacklistEntry{}, Event{}, fmt.Errorf("commit block user: %w", err)
	}
	return entry, event, nil
}

func (s *Store) UnblockUser(ctx context.Context, params BlacklistParams) (BlacklistEntry, Event, error) {
	if err := params.Owner.Validate(); err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	if err := params.Blocked.Validate(); err != nil {
		return BlacklistEntry{}, Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BlacklistEntry{}, Event{}, fmt.Errorf("begin unblock user: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getBlacklistEntryTx(ctx, tx, params.Owner, params.Blocked)
	if err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	if current.DeletedAt != nil {
		return BlacklistEntry{}, Event{}, ErrNotFound
	}

	now := s.clock.Now()
	current.DeletedAt = &now
	current.OriginNodeID = s.nodeID
	if err := s.upsertBlacklistEntryTx(ctx, tx, current); err != nil {
		return BlacklistEntry{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserUnblocked,
		Aggregate:       "blacklist",
		AggregateNodeID: params.Owner.NodeID,
		AggregateID:     params.Owner.UserID,
		HLC:             now,
		Body:            userUnblockedProtoFromBlacklist(current),
	})
	if err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return BlacklistEntry{}, Event{}, fmt.Errorf("commit unblock user: %w", err)
	}
	return current, event, nil
}

func (s *Store) ListBlockedUsers(ctx context.Context, owner UserKey) ([]BlacklistEntry, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	if _, err := s.GetUser(ctx, owner); err != nil {
		return nil, err
	}
	return s.blacklists.ListActiveBlockedUsers(ctx, owner)
}

func (s *Store) IsBlockedByRecipient(ctx context.Context, recipient, sender UserKey) (bool, error) {
	if err := recipient.Validate(); err != nil {
		return false, err
	}
	if err := sender.Validate(); err != nil {
		return false, err
	}
	recipientUser, err := s.GetUser(ctx, recipient)
	if err != nil {
		return false, err
	}
	if !recipientUser.CanLogin() {
		return false, nil
	}
	senderUser, err := s.GetUser(ctx, sender)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if senderUser.Role != RoleUser {
		return false, nil
	}
	return s.blacklists.HasActiveBlock(ctx, recipient, sender, nil)
}

func (s *Store) IsMessageHiddenByBlacklist(ctx context.Context, owner, sender UserKey, createdAt clock.Timestamp) (bool, error) {
	if err := owner.Validate(); err != nil {
		return false, err
	}
	if err := sender.Validate(); err != nil {
		return false, err
	}
	senderUser, err := s.GetUser(ctx, sender)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if senderUser.Role != RoleUser {
		return false, nil
	}
	return s.blacklists.HasActiveBlock(ctx, owner, sender, &createdAt)
}

func (s *Store) validateBlacklistUsersTx(ctx context.Context, tx *sql.Tx, ownerKey, blockedKey UserKey) error {
	owner, err := s.getUserByIDTx(ctx, tx, ownerKey, false)
	if err != nil {
		return err
	}
	if !owner.CanLogin() {
		return fmt.Errorf("%w: blacklist owner must be a login user", ErrInvalidInput)
	}
	blocked, err := s.getUserByIDTx(ctx, tx, blockedKey, false)
	if err != nil {
		return err
	}
	if blocked.Role != RoleUser {
		return fmt.Errorf("%w: blacklist target must be a user", ErrInvalidInput)
	}
	return nil
}

func (s *Store) getBlacklistEntryTx(ctx context.Context, tx *sql.Tx, owner, blocked UserKey) (BlacklistEntry, error) {
	row := tx.QueryRowContext(ctx, `
SELECT owner_node_id, owner_user_id, blocked_node_id, blocked_user_id, blocked_at_hlc, deleted_at_hlc, origin_node_id
FROM user_blacklists
WHERE owner_node_id = ? AND owner_user_id = ? AND blocked_node_id = ? AND blocked_user_id = ?
`, owner.NodeID, owner.UserID, blocked.NodeID, blocked.UserID)
	entry, err := scanBlacklistEntry(row)
	if err == sql.ErrNoRows {
		return BlacklistEntry{}, ErrNotFound
	}
	if err != nil {
		return BlacklistEntry{}, err
	}
	return entry, nil
}

func (s *Store) upsertBlacklistEntryTx(ctx context.Context, tx *sql.Tx, entry BlacklistEntry) error {
	if err := entry.Owner.Validate(); err != nil {
		return err
	}
	if err := entry.Blocked.Validate(); err != nil {
		return err
	}
	if entry.BlockedAt == (clock.Timestamp{}) {
		return fmt.Errorf("%w: blocked_at is required", ErrInvalidInput)
	}
	if entry.OriginNodeID <= 0 {
		return fmt.Errorf("%w: blacklist origin node id is required", ErrInvalidInput)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO user_blacklists(
    owner_node_id, owner_user_id, blocked_node_id, blocked_user_id, blocked_at_hlc, deleted_at_hlc, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(owner_node_id, owner_user_id, blocked_node_id, blocked_user_id) DO UPDATE SET
    blocked_at_hlc = excluded.blocked_at_hlc,
    deleted_at_hlc = excluded.deleted_at_hlc,
    origin_node_id = excluded.origin_node_id
`, entry.Owner.NodeID, entry.Owner.UserID, entry.Blocked.NodeID, entry.Blocked.UserID,
		entry.BlockedAt.String(), nullableTimestampString(entry.DeletedAt), entry.OriginNodeID); err != nil {
		return fmt.Errorf("upsert blacklist: %w", err)
	}
	return nil
}

func (s *Store) isBlockedByRecipientTx(ctx context.Context, tx *sql.Tx, owner, blocked UserKey, createdAt *clock.Timestamp) (bool, error) {
	if err := owner.Validate(); err != nil {
		return false, err
	}
	if err := blocked.Validate(); err != nil {
		return false, err
	}

	query := `
SELECT COUNT(*)
FROM user_blacklists
WHERE owner_node_id = ? AND owner_user_id = ?
  AND blocked_node_id = ? AND blocked_user_id = ?
  AND deleted_at_hlc IS NULL`
	args := []any{owner.NodeID, owner.UserID, blocked.NodeID, blocked.UserID}
	if createdAt != nil {
		query += ` AND blocked_at_hlc <= ?`
		args = append(args, createdAt.String())
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, fmt.Errorf("check blacklist: %w", err)
	}
	return count > 0, nil
}
