package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/tursom/turntf/internal/clock"
)

// BlockUser 将用户加入黑名单。底层委托给 UpsertAttachment（AttachmentTypeUserBlacklist）。
func (s *Store) BlockUser(ctx context.Context, params BlacklistParams) (BlacklistEntry, Event, error) {
	attachment, event, err := s.UpsertAttachment(ctx, UpsertAttachmentParams{
		Owner:      params.Owner,
		Subject:    params.Blocked,
		Type:       AttachmentTypeUserBlacklist,
		ConfigJSON: "{}",
	})
	if err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	return blacklistEntryFromAttachment(attachment), event, nil
}

// UnblockUser 取消拉黑用户。底层委托给 DeleteAttachment（AttachmentTypeUserBlacklist）。
func (s *Store) UnblockUser(ctx context.Context, params BlacklistParams) (BlacklistEntry, Event, error) {
	attachment, event, err := s.DeleteAttachment(ctx, DeleteAttachmentParams{
		Owner:   params.Owner,
		Subject: params.Blocked,
		Type:    AttachmentTypeUserBlacklist,
	})
	if err != nil {
		return BlacklistEntry{}, Event{}, err
	}
	return blacklistEntryFromAttachment(attachment), event, nil
}

// ListBlockedUsers 列出用户的所有黑名单条目。
func (s *Store) ListBlockedUsers(ctx context.Context, owner UserKey) ([]BlacklistEntry, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	if _, err := s.GetUser(ctx, owner); err != nil {
		return nil, err
	}
	attachments, err := s.ListUserAttachments(ctx, owner, AttachmentTypeUserBlacklist)
	if err != nil {
		return nil, err
	}
	entries := make([]BlacklistEntry, 0, len(attachments))
	for _, attachment := range attachments {
		entries = append(entries, blacklistEntryFromAttachment(attachment))
	}
	return entries, nil
}

// IsBlockedByRecipient 检查 sender 是否被 recipient 拉黑。
// 需要双方角色都满足条件（recipient 需可登录，sender 需为普通用户）。
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

// IsMessageHiddenByBlacklist 检查消息是否因黑名单而应隐藏。
// 与 IsBlockedByRecipient 不同，此方法根据消息创建时的黑名单状态进行判断（使用 createdAt 时间戳）。
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

// isBlockedByRecipientTx 在事务中检查 owner 是否拉黑了 blocked 用户。
// 可选按 attachedAt 时间戳过滤（用于检查历史黑名单状态）。
func (s *Store) isBlockedByRecipientTx(ctx context.Context, tx *sql.Tx, owner, blocked UserKey, attachedAt *clock.Timestamp) (bool, error) {
	if err := owner.Validate(); err != nil {
		return false, err
	}
	if err := blocked.Validate(); err != nil {
		return false, err
	}
	query := `
SELECT COUNT(*)
FROM user_attachments
WHERE owner_node_id = ? AND owner_user_id = ?
  AND subject_node_id = ? AND subject_user_id = ?
  AND attachment_type = ?
  AND deleted_at_hlc IS NULL`
	args := []any{owner.NodeID, owner.UserID, blocked.NodeID, blocked.UserID, string(AttachmentTypeUserBlacklist)}
	if attachedAt != nil {
		query += ` AND attached_at_hlc <= ?`
		args = append(args, attachedAt.String())
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
