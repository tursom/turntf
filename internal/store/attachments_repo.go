package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
)

// sqliteUserAttachmentRepository 是 AttachmentRepository 的 SQLite 实现，
// 直接操作 user_attachments 表。
type sqliteUserAttachmentRepository struct {
	db *sql.DB
}

// ListActiveByOwner 列出所有者的活跃附件，可按类型过滤。
func (r *sqliteUserAttachmentRepository) ListActiveByOwner(ctx context.Context, owner UserKey, attachmentType AttachmentType) ([]Attachment, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}

	query := `
SELECT owner_node_id, owner_user_id, subject_node_id, subject_user_id, attachment_type, config_json, attached_at_hlc, deleted_at_hlc, origin_node_id
FROM user_attachments
WHERE owner_node_id = ? AND owner_user_id = ? AND deleted_at_hlc IS NULL`
	args := []any{owner.NodeID, owner.UserID}
	if attachmentType != "" {
		query += ` AND attachment_type = ?`
		args = append(args, string(attachmentType))
	}
	query += ` ORDER BY attachment_type ASC, subject_node_id ASC, subject_user_id ASC`

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list active attachments by owner: %w", err)
	}
	defer rows.Close()

	attachments := make([]Attachment, 0)
	for rows.Next() {
		attachment, err := scanAttachment(rows)
		if err != nil {
			return nil, err
		}
		attachments = append(attachments, attachment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active attachments by owner: %w", err)
	}
	return attachments, nil
}

// ListActiveBySubject 列出目标主体的活跃附件（按 subject 查询），用于复制时按 subject 维度同步。
func (r *sqliteUserAttachmentRepository) ListActiveBySubject(ctx context.Context, subject UserKey, attachmentType AttachmentType) ([]Attachment, error) {
	if err := subject.Validate(); err != nil {
		return nil, err
	}

	query := `
SELECT owner_node_id, owner_user_id, subject_node_id, subject_user_id, attachment_type, config_json, attached_at_hlc, deleted_at_hlc, origin_node_id
FROM user_attachments
WHERE subject_node_id = ? AND subject_user_id = ? AND deleted_at_hlc IS NULL`
	args := []any{subject.NodeID, subject.UserID}
	if attachmentType != "" {
		query += ` AND attachment_type = ?`
		args = append(args, string(attachmentType))
	}
	query += ` ORDER BY attachment_type ASC, owner_node_id ASC, owner_user_id ASC`

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list active attachments by subject: %w", err)
	}
	defer rows.Close()

	attachments := make([]Attachment, 0)
	for rows.Next() {
		attachment, err := scanAttachment(rows)
		if err != nil {
			return nil, err
		}
		attachments = append(attachments, attachment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active attachments by subject: %w", err)
	}
	return attachments, nil
}

// HasActive 检查指定的附件关系是否存在且活跃。attachedAt 可选过滤创建时间。
func (r *sqliteUserAttachmentRepository) HasActive(ctx context.Context, owner, subject UserKey, attachmentType AttachmentType, attachedAt *clock.Timestamp) (bool, error) {
	if err := owner.Validate(); err != nil {
		return false, err
	}
	if err := subject.Validate(); err != nil {
		return false, err
	}
	if _, err := NormalizeAttachmentType(string(attachmentType)); err != nil {
		return false, err
	}

	query := `
SELECT COUNT(*)
FROM user_attachments
WHERE owner_node_id = ? AND owner_user_id = ?
  AND subject_node_id = ? AND subject_user_id = ?
  AND attachment_type = ?
  AND deleted_at_hlc IS NULL`
	args := []any{owner.NodeID, owner.UserID, subject.NodeID, subject.UserID, string(attachmentType)}
	if attachedAt != nil {
		query += ` AND attached_at_hlc <= ?`
		args = append(args, attachedAt.String())
	}
	var count int
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, fmt.Errorf("check active attachment: %w", err)
	}
	return count > 0, nil
}

// scanAttachment 从 SQL scanner 扫描一行到 Attachment，解析和校验所有字段。
func scanAttachment(scanner interface{ Scan(...any) error }) (Attachment, error) {
	var (
		attachment    Attachment
		rawType       string
		rawConfig     string
		rawAttachedAt string
		rawDeletedAt  sql.NullString
	)
	if err := scanner.Scan(
		&attachment.Owner.NodeID,
		&attachment.Owner.UserID,
		&attachment.Subject.NodeID,
		&attachment.Subject.UserID,
		&rawType,
		&rawConfig,
		&rawAttachedAt,
		&rawDeletedAt,
		&attachment.OriginNodeID,
	); err != nil {
		return Attachment{}, err
	}

	attachmentType, err := NormalizeAttachmentType(rawType)
	if err != nil {
		return Attachment{}, err
	}
	attachment.Type = attachmentType
	attachment.ConfigJSON = defaultJSON(rawConfig)

	attachedAt, err := parseRequiredTimestamp(rawAttachedAt, "attachment attached_at")
	if err != nil {
		return Attachment{}, err
	}
	attachment.AttachedAt = attachedAt

	if rawDeletedAt.Valid && strings.TrimSpace(rawDeletedAt.String) != "" {
		deletedAt, err := parseRequiredTimestamp(rawDeletedAt.String, "attachment deleted_at")
		if err != nil {
			return Attachment{}, err
		}
		attachment.DeletedAt = &deletedAt
	}

	return attachment, nil
}

// normalizeAttachmentConfigJSON 校验并标准化附件配置 JSON，空值默认为 "{}"。
func normalizeAttachmentConfigJSON(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}", nil
	}
	if !json.Valid([]byte(raw)) {
		return "", fmt.Errorf("%w: config_json must be valid json", ErrInvalidInput)
	}
	return raw, nil
}

// attachmentFromSubscription 将 Subscription 转换为 Attachment（AttachmentTypeChannelSubscription）。
func attachmentFromSubscription(subscription Subscription) Attachment {
	return Attachment{
		Owner:        subscription.Subscriber,
		Subject:      subscription.Channel,
		Type:         AttachmentTypeChannelSubscription,
		ConfigJSON:   "{}",
		AttachedAt:   subscription.SubscribedAt,
		DeletedAt:    subscription.DeletedAt,
		OriginNodeID: subscription.OriginNodeID,
	}
}

// attachmentFromBlacklistEntry 将 BlacklistEntry 转换为 Attachment（AttachmentTypeUserBlacklist）。
func attachmentFromBlacklistEntry(entry BlacklistEntry) Attachment {
	return Attachment{
		Owner:        entry.Owner,
		Subject:      entry.Blocked,
		Type:         AttachmentTypeUserBlacklist,
		ConfigJSON:   "{}",
		AttachedAt:   entry.BlockedAt,
		DeletedAt:    entry.DeletedAt,
		OriginNodeID: entry.OriginNodeID,
	}
}

// subscriptionFromAttachment 将 Attachment 转换为 Subscription。
func subscriptionFromAttachment(attachment Attachment) Subscription {
	return Subscription{
		Subscriber:   attachment.Owner,
		Channel:      attachment.Subject,
		SubscribedAt: attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeID: attachment.OriginNodeID,
	}
}

// blacklistEntryFromAttachment 将 Attachment 转换为 BlacklistEntry。
func blacklistEntryFromAttachment(attachment Attachment) BlacklistEntry {
	return BlacklistEntry{
		Owner:        attachment.Owner,
		Blocked:      attachment.Subject,
		BlockedAt:    attachment.AttachedAt,
		DeletedAt:    attachment.DeletedAt,
		OriginNodeID: attachment.OriginNodeID,
	}
}
