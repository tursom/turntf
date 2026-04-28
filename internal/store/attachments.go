package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tursom/turntf/internal/clock"
)

func (s *Store) UpsertAttachment(ctx context.Context, params UpsertAttachmentParams) (Attachment, Event, error) {
	if err := params.Owner.Validate(); err != nil {
		return Attachment{}, Event{}, err
	}
	if err := params.Subject.Validate(); err != nil {
		return Attachment{}, Event{}, err
	}
	attachmentType, err := NormalizeAttachmentType(string(params.Type))
	if err != nil {
		return Attachment{}, Event{}, err
	}
	configJSON, err := normalizeAttachmentConfigJSON(params.ConfigJSON)
	if err != nil {
		return Attachment{}, Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attachment{}, Event{}, fmt.Errorf("begin upsert attachment: %w", err)
	}
	defer tx.Rollback()

	if err := s.validateAttachmentUsersTx(ctx, tx, params.Owner, params.Subject, attachmentType); err != nil {
		return Attachment{}, Event{}, err
	}

	now := s.clock.Now()
	attachment := Attachment{
		Owner:        params.Owner,
		Subject:      params.Subject,
		Type:         attachmentType,
		ConfigJSON:   configJSON,
		AttachedAt:   now,
		OriginNodeID: s.nodeID,
	}
	if current, err := s.getAttachmentTx(ctx, tx, params.Owner, params.Subject, attachmentType); err == nil {
		attachment = current
		attachment.ConfigJSON = configJSON
		attachment.AttachedAt = now
		attachment.DeletedAt = nil
		attachment.OriginNodeID = s.nodeID
	} else if err != ErrNotFound {
		return Attachment{}, Event{}, err
	}

	if err := s.upsertAttachmentTx(ctx, tx, attachment); err != nil {
		return Attachment{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserAttachmentUpserted,
		Aggregate:       "attachment",
		AggregateNodeID: params.Owner.NodeID,
		AggregateID:     params.Owner.UserID,
		HLC:             now,
		Body:            userAttachmentUpsertedProtoFromAttachment(attachment),
	})
	if err != nil {
		return Attachment{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return Attachment{}, Event{}, fmt.Errorf("commit upsert attachment: %w", err)
	}
	return attachment, event, nil
}

func (s *Store) DeleteAttachment(ctx context.Context, params DeleteAttachmentParams) (Attachment, Event, error) {
	if err := params.Owner.Validate(); err != nil {
		return Attachment{}, Event{}, err
	}
	if err := params.Subject.Validate(); err != nil {
		return Attachment{}, Event{}, err
	}
	attachmentType, err := NormalizeAttachmentType(string(params.Type))
	if err != nil {
		return Attachment{}, Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attachment{}, Event{}, fmt.Errorf("begin delete attachment: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getAttachmentTx(ctx, tx, params.Owner, params.Subject, attachmentType)
	if err != nil {
		return Attachment{}, Event{}, err
	}
	if current.DeletedAt != nil {
		return Attachment{}, Event{}, ErrNotFound
	}
	if attachmentType == AttachmentTypeChannelManager {
		count, err := s.countActiveAttachmentsByOwnerTx(ctx, tx, params.Owner, attachmentType)
		if err != nil {
			return Attachment{}, Event{}, err
		}
		if count <= 1 {
			return Attachment{}, Event{}, ErrForbidden
		}
	}

	now := s.clock.Now()
	current.DeletedAt = &now
	current.OriginNodeID = s.nodeID
	if err := s.upsertAttachmentTx(ctx, tx, current); err != nil {
		return Attachment{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeUserAttachmentDeleted,
		Aggregate:       "attachment",
		AggregateNodeID: params.Owner.NodeID,
		AggregateID:     params.Owner.UserID,
		HLC:             now,
		Body:            userAttachmentDeletedProtoFromAttachment(current),
	})
	if err != nil {
		return Attachment{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return Attachment{}, Event{}, fmt.Errorf("commit delete attachment: %w", err)
	}
	return current, event, nil
}

func (s *Store) ListUserAttachments(ctx context.Context, owner UserKey, attachmentType AttachmentType) ([]Attachment, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	if attachmentType != "" {
		if _, err := NormalizeAttachmentType(string(attachmentType)); err != nil {
			return nil, err
		}
	}
	if _, err := s.GetUser(ctx, owner); err != nil {
		return nil, err
	}
	return s.attachments.ListActiveByOwner(ctx, owner, attachmentType)
}

func (s *Store) IsChannelManager(ctx context.Context, channel, subject UserKey) (bool, error) {
	return s.attachments.HasActive(ctx, channel, subject, AttachmentTypeChannelManager, nil)
}

func (s *Store) IsChannelWriter(ctx context.Context, channel, subject UserKey) (bool, error) {
	return s.attachments.HasActive(ctx, channel, subject, AttachmentTypeChannelWriter, nil)
}

func (s *Store) validateAttachmentUsersTx(ctx context.Context, tx *sql.Tx, ownerKey, subjectKey UserKey, attachmentType AttachmentType) error {
	owner, err := s.getUserByIDTx(ctx, tx, ownerKey, false)
	if err != nil {
		return err
	}
	subject, err := s.getUserByIDTx(ctx, tx, subjectKey, false)
	if err != nil {
		return err
	}

	switch attachmentType {
	case AttachmentTypeChannelManager, AttachmentTypeChannelWriter:
		if owner.Role != RoleChannel {
			return fmt.Errorf("%w: attachment owner must be a channel", ErrInvalidInput)
		}
		if !subject.CanLogin() {
			return fmt.Errorf("%w: attachment subject must be a login user", ErrInvalidInput)
		}
	case AttachmentTypeChannelSubscription:
		if ownerKey == subjectKey {
			return fmt.Errorf("%w: subscriber cannot subscribe to itself", ErrInvalidInput)
		}
		if !owner.CanLogin() {
			return fmt.Errorf("%w: subscription owner must be a login user", ErrInvalidInput)
		}
		if subject.Role != RoleChannel {
			return fmt.Errorf("%w: subscription subject must be a channel", ErrInvalidInput)
		}
	case AttachmentTypeUserBlacklist:
		if ownerKey == subjectKey {
			return fmt.Errorf("%w: owner cannot block itself", ErrInvalidInput)
		}
		if !owner.CanLogin() {
			return fmt.Errorf("%w: blacklist owner must be a login user", ErrInvalidInput)
		}
		if subject.Role != RoleUser {
			return fmt.Errorf("%w: blacklist subject must be a user", ErrInvalidInput)
		}
	default:
		return fmt.Errorf("%w: unsupported attachment type %q", ErrInvalidInput, attachmentType)
	}
	return nil
}

func (s *Store) getAttachmentTx(ctx context.Context, tx *sql.Tx, owner, subject UserKey, attachmentType AttachmentType) (Attachment, error) {
	row := tx.QueryRowContext(ctx, `
SELECT owner_node_id, owner_user_id, subject_node_id, subject_user_id, attachment_type, config_json, attached_at_hlc, deleted_at_hlc, origin_node_id
FROM user_attachments
WHERE owner_node_id = ? AND owner_user_id = ? AND subject_node_id = ? AND subject_user_id = ? AND attachment_type = ?
`, owner.NodeID, owner.UserID, subject.NodeID, subject.UserID, string(attachmentType))
	attachment, err := scanAttachment(row)
	if err == sql.ErrNoRows {
		return Attachment{}, ErrNotFound
	}
	if err != nil {
		return Attachment{}, err
	}
	return attachment, nil
}

func (s *Store) upsertAttachmentTx(ctx context.Context, tx *sql.Tx, attachment Attachment) error {
	if err := attachment.Owner.Validate(); err != nil {
		return err
	}
	if err := attachment.Subject.Validate(); err != nil {
		return err
	}
	if _, err := NormalizeAttachmentType(string(attachment.Type)); err != nil {
		return err
	}
	configJSON, err := normalizeAttachmentConfigJSON(attachment.ConfigJSON)
	if err != nil {
		return err
	}
	if attachment.AttachedAt == (clock.Timestamp{}) {
		return fmt.Errorf("%w: attached_at is required", ErrInvalidInput)
	}
	if attachment.OriginNodeID <= 0 {
		return fmt.Errorf("%w: attachment origin node id is required", ErrInvalidInput)
	}

	deletedAt := nullableTimestampString(attachment.DeletedAt)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO user_attachments(
    owner_node_id, owner_user_id, subject_node_id, subject_user_id,
    attachment_type, config_json, attached_at_hlc, deleted_at_hlc, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(owner_node_id, owner_user_id, subject_node_id, subject_user_id, attachment_type) DO UPDATE SET
    config_json = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_attachments.deleted_at_hlc IS NULL OR excluded.attached_at_hlc > user_attachments.deleted_at_hlc
        ) AND excluded.attached_at_hlc >= user_attachments.attached_at_hlc THEN excluded.config_json
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            user_attachments.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > user_attachments.deleted_at_hlc
        ) THEN excluded.config_json
        ELSE user_attachments.config_json
    END,
    attached_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_attachments.deleted_at_hlc IS NULL OR excluded.attached_at_hlc > user_attachments.deleted_at_hlc
        ) AND excluded.attached_at_hlc > user_attachments.attached_at_hlc THEN excluded.attached_at_hlc
        ELSE user_attachments.attached_at_hlc
    END,
    deleted_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_attachments.deleted_at_hlc IS NULL OR excluded.attached_at_hlc > user_attachments.deleted_at_hlc
        ) THEN NULL
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            user_attachments.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > user_attachments.deleted_at_hlc
        ) AND excluded.deleted_at_hlc >= user_attachments.attached_at_hlc THEN excluded.deleted_at_hlc
        ELSE user_attachments.deleted_at_hlc
    END,
    origin_node_id = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            user_attachments.deleted_at_hlc IS NULL OR excluded.attached_at_hlc > user_attachments.deleted_at_hlc
        ) AND excluded.attached_at_hlc >= user_attachments.attached_at_hlc THEN excluded.origin_node_id
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            user_attachments.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > user_attachments.deleted_at_hlc
        ) THEN excluded.origin_node_id
        ELSE user_attachments.origin_node_id
    END
`, attachment.Owner.NodeID, attachment.Owner.UserID, attachment.Subject.NodeID, attachment.Subject.UserID,
		string(attachment.Type), configJSON, attachment.AttachedAt.String(), deletedAt, attachment.OriginNodeID); err != nil {
		return fmt.Errorf("upsert attachment: %w", err)
	}
	return nil
}

func (s *Store) countActiveAttachmentsByOwnerTx(ctx context.Context, tx *sql.Tx, owner UserKey, attachmentType AttachmentType) (int, error) {
	if err := owner.Validate(); err != nil {
		return 0, err
	}
	if _, err := NormalizeAttachmentType(string(attachmentType)); err != nil {
		return 0, err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM user_attachments
WHERE owner_node_id = ? AND owner_user_id = ? AND attachment_type = ? AND deleted_at_hlc IS NULL
`, owner.NodeID, owner.UserID, string(attachmentType)).Scan(&count); err != nil {
		return 0, fmt.Errorf("count active attachments: %w", err)
	}
	return count, nil
}
