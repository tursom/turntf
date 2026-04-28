package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	clusterproto "github.com/tursom/turntf/internal/proto"
)

const (
	SnapshotUsersPartition          = "users/full"
	SnapshotAttachmentsPartition    = "attachments/full"
	SnapshotMessagesPrefix          = "messages/"
	snapshotPartitionKindUsers      = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS
	snapshotPartitionKindMessage    = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_MESSAGES
	snapshotPartitionKindAttachment = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_ATTACHMENTS
)

func MessageSnapshotPartition(originNodeID int64) string {
	return SnapshotMessagesPrefix + strconv.FormatInt(originNodeID, 10)
}

func (s *Store) BuildSnapshotDigest(ctx context.Context, producerNodeIDs []int64) (*clusterproto.SnapshotDigest, error) {
	partitions := make([]*clusterproto.SnapshotPartitionDigest, 0, 1+len(producerNodeIDs))

	userRows, err := s.buildUserSnapshotRows(ctx)
	if err != nil {
		return nil, err
	}
	userHash, err := hashSnapshotRows(userRows)
	if err != nil {
		return nil, err
	}
	partitions = append(partitions, &clusterproto.SnapshotPartitionDigest{
		Partition: SnapshotUsersPartition,
		Kind:      snapshotPartitionKindUsers,
		RowCount:  uint64(len(userRows)),
		Hash:      userHash,
	})

	attachmentRows, err := s.buildAttachmentSnapshotRows(ctx)
	if err != nil {
		return nil, err
	}
	attachmentHash, err := hashSnapshotRows(attachmentRows)
	if err != nil {
		return nil, err
	}
	partitions = append(partitions, &clusterproto.SnapshotPartitionDigest{
		Partition: SnapshotAttachmentsPartition,
		Kind:      snapshotPartitionKindAttachment,
		RowCount:  uint64(len(attachmentRows)),
		Hash:      attachmentHash,
	})

	for _, producer := range normalizeProducerNodeIDs(producerNodeIDs) {
		rows, err := s.backend.MessageProjection().BuildMessageSnapshotRows(ctx, producer)
		if err != nil {
			return nil, err
		}
		rowHash, err := hashSnapshotRows(rows)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, &clusterproto.SnapshotPartitionDigest{
			Partition: MessageSnapshotPartition(producer),
			Kind:      snapshotPartitionKindMessage,
			RowCount:  uint64(len(rows)),
			Hash:      rowHash,
		})
	}

	return &clusterproto.SnapshotDigest{Partitions: partitions}, nil
}

func (s *Store) BuildSnapshotChunk(ctx context.Context, partition string) (*clusterproto.SnapshotChunk, error) {
	partition = strings.TrimSpace(partition)
	switch {
	case partition == SnapshotUsersPartition:
		rows, err := s.buildUserSnapshotRows(ctx)
		if err != nil {
			return nil, err
		}
		return &clusterproto.SnapshotChunk{
			Partition: partition,
			Kind:      snapshotPartitionKindUsers,
			Rows:      rows,
		}, nil
	case partition == SnapshotAttachmentsPartition:
		rows, err := s.buildAttachmentSnapshotRows(ctx)
		if err != nil {
			return nil, err
		}
		return &clusterproto.SnapshotChunk{
			Partition: partition,
			Kind:      snapshotPartitionKindAttachment,
			Rows:      rows,
		}, nil
	case strings.HasPrefix(partition, SnapshotMessagesPrefix):
		producer, err := parseSnapshotProducer(partition)
		if err != nil {
			return nil, fmt.Errorf("%w: message snapshot partition missing producer", ErrInvalidInput)
		}
		rows, err := s.backend.MessageProjection().BuildMessageSnapshotRows(ctx, producer)
		if err != nil {
			return nil, err
		}
		return &clusterproto.SnapshotChunk{
			Partition: partition,
			Kind:      snapshotPartitionKindMessage,
			Rows:      rows,
		}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported snapshot partition %q", ErrInvalidInput, partition)
	}
}

func (s *Store) ApplySnapshotChunk(ctx context.Context, chunk *clusterproto.SnapshotChunk) error {
	if chunk == nil {
		return fmt.Errorf("%w: snapshot chunk cannot be nil", ErrInvalidInput)
	}
	maxTimestamp, err := MaxSnapshotChunkTimestamp(chunk)
	if err != nil {
		return err
	}
	partition := strings.TrimSpace(chunk.Partition)
	if partition == "" {
		return fmt.Errorf("%w: snapshot partition cannot be empty", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin apply snapshot chunk: %w", err)
	}
	defer tx.Rollback()

	switch {
	case partition == SnapshotUsersPartition:
		if chunk.Kind != snapshotPartitionKindUsers {
			return fmt.Errorf("%w: users snapshot chunk has kind %s", ErrInvalidInput, chunk.Kind)
		}
		for _, row := range chunk.Rows {
			if err := s.applyUserSnapshotRowTx(ctx, tx, row); err != nil {
				return err
			}
		}
	case partition == SnapshotAttachmentsPartition:
		if chunk.Kind != snapshotPartitionKindAttachment {
			return fmt.Errorf("%w: attachments snapshot chunk has kind %s", ErrInvalidInput, chunk.Kind)
		}
		for _, row := range chunk.Rows {
			if err := s.applyAttachmentSnapshotRowTx(ctx, tx, row); err != nil {
				return err
			}
		}
	case strings.HasPrefix(partition, SnapshotMessagesPrefix):
		if chunk.Kind != snapshotPartitionKindMessage {
			return fmt.Errorf("%w: messages snapshot chunk has kind %s", ErrInvalidInput, chunk.Kind)
		}
		producer, err := parseSnapshotProducer(partition)
		if err != nil {
			return fmt.Errorf("%w: message snapshot partition missing producer", ErrInvalidInput)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit apply snapshot chunk: %w", err)
		}
		return s.backend.MessageProjection().ApplyMessageSnapshotRows(ctx, producer, chunk.Rows)
	default:
		return fmt.Errorf("%w: unsupported snapshot partition %q", ErrInvalidInput, partition)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit apply snapshot chunk: %w", err)
	}
	if partition == SnapshotUsersPartition {
		s.invalidateUserCache()
	}
	if maxTimestamp != (clock.Timestamp{}) {
		s.clock.Observe(maxTimestamp)
	}
	return nil
}

func MaxSnapshotChunkTimestamp(chunk *clusterproto.SnapshotChunk) (clock.Timestamp, error) {
	if chunk == nil {
		return clock.Timestamp{}, fmt.Errorf("%w: snapshot chunk cannot be nil", ErrInvalidInput)
	}

	maxTimestamp := clock.Timestamp{}
	record := func(ts clock.Timestamp) {
		if maxTimestamp == (clock.Timestamp{}) || ts.Compare(maxTimestamp) > 0 {
			maxTimestamp = ts
		}
	}

	for _, row := range chunk.GetRows() {
		if row == nil {
			return clock.Timestamp{}, fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
		}
		if tombstone := row.GetTombstone(); tombstone != nil {
			ts, err := parseRequiredTimestamp(tombstone.DeletedAtHlc, "snapshot tombstone deleted_at")
			if err != nil {
				return clock.Timestamp{}, err
			}
			record(ts)
			continue
		}
		if userRow := row.GetUser(); userRow != nil {
			for _, field := range []struct {
				raw  string
				name string
			}{
				{raw: userRow.CreatedAtHlc, name: "snapshot user created_at"},
				{raw: userRow.UpdatedAtHlc, name: "snapshot user updated_at"},
				{raw: userRow.VersionUsername, name: "snapshot user version_username"},
				{raw: userRow.VersionPasswordHash, name: "snapshot user version_password_hash"},
				{raw: userRow.VersionProfile, name: "snapshot user version_profile"},
				{raw: userRow.VersionRole, name: "snapshot user version_role"},
			} {
				ts, err := parseRequiredTimestamp(field.raw, field.name)
				if err != nil {
					return clock.Timestamp{}, err
				}
				record(ts)
			}
			if strings.TrimSpace(userRow.DeletedAtHlc) != "" {
				ts, err := parseRequiredTimestamp(userRow.DeletedAtHlc, "snapshot user deleted_at")
				if err != nil {
					return clock.Timestamp{}, err
				}
				record(ts)
			}
			if strings.TrimSpace(userRow.VersionDeleted) != "" {
				ts, err := parseRequiredTimestamp(userRow.VersionDeleted, "snapshot user version_deleted")
				if err != nil {
					return clock.Timestamp{}, err
				}
				record(ts)
			}
			continue
		}
		if messageRow := row.GetMessage(); messageRow != nil {
			ts, err := parseRequiredTimestamp(messageRow.CreatedAtHlc, "snapshot message created_at")
			if err != nil {
				return clock.Timestamp{}, err
			}
			record(ts)
			continue
		}
		if attachmentRow := row.GetAttachment(); attachmentRow != nil {
			ts, err := parseRequiredTimestamp(attachmentRow.AttachedAtHlc, "snapshot attachment attached_at")
			if err != nil {
				return clock.Timestamp{}, err
			}
			record(ts)
			if strings.TrimSpace(attachmentRow.DeletedAtHlc) != "" {
				ts, err := parseRequiredTimestamp(attachmentRow.DeletedAtHlc, "snapshot attachment deleted_at")
				if err != nil {
					return clock.Timestamp{}, err
				}
				record(ts)
			}
			continue
		}
		return clock.Timestamp{}, fmt.Errorf("%w: snapshot row body cannot be empty", ErrInvalidInput)
	}
	return maxTimestamp, nil
}

func (s *Store) buildUserSnapshotRows(ctx context.Context) ([]*clusterproto.SnapshotRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
FROM users
ORDER BY node_id ASC, user_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query snapshot users: %w", err)
	}
	defer rows.Close()

	snapshotRows := make([]*clusterproto.SnapshotRow, 0)
	for rows.Next() {
		user, err := scanUser(rows)
		if err != nil {
			return nil, err
		}
		snapshotRows = append(snapshotRows, snapshotRowFromUser(user))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot users: %w", err)
	}

	tombstoneRows, err := s.db.QueryContext(ctx, `
SELECT entity_type, entity_node_id, entity_id, deleted_at_hlc, origin_node_id
FROM tombstones
WHERE entity_type = 'user'
ORDER BY entity_type ASC, entity_node_id ASC, entity_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query snapshot tombstones: %w", err)
	}
	defer tombstoneRows.Close()

	for tombstoneRows.Next() {
		var entityType, deletedAt string
		var entityNodeID, originNodeID int64
		var entityID int64
		if err := tombstoneRows.Scan(&entityType, &entityNodeID, &entityID, &deletedAt, &originNodeID); err != nil {
			return nil, fmt.Errorf("scan snapshot tombstone: %w", err)
		}
		snapshotRows = append(snapshotRows, &clusterproto.SnapshotRow{
			Body: &clusterproto.SnapshotRow_Tombstone{
				Tombstone: &clusterproto.SnapshotTombstoneRow{
					EntityType:   entityType,
					EntityNodeId: entityNodeID,
					EntityId:     entityID,
					DeletedAtHlc: deletedAt,
					OriginNodeId: originNodeID,
				},
			},
		})
	}
	if err := tombstoneRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot tombstones: %w", err)
	}
	return snapshotRows, nil
}

func (s *Store) buildMessageSnapshotRows(ctx context.Context, producer int64) ([]*clusterproto.SnapshotRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc
FROM messages
WHERE node_id = ?
ORDER BY user_node_id ASC, user_id ASC, created_at_hlc DESC, node_id ASC, seq DESC
`, producer)
	if err != nil {
		return nil, fmt.Errorf("query snapshot messages: %w", err)
	}
	defer rows.Close()

	snapshotRows := make([]*clusterproto.SnapshotRow, 0)
	for rows.Next() {
		message, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		snapshotRows = append(snapshotRows, snapshotRowFromMessage(message))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot messages: %w", err)
	}
	return snapshotRows, nil
}

func (s *Store) buildAttachmentSnapshotRows(ctx context.Context) ([]*clusterproto.SnapshotRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT owner_node_id, owner_user_id, subject_node_id, subject_user_id, attachment_type, config_json, attached_at_hlc, deleted_at_hlc, origin_node_id
FROM user_attachments
ORDER BY owner_node_id ASC, owner_user_id ASC, attachment_type ASC, subject_node_id ASC, subject_user_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query snapshot attachments: %w", err)
	}
	defer rows.Close()

	snapshotRows := make([]*clusterproto.SnapshotRow, 0)
	for rows.Next() {
		attachment, err := scanAttachment(rows)
		if err != nil {
			return nil, err
		}
		snapshotRows = append(snapshotRows, snapshotRowFromAttachment(attachment))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot attachments: %w", err)
	}
	return snapshotRows, nil
}

func (s *Store) applyUserSnapshotRowTx(ctx context.Context, tx *sql.Tx, row *clusterproto.SnapshotRow) error {
	if row == nil {
		return fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	if tombstone := row.GetTombstone(); tombstone != nil {
		return s.applySnapshotTombstoneTx(ctx, tx, tombstone)
	}

	userRow := row.GetUser()
	if userRow == nil {
		return fmt.Errorf("%w: users snapshot contains non-user row", ErrInvalidInput)
	}
	user, err := userFromSnapshotRow(userRow)
	if err != nil {
		return err
	}
	if user.DeletedAt != nil || user.VersionDeleted != nil {
		deletedAt := user.DeletedAt
		if deletedAt == nil {
			deletedAt = user.VersionDeleted
		}
		return s.applyUserDeleteTx(ctx, tx, user.Key(), *deletedAt, user.OriginNodeID, false)
	}

	return s.applyReplicatedUserUpsert(ctx, tx, userUpdatedProtoFromUser(user))
}

func (s *Store) applySnapshotTombstoneTx(ctx context.Context, tx *sql.Tx, row *clusterproto.SnapshotTombstoneRow) error {
	if row == nil {
		return fmt.Errorf("%w: snapshot tombstone cannot be nil", ErrInvalidInput)
	}
	if row.EntityType != "user" {
		return fmt.Errorf("%w: unsupported tombstone entity type %q", ErrInvalidInput, row.EntityType)
	}
	key := UserKey{NodeID: row.EntityNodeId, UserID: row.EntityId}
	if err := key.Validate(); err != nil {
		return err
	}
	deletedAt, err := parseRequiredTimestamp(row.DeletedAtHlc, "snapshot tombstone deleted_at")
	if err != nil {
		return err
	}
	return s.applyUserDeleteTx(ctx, tx, key, deletedAt, row.OriginNodeId, false)
}

func (s *Store) applyMessageSnapshotRowTx(ctx context.Context, tx *sql.Tx, producer int64, row *clusterproto.SnapshotRow) (UserKey, error) {
	if row == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	messageRow := row.GetMessage()
	if messageRow == nil {
		return UserKey{}, fmt.Errorf("%w: messages snapshot contains non-message row", ErrInvalidInput)
	}
	if messageRow.Recipient == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot message recipient cannot be empty", ErrInvalidInput)
	}
	key := UserKey{NodeID: messageRow.Recipient.NodeId, UserID: messageRow.Recipient.UserId}
	if err := validateMessageIdentity(key, messageRow.NodeId, messageRow.Seq); err != nil {
		return UserKey{}, err
	}
	if messageRow.NodeId != producer {
		return UserKey{}, fmt.Errorf("%w: message node id %d does not match partition producer %d", ErrInvalidInput, messageRow.NodeId, producer)
	}
	if _, err := parseRequiredTimestamp(messageRow.CreatedAtHlc, "snapshot message created_at"); err != nil {
		return UserKey{}, err
	}
	if _, err := s.getUserByIDTx(ctx, tx, key, false); err != nil {
		if errors.Is(err, ErrNotFound) {
			return UserKey{}, nil
		}
		return UserKey{}, err
	}

	if messageRow.Sender == nil {
		return UserKey{}, fmt.Errorf("%w: snapshot message sender cannot be empty", ErrInvalidInput)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
`, messageRow.Recipient.NodeId, messageRow.Recipient.UserId, messageRow.NodeId, messageRow.Seq, messageRow.Sender.NodeId, messageRow.Sender.UserId, messageRow.Body, messageRow.CreatedAtHlc); err != nil {
		if isUniqueConstraint(err) {
			return key, nil
		}
		return UserKey{}, fmt.Errorf("insert snapshot message: %w", err)
	}
	return key, nil
}

func (s *Store) applyAttachmentSnapshotRowTx(ctx context.Context, tx *sql.Tx, row *clusterproto.SnapshotRow) error {
	if row == nil {
		return fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	attachmentRow := row.GetAttachment()
	if attachmentRow == nil {
		return fmt.Errorf("%w: attachments snapshot contains non-attachment row", ErrInvalidInput)
	}
	attachment, err := attachmentFromSnapshotRow(attachmentRow)
	if err != nil {
		return err
	}
	if attachment.DeletedAt == nil {
		if err := s.validateAttachmentUsersTx(ctx, tx, attachment.Owner, attachment.Subject, attachment.Type); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
	} else {
		if _, err := s.getUserByIDTx(ctx, tx, attachment.Owner, false); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
		if _, err := s.getUserByIDTx(ctx, tx, attachment.Subject, false); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
	}
	return s.upsertAttachmentTx(ctx, tx, attachment)
}

func snapshotRowFromUser(user User) *clusterproto.SnapshotRow {
	return &clusterproto.SnapshotRow{
		Body: &clusterproto.SnapshotRow_User{
			User: &clusterproto.SnapshotUserRow{
				NodeId:              user.NodeID,
				UserId:              user.ID,
				Username:            user.Username,
				PasswordHash:        user.PasswordHash,
				Profile:             user.Profile,
				Role:                user.Role,
				SystemReserved:      user.SystemReserved,
				CreatedAtHlc:        user.CreatedAt.String(),
				UpdatedAtHlc:        user.UpdatedAt.String(),
				DeletedAtHlc:        timestampSnapshotString(user.DeletedAt),
				VersionUsername:     user.VersionUsername.String(),
				VersionPasswordHash: user.VersionPasswordHash.String(),
				VersionProfile:      user.VersionProfile.String(),
				VersionRole:         user.VersionRole.String(),
				VersionDeleted:      timestampSnapshotString(user.VersionDeleted),
				OriginNodeId:        user.OriginNodeID,
			},
		},
	}
}

func snapshotRowFromMessage(message Message) *clusterproto.SnapshotRow {
	return &clusterproto.SnapshotRow{
		Body: &clusterproto.SnapshotRow_Message{
			Message: &clusterproto.SnapshotMessageRow{
				Recipient:    &clusterproto.ClusterUserRef{NodeId: message.Recipient.NodeID, UserId: message.Recipient.UserID},
				NodeId:       message.NodeID,
				Seq:          message.Seq,
				Sender:       &clusterproto.ClusterUserRef{NodeId: message.Sender.NodeID, UserId: message.Sender.UserID},
				Body:         message.Body,
				CreatedAtHlc: message.CreatedAt.String(),
			},
		},
	}
}

func snapshotRowFromAttachment(attachment Attachment) *clusterproto.SnapshotRow {
	row := &clusterproto.SnapshotAttachmentRow{
		Owner:          &clusterproto.ClusterUserRef{NodeId: attachment.Owner.NodeID, UserId: attachment.Owner.UserID},
		Subject:        &clusterproto.ClusterUserRef{NodeId: attachment.Subject.NodeID, UserId: attachment.Subject.UserID},
		AttachmentType: string(attachment.Type),
		ConfigJson:     attachment.ConfigJSON,
		AttachedAtHlc:  attachment.AttachedAt.String(),
		OriginNodeId:   attachment.OriginNodeID,
	}
	if attachment.DeletedAt != nil {
		row.DeletedAtHlc = attachment.DeletedAt.String()
	}
	return &clusterproto.SnapshotRow{
		Body: &clusterproto.SnapshotRow_Attachment{
			Attachment: row,
		},
	}
}

func userFromSnapshotRow(row *clusterproto.SnapshotUserRow) (User, error) {
	key := UserKey{NodeID: row.NodeId, UserID: row.UserId}
	if err := key.Validate(); err != nil {
		return User{}, err
	}
	createdAt, err := parseRequiredTimestamp(row.CreatedAtHlc, "snapshot user created_at")
	if err != nil {
		return User{}, err
	}
	updatedAt, err := parseRequiredTimestamp(row.UpdatedAtHlc, "snapshot user updated_at")
	if err != nil {
		return User{}, err
	}
	versionUsername, err := parseRequiredTimestamp(row.VersionUsername, "snapshot user version_username")
	if err != nil {
		return User{}, err
	}
	versionPasswordHash, err := parseRequiredTimestamp(row.VersionPasswordHash, "snapshot user version_password_hash")
	if err != nil {
		return User{}, err
	}
	versionProfile, err := parseRequiredTimestamp(row.VersionProfile, "snapshot user version_profile")
	if err != nil {
		return User{}, err
	}
	versionRole, err := parseRequiredTimestamp(row.VersionRole, "snapshot user version_role")
	if err != nil {
		return User{}, err
	}
	role, err := normalizeAnyRole(row.Role)
	if err != nil {
		return User{}, err
	}

	user := User{
		NodeID:              row.NodeId,
		ID:                  row.UserId,
		Username:            row.Username,
		PasswordHash:        row.PasswordHash,
		Profile:             defaultJSON(row.Profile),
		Role:                role,
		SystemReserved:      row.SystemReserved,
		CreatedAt:           createdAt,
		UpdatedAt:           updatedAt,
		VersionUsername:     versionUsername,
		VersionPasswordHash: versionPasswordHash,
		VersionProfile:      versionProfile,
		VersionRole:         versionRole,
		OriginNodeID:        row.OriginNodeId,
	}
	if strings.TrimSpace(row.DeletedAtHlc) != "" {
		deletedAt, err := parseRequiredTimestamp(row.DeletedAtHlc, "snapshot user deleted_at")
		if err != nil {
			return User{}, err
		}
		user.DeletedAt = &deletedAt
	}
	if strings.TrimSpace(row.VersionDeleted) != "" {
		versionDeleted, err := parseRequiredTimestamp(row.VersionDeleted, "snapshot user version_deleted")
		if err != nil {
			return User{}, err
		}
		user.VersionDeleted = &versionDeleted
	}
	user.SystemReserved = user.SystemReserved && isSystemReservedUserID(user.ID)
	return user, nil
}

func attachmentFromSnapshotRow(row *clusterproto.SnapshotAttachmentRow) (Attachment, error) {
	if row == nil {
		return Attachment{}, fmt.Errorf("%w: snapshot attachment cannot be nil", ErrInvalidInput)
	}
	return attachmentFromData(row.Owner, row.Subject, row.AttachmentType, row.ConfigJson, row.AttachedAtHlc, row.DeletedAtHlc, row.OriginNodeId)
}

func timestampSnapshotString(ts *clock.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.String()
}

func hashSnapshotRows(rows []*clusterproto.SnapshotRow) ([]byte, error) {
	hasher := sha256.New()
	var length [8]byte
	marshalOptions := gproto.MarshalOptions{Deterministic: true}
	for _, row := range rows {
		data, err := marshalOptions.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("marshal snapshot row for hash: %w", err)
		}
		binary.BigEndian.PutUint64(length[:], uint64(len(data)))
		if _, err := hasher.Write(length[:]); err != nil {
			return nil, err
		}
		if _, err := hasher.Write(data); err != nil {
			return nil, err
		}
	}
	return hasher.Sum(nil), nil
}

func parseSnapshotProducer(partition string) (int64, error) {
	raw := strings.TrimSpace(strings.TrimPrefix(partition, SnapshotMessagesPrefix))
	if raw == "" {
		return 0, fmt.Errorf("empty producer")
	}
	producer, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || producer <= 0 {
		return 0, fmt.Errorf("invalid producer %q", raw)
	}
	return producer, nil
}

func normalizeProducerNodeIDs(nodeIDs []int64) []int64 {
	seen := make(map[int64]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID <= 0 {
			continue
		}
		seen[nodeID] = struct{}{}
	}
	normalized := make([]int64, 0, len(seen))
	for nodeID := range seen {
		normalized = append(normalized, nodeID)
	}
	sort.Slice(normalized, func(i, j int) bool {
		return normalized[i] < normalized[j]
	})
	return normalized
}
