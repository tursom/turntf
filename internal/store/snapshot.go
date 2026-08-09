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

// 快照分区键常量。
// 非消息数据（users, login_names, attachments, user_metadata）使用全量分区，
// 消息数据按来源节点分片（messages/{originNodeID}）。
const (
	// 快照共分为 5 个分区（partition）：
	//   users/full           - 全量用户数据
	//   login_names/full     - 全量登录名数据
	//   attachments/full     - 全量附件数据
	//   user_metadata/full   - 全量用户元数据
	//   messages/{nodeID}    - 按来源节点分片的消息数据
	// 非消息数据使用全量分区（单个 chunk），消息数据按来源节点分片以支持并行传输。
	SnapshotUsersPartition        = "users/full"
	SnapshotLoginNamesPartition   = "login_names/full"
	SnapshotAttachmentsPartition  = "attachments/full"
	SnapshotUserMetadataPartition = "user_metadata/full"
	SnapshotMessagesPrefix        = "messages/"

	snapshotPartitionKindUsers      = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS
	snapshotPartitionKindMessage    = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_MESSAGES
	snapshotPartitionKindAttachment = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_ATTACHMENTS
	snapshotPartitionKindMetadata   = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USER_METADATA
	snapshotPartitionKindLoginNames = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_LOGIN_NAMES
)

// MessageSnapshotPartition 构造指定来源节点的消息快照分区键。
func MessageSnapshotPartition(originNodeID int64) string {
	return SnapshotMessagesPrefix + strconv.FormatInt(originNodeID, 10)
}

// BuildSnapshotDigest 构建所有分区的 SHA-256 哈希摘要，用于与 peer 比较快照差异。
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

	loginNameRows, err := s.buildLoginNameSnapshotRows(ctx)
	if err != nil {
		return nil, err
	}
	loginNameHash, err := hashSnapshotRows(loginNameRows)
	if err != nil {
		return nil, err
	}
	partitions = append(partitions, &clusterproto.SnapshotPartitionDigest{
		Partition: SnapshotLoginNamesPartition,
		Kind:      snapshotPartitionKindLoginNames,
		RowCount:  uint64(len(loginNameRows)),
		Hash:      loginNameHash,
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

	metadataRows, err := s.buildUserMetadataSnapshotRows(ctx)
	if err != nil {
		return nil, err
	}
	metadataHash, err := hashSnapshotRows(metadataRows)
	if err != nil {
		return nil, err
	}
	partitions = append(partitions, &clusterproto.SnapshotPartitionDigest{
		Partition: SnapshotUserMetadataPartition,
		Kind:      snapshotPartitionKindMetadata,
		RowCount:  uint64(len(metadataRows)),
		Hash:      metadataHash,
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

// BuildSnapshotChunk 构建指定分区的快照 chunk，包含该分区的所有数据行。
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
	case partition == SnapshotLoginNamesPartition:
		rows, err := s.buildLoginNameSnapshotRows(ctx)
		if err != nil {
			return nil, err
		}
		return &clusterproto.SnapshotChunk{
			Partition: partition,
			Kind:      snapshotPartitionKindLoginNames,
			Rows:      rows,
		}, nil
	case partition == SnapshotUserMetadataPartition:
		rows, err := s.buildUserMetadataSnapshotRows(ctx)
		if err != nil {
			return nil, err
		}
		return &clusterproto.SnapshotChunk{
			Partition: partition,
			Kind:      snapshotPartitionKindMetadata,
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

// ApplySnapshotChunk 应用快照 chunk，替换本地该分区的数据。
// 在事务中按行应用（插入或 CRDT 风格更新），应用后更新 origin cursor 防止重复。
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

	var subscriptionChanges []subscriptionChangeKey
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
			attachment, err := attachmentFromSnapshotRow(row.GetAttachment())
			if err != nil {
				return err
			}
			if attachment.Type == AttachmentTypeChannelSubscription {
				subscriptionChanges = append(subscriptionChanges, subscriptionChangeKey{
					subscriber: attachment.Owner,
					channel:    attachment.Subject,
				})
			}
		}
	case partition == SnapshotLoginNamesPartition:
		if chunk.Kind != snapshotPartitionKindLoginNames {
			return fmt.Errorf("%w: login names snapshot chunk has kind %s", ErrInvalidInput, chunk.Kind)
		}
		for _, row := range chunk.Rows {
			if err := s.applyLoginNameSnapshotRowTx(ctx, tx, row); err != nil {
				return err
			}
		}
	case partition == SnapshotUserMetadataPartition:
		if chunk.Kind != snapshotPartitionKindMetadata {
			return fmt.Errorf("%w: metadata snapshot chunk has kind %s", ErrInvalidInput, chunk.Kind)
		}
		for _, row := range chunk.Rows {
			if err := s.applyUserMetadataSnapshotRowTx(ctx, tx, row); err != nil {
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
	s.notifySubscriptionChanges(subscriptionChanges)
	if maxTimestamp != (clock.Timestamp{}) {
		s.clock.Observe(maxTimestamp)
	}
	return nil
}

// MaxSnapshotChunkTimestamp 从 chunk 所有行中提取最大的 HLC 时间戳。
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
		if metadataRow := row.GetUserMetadata(); metadataRow != nil {
			ts, err := parseRequiredTimestamp(metadataRow.UpdatedAtHlc, "snapshot metadata updated_at")
			if err != nil {
				return clock.Timestamp{}, err
			}
			record(ts)
			if strings.TrimSpace(metadataRow.DeletedAtHlc) != "" {
				ts, err := parseRequiredTimestamp(metadataRow.DeletedAtHlc, "snapshot metadata deleted_at")
				if err != nil {
					return clock.Timestamp{}, err
				}
				record(ts)
			}
			continue
		}
		if loginNameRow := row.GetLoginName(); loginNameRow != nil {
			ts, err := parseRequiredTimestamp(loginNameRow.BoundAtHlc, "snapshot login name bound_at")
			if err != nil {
				return clock.Timestamp{}, err
			}
			record(ts)
			if strings.TrimSpace(loginNameRow.DeletedAtHlc) != "" {
				ts, err := parseRequiredTimestamp(loginNameRow.DeletedAtHlc, "snapshot login name deleted_at")
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

// buildUserSnapshotRows 构建用户数据快照：从 users 表读取所有用户记录，同时查询 tombstones
// 表中已删除的用户墓碑。返回的 SnapshotRow 数组包含用户行和墓碑行两种类型。
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

// buildMessageSnapshotRows 按来源节点（producer）构建消息数据快照。
// 注意：这是一个私有辅助函数，与 storeBackend.MessageProjection().BuildMessageSnapshotRows() 不同，
// 后者由具体后端（SQLite/Pebble）实现。此函数直接从 messages 表查询数据。
func (s *Store) buildMessageSnapshotRows(ctx context.Context, producer int64) ([]*clusterproto.SnapshotRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session
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

// buildAttachmentSnapshotRows 构建附件数据快照：从 user_attachments 表读取所有记录。
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

// buildLoginNameSnapshotRows 构建登录名数据快照：从 user_login_names 表读取所有记录。
func (s *Store) buildLoginNameSnapshotRows(ctx context.Context) ([]*clusterproto.SnapshotRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT login_name, user_node_id, user_id, bound_at_hlc, deleted_at_hlc, origin_node_id
FROM user_login_names
ORDER BY login_name ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query snapshot login names: %w", err)
	}
	defer rows.Close()

	snapshotRows := make([]*clusterproto.SnapshotRow, 0)
	for rows.Next() {
		item, err := scanUserLoginNameRaw(rows)
		if err != nil {
			return nil, err
		}
		snapshotRows = append(snapshotRows, snapshotRowFromLoginName(item))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot login names: %w", err)
	}
	return snapshotRows, nil
}

// buildUserMetadataSnapshotRows 构建用户元数据快照：从 user_metadata 表读取所有记录。
func (s *Store) buildUserMetadataSnapshotRows(ctx context.Context) ([]*clusterproto.SnapshotRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT owner_node_id, owner_user_id, key, value, updated_at_hlc, deleted_at_hlc, expires_at, origin_node_id
FROM user_metadata
ORDER BY owner_node_id ASC, owner_user_id ASC, key ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query snapshot user metadata: %w", err)
	}
	defer rows.Close()

	snapshotRows := make([]*clusterproto.SnapshotRow, 0)
	for rows.Next() {
		metadata, err := scanUserMetadata(rows)
		if err != nil {
			return nil, err
		}
		snapshotRows = append(snapshotRows, snapshotRowFromUserMetadata(metadata))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot user metadata: %w", err)
	}
	return snapshotRows, nil
}

// applyUserSnapshotRowTx 在事务中应用一条用户快照行。
// 如果是墓碑行则调用 applySnapshotTombstoneTx 执行删除。
// 如果是用户行且标记为已删除（DeletedAt 或 VersionDeleted 不为空），则执行软删除。
// 否则通过 applyReplicatedUserUpsert 以 CRDT 方式插入或合并用户数据。
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

// applySnapshotTombstoneTx 在事务中应用墓碑行：构造 UserKey 并执行软删除。
// 当前只支持 "user" 类型的墓碑。
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

// applyMessageSnapshotRowTx 在事务中应用一条消息快照行。
// 校验消息标识（recipient, node_id, seq, producer），校验收件人存在性后插入 messages 表。
// 如果消息已存在（唯一约束冲突）则跳过，返回收件人的 UserKey。
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
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
`, messageRow.Recipient.NodeId, messageRow.Recipient.UserId, messageRow.NodeId, messageRow.Seq, messageRow.Sender.NodeId, messageRow.Sender.UserId, messageRow.Body, messageRow.CreatedAtHlc,
		MessageSession(UserKey{NodeID: messageRow.Sender.NodeId, UserID: messageRow.Sender.UserId}, UserKey{NodeID: messageRow.Recipient.NodeId, UserID: messageRow.Recipient.UserId}),
	); err != nil {
		if isUniqueConstraint(err) {
			return key, nil
		}
		return UserKey{}, fmt.Errorf("insert snapshot message: %w", err)
	}
	return key, nil
}

// applyAttachmentSnapshotRowTx 在事务中应用一条附件快照行。
// 校验所有者和被关联用户存在性后委托 upsertAttachmentTx 执行 upsert。
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

// applyLoginNameSnapshotRowTx 在事务中应用一条登录名快照行。
// 对于未删除的登录名：校验用户存在性、清除冲突登录名（CRDT 绑定时间戳比较），确保最终一致性。
func (s *Store) applyLoginNameSnapshotRowTx(ctx context.Context, tx *sql.Tx, row *clusterproto.SnapshotRow) error {
	if row == nil {
		return fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	loginNameRow := row.GetLoginName()
	if loginNameRow == nil {
		return fmt.Errorf("%w: login names snapshot contains non-login-name row", ErrInvalidInput)
	}
	item, err := userLoginNameFromSnapshotRow(loginNameRow)
	if err != nil {
		return err
	}
	if item.DeletedAt == nil {
		user, err := s.getUserByIDTx(ctx, tx, item.User, false)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
		if err := validateLoginNameUser(user); err != nil {
			if errors.Is(err, ErrInvalidInput) {
				return nil
			}
			return err
		}
		if _, err := s.clearOtherActiveUserLoginNamesTx(ctx, tx, item.User, item.LoginName, item.BoundAt, item.OriginNodeID); err != nil {
			return err
		}
		remaining, err := s.listActiveUserLoginNamesTx(ctx, tx, item.User, item.LoginName)
		if err != nil {
			return err
		}
		for _, other := range remaining {
			if other.BoundAt.Compare(item.BoundAt) > 0 {
				return nil
			}
		}
	}
	return s.upsertUserLoginNameTx(ctx, tx, item)
}

// applyUserMetadataSnapshotRowTx 在事务中应用一条用户元数据快照行。
// 校验所有者存在性后委托 upsertUserMetadataTx 执行 upsert。
func (s *Store) applyUserMetadataSnapshotRowTx(ctx context.Context, tx *sql.Tx, row *clusterproto.SnapshotRow) error {
	if row == nil {
		return fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	metadataRow := row.GetUserMetadata()
	if metadataRow == nil {
		return fmt.Errorf("%w: metadata snapshot contains non-metadata row", ErrInvalidInput)
	}
	metadata, err := userMetadataFromSnapshotRow(metadataRow)
	if err != nil {
		return err
	}
	if metadata.DeletedAt == nil {
		if err := s.validateUserMetadataOwnerTx(ctx, tx, metadata.Owner); err != nil {
			return err
		}
	} else {
		owner, err := s.getUserByIDTx(ctx, tx, metadata.Owner, false)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
		if err := validateUserMetadataOwner(owner); err != nil {
			if errors.Is(err, ErrInvalidInput) {
				return nil
			}
			return err
		}
	}
	return s.upsertUserMetadataTx(ctx, tx, metadata)
}

// snapshotRowFromUser 将内部 User 结构转换为 protobuf SnapshotRow（用户类型）。
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

// snapshotRowFromMessage 将内部 Message 结构转换为 protobuf SnapshotRow（消息类型）。
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

// snapshotRowFromAttachment 将内部 Attachment 结构转换为 protobuf SnapshotRow（附件类型）。
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

// snapshotRowFromLoginName 将内部 UserLoginName 结构转换为 protobuf SnapshotRow（登录名类型）。
func snapshotRowFromLoginName(item UserLoginName) *clusterproto.SnapshotRow {
	row := &clusterproto.SnapshotLoginNameRow{
		LoginName:    item.LoginName,
		UserNodeId:   item.User.NodeID,
		UserId:       item.User.UserID,
		BoundAtHlc:   item.BoundAt.String(),
		OriginNodeId: item.OriginNodeID,
	}
	if item.DeletedAt != nil {
		row.DeletedAtHlc = item.DeletedAt.String()
	}
	return &clusterproto.SnapshotRow{
		Body: &clusterproto.SnapshotRow_LoginName{
			LoginName: row,
		},
	}
}

// snapshotRowFromUserMetadata 将内部 UserMetadata 结构转换为 protobuf SnapshotRow（用户元数据类型）。
func snapshotRowFromUserMetadata(metadata UserMetadata) *clusterproto.SnapshotRow {
	row := &clusterproto.SnapshotUserMetadataRow{
		Owner:        &clusterproto.ClusterUserRef{NodeId: metadata.Owner.NodeID, UserId: metadata.Owner.UserID},
		Key:          metadata.Key,
		Value:        append([]byte(nil), metadata.Value...),
		UpdatedAtHlc: metadata.UpdatedAt.String(),
		OriginNodeId: metadata.OriginNodeID,
	}
	if metadata.DeletedAt != nil {
		row.DeletedAtHlc = metadata.DeletedAt.String()
	}
	if metadata.ExpiresAt != nil {
		row.ExpiresAt = FormatUserMetadataExpiresAt(*metadata.ExpiresAt)
	}
	return &clusterproto.SnapshotRow{
		Body: &clusterproto.SnapshotRow_UserMetadata{
			UserMetadata: row,
		},
	}
}

// userFromSnapshotRow 从 protobuf SnapshotUserRow 反序列化为内部 User 结构。
// 解析所有时间戳字段和版本戳，校验用户 Key 有效性。
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

// attachmentFromSnapshotRow 从 protobuf SnapshotAttachmentRow 构造 Attachment。
func attachmentFromSnapshotRow(row *clusterproto.SnapshotAttachmentRow) (Attachment, error) {
	if row == nil {
		return Attachment{}, fmt.Errorf("%w: snapshot attachment cannot be nil", ErrInvalidInput)
	}
	return attachmentFromData(row.Owner, row.Subject, row.AttachmentType, row.ConfigJson, row.AttachedAtHlc, row.DeletedAtHlc, row.OriginNodeId)
}

// userMetadataFromSnapshotRow 从 protobuf SnapshotUserMetadataRow 构造 UserMetadata。
func userMetadataFromSnapshotRow(row *clusterproto.SnapshotUserMetadataRow) (UserMetadata, error) {
	if row == nil {
		return UserMetadata{}, fmt.Errorf("%w: snapshot metadata cannot be nil", ErrInvalidInput)
	}
	return userMetadataFromData(row.Owner, row.Key, row.Value, row.UpdatedAtHlc, row.DeletedAtHlc, row.ExpiresAt, row.OriginNodeId)
}

// userLoginNameFromSnapshotRow 从 protobuf SnapshotLoginNameRow 构造 UserLoginName。
func userLoginNameFromSnapshotRow(row *clusterproto.SnapshotLoginNameRow) (UserLoginName, error) {
	if row == nil {
		return UserLoginName{}, fmt.Errorf("%w: snapshot login name cannot be nil", ErrInvalidInput)
	}
	return userLoginNameFromData(row.LoginName, row.UserNodeId, row.UserId, row.BoundAtHlc, row.DeletedAtHlc, row.OriginNodeId)
}

// timestampSnapshotString 将可选时间戳转换为快照序列化用的字符串（空指针返回空字符串）。
func timestampSnapshotString(ts *clock.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.String()
}

// hashSnapshotRows 计算一组快照行的 SHA-256 摘要，使用确定性 protobuf 序列化保证跨节点一致。
// 用于快照摘要比较：当两个节点的分区哈希一致时，无需传输该分区数据。
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

// parseSnapshotProducer 从消息分区键（格式 "messages/{nodeID}"）中解析来源节点 ID。
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

// normalizeProducerNodeIDs 去重并排序来源节点 ID 列表。
// 过滤掉非正数 ID，确保快照构建和处理顺序确定。
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
