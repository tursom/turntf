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
	SnapshotUsersPartition            = "users/full"
	SnapshotSubscriptionsPartition    = "subscriptions/full"
	SnapshotMessagesPrefix            = "messages/"
	snapshotPartitionKindUsers        = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_USERS
	snapshotPartitionKindMessage      = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_MESSAGES
	snapshotPartitionKindSubscription = clusterproto.SnapshotPartitionKind_SNAPSHOT_PARTITION_KIND_SUBSCRIPTIONS
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

	subscriptionRows, err := s.buildSubscriptionSnapshotRows(ctx)
	if err != nil {
		return nil, err
	}
	subscriptionHash, err := hashSnapshotRows(subscriptionRows)
	if err != nil {
		return nil, err
	}
	partitions = append(partitions, &clusterproto.SnapshotPartitionDigest{
		Partition: SnapshotSubscriptionsPartition,
		Kind:      snapshotPartitionKindSubscription,
		RowCount:  uint64(len(subscriptionRows)),
		Hash:      subscriptionHash,
	})

	for _, producer := range normalizeProducerNodeIDs(producerNodeIDs) {
		rows, err := s.messageProjection.BuildMessageSnapshotRows(ctx, producer)
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
	case partition == SnapshotSubscriptionsPartition:
		rows, err := s.buildSubscriptionSnapshotRows(ctx)
		if err != nil {
			return nil, err
		}
		return &clusterproto.SnapshotChunk{
			Partition: partition,
			Kind:      snapshotPartitionKindSubscription,
			Rows:      rows,
		}, nil
	case strings.HasPrefix(partition, SnapshotMessagesPrefix):
		producer, err := parseSnapshotProducer(partition)
		if err != nil {
			return nil, fmt.Errorf("%w: message snapshot partition missing producer", ErrInvalidInput)
		}
		rows, err := s.messageProjection.BuildMessageSnapshotRows(ctx, producer)
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
	case partition == SnapshotSubscriptionsPartition:
		if chunk.Kind != snapshotPartitionKindSubscription {
			return fmt.Errorf("%w: subscriptions snapshot chunk has kind %s", ErrInvalidInput, chunk.Kind)
		}
		for _, row := range chunk.Rows {
			if err := s.applySubscriptionSnapshotRowTx(ctx, tx, row); err != nil {
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
		return s.messageProjection.ApplyMessageSnapshotRows(ctx, producer, chunk.Rows)
	default:
		return fmt.Errorf("%w: unsupported snapshot partition %q", ErrInvalidInput, partition)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit apply snapshot chunk: %w", err)
	}
	if partition == SnapshotUsersPartition {
		s.invalidateUserCache()
	}
	return nil
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
SELECT user_node_id, user_id, node_id, seq, sender, body, created_at_hlc
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

func (s *Store) buildSubscriptionSnapshotRows(ctx context.Context) ([]*clusterproto.SnapshotRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id, subscribed_at_hlc, deleted_at_hlc, origin_node_id
FROM channel_subscriptions
ORDER BY subscriber_node_id ASC, subscriber_user_id ASC, channel_node_id ASC, channel_user_id ASC
`)
	if err != nil {
		return nil, fmt.Errorf("query snapshot subscriptions: %w", err)
	}
	defer rows.Close()

	snapshotRows := make([]*clusterproto.SnapshotRow, 0)
	for rows.Next() {
		subscription, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		snapshotRows = append(snapshotRows, snapshotRowFromSubscription(subscription))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot subscriptions: %w", err)
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
	key := UserKey{NodeID: messageRow.UserNodeId, UserID: messageRow.UserId}
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

	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender, body, created_at_hlc)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, messageRow.UserNodeId, messageRow.UserId, messageRow.NodeId, messageRow.Seq, messageRow.Sender, messageRow.Body, messageRow.CreatedAtHlc); err != nil {
		if isUniqueConstraint(err) {
			return key, nil
		}
		return UserKey{}, fmt.Errorf("insert snapshot message: %w", err)
	}
	return key, nil
}

func (s *Store) applySubscriptionSnapshotRowTx(ctx context.Context, tx *sql.Tx, row *clusterproto.SnapshotRow) error {
	if row == nil {
		return fmt.Errorf("%w: snapshot row cannot be nil", ErrInvalidInput)
	}
	subscriptionRow := row.GetSubscription()
	if subscriptionRow == nil {
		return fmt.Errorf("%w: subscriptions snapshot contains non-subscription row", ErrInvalidInput)
	}
	subscription, err := subscriptionFromSnapshotRow(subscriptionRow)
	if err != nil {
		return err
	}
	if subscription.DeletedAt == nil {
		if err := s.validateSubscriptionUsersTx(ctx, tx, subscription.Subscriber, subscription.Channel); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
	} else {
		if _, err := s.getUserByIDTx(ctx, tx, subscription.Subscriber, false); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
		if _, err := s.getUserByIDTx(ctx, tx, subscription.Channel, false); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			return err
		}
	}
	return s.upsertSubscriptionTx(ctx, tx, subscription)
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
				UserNodeId:   message.UserNodeID,
				UserId:       message.UserID,
				NodeId:       message.NodeID,
				Seq:          message.Seq,
				Sender:       message.Sender,
				Body:         message.Body,
				CreatedAtHlc: message.CreatedAt.String(),
			},
		},
	}
}

func snapshotRowFromSubscription(subscription Subscription) *clusterproto.SnapshotRow {
	row := &clusterproto.SnapshotSubscriptionRow{
		SubscriberNodeId: subscription.Subscriber.NodeID,
		SubscriberUserId: subscription.Subscriber.UserID,
		ChannelNodeId:    subscription.Channel.NodeID,
		ChannelUserId:    subscription.Channel.UserID,
		SubscribedAtHlc:  subscription.SubscribedAt.String(),
		OriginNodeId:     subscription.OriginNodeID,
	}
	if subscription.DeletedAt != nil {
		row.DeletedAtHlc = subscription.DeletedAt.String()
	}
	return &clusterproto.SnapshotRow{
		Body: &clusterproto.SnapshotRow_Subscription{
			Subscription: row,
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

func subscriptionFromSnapshotRow(row *clusterproto.SnapshotSubscriptionRow) (Subscription, error) {
	if row == nil {
		return Subscription{}, fmt.Errorf("%w: snapshot subscription cannot be nil", ErrInvalidInput)
	}
	subscription := Subscription{
		Subscriber:   UserKey{NodeID: row.SubscriberNodeId, UserID: row.SubscriberUserId},
		Channel:      UserKey{NodeID: row.ChannelNodeId, UserID: row.ChannelUserId},
		OriginNodeID: row.OriginNodeId,
	}
	if err := subscription.Subscriber.Validate(); err != nil {
		return Subscription{}, err
	}
	if err := subscription.Channel.Validate(); err != nil {
		return Subscription{}, err
	}
	subscribedAt, err := parseRequiredTimestamp(row.SubscribedAtHlc, "snapshot subscription subscribed_at")
	if err != nil {
		return Subscription{}, err
	}
	subscription.SubscribedAt = subscribedAt
	if strings.TrimSpace(row.DeletedAtHlc) != "" {
		deletedAt, err := parseRequiredTimestamp(row.DeletedAtHlc, "snapshot subscription deleted_at")
		if err != nil {
			return Subscription{}, err
		}
		subscription.DeletedAt = &deletedAt
	}
	if subscription.OriginNodeID <= 0 {
		return Subscription{}, fmt.Errorf("%w: snapshot subscription origin node id is required", ErrInvalidInput)
	}
	return subscription, nil
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
