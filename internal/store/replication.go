package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

// ToReplicatedEvent 将本地 Event 转换为 protobuf ReplicatedEvent 格式，用于跨节点 mesh 传输。
// 序列化失败时返回 nil。
func ToReplicatedEvent(event Event) *internalproto.ReplicatedEvent {
	replicated := &internalproto.ReplicatedEvent{
		EventId:         event.EventID,
		AggregateType:   event.Aggregate,
		AggregateNodeId: event.AggregateNodeID,
		AggregateId:     event.AggregateID,
		Hlc:             event.HLC.String(),
		OriginNodeId:    event.OriginNodeID,
	}
	if err := replicated.SetTypedBody(event.Body); err != nil {
		return nil
	}
	return replicated
}

// ApplyReplicatedEvent 应用来自 peer 的复制事件。去重后提交到事件日志，并投影副作用
// （用户 upsert/delete、消息创建、附件 upsert/delete、元数据 upsert/delete、登录名变更）。
func (s *Store) ApplyReplicatedEvent(ctx context.Context, event *internalproto.ReplicatedEvent) error {
	if event == nil {
		return fmt.Errorf("%w: replicated event cannot be nil", ErrInvalidInput)
	}
	if event.EventId == 0 {
		return fmt.Errorf("%w: event id cannot be empty", ErrInvalidInput)
	}

	decoded, err := eventFromReplicatedEvent(event)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin apply replicated event: %w", err)
	}
	defer tx.Rollback()

	applied, err := s.isEventAppliedTx(ctx, tx, event.OriginNodeId, event.EventId)
	if err != nil {
		return err
	}
	if applied {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit duplicate replicated event: %w", err)
		}
		return nil
	}

	deferredMessageProjection := false
	changedUser := false
	switch body := decoded.Body.(type) {
	case *internalproto.UserCreatedEvent, *internalproto.UserUpdatedEvent:
		if err := s.applyReplicatedUserUpsert(ctx, tx, body); err != nil {
			return err
		}
		changedUser = true
	case *internalproto.UserDeletedEvent:
		if err := s.applyReplicatedUserDeleted(ctx, tx, body, decoded.OriginNodeID); err != nil {
			return err
		}
		changedUser = true
	case *internalproto.MessageCreatedEvent:
		deferredMessageProjection = true
	case *internalproto.UserAttachmentUpsertedEvent, *internalproto.UserAttachmentDeletedEvent:
		if err := s.applyReplicatedAttachment(ctx, tx, body, decoded.OriginNodeID); err != nil {
			return err
		}
	case *internalproto.UserMetadataUpsertedEvent, *internalproto.UserMetadataDeletedEvent:
		if err := s.applyReplicatedUserMetadata(ctx, tx, body, decoded.OriginNodeID); err != nil {
			return err
		}
	case *internalproto.UserLoginNameUpsertedEvent, *internalproto.UserLoginNameDeletedEvent:
		if err := s.applyReplicatedUserLoginName(ctx, tx, body, decoded.OriginNodeID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unsupported replicated event body %T", ErrInvalidInput, decoded.Body)
	}

	stored, inserted, err := s.backend.StoreReplicatedEventTx(ctx, tx, event, decoded)
	if err != nil {
		return err
	}
	if !inserted {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit duplicate event log entry: %w", err)
		}
		return nil
	}
	decoded.Sequence = stored.Sequence

	appliedAt := s.clock.Observe(decoded.HLC)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO applied_events(event_id, source_node_id, applied_at_hlc)
VALUES(?, ?, ?)
`, decoded.EventID, decoded.OriginNodeID, appliedAt.String()); err != nil {
		if isUniqueConstraint(err) {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit duplicate applied event: %w", err)
			}
			return nil
		}
		return fmt.Errorf("insert applied event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit replicated event: %w", err)
	}
	if changedUser {
		s.invalidateUserCache()
	}
	if !deferredMessageProjection {
		return nil
	}
	if err := s.projectMessageEvent(ctx, decoded); err != nil {
		if recordErr := s.recordPendingProjection(ctx, decoded, err); recordErr != nil {
			return fmt.Errorf("record pending projection for replicated event %d:%d: %w", decoded.OriginNodeID, decoded.EventID, recordErr)
		}
		return nil
	}
	return nil
}

// eventFromReplicatedEvent 将 protobuf ReplicatedEvent 解析为内部 Event 结构。
// 提取 HLC 时间戳、事件体（EventBody）和其他元数据字段。
func eventFromReplicatedEvent(event *internalproto.ReplicatedEvent) (Event, error) {
	hlc, err := parseRequiredTimestamp(event.Hlc, "event hlc")
	if err != nil {
		return Event{}, err
	}
	body := event.GetTypedBody()
	if body == nil {
		return Event{}, fmt.Errorf("%w: replicated event body cannot be empty", ErrInvalidInput)
	}
	return Event{
		EventID:         event.EventId,
		EventType:       EventType(internalproto.EventTypeFromBody(body)),
		Aggregate:       event.AggregateType,
		AggregateNodeID: event.AggregateNodeId,
		AggregateID:     event.AggregateId,
		HLC:             hlc,
		OriginNodeID:    event.OriginNodeId,
		Body:            body,
	}, nil
}

// applyReplicatedUserDeleted 处理来自 peer 的用户删除事件。
// 构造 UserKey 后委托 applyUserDeleteTx 执行软删除，设置 originNodeID 标明事件来源。
func (s *Store) applyReplicatedUserDeleted(ctx context.Context, tx *sql.Tx, body *internalproto.UserDeletedEvent, originNodeID int64) error {
	if body == nil {
		return fmt.Errorf("%w: user deleted event cannot be nil", ErrInvalidInput)
	}
	key := UserKey{NodeID: body.NodeId, UserID: body.UserId}
	if err := key.Validate(); err != nil {
		return err
	}

	deletedAt, err := parseRequiredTimestamp(body.DeletedAtHlc, "deleted_at_hlc")
	if err != nil {
		return err
	}
	return s.applyUserDeleteTx(ctx, tx, key, deletedAt, originNodeID, false)
}

// applyReplicatedMessageCreated 处理来自 peer 的消息创建事件。
// 校验消息标识、收件人和发件人信息后，直接插入 messages 表（无事件日志写入，事件日志由调用方负责）。
// 插入后触发消息修剪（trimMessagesForUserTx），并利用唯一约束实现幂等。
func (s *Store) applyReplicatedMessageCreated(ctx context.Context, tx *sql.Tx, body *internalproto.MessageCreatedEvent, originNodeID int64) error {
	if body == nil {
		return fmt.Errorf("%w: message created event cannot be nil", ErrInvalidInput)
	}
	if body.Recipient == nil {
		return fmt.Errorf("%w: message recipient cannot be empty", ErrInvalidInput)
	}
	key := UserKey{NodeID: body.Recipient.NodeId, UserID: body.Recipient.UserId}
	if err := validateMessageIdentity(key, body.NodeId, body.Seq); err != nil {
		return err
	}
	if originNodeID != 0 && originNodeID != body.NodeId {
		return fmt.Errorf("%w: message node id %d does not match event origin %d", ErrInvalidInput, body.NodeId, originNodeID)
	}
	if body.Sender == nil {
		return fmt.Errorf("%w: message sender cannot be empty", ErrInvalidInput)
	}

	if _, err := s.getUserByIDTx(ctx, tx, key, false); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc, session)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
`, body.Recipient.NodeId, body.Recipient.UserId, body.NodeId, body.Seq, body.Sender.NodeId, body.Sender.UserId, body.Body, body.CreatedAtHlc,
		MessageSession(UserKey{NodeID: body.Sender.NodeId, UserID: body.Sender.UserId}, UserKey{NodeID: body.Recipient.NodeId, UserID: body.Recipient.UserId}),
	); err != nil {
		if isUniqueConstraint(err) {
			return nil
		}
		return fmt.Errorf("insert replicated message: %w", err)
	}
	if err := s.trimMessagesForUserTx(ctx, tx, key); err != nil {
		return err
	}
	return nil
}

// applyReplicatedAttachment 处理来自 peer 的用户附件变更（新增或删除）。
// 先解析事件体中的附件数据，校验所有者与被关联用户的有效性，然后委托 upsertAttachmentTx。
func (s *Store) applyReplicatedAttachment(ctx context.Context, tx *sql.Tx, body internalproto.EventBody, originNodeID int64) error {
	attachment, err := attachmentFromEventBody(body)
	if err != nil {
		return err
	}
	if originNodeID != 0 && originNodeID != attachment.OriginNodeID {
		return fmt.Errorf("%w: attachment origin node id %d does not match event origin %d", ErrInvalidInput, attachment.OriginNodeID, originNodeID)
	}
	if attachment.DeletedAt == nil {
		if err := s.validateAttachmentUsersTx(ctx, tx, attachment.Owner, attachment.Subject, attachment.Type); err != nil {
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

// attachmentFromEventBody 从事件体中提取 Attachment 结构，支持 UserAttachmentUpsertedEvent 和 UserAttachmentDeletedEvent。
func attachmentFromEventBody(body internalproto.EventBody) (Attachment, error) {
	switch typed := body.(type) {
	case *internalproto.UserAttachmentUpsertedEvent:
		return attachmentFromData(typed.Owner, typed.Subject, typed.AttachmentType, typed.ConfigJson, typed.AttachedAtHlc, "", typed.OriginNodeId)
	case *internalproto.UserAttachmentDeletedEvent:
		return attachmentFromData(typed.Owner, typed.Subject, typed.AttachmentType, typed.ConfigJson, typed.AttachedAtHlc, typed.DeletedAtHlc, typed.OriginNodeId)
	default:
		return Attachment{}, fmt.Errorf("%w: unsupported attachment body %T", ErrInvalidInput, body)
	}
}

// applyReplicatedUserMetadata 处理来自 peer 的用户元数据变更（写入或删除）。
// 校验元数据所有者有效后委托 upsertUserMetadataTx 执行。
func (s *Store) applyReplicatedUserMetadata(ctx context.Context, tx *sql.Tx, body internalproto.EventBody, originNodeID int64) error {
	metadata, err := userMetadataFromEventBody(body)
	if err != nil {
		return err
	}
	if originNodeID != 0 && originNodeID != metadata.OriginNodeID {
		return fmt.Errorf("%w: metadata origin node id %d does not match event origin %d", ErrInvalidInput, metadata.OriginNodeID, originNodeID)
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

// userMetadataFromEventBody 从事件体中提取 UserMetadata 结构，支持 Upserted 和 Deleted 两种事件体类型。
func userMetadataFromEventBody(body internalproto.EventBody) (UserMetadata, error) {
	switch typed := body.(type) {
	case *internalproto.UserMetadataUpsertedEvent:
		return userMetadataFromData(typed.Owner, typed.Key, typed.Value, typed.UpdatedAtHlc, "", typed.ExpiresAt, typed.OriginNodeId)
	case *internalproto.UserMetadataDeletedEvent:
		return userMetadataFromData(typed.Owner, typed.Key, nil, "", typed.DeletedAtHlc, "", typed.OriginNodeId)
	default:
		return UserMetadata{}, fmt.Errorf("%w: unsupported metadata body %T", ErrInvalidInput, body)
	}
}

// applyReplicatedUserLoginName 处理来自 peer 的用户登录名变更（绑定或解绑）。
// 若为绑定操作则先清除该用户的其他活跃登录名，并通过 BoundAt 时间戳比较确保最新绑定生效（CRDT 风格）。
func (s *Store) applyReplicatedUserLoginName(ctx context.Context, tx *sql.Tx, body internalproto.EventBody, originNodeID int64) error {
	binding, err := userLoginNameFromEventBody(body)
	if err != nil {
		return err
	}
	if originNodeID != 0 && originNodeID != binding.OriginNodeID {
		return fmt.Errorf("%w: login name origin node id %d does not match event origin %d", ErrInvalidInput, binding.OriginNodeID, originNodeID)
	}
	if binding.DeletedAt == nil {
		user, err := s.getUserByIDTx(ctx, tx, binding.User, false)
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
		if _, err := s.clearOtherActiveUserLoginNamesTx(ctx, tx, binding.User, binding.LoginName, binding.BoundAt, binding.OriginNodeID); err != nil {
			return err
		}
		remaining, err := s.listActiveUserLoginNamesTx(ctx, tx, binding.User, binding.LoginName)
		if err != nil {
			return err
		}
		for _, item := range remaining {
			if item.BoundAt.Compare(binding.BoundAt) > 0 {
				return nil
			}
		}
	}
	return s.upsertUserLoginNameTx(ctx, tx, binding)
}

// userLoginNameFromEventBody 从事件体中提取 UserLoginName 结构，支持 Upserted 和 Deleted 两种事件体类型。
func userLoginNameFromEventBody(body internalproto.EventBody) (UserLoginName, error) {
	switch typed := body.(type) {
	case *internalproto.UserLoginNameUpsertedEvent:
		return userLoginNameFromData(typed.LoginName, typed.UserNodeId, typed.UserId, typed.BoundAtHlc, "", typed.OriginNodeId)
	case *internalproto.UserLoginNameDeletedEvent:
		return userLoginNameFromData(typed.LoginName, typed.UserNodeId, typed.UserId, typed.BoundAtHlc, typed.DeletedAtHlc, typed.OriginNodeId)
	default:
		return UserLoginName{}, fmt.Errorf("%w: unsupported login name body %T", ErrInvalidInput, body)
	}
}

// attachmentFromData 从 protobuf 引用和时间戳原始字符串构造 Attachment 结构。
// 校验所有者和被关联用户的有效性、归一化附件类型和配置 JSON。
func attachmentFromData(ownerRef, subjectRef *internalproto.ClusterUserRef, rawType, rawConfig, attachedAtRaw, deletedAtRaw string, originNodeID int64) (Attachment, error) {
	if ownerRef == nil {
		return Attachment{}, fmt.Errorf("%w: owner cannot be empty", ErrInvalidInput)
	}
	if subjectRef == nil {
		return Attachment{}, fmt.Errorf("%w: subject cannot be empty", ErrInvalidInput)
	}
	attachmentType, err := NormalizeAttachmentType(rawType)
	if err != nil {
		return Attachment{}, err
	}
	configJSON, err := normalizeAttachmentConfigJSON(rawConfig)
	if err != nil {
		return Attachment{}, err
	}
	attachment := Attachment{
		Owner:        UserKey{NodeID: ownerRef.NodeId, UserID: ownerRef.UserId},
		Subject:      UserKey{NodeID: subjectRef.NodeId, UserID: subjectRef.UserId},
		Type:         attachmentType,
		ConfigJSON:   configJSON,
		OriginNodeID: originNodeID,
	}
	if err := attachment.Owner.Validate(); err != nil {
		return Attachment{}, err
	}
	if err := attachment.Subject.Validate(); err != nil {
		return Attachment{}, err
	}
	attachedAt, err := parseRequiredTimestamp(attachedAtRaw, "attachment attached_at")
	if err != nil {
		return Attachment{}, err
	}
	attachment.AttachedAt = attachedAt
	if strings.TrimSpace(deletedAtRaw) != "" {
		deletedAt, err := parseRequiredTimestamp(deletedAtRaw, "attachment deleted_at")
		if err != nil {
			return Attachment{}, err
		}
		attachment.DeletedAt = &deletedAt
	}
	if attachment.OriginNodeID <= 0 {
		return Attachment{}, fmt.Errorf("%w: attachment origin node id is required", ErrInvalidInput)
	}
	return attachment, nil
}

// userMetadataFromData 从 protobuf 引用和时间戳原始字符串构造 UserMetadata 结构。
// 校验所有者有效性、归一化元数据键、解析到期时间。
func userMetadataFromData(ownerRef *internalproto.ClusterUserRef, rawKey string, value []byte, updatedAtRaw, deletedAtRaw, expiresAtRaw string, originNodeID int64) (UserMetadata, error) {
	if ownerRef == nil {
		return UserMetadata{}, fmt.Errorf("%w: owner cannot be empty", ErrInvalidInput)
	}
	key, err := NormalizeUserMetadataKey(rawKey)
	if err != nil {
		return UserMetadata{}, err
	}
	metadata := UserMetadata{
		Owner:        UserKey{NodeID: ownerRef.NodeId, UserID: ownerRef.UserId},
		Key:          key,
		Value:        append([]byte(nil), value...),
		OriginNodeID: originNodeID,
	}
	if err := metadata.Owner.Validate(); err != nil {
		return UserMetadata{}, err
	}
	if strings.TrimSpace(updatedAtRaw) != "" {
		updatedAt, err := parseRequiredTimestamp(updatedAtRaw, "metadata updated_at")
		if err != nil {
			return UserMetadata{}, err
		}
		metadata.UpdatedAt = updatedAt
	}
	if strings.TrimSpace(deletedAtRaw) != "" {
		deletedAt, err := parseRequiredTimestamp(deletedAtRaw, "metadata deleted_at")
		if err != nil {
			return UserMetadata{}, err
		}
		metadata.DeletedAt = &deletedAt
	}
	expiresAt, err := ParseUserMetadataExpiresAt(expiresAtRaw)
	if err != nil {
		return UserMetadata{}, err
	}
	metadata.ExpiresAt = expiresAt
	if metadata.OriginNodeID <= 0 {
		return UserMetadata{}, fmt.Errorf("%w: metadata origin node id is required", ErrInvalidInput)
	}
	if metadata.DeletedAt == nil && metadata.UpdatedAt == (clock.Timestamp{}) {
		return UserMetadata{}, fmt.Errorf("%w: metadata updated_at is required", ErrInvalidInput)
	}
	return metadata, nil
}

// userLoginNameFromData 从原始字符串数据构造 UserLoginName 结构。
// 归一化登录名、解析绑定时间戳、校验用户 Key 有效性。
func userLoginNameFromData(rawLoginName string, userNodeID, userID int64, boundAtRaw, deletedAtRaw string, originNodeID int64) (UserLoginName, error) {
	item := UserLoginName{
		LoginName:    normalizeLoginName(rawLoginName),
		User:         UserKey{NodeID: userNodeID, UserID: userID},
		OriginNodeID: originNodeID,
	}
	if item.LoginName == "" {
		return UserLoginName{}, fmt.Errorf("%w: login_name cannot be empty", ErrInvalidInput)
	}
	if err := item.User.Validate(); err != nil {
		return UserLoginName{}, err
	}
	boundAt, err := parseRequiredTimestamp(boundAtRaw, "login name bound_at")
	if err != nil {
		return UserLoginName{}, err
	}
	item.BoundAt = boundAt
	if strings.TrimSpace(deletedAtRaw) != "" {
		deletedAt, err := parseRequiredTimestamp(deletedAtRaw, "login name deleted_at")
		if err != nil {
			return UserLoginName{}, err
		}
		item.DeletedAt = &deletedAt
	}
	if item.OriginNodeID <= 0 {
		return UserLoginName{}, fmt.Errorf("%w: login name origin node id is required", ErrInvalidInput)
	}
	return item, nil
}

// isEventAppliedTx 在事务中检查某来源节点的事件是否已被应用（查 applied_events 表）。
// 用于复制事件的去重判断，确保幂等性。
func (s *Store) isEventAppliedTx(ctx context.Context, tx *sql.Tx, sourceNodeID, eventID int64) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM applied_events WHERE source_node_id = ? AND event_id = ?`, sourceNodeID, eventID).Scan(&count); err != nil {
		return false, fmt.Errorf("check applied event: %w", err)
	}
	return count > 0, nil
}

func (s *Store) getUserByIDTx(ctx context.Context, tx *sql.Tx, key UserKey, includeDeleted bool) (User, error) {
	if err := key.Validate(); err != nil {
		return User{}, err
	}
	query := `
SELECT node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
FROM users
WHERE node_id = ? AND user_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at_hlc IS NULL`
	}
	row := tx.QueryRowContext(ctx, query, key.NodeID, key.UserID)
	user, err := scanUser(row)
	if err == sql.ErrNoRows {
		return User{}, ErrNotFound
	}
	return user, err
}

// parseRequiredTimestamp 解析必需的 HLC 时间戳字符串，field 用于错误描述。
// 时间戳为空或无效时返回错误。
func parseRequiredTimestamp(raw, field string) (clock.Timestamp, error) {
	ts, err := clock.ParseTimestamp(strings.TrimSpace(raw))
	if err != nil {
		return clock.Timestamp{}, fmt.Errorf("%s: %w", field, err)
	}
	return ts, nil
}

// defaultJSON 为空字符串时返回 "{}" 作为默认 JSON 值。
func defaultJSON(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return value
}

// isUniqueConstraint 检查错误是否为 SQL UNIQUE 约束违反，用于幂等写入判断。
func isUniqueConstraint(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "unique")
}

// userFromCreatedEvent 从 UserCreatedEvent protobuf 构造 User 结构。
// 解析所有版本时间戳（username、password_hash、profile、role），归一化角色并校验用户 Key。
func userFromCreatedEvent(data *internalproto.UserCreatedEvent, eventOriginNodeID int64) (User, error) {
	if data == nil {
		return User{}, fmt.Errorf("%w: user created event cannot be nil", ErrInvalidInput)
	}
	createdAt, err := parseRequiredTimestamp(data.CreatedAtHlc, "user created_at")
	if err != nil {
		return User{}, err
	}
	updatedAt, err := parseRequiredTimestamp(data.UpdatedAtHlc, "user updated_at")
	if err != nil {
		return User{}, err
	}
	versionUsername, err := parseRequiredTimestamp(data.VersionUsername, "user version_username")
	if err != nil {
		return User{}, err
	}
	versionPasswordHash, err := parseRequiredTimestamp(data.VersionPasswordHash, "user version_password_hash")
	if err != nil {
		return User{}, err
	}
	versionProfile, err := parseRequiredTimestamp(data.VersionProfile, "user version_profile")
	if err != nil {
		return User{}, err
	}
	versionRole, err := parseRequiredTimestamp(data.VersionRole, "user version_role")
	if err != nil {
		return User{}, err
	}
	role, err := normalizeAnyRole(data.Role)
	if err != nil {
		return User{}, err
	}
	if eventOriginNodeID != 0 && data.OriginNodeId != eventOriginNodeID {
		return User{}, fmt.Errorf("%w: user origin node id %d does not match event origin %d", ErrInvalidInput, data.OriginNodeId, eventOriginNodeID)
	}
	key := UserKey{NodeID: data.NodeId, UserID: data.UserId}
	if err := key.Validate(); err != nil {
		return User{}, err
	}

	user := User{
		NodeID:              data.NodeId,
		ID:                  data.UserId,
		Username:            data.Username,
		PasswordHash:        data.PasswordHash,
		Profile:             defaultJSON(data.Profile),
		Role:                role,
		SystemReserved:      data.SystemReserved,
		CreatedAt:           createdAt,
		UpdatedAt:           updatedAt,
		VersionUsername:     versionUsername,
		VersionPasswordHash: versionPasswordHash,
		VersionProfile:      versionProfile,
		VersionRole:         versionRole,
		OriginNodeID:        data.OriginNodeId,
	}
	user.SystemReserved = user.SystemReserved && isSystemReservedUserID(user.ID)
	return user, nil
}

// userFromUpdatedEvent 从 UserUpdatedEvent protobuf 构造 User 结构。
// 在 userFromCreatedEvent 基础上额外解析 VersionDeleted 和 DeletedAt 字段。
func userFromUpdatedEvent(data *internalproto.UserUpdatedEvent, eventOriginNodeID int64) (User, error) {
	if data == nil {
		return User{}, fmt.Errorf("%w: user updated event cannot be nil", ErrInvalidInput)
	}
	user, err := userFromCreatedEvent(&internalproto.UserCreatedEvent{
		NodeId:              data.NodeId,
		UserId:              data.UserId,
		Username:            data.Username,
		PasswordHash:        data.PasswordHash,
		Profile:             data.Profile,
		Role:                data.Role,
		SystemReserved:      data.SystemReserved,
		CreatedAtHlc:        data.CreatedAtHlc,
		UpdatedAtHlc:        data.UpdatedAtHlc,
		VersionUsername:     data.VersionUsername,
		VersionPasswordHash: data.VersionPasswordHash,
		VersionProfile:      data.VersionProfile,
		VersionRole:         data.VersionRole,
		OriginNodeId:        data.OriginNodeId,
	}, eventOriginNodeID)
	if err != nil {
		return User{}, err
	}
	if strings.TrimSpace(data.VersionDeleted) != "" {
		parsed, err := parseRequiredTimestamp(data.VersionDeleted, "user version_deleted")
		if err != nil {
			return User{}, err
		}
		user.VersionDeleted = &parsed
	}
	if strings.TrimSpace(data.DeletedAtHlc) != "" {
		parsed, err := parseRequiredTimestamp(data.DeletedAtHlc, "user deleted_at")
		if err != nil {
			return User{}, err
		}
		user.DeletedAt = &parsed
	}
	return user, nil
}

// applyReplicatedUserUpsert 是用户数据的复制入口：处理来自 peer 的用户创建/更新事件。
// CRDT 冲突解决采用"每个字段独立版本化"的策略（mergeReplicatedUser）：
//   - 如果本地不存在该用户，直接插入
//   - 如果本地已存在，对 username、password_hash、profile、role 四个字段分别按版本时间戳取最大值
//
// 此外还处理墓碑（tombstone）检查、系统保留用户不变性维护、bootstrap 管理员修复。
func (s *Store) applyReplicatedUserUpsert(ctx context.Context, tx *sql.Tx, body internalproto.EventBody) error {
	var (
		incoming User
		err      error
	)
	switch typed := body.(type) {
	case *internalproto.UserCreatedEvent:
		incoming, err = userFromCreatedEvent(typed, typed.OriginNodeId)
	case *internalproto.UserUpdatedEvent:
		incoming, err = userFromUpdatedEvent(typed, typed.OriginNodeId)
	default:
		return fmt.Errorf("%w: unsupported user body %T", ErrInvalidInput, body)
	}
	if err != nil {
		return err
	}

	key := incoming.Key()
	if _, exists, err := s.getTombstoneTx(ctx, tx, "user", key); err != nil {
		return err
	} else if exists {
		return nil
	}
	incoming = s.applyReservedUserInvariants(incoming)

	current, err := s.getUserByIDTx(ctx, tx, key, true)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		incoming.UpdatedAt = latestUserVersion(incoming)
		if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    node_id, user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, incoming.NodeID, incoming.ID, incoming.Username, incoming.PasswordHash, defaultJSON(incoming.Profile), incoming.Role,
			boolToInt(incoming.SystemReserved), incoming.CreatedAt.String(), incoming.UpdatedAt.String(),
			nullableTimestampString(incoming.DeletedAt), incoming.VersionUsername.String(),
			incoming.VersionPasswordHash.String(), incoming.VersionProfile.String(),
			incoming.VersionRole.String(), nullableTimestampString(incoming.VersionDeleted),
			incoming.OriginNodeID); err != nil {
			return fmt.Errorf("insert replicated user: %w", err)
		}
		return s.reconcileBootstrapAdminsTx(ctx, tx)
	default:
		return err
	}

	if current.DeletedAt != nil || current.VersionDeleted != nil {
		return nil
	}

	merged := mergeReplicatedUser(current, incoming)
	merged = s.applyReservedUserInvariants(merged)
	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, role = ?, system_reserved = ?, updated_at_hlc = ?,
    version_username = ?, version_password_hash = ?, version_profile = ?, version_role = ?
WHERE node_id = ? AND user_id = ?
`, merged.Username, merged.PasswordHash, defaultJSON(merged.Profile), merged.Role, boolToInt(merged.SystemReserved),
		merged.UpdatedAt.String(), merged.VersionUsername.String(), merged.VersionPasswordHash.String(),
		merged.VersionProfile.String(), merged.VersionRole.String(), merged.NodeID, merged.ID); err != nil {
		return fmt.Errorf("update replicated user: %w", err)
	}
	return s.reconcileBootstrapAdminsTx(ctx, tx)
}

// mergeReplicatedUser 实现 CRDT 风格的每个字段冲突解决（Merge Strategy）。
// 对 username、password_hash、profile、role 四个字段分别比较各自的版本时间戳：
//   - 版本时间戳更新的字段胜出（Last Writer Wins, LWW）
//   - 如果某字段的 incoming 版本比 current 更新，则替换该字段及其版本戳
//   - systemReserved 取或运算（只要任一为 true 则保留 true）
//   - UpdatedAt 取所有字段中最大的版本时间戳
//
// 这种设计保证了即使在不同节点上并发修改同一用户的不同字段也不会丢失更新。
func mergeReplicatedUser(current, incoming User) User {
	merged := current

	if incoming.VersionUsername.Compare(current.VersionUsername) > 0 {
		merged.Username = incoming.Username
		merged.VersionUsername = incoming.VersionUsername
	}
	if incoming.VersionPasswordHash.Compare(current.VersionPasswordHash) > 0 {
		merged.PasswordHash = incoming.PasswordHash
		merged.VersionPasswordHash = incoming.VersionPasswordHash
	}
	if incoming.VersionProfile.Compare(current.VersionProfile) > 0 {
		merged.Profile = defaultJSON(incoming.Profile)
		merged.VersionProfile = incoming.VersionProfile
	}
	if incoming.VersionRole.Compare(current.VersionRole) > 0 {
		merged.Role = incoming.Role
		merged.VersionRole = incoming.VersionRole
	}
	if current.SystemReserved || incoming.SystemReserved {
		merged.SystemReserved = true
	}

	merged.UpdatedAt = latestUserVersion(merged)
	return merged
}

// latestUserVersion 返回用户所有版本化字段中的最大时间戳。
// 用于确定用户整体的最新更新时间（UpdatedAt）。
func latestUserVersion(user User) clock.Timestamp {
	latest := user.VersionUsername
	if user.VersionPasswordHash.Compare(latest) > 0 {
		latest = user.VersionPasswordHash
	}
	if user.VersionProfile.Compare(latest) > 0 {
		latest = user.VersionProfile
	}
	if user.VersionRole.Compare(latest) > 0 {
		latest = user.VersionRole
	}
	if user.VersionDeleted != nil && user.VersionDeleted.Compare(latest) > 0 {
		latest = *user.VersionDeleted
	}
	return latest
}

// nullableTimestampString 将可选时间戳转换为 SQL 可接受的 nil 或字符串。
func nullableTimestampString(ts *clock.Timestamp) any {
	if ts == nil {
		return nil
	}
	return ts.String()
}
