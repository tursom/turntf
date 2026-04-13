package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

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
	case *internalproto.ChannelSubscribedEvent:
		if err := s.applyReplicatedChannelSubscription(ctx, tx, body, false, decoded.OriginNodeID); err != nil {
			return err
		}
	case *internalproto.ChannelUnsubscribedEvent:
		if err := s.applyReplicatedChannelSubscription(ctx, tx, body, true, decoded.OriginNodeID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unsupported replicated event body %T", ErrInvalidInput, decoded.Body)
	}

	if s.engine == EngineSQLite {
		value, err := gproto.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal replicated event: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO event_log(event_id, origin_node_id, value)
VALUES(?, ?, ?)
`, event.EventId, event.OriginNodeId, value); err != nil {
			if isUniqueConstraint(err) {
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("commit duplicate event log entry: %w", err)
				}
				return nil
			}
			return fmt.Errorf("insert replicated event log: %w", err)
		}
	} else {
		stored, inserted, err := s.eventLog.AppendReplicated(ctx, decoded)
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
	}

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
	if err := s.clearPendingProjection(ctx, decoded.OriginNodeID, decoded.EventID); err != nil {
		return err
	}
	return nil
}

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
INSERT INTO messages(user_node_id, user_id, node_id, seq, sender_node_id, sender_user_id, body, created_at_hlc)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
`, body.Recipient.NodeId, body.Recipient.UserId, body.NodeId, body.Seq, body.Sender.NodeId, body.Sender.UserId, body.Body, body.CreatedAtHlc); err != nil {
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

func (s *Store) applyReplicatedChannelSubscription(ctx context.Context, tx *sql.Tx, body internalproto.EventBody, deleted bool, originNodeID int64) error {
	subscription, err := subscriptionFromEventBody(body)
	if err != nil {
		return err
	}
	if originNodeID != 0 && originNodeID != subscription.OriginNodeID {
		return fmt.Errorf("%w: subscription origin node id %d does not match event origin %d", ErrInvalidInput, subscription.OriginNodeID, originNodeID)
	}

	if deleted {
		if subscription.DeletedAt == nil {
			return fmt.Errorf("%w: channel_unsubscribed missing deleted_at", ErrInvalidInput)
		}
	} else if subscription.DeletedAt != nil {
		return fmt.Errorf("%w: channel_subscribed cannot include deleted_at", ErrInvalidInput)
	}

	if subscription.DeletedAt == nil {
		if err := s.validateSubscriptionUsersTx(ctx, tx, subscription.Subscriber, subscription.Channel); err != nil {
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

func subscriptionFromEventBody(body internalproto.EventBody) (Subscription, error) {
	switch typed := body.(type) {
	case *internalproto.ChannelSubscribedEvent:
		return subscriptionFromChannelData(typed.Subscriber, typed.Channel, typed.SubscribedAtHlc, "", typed.OriginNodeId)
	case *internalproto.ChannelUnsubscribedEvent:
		return subscriptionFromChannelData(typed.Subscriber, typed.Channel, typed.SubscribedAtHlc, typed.DeletedAtHlc, typed.OriginNodeId)
	default:
		return Subscription{}, fmt.Errorf("%w: unsupported subscription body %T", ErrInvalidInput, body)
	}
}

func subscriptionFromChannelData(subscriberRef, channelRef *internalproto.ClusterUserRef, subscribedAtRaw, deletedAtRaw string, originNodeID int64) (Subscription, error) {
	if subscriberRef == nil {
		return Subscription{}, fmt.Errorf("%w: subscriber cannot be empty", ErrInvalidInput)
	}
	if channelRef == nil {
		return Subscription{}, fmt.Errorf("%w: channel cannot be empty", ErrInvalidInput)
	}
	subscription := Subscription{
		Subscriber:   UserKey{NodeID: subscriberRef.NodeId, UserID: subscriberRef.UserId},
		Channel:      UserKey{NodeID: channelRef.NodeId, UserID: channelRef.UserId},
		OriginNodeID: originNodeID,
	}
	if err := subscription.Subscriber.Validate(); err != nil {
		return Subscription{}, err
	}
	if err := subscription.Channel.Validate(); err != nil {
		return Subscription{}, err
	}
	subscribedAt, err := parseRequiredTimestamp(subscribedAtRaw, "subscription subscribed_at")
	if err != nil {
		return Subscription{}, err
	}
	subscription.SubscribedAt = subscribedAt
	if strings.TrimSpace(deletedAtRaw) != "" {
		deletedAt, err := parseRequiredTimestamp(deletedAtRaw, "subscription deleted_at")
		if err != nil {
			return Subscription{}, err
		}
		subscription.DeletedAt = &deletedAt
	}
	if subscription.OriginNodeID <= 0 {
		return Subscription{}, fmt.Errorf("%w: subscription origin node id is required", ErrInvalidInput)
	}
	return subscription, nil
}

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

func parseRequiredTimestamp(raw, field string) (clock.Timestamp, error) {
	ts, err := clock.ParseTimestamp(strings.TrimSpace(raw))
	if err != nil {
		return clock.Timestamp{}, fmt.Errorf("%s: %w", field, err)
	}
	return ts, nil
}

func defaultJSON(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return value
}

func isUniqueConstraint(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "unique")
}

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

func nullableTimestampString(ts *clock.Timestamp) any {
	if ts == nil {
		return nil
	}
	return ts.String()
}
