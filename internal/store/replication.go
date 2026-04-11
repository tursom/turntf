package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"notifier/internal/clock"
	clusterproto "notifier/internal/proto"
)

type replicatedUserPayload struct {
	ID                  int64  `json:"id"`
	Username            string `json:"username"`
	PasswordHash        string `json:"password_hash"`
	Profile             string `json:"profile"`
	Role                string `json:"role"`
	SystemReserved      bool   `json:"system_reserved"`
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at"`
	VersionUsername     string `json:"version_username"`
	VersionPasswordHash string `json:"version_password_hash"`
	VersionProfile      string `json:"version_profile"`
	VersionRole         string `json:"version_role"`
	VersionDeleted      string `json:"version_deleted,omitempty"`
	DeletedAt           string `json:"deleted_at,omitempty"`
	OriginNodeID        int64  `json:"origin_node_id"`
}

type replicatedUserEnvelope struct {
	User replicatedUserPayload `json:"user"`
}

type replicatedDeletePayload struct {
	UserID       int64  `json:"user_id"`
	DeletedAtHLC string `json:"deleted_at_hlc"`
}

type replicatedMessagePayload struct {
	UserID    int64  `json:"user_id"`
	NodeID    int64  `json:"node_id"`
	Seq       int64  `json:"seq"`
	Sender    string `json:"sender"`
	Body      string `json:"body"`
	Metadata  string `json:"metadata,omitempty"`
	CreatedAt string `json:"created_at"`
}

type replicatedMessageEnvelope struct {
	Message replicatedMessagePayload `json:"message"`
}

func encodeCreateUserPayload(user User) (string, error) {
	return marshalPayload(replicatedUserEnvelope{
		User: replicatedUserPayload{
			ID:                  user.ID,
			Username:            user.Username,
			PasswordHash:        user.PasswordHash,
			Profile:             user.Profile,
			Role:                user.Role,
			SystemReserved:      user.SystemReserved,
			CreatedAt:           user.CreatedAt.String(),
			UpdatedAt:           user.UpdatedAt.String(),
			VersionUsername:     user.VersionUsername.String(),
			VersionPasswordHash: user.VersionPasswordHash.String(),
			VersionProfile:      user.VersionProfile.String(),
			VersionRole:         user.VersionRole.String(),
			OriginNodeID:        user.OriginNodeID,
		},
	})
}

func encodeUpdateUserPayload(user User) (string, error) {
	payload := replicatedUserPayload{
		ID:                  user.ID,
		Username:            user.Username,
		PasswordHash:        user.PasswordHash,
		Profile:             user.Profile,
		Role:                user.Role,
		SystemReserved:      user.SystemReserved,
		CreatedAt:           user.CreatedAt.String(),
		UpdatedAt:           user.UpdatedAt.String(),
		VersionUsername:     user.VersionUsername.String(),
		VersionPasswordHash: user.VersionPasswordHash.String(),
		VersionProfile:      user.VersionProfile.String(),
		VersionRole:         user.VersionRole.String(),
		OriginNodeID:        user.OriginNodeID,
	}
	if user.VersionDeleted != nil {
		payload.VersionDeleted = user.VersionDeleted.String()
	}
	if user.DeletedAt != nil {
		payload.DeletedAt = user.DeletedAt.String()
	}
	return marshalPayload(replicatedUserEnvelope{User: payload})
}

func encodeDeleteUserPayload(userID int64, deletedAt string) (string, error) {
	return marshalPayload(replicatedDeletePayload{
		UserID:       userID,
		DeletedAtHLC: deletedAt,
	})
}

func encodeCreateMessagePayload(message Message) (string, error) {
	return marshalPayload(replicatedMessageEnvelope{
		Message: replicatedMessagePayload{
			UserID:    message.UserID,
			NodeID:    message.NodeID,
			Seq:       message.Seq,
			Sender:    message.Sender,
			Body:      message.Body,
			Metadata:  message.Metadata,
			CreatedAt: message.CreatedAt.String(),
		},
	})
}

func marshalPayload(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func ToReplicatedEvent(event Event) *clusterproto.ReplicatedEvent {
	return &clusterproto.ReplicatedEvent{
		EventId:       event.EventID,
		Kind:          event.Kind,
		AggregateType: event.Aggregate,
		AggregateId:   event.AggregateID,
		Hlc:           event.HLC.String(),
		OriginNodeId:  event.OriginNodeID,
		Payload:       []byte(event.Payload),
	}
}

func (s *Store) ApplyReplicatedEvent(ctx context.Context, event *clusterproto.ReplicatedEvent) error {
	if event == nil {
		return fmt.Errorf("%w: replicated event cannot be nil", ErrInvalidInput)
	}
	if event.EventId == 0 {
		return fmt.Errorf("%w: event id cannot be empty", ErrInvalidInput)
	}
	if strings.TrimSpace(event.Kind) == "" {
		return fmt.Errorf("%w: event kind cannot be empty", ErrInvalidInput)
	}

	hlc, err := parseRequiredTimestamp(event.Hlc, "event hlc")
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin apply replicated event: %w", err)
	}
	defer tx.Rollback()

	applied, err := s.isEventAppliedTx(ctx, tx, event.EventId)
	if err != nil {
		return err
	}
	if applied {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit duplicate replicated event: %w", err)
		}
		return nil
	}

	switch event.Kind {
	case "user.created":
		if err := s.applyReplicatedUserCreated(ctx, tx, event); err != nil {
			return err
		}
	case "user.updated":
		if err := s.applyReplicatedUserUpdated(ctx, tx, event); err != nil {
			return err
		}
	case "user.deleted":
		if err := s.applyReplicatedUserDeleted(ctx, tx, event); err != nil {
			return err
		}
	case "message.created":
		if err := s.applyReplicatedMessageCreated(ctx, tx, event); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: unsupported event kind %q", ErrInvalidInput, event.Kind)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO event_log(event_id, kind, aggregate_type, aggregate_id, hlc, origin_node_id, payload)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, event.EventId, event.Kind, event.AggregateType, event.AggregateId, hlc.String(), event.OriginNodeId, string(event.Payload)); err != nil {
		if isUniqueConstraint(err) {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit duplicate event log entry: %w", err)
			}
			return nil
		}
		return fmt.Errorf("insert replicated event log: %w", err)
	}

	appliedAt := s.clock.Observe(hlc)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO applied_events(event_id, source_node_id, applied_at_hlc)
VALUES(?, ?, ?)
`, event.EventId, event.OriginNodeId, appliedAt.String()); err != nil {
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
	return nil
}

func (s *Store) applyReplicatedUserCreated(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent) error {
	return s.applyReplicatedUserUpsert(ctx, tx, event, "user.created")
}

func (s *Store) applyReplicatedUserUpdated(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent) error {
	return s.applyReplicatedUserUpsert(ctx, tx, event, "user.updated")
}

func (s *Store) applyReplicatedUserDeleted(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent) error {
	var payload replicatedDeletePayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode user.deleted payload: %w", err)
	}
	if payload.UserID == 0 {
		return fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}

	deletedAt, err := parseRequiredTimestamp(payload.DeletedAtHLC, "deleted_at_hlc")
	if err != nil {
		return err
	}
	return s.applyUserDeleteTx(ctx, tx, payload.UserID, deletedAt, event.OriginNodeId, false)
}

func (s *Store) applyReplicatedMessageCreated(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent) error {
	var payload replicatedMessageEnvelope
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode message.created payload: %w", err)
	}
	message := payload.Message
	if message.UserID == 0 || message.NodeID <= 0 || message.Seq <= 0 {
		return fmt.Errorf("%w: message user id, node id, and seq are required", ErrInvalidInput)
	}
	if event.OriginNodeId != 0 && event.OriginNodeId != message.NodeID {
		return fmt.Errorf("%w: message node id %d does not match event origin %d", ErrInvalidInput, message.NodeID, event.OriginNodeId)
	}

	if _, err := s.getUserByIDTx(ctx, tx, message.UserID, false); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(user_id, node_id, seq, sender, body, metadata, created_at_hlc)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, message.UserID, message.NodeID, message.Seq, message.Sender, message.Body, nullIfEmpty(message.Metadata),
		message.CreatedAt); err != nil {
		if isUniqueConstraint(err) {
			return s.recordMessageSeqTx(ctx, tx, message.UserID, message.NodeID, message.Seq)
		}
		return fmt.Errorf("insert replicated message: %w", err)
	}
	if err := s.recordMessageSeqTx(ctx, tx, message.UserID, message.NodeID, message.Seq); err != nil {
		return err
	}
	if err := s.trimMessagesForUserTx(ctx, tx, message.UserID); err != nil {
		return err
	}
	return nil
}

func (s *Store) isEventAppliedTx(ctx context.Context, tx *sql.Tx, eventID int64) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM applied_events WHERE event_id = ?`, eventID).Scan(&count); err != nil {
		return false, fmt.Errorf("check applied event: %w", err)
	}
	return count > 0, nil
}

func (s *Store) getUserByIDTx(ctx context.Context, tx *sql.Tx, userID int64, includeDeleted bool) (User, error) {
	query := `
SELECT user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_role, version_deleted, origin_node_id
FROM users
WHERE user_id = ?`
	if !includeDeleted {
		query += ` AND deleted_at_hlc IS NULL`
	}
	row := tx.QueryRowContext(ctx, query, userID)
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

func decodeReplicatedUser(userPayload []byte, kind string) (User, error) {
	var envelope replicatedUserEnvelope
	if err := json.Unmarshal(userPayload, &envelope); err != nil {
		return User{}, fmt.Errorf("decode %s payload: %w", kind, err)
	}

	payload := envelope.User
	if payload.ID == 0 {
		return User{}, fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}

	createdAt, err := parseRequiredTimestamp(payload.CreatedAt, "user created_at")
	if err != nil {
		return User{}, err
	}
	updatedAt, err := parseRequiredTimestamp(payload.UpdatedAt, "user updated_at")
	if err != nil {
		return User{}, err
	}
	versionUsername, err := parseRequiredTimestamp(payload.VersionUsername, "user version_username")
	if err != nil {
		return User{}, err
	}
	versionPasswordHash, err := parseRequiredTimestamp(payload.VersionPasswordHash, "user version_password_hash")
	if err != nil {
		return User{}, err
	}
	versionProfile, err := parseRequiredTimestamp(payload.VersionProfile, "user version_profile")
	if err != nil {
		return User{}, err
	}
	versionRole, err := parseRequiredTimestamp(payload.VersionRole, "user version_role")
	if err != nil {
		return User{}, err
	}
	role, err := normalizeAnyRole(payload.Role)
	if err != nil {
		return User{}, err
	}

	user := User{
		ID:                  payload.ID,
		Username:            payload.Username,
		PasswordHash:        payload.PasswordHash,
		Profile:             defaultJSON(payload.Profile),
		Role:                role,
		SystemReserved:      payload.SystemReserved,
		CreatedAt:           createdAt,
		UpdatedAt:           updatedAt,
		VersionUsername:     versionUsername,
		VersionPasswordHash: versionPasswordHash,
		VersionProfile:      versionProfile,
		VersionRole:         versionRole,
		OriginNodeID:        payload.OriginNodeID,
	}
	user = applyReplicatedBootstrapInvariant(user)

	if strings.TrimSpace(payload.VersionDeleted) != "" {
		parsed, err := parseRequiredTimestamp(payload.VersionDeleted, "user version_deleted")
		if err != nil {
			return User{}, err
		}
		user.VersionDeleted = &parsed
	}
	if strings.TrimSpace(payload.DeletedAt) != "" {
		parsed, err := parseRequiredTimestamp(payload.DeletedAt, "user deleted_at")
		if err != nil {
			return User{}, err
		}
		user.DeletedAt = &parsed
	}
	return user, nil
}

func (s *Store) applyReplicatedUserUpsert(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent, kind string) error {
	incoming, err := decodeReplicatedUser(event.Payload, kind)
	if err != nil {
		return err
	}

	if _, exists, err := s.getTombstoneTx(ctx, tx, "user", incoming.ID); err != nil {
		return err
	} else if exists {
		return nil
	}

	current, err := s.getUserByIDTx(ctx, tx, incoming.ID, true)
	switch {
	case err == nil:
	case errors.Is(err, ErrNotFound):
		incoming.UpdatedAt = latestUserVersion(incoming)
		if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    user_id, username, password_hash, profile, role, system_reserved, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_role, version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`, incoming.ID, incoming.Username, incoming.PasswordHash, defaultJSON(incoming.Profile), incoming.Role,
			boolToInt(incoming.SystemReserved), incoming.CreatedAt.String(), incoming.UpdatedAt.String(),
			nullableTimestampString(incoming.DeletedAt), incoming.VersionUsername.String(),
			incoming.VersionPasswordHash.String(), incoming.VersionProfile.String(),
			incoming.VersionRole.String(), nullableTimestampString(incoming.VersionDeleted),
			incoming.OriginNodeID); err != nil {
			return fmt.Errorf("insert replicated user: %w", err)
		}
		return nil
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
WHERE user_id = ?
`, merged.Username, merged.PasswordHash, defaultJSON(merged.Profile), merged.Role, boolToInt(merged.SystemReserved),
		merged.UpdatedAt.String(), merged.VersionUsername.String(), merged.VersionPasswordHash.String(),
		merged.VersionProfile.String(), merged.VersionRole.String(), merged.ID); err != nil {
		return fmt.Errorf("update replicated user: %w", err)
	}
	return nil
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

func applyReplicatedBootstrapInvariant(user User) User {
	if user.ID != BootstrapAdminUserID {
		user.SystemReserved = false
		return user
	}
	user.Role = RoleSuperAdmin
	user.SystemReserved = true
	return user
}

func nullableTimestampString(ts *clock.Timestamp) any {
	if ts == nil {
		return nil
	}
	return ts.String()
}
