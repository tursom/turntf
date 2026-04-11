package store

import (
	"context"
	"database/sql"
	"encoding/json"
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
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at"`
	VersionUsername     string `json:"version_username"`
	VersionPasswordHash string `json:"version_password_hash"`
	VersionProfile      string `json:"version_profile"`
	VersionDeleted      string `json:"version_deleted,omitempty"`
	DeletedAt           string `json:"deleted_at,omitempty"`
	OriginNodeID        string `json:"origin_node_id"`
}

type replicatedUserEnvelope struct {
	User replicatedUserPayload `json:"user"`
}

type replicatedDeletePayload struct {
	UserID       int64  `json:"user_id"`
	DeletedAtHLC string `json:"deleted_at_hlc"`
}

type replicatedMessagePayload struct {
	ID           int64  `json:"id"`
	UserID       int64  `json:"user_id"`
	Sender       string `json:"sender"`
	Body         string `json:"body"`
	Metadata     string `json:"metadata,omitempty"`
	CreatedAt    string `json:"created_at"`
	OriginNodeID string `json:"origin_node_id"`
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
			CreatedAt:           user.CreatedAt.String(),
			UpdatedAt:           user.UpdatedAt.String(),
			VersionUsername:     user.VersionUsername.String(),
			VersionPasswordHash: user.VersionPasswordHash.String(),
			VersionProfile:      user.VersionProfile.String(),
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
		CreatedAt:           user.CreatedAt.String(),
		UpdatedAt:           user.UpdatedAt.String(),
		VersionUsername:     user.VersionUsername.String(),
		VersionPasswordHash: user.VersionPasswordHash.String(),
		VersionProfile:      user.VersionProfile.String(),
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
			ID:           message.ID,
			UserID:       message.UserID,
			Sender:       message.Sender,
			Body:         message.Body,
			Metadata:     message.Metadata,
			CreatedAt:    message.CreatedAt.String(),
			OriginNodeID: message.OriginNodeID,
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
	var payload replicatedUserEnvelope
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode user.created payload: %w", err)
	}

	user := payload.User
	if user.ID == 0 {
		return fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}

	_, err := s.getUserByIDTx(ctx, tx, user.ID, true)
	if err != nil && err != ErrNotFound {
		return err
	}
	if err == nil {
		return nil
	}

	exists, err := activeUsernameExists(ctx, tx, user.Username, user.ID)
	if err != nil {
		return err
	}
	if exists {
		return ErrConflict
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO users(
    user_id, username, password_hash, profile, created_at_hlc, updated_at_hlc,
    deleted_at_hlc, version_username, version_password_hash, version_profile,
    version_deleted, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, NULL, ?)
`, user.ID, user.Username, user.PasswordHash, defaultJSON(user.Profile),
		user.CreatedAt, user.UpdatedAt, user.VersionUsername, user.VersionPasswordHash,
		user.VersionProfile, user.OriginNodeID); err != nil {
		return fmt.Errorf("insert replicated user: %w", err)
	}
	return nil
}

func (s *Store) applyReplicatedUserUpdated(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent) error {
	var payload replicatedUserEnvelope
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode user.updated payload: %w", err)
	}
	user := payload.User
	if user.ID == 0 {
		return fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}

	if _, err := s.getUserByIDTx(ctx, tx, user.ID, false); err != nil {
		return err
	}

	exists, err := activeUsernameExists(ctx, tx, user.Username, user.ID)
	if err != nil {
		return err
	}
	if exists {
		return ErrConflict
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET username = ?, password_hash = ?, profile = ?, updated_at_hlc = ?,
    version_username = ?, version_password_hash = ?, version_profile = ?,
    origin_node_id = ?
WHERE user_id = ? AND deleted_at_hlc IS NULL
`, user.Username, user.PasswordHash, defaultJSON(user.Profile), user.UpdatedAt,
		user.VersionUsername, user.VersionPasswordHash, user.VersionProfile,
		user.OriginNodeID, user.ID); err != nil {
		return fmt.Errorf("update replicated user: %w", err)
	}
	return nil
}

func (s *Store) applyReplicatedUserDeleted(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent) error {
	var payload replicatedDeletePayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode user.deleted payload: %w", err)
	}
	if payload.UserID == 0 {
		return fmt.Errorf("%w: user id cannot be empty", ErrInvalidInput)
	}

	if _, err := s.getUserByIDTx(ctx, tx, payload.UserID, false); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE users
SET deleted_at_hlc = ?, updated_at_hlc = ?, version_deleted = ?
WHERE user_id = ? AND deleted_at_hlc IS NULL
`, payload.DeletedAtHLC, payload.DeletedAtHLC, payload.DeletedAtHLC, payload.UserID); err != nil {
		return fmt.Errorf("delete replicated user: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO tombstones(entity_type, entity_id, deleted_at_hlc, expires_at_hlc, origin_node_id)
VALUES('user', ?, ?, NULL, ?)
ON CONFLICT(entity_type, entity_id) DO UPDATE SET
    deleted_at_hlc = excluded.deleted_at_hlc,
    origin_node_id = excluded.origin_node_id
`, payload.UserID, payload.DeletedAtHLC, event.OriginNodeId); err != nil {
		return fmt.Errorf("insert replicated tombstone: %w", err)
	}
	return nil
}

func (s *Store) applyReplicatedMessageCreated(ctx context.Context, tx *sql.Tx, event *clusterproto.ReplicatedEvent) error {
	var payload replicatedMessageEnvelope
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("decode message.created payload: %w", err)
	}
	message := payload.Message
	if message.ID == 0 || message.UserID == 0 {
		return fmt.Errorf("%w: message id and user id are required", ErrInvalidInput)
	}

	if _, err := s.getUserByIDTx(ctx, tx, message.UserID, false); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO messages(message_id, user_id, sender, body, metadata, created_at_hlc, origin_node_id)
VALUES(?, ?, ?, ?, ?, ?, ?)
`, message.ID, message.UserID, message.Sender, message.Body, nullIfEmpty(message.Metadata),
		message.CreatedAt, message.OriginNodeID); err != nil {
		if isUniqueConstraint(err) {
			return nil
		}
		return fmt.Errorf("insert replicated message: %w", err)
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
SELECT user_id, username, password_hash, profile, created_at_hlc, updated_at_hlc,
       deleted_at_hlc, version_username, version_password_hash, version_profile,
       version_deleted, origin_node_id
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
