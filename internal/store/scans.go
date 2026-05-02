package store

import (
	"database/sql"
	"fmt"

	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
)

// scanUser 从 SQL scanner 扫描 users 表一行到 User，解析所有 HLC 时间戳字段。
func scanUser(scanner interface {
	Scan(dest ...any) error
}) (User, error) {
	var user User
	var systemReserved int
	var createdAtRaw string
	var updatedAtRaw string
	var deletedAtRaw sql.NullString
	var versionUsernameRaw string
	var versionPasswordRaw string
	var versionProfileRaw string
	var versionRoleRaw string
	var versionDeletedRaw sql.NullString

	if err := scanner.Scan(
		&user.NodeID,
		&user.ID,
		&user.Username,
		&user.PasswordHash,
		&user.Profile,
		&user.Role,
		&systemReserved,
		&createdAtRaw,
		&updatedAtRaw,
		&deletedAtRaw,
		&versionUsernameRaw,
		&versionPasswordRaw,
		&versionProfileRaw,
		&versionRoleRaw,
		&versionDeletedRaw,
		&user.OriginNodeID,
	); err != nil {
		return User{}, err
	}

	var err error
	user.CreatedAt, err = clock.ParseTimestamp(createdAtRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user created_at: %w", err)
	}
	user.UpdatedAt, err = clock.ParseTimestamp(updatedAtRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user updated_at: %w", err)
	}
	user.VersionUsername, err = clock.ParseTimestamp(versionUsernameRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_username: %w", err)
	}
	user.VersionPasswordHash, err = clock.ParseTimestamp(versionPasswordRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_password_hash: %w", err)
	}
	user.VersionProfile, err = clock.ParseTimestamp(versionProfileRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_profile: %w", err)
	}
	user.VersionRole, err = clock.ParseTimestamp(versionRoleRaw)
	if err != nil {
		return User{}, fmt.Errorf("parse user version_role: %w", err)
	}
	if deletedAtRaw.Valid {
		parsed, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return User{}, fmt.Errorf("parse user deleted_at: %w", err)
		}
		user.DeletedAt = &parsed
	}
	if versionDeletedRaw.Valid {
		parsed, err := clock.ParseTimestamp(versionDeletedRaw.String)
		if err != nil {
			return User{}, fmt.Errorf("parse user version_deleted: %w", err)
		}
		user.VersionDeleted = &parsed
	}
	user.SystemReserved = systemReserved != 0
	return user, nil
}

// scanMessage 从 SQL scanner 扫描 messages 投影表一行到 Message，解析 HLC 时间戳。
func scanMessage(scanner interface {
	Scan(dest ...any) error
}) (Message, error) {
	var message Message
	var createdAtRaw string

	if err := scanner.Scan(
		&message.Recipient.NodeID,
		&message.Recipient.UserID,
		&message.NodeID,
		&message.Seq,
		&message.Sender.NodeID,
		&message.Sender.UserID,
		&message.Body,
		&createdAtRaw,
		&message.Session,
	); err != nil {
		return Message{}, err
	}

	createdAt, err := clock.ParseTimestamp(createdAtRaw)
	if err != nil {
		return Message{}, fmt.Errorf("parse message created_at: %w", err)
	}
	message.CreatedAt = createdAt
	return message, nil
}

// scanSubscription 从 SQL scanner 扫描订阅视图一行到 Subscription，解析 HLC 时间戳。
func scanSubscription(scanner interface {
	Scan(dest ...any) error
}) (Subscription, error) {
	var subscription Subscription
	var subscribedAtRaw string
	var deletedAtRaw sql.NullString

	if err := scanner.Scan(
		&subscription.Subscriber.NodeID,
		&subscription.Subscriber.UserID,
		&subscription.Channel.NodeID,
		&subscription.Channel.UserID,
		&subscribedAtRaw,
		&deletedAtRaw,
		&subscription.OriginNodeID,
	); err != nil {
		return Subscription{}, err
	}

	subscribedAt, err := clock.ParseTimestamp(subscribedAtRaw)
	if err != nil {
		return Subscription{}, fmt.Errorf("parse subscription subscribed_at: %w", err)
	}
	subscription.SubscribedAt = subscribedAt
	if deletedAtRaw.Valid {
		deletedAt, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return Subscription{}, fmt.Errorf("parse subscription deleted_at: %w", err)
		}
		subscription.DeletedAt = &deletedAt
	}
	return subscription, nil
}

// scanBlacklistEntry 从 SQL scanner 扫描黑名单视图一行到 BlacklistEntry，解析 HLC 时间戳。
func scanBlacklistEntry(scanner interface {
	Scan(dest ...any) error
}) (BlacklistEntry, error) {
	var entry BlacklistEntry
	var blockedAtRaw string
	var deletedAtRaw sql.NullString

	if err := scanner.Scan(
		&entry.Owner.NodeID,
		&entry.Owner.UserID,
		&entry.Blocked.NodeID,
		&entry.Blocked.UserID,
		&blockedAtRaw,
		&deletedAtRaw,
		&entry.OriginNodeID,
	); err != nil {
		return BlacklistEntry{}, err
	}

	blockedAt, err := clock.ParseTimestamp(blockedAtRaw)
	if err != nil {
		return BlacklistEntry{}, fmt.Errorf("parse blacklist blocked_at: %w", err)
	}
	entry.BlockedAt = blockedAt
	if deletedAtRaw.Valid {
		deletedAt, err := clock.ParseTimestamp(deletedAtRaw.String)
		if err != nil {
			return BlacklistEntry{}, fmt.Errorf("parse blacklist deleted_at: %w", err)
		}
		entry.DeletedAt = &deletedAt
	}
	return entry, nil
}

// scanEvent 从 SQL scanner 扫描 event_log 一行到 Event，反序列化 protobuf value 并解码。
func scanEvent(scanner interface {
	Scan(dest ...any) error
}) (Event, error) {
	var sequence int64
	var eventID int64
	var originNodeID int64
	var value []byte

	if err := scanner.Scan(
		&sequence,
		&eventID,
		&originNodeID,
		&value,
	); err != nil {
		return Event{}, err
	}

	var replicated internalproto.ReplicatedEvent
	if err := gproto.Unmarshal(value, &replicated); err != nil {
		return Event{}, fmt.Errorf("unmarshal event value: %w", err)
	}

	event, err := eventFromReplicatedEvent(&replicated)
	if err != nil {
		return Event{}, err
	}
	event.Sequence = sequence
	event.EventID = eventID
	event.OriginNodeID = originNodeID
	return event, nil
}
