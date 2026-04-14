package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/tursom/turntf/internal/clock"
)

func (s *Store) SubscribeChannel(ctx context.Context, params ChannelSubscriptionParams) (Subscription, Event, error) {
	if err := params.Subscriber.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}
	if err := params.Channel.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}
	if params.Subscriber == params.Channel {
		return Subscription{}, Event{}, fmt.Errorf("%w: subscriber cannot subscribe to itself", ErrInvalidInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Subscription{}, Event{}, fmt.Errorf("begin subscribe channel: %w", err)
	}
	defer tx.Rollback()

	if err := s.validateSubscriptionUsersTx(ctx, tx, params.Subscriber, params.Channel); err != nil {
		return Subscription{}, Event{}, err
	}

	now := s.clock.Now()
	subscription := Subscription{
		Subscriber:   params.Subscriber,
		Channel:      params.Channel,
		SubscribedAt: now,
		OriginNodeID: s.nodeID,
	}
	if err := s.upsertSubscriptionTx(ctx, tx, subscription); err != nil {
		return Subscription{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeChannelSubscribed,
		Aggregate:       "subscription",
		AggregateNodeID: params.Subscriber.NodeID,
		AggregateID:     params.Subscriber.UserID,
		HLC:             now,
		Body:            channelSubscribedProtoFromSubscription(subscription),
	})
	if err != nil {
		return Subscription{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return Subscription{}, Event{}, fmt.Errorf("commit subscribe channel: %w", err)
	}
	return subscription, event, nil
}

func (s *Store) UnsubscribeChannel(ctx context.Context, params ChannelSubscriptionParams) (Subscription, Event, error) {
	if err := params.Subscriber.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}
	if err := params.Channel.Validate(); err != nil {
		return Subscription{}, Event{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Subscription{}, Event{}, fmt.Errorf("begin unsubscribe channel: %w", err)
	}
	defer tx.Rollback()

	current, err := s.getSubscriptionTx(ctx, tx, params.Subscriber, params.Channel)
	if err != nil {
		return Subscription{}, Event{}, err
	}
	if current.DeletedAt != nil {
		return Subscription{}, Event{}, ErrNotFound
	}

	now := s.clock.Now()
	current.DeletedAt = &now
	current.OriginNodeID = s.nodeID
	if err := s.upsertSubscriptionTx(ctx, tx, current); err != nil {
		return Subscription{}, Event{}, err
	}

	event, err := s.insertEvent(ctx, tx, Event{
		EventType:       EventTypeChannelUnsubscribed,
		Aggregate:       "subscription",
		AggregateNodeID: params.Subscriber.NodeID,
		AggregateID:     params.Subscriber.UserID,
		HLC:             now,
		Body:            channelUnsubscribedProtoFromSubscription(current),
	})
	if err != nil {
		return Subscription{}, Event{}, err
	}
	if err := tx.Commit(); err != nil {
		return Subscription{}, Event{}, fmt.Errorf("commit unsubscribe channel: %w", err)
	}
	return current, event, nil
}

func (s *Store) ListChannelSubscriptions(ctx context.Context, subscriber UserKey) ([]Subscription, error) {
	if err := subscriber.Validate(); err != nil {
		return nil, err
	}
	if _, err := s.GetUser(ctx, subscriber); err != nil {
		return nil, err
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id, subscribed_at_hlc, deleted_at_hlc, origin_node_id
FROM channel_subscriptions
WHERE subscriber_node_id = ? AND subscriber_user_id = ? AND deleted_at_hlc IS NULL
ORDER BY channel_node_id ASC, channel_user_id ASC
`, subscriber.NodeID, subscriber.UserID)
	if err != nil {
		return nil, fmt.Errorf("list channel subscriptions: %w", err)
	}
	defer rows.Close()

	var subscriptions []Subscription
	for rows.Next() {
		subscription, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, subscription)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel subscriptions: %w", err)
	}
	return subscriptions, nil
}

func (s *Store) IsSubscribedToChannel(ctx context.Context, subscriber, channel UserKey) (bool, error) {
	if err := subscriber.Validate(); err != nil {
		return false, err
	}
	if err := channel.Validate(); err != nil {
		return false, err
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM channel_subscriptions
WHERE subscriber_node_id = ? AND subscriber_user_id = ?
  AND channel_node_id = ? AND channel_user_id = ?
  AND deleted_at_hlc IS NULL
`, subscriber.NodeID, subscriber.UserID, channel.NodeID, channel.UserID).Scan(&count); err != nil {
		return false, fmt.Errorf("check channel subscription: %w", err)
	}
	return count > 0, nil
}

func (s *Store) validateSubscriptionUsersTx(ctx context.Context, tx *sql.Tx, subscriberKey, channelKey UserKey) error {
	subscriber, err := s.getUserByIDTx(ctx, tx, subscriberKey, false)
	if err != nil {
		return err
	}
	if !subscriber.CanLogin() {
		return fmt.Errorf("%w: subscriber must be a login user", ErrInvalidInput)
	}
	channel, err := s.getUserByIDTx(ctx, tx, channelKey, false)
	if err != nil {
		return err
	}
	if channel.Role != RoleChannel {
		return fmt.Errorf("%w: subscription target must be a channel", ErrInvalidInput)
	}
	return nil
}

func (s *Store) getSubscriptionTx(ctx context.Context, tx *sql.Tx, subscriber, channel UserKey) (Subscription, error) {
	row := tx.QueryRowContext(ctx, `
SELECT subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id, subscribed_at_hlc, deleted_at_hlc, origin_node_id
FROM channel_subscriptions
WHERE subscriber_node_id = ? AND subscriber_user_id = ? AND channel_node_id = ? AND channel_user_id = ?
`, subscriber.NodeID, subscriber.UserID, channel.NodeID, channel.UserID)
	subscription, err := scanSubscription(row)
	if err == sql.ErrNoRows {
		return Subscription{}, ErrNotFound
	}
	if err != nil {
		return Subscription{}, err
	}
	return subscription, nil
}

func (s *Store) upsertSubscriptionTx(ctx context.Context, tx *sql.Tx, subscription Subscription) error {
	if err := subscription.Subscriber.Validate(); err != nil {
		return err
	}
	if err := subscription.Channel.Validate(); err != nil {
		return err
	}
	if subscription.SubscribedAt == (clock.Timestamp{}) {
		return fmt.Errorf("%w: subscribed_at is required", ErrInvalidInput)
	}
	if subscription.OriginNodeID <= 0 {
		return fmt.Errorf("%w: subscription origin node id is required", ErrInvalidInput)
	}

	deletedAt := nullableTimestampString(subscription.DeletedAt)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO channel_subscriptions(
    subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id,
    subscribed_at_hlc, deleted_at_hlc, origin_node_id
)
VALUES(?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(subscriber_node_id, subscriber_user_id, channel_node_id, channel_user_id) DO UPDATE SET
    subscribed_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.subscribed_at_hlc > channel_subscriptions.deleted_at_hlc
        ) AND excluded.subscribed_at_hlc > channel_subscriptions.subscribed_at_hlc THEN excluded.subscribed_at_hlc
        ELSE channel_subscriptions.subscribed_at_hlc
    END,
    deleted_at_hlc = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.subscribed_at_hlc > channel_subscriptions.deleted_at_hlc
        ) THEN NULL
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > channel_subscriptions.deleted_at_hlc
        ) AND excluded.deleted_at_hlc >= channel_subscriptions.subscribed_at_hlc THEN excluded.deleted_at_hlc
        ELSE channel_subscriptions.deleted_at_hlc
    END,
    origin_node_id = CASE
        WHEN excluded.deleted_at_hlc IS NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.subscribed_at_hlc > channel_subscriptions.deleted_at_hlc
        ) AND excluded.subscribed_at_hlc >= channel_subscriptions.subscribed_at_hlc THEN excluded.origin_node_id
        WHEN excluded.deleted_at_hlc IS NOT NULL AND (
            channel_subscriptions.deleted_at_hlc IS NULL OR excluded.deleted_at_hlc > channel_subscriptions.deleted_at_hlc
        ) THEN excluded.origin_node_id
        ELSE channel_subscriptions.origin_node_id
    END
`, subscription.Subscriber.NodeID, subscription.Subscriber.UserID, subscription.Channel.NodeID, subscription.Channel.UserID,
		subscription.SubscribedAt.String(), deletedAt, subscription.OriginNodeID); err != nil {
		return fmt.Errorf("upsert channel subscription: %w", err)
	}
	return nil
}
