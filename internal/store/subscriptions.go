package store

import "context"

// SubscribeChannel 订阅一个 channel。底层委托给 UpsertAttachment（AttachmentTypeChannelSubscription）。
func (s *Store) SubscribeChannel(ctx context.Context, params ChannelSubscriptionParams) (Subscription, Event, error) {
	attachment, event, err := s.UpsertAttachment(ctx, UpsertAttachmentParams{
		Owner:      params.Subscriber,
		Subject:    params.Channel,
		Type:       AttachmentTypeChannelSubscription,
		ConfigJSON: "{}",
	})
	if err != nil {
		return Subscription{}, Event{}, err
	}
	return subscriptionFromAttachment(attachment), event, nil
}

// UnsubscribeChannel 取消订阅一个 channel。底层委托给 DeleteAttachment（AttachmentTypeChannelSubscription）。
func (s *Store) UnsubscribeChannel(ctx context.Context, params ChannelSubscriptionParams) (Subscription, Event, error) {
	attachment, event, err := s.DeleteAttachment(ctx, DeleteAttachmentParams{
		Owner:   params.Subscriber,
		Subject: params.Channel,
		Type:    AttachmentTypeChannelSubscription,
	})
	if err != nil {
		return Subscription{}, Event{}, err
	}
	return subscriptionFromAttachment(attachment), event, nil
}

// ListChannelSubscriptions 列出用户的所有频道订阅。
func (s *Store) ListChannelSubscriptions(ctx context.Context, subscriber UserKey) ([]Subscription, error) {
	attachments, err := s.ListUserAttachments(ctx, subscriber, AttachmentTypeChannelSubscription)
	if err != nil {
		return nil, err
	}
	subscriptions := make([]Subscription, 0, len(attachments))
	for _, attachment := range attachments {
		subscriptions = append(subscriptions, subscriptionFromAttachment(attachment))
	}
	return subscriptions, nil
}

// IsSubscribedToChannel 检查用户是否已订阅指定频道。
func (s *Store) IsSubscribedToChannel(ctx context.Context, subscriber, channel UserKey) (bool, error) {
	return s.attachments.HasActive(ctx, subscriber, channel, AttachmentTypeChannelSubscription, nil)
}
