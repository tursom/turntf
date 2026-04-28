package store

import "context"

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

func (s *Store) IsSubscribedToChannel(ctx context.Context, subscriber, channel UserKey) (bool, error) {
	return s.attachments.HasActive(ctx, subscriber, channel, AttachmentTypeChannelSubscription, nil)
}
