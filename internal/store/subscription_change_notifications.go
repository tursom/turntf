package store

import (
	"context"
	"sync"
)

// SubscriptionChange 描述一条已提交频道订阅关系的当前权威状态。
// Reload 表示 Store 无法读取最终状态，消费方必须重新加载该订阅者的完整订阅集合。
type SubscriptionChange struct {
	Subscriber UserKey
	Channel    UserKey
	Active     bool
	Reload     bool
}

type subscriptionChangeKey struct {
	subscriber UserKey
	channel    UserKey
}

// SubscribeSubscriptionChanges 注册提交后订阅变化监听器。
// 监听器会被同步、串行调用，必须快速返回且不得重新进入 Store 的订阅写路径。
func (s *Store) SubscribeSubscriptionChanges(handler func([]SubscriptionChange)) func() {
	if s == nil || handler == nil {
		return func() {}
	}

	s.subscriptionChangeMu.Lock()
	if s.subscriptionChangeHandlers == nil {
		s.subscriptionChangeHandlers = make(map[uint64]func([]SubscriptionChange))
	}
	s.subscriptionChangeNextID++
	id := s.subscriptionChangeNextID
	s.subscriptionChangeHandlers[id] = handler
	s.subscriptionChangeMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.subscriptionChangeMu.Lock()
			delete(s.subscriptionChangeHandlers, id)
			s.subscriptionChangeMu.Unlock()
		})
	}
}

func (s *Store) notifySubscriptionAttachmentChanged(attachment Attachment) {
	if attachment.Type != AttachmentTypeChannelSubscription {
		return
	}
	s.notifySubscriptionChanges([]subscriptionChangeKey{{
		subscriber: attachment.Owner,
		channel:    attachment.Subject,
	}})
}

func (s *Store) notifySubscriptionChanges(keys []subscriptionChangeKey) {
	if s == nil || len(keys) == 0 {
		return
	}

	unique := make(map[subscriptionChangeKey]struct{}, len(keys))
	for _, key := range keys {
		if key.subscriber.Validate() != nil || key.channel.Validate() != nil {
			continue
		}
		unique[key] = struct{}{}
	}
	if len(unique) == 0 {
		return
	}

	s.subscriptionChangeMu.Lock()
	defer s.subscriptionChangeMu.Unlock()
	if len(s.subscriptionChangeHandlers) == 0 {
		return
	}

	changes := make([]SubscriptionChange, 0, len(unique))
	for key := range unique {
		active, err := s.IsSubscribedToChannel(context.Background(), key.subscriber, key.channel)
		changes = append(changes, SubscriptionChange{
			Subscriber: key.subscriber,
			Channel:    key.channel,
			Active:     active,
			Reload:     err != nil,
		})
	}
	for _, handler := range s.subscriptionChangeHandlers {
		handler(changes)
	}
}
