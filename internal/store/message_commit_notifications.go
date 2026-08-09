package store

import "sync"

// SubscribeMessageCommits 订阅已提交消息事件的进程内通知。
// 通知不携带事件内容且允许合并；消费方仍需以事件日志序列号查询实际变更。
func (s *Store) SubscribeMessageCommits() (<-chan struct{}, func()) {
	notifications := make(chan struct{}, 1)
	if s == nil {
		return notifications, func() {}
	}

	s.messageCommitSubscribersMu.Lock()
	if s.messageCommitSubscribers == nil {
		s.messageCommitSubscribers = make(map[chan struct{}]struct{})
	}
	s.messageCommitSubscribers[notifications] = struct{}{}
	s.messageCommitSubscribersMu.Unlock()

	var unsubscribeOnce sync.Once
	return notifications, func() {
		unsubscribeOnce.Do(func() {
			s.messageCommitSubscribersMu.Lock()
			delete(s.messageCommitSubscribers, notifications)
			s.messageCommitSubscribersMu.Unlock()
		})
	}
}

// notifyMessageCommitted 向所有订阅者发送可合并的非阻塞唤醒提示。
func (s *Store) notifyMessageCommitted() {
	if s == nil {
		return
	}
	s.messageCommitSubscribersMu.RLock()
	defer s.messageCommitSubscribersMu.RUnlock()
	for subscriber := range s.messageCommitSubscribers {
		select {
		case subscriber <- struct{}{}:
		default:
		}
	}
}
