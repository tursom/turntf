package api

import (
	"context"
	"fmt"
	"sync"

	"github.com/tursom/turntf/internal/permission"
	"github.com/tursom/turntf/internal/store"
)

type persistentSubscriptionLoader func(context.Context, store.UserKey) ([]store.Subscription, error)

type persistentSubscriberState struct {
	sessions map[*clientWSSession]struct{}
	channels map[store.UserKey]struct{}
	revision uint64
	loading  int
	loaded   bool
	dirty    bool
}

// persistentSessionIndex 维护节点本地持久化会话及频道订阅的双向索引。
type persistentSessionIndex struct {
	mu          sync.RWMutex
	load        persistentSubscriptionLoader
	all         map[*clientWSSession]struct{}
	admins      map[*clientWSSession]struct{}
	subscribers map[store.UserKey]*persistentSubscriberState
	channels    map[store.UserKey]map[*clientWSSession]struct{}
	dirty       map[store.UserKey]struct{}
}

func newPersistentSessionIndex(load persistentSubscriptionLoader) *persistentSessionIndex {
	return &persistentSessionIndex{
		load:        load,
		all:         make(map[*clientWSSession]struct{}),
		admins:      make(map[*clientWSSession]struct{}),
		subscribers: make(map[store.UserKey]*persistentSubscriberState),
		channels:    make(map[store.UserKey]map[*clientWSSession]struct{}),
		dirty:       make(map[store.UserKey]struct{}),
	}
}

func (i *persistentSessionIndex) Register(ctx context.Context, sess *clientWSSession) error {
	if i == nil || sess == nil || sess.principal == nil {
		return fmt.Errorf("persistent session principal is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := sess.principal.User.Key()
	if err := key.Validate(); err != nil {
		return err
	}

	for {
		i.mu.Lock()
		if _, exists := i.all[sess]; exists {
			i.mu.Unlock()
			return nil
		}
		state := i.ensureSubscriberLocked(key)
		if state.loaded && !state.dirty {
			i.addSessionLocked(state, sess)
			i.mu.Unlock()
			return nil
		}
		revision := state.revision
		state.loading++
		i.mu.Unlock()

		channels, err := i.loadSubscriberChannels(ctx, key)

		i.mu.Lock()
		state = i.ensureSubscriberLocked(key)
		state.loading--
		if err != nil {
			i.cleanupSubscriberLocked(key, state)
			i.mu.Unlock()
			return err
		}
		if state.revision != revision {
			i.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		i.replaceSubscriberChannelsLocked(state, channels)
		state.loaded = true
		state.dirty = false
		delete(i.dirty, key)
		i.addSessionLocked(state, sess)
		i.mu.Unlock()
		return nil
	}
}

func (i *persistentSessionIndex) Unregister(sess *clientWSSession) {
	if i == nil || sess == nil || sess.principal == nil {
		return
	}
	key := sess.principal.User.Key()

	i.mu.Lock()
	defer i.mu.Unlock()
	if _, exists := i.all[sess]; !exists {
		return
	}
	delete(i.all, sess)
	delete(i.admins, sess)
	state := i.subscribers[key]
	if state == nil {
		return
	}
	delete(state.sessions, sess)
	if len(state.sessions) == 0 {
		delete(i.dirty, key)
	}
	for channel := range state.channels {
		i.removeChannelSessionLocked(channel, sess)
	}
	i.cleanupSubscriberLocked(key, state)
}

func (i *persistentSessionIndex) ApplySubscriptionChanges(changes []store.SubscriptionChange) {
	if i == nil || len(changes) == 0 {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, change := range changes {
		state := i.subscribers[change.Subscriber]
		if state == nil {
			continue
		}
		state.revision++
		if change.Reload {
			state.dirty = true
			if len(state.sessions) > 0 {
				i.dirty[change.Subscriber] = struct{}{}
			}
			continue
		}
		if change.Active {
			if _, exists := state.channels[change.Channel]; exists {
				continue
			}
			state.channels[change.Channel] = struct{}{}
			for sess := range state.sessions {
				i.addChannelSessionLocked(change.Channel, sess)
			}
			continue
		}
		if _, exists := state.channels[change.Channel]; !exists {
			continue
		}
		delete(state.channels, change.Channel)
		for sess := range state.sessions {
			i.removeChannelSessionLocked(change.Channel, sess)
		}
	}
}

func (i *persistentSessionIndex) HasSessions() bool {
	if i == nil {
		return false
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return len(i.all) > 0
}

func (i *persistentSessionIndex) AllCandidates() []*clientWSSession {
	if i == nil {
		return nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	return clonePersistentSessionSet(i.all)
}

func (i *persistentSessionIndex) DirectCandidates(recipient store.UserKey) []*clientWSSession {
	if i == nil {
		return nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	state := i.subscribers[recipient]
	capacity := len(i.admins)
	if state != nil {
		capacity += len(state.sessions)
	}
	dedup := make(map[*clientWSSession]struct{}, capacity)
	if state != nil {
		for sess := range state.sessions {
			dedup[sess] = struct{}{}
		}
	}
	for sess := range i.admins {
		dedup[sess] = struct{}{}
	}
	return clonePersistentSessionSet(dedup)
}

func (i *persistentSessionIndex) ChannelCandidates(ctx context.Context, channel store.UserKey) ([]*clientWSSession, error) {
	if i == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		i.mu.RLock()
		dirty := make([]store.UserKey, 0, len(i.dirty))
		for subscriber := range i.dirty {
			dirty = append(dirty, subscriber)
		}
		if len(dirty) == 0 {
			dedup := make(map[*clientWSSession]struct{}, len(i.admins)+len(i.channels[channel]))
			for sess := range i.channels[channel] {
				dedup[sess] = struct{}{}
			}
			for sess := range i.admins {
				dedup[sess] = struct{}{}
			}
			i.mu.RUnlock()
			return clonePersistentSessionSet(dedup), nil
		}
		i.mu.RUnlock()
		for _, subscriber := range dirty {
			if err := i.reloadSubscriber(ctx, subscriber); err != nil {
				return nil, err
			}
		}
	}
}

func (i *persistentSessionIndex) reloadSubscriber(ctx context.Context, subscriber store.UserKey) error {
	for {
		i.mu.Lock()
		state := i.subscribers[subscriber]
		if state == nil || !state.dirty || len(state.sessions) == 0 {
			i.cleanupSubscriberLocked(subscriber, state)
			i.mu.Unlock()
			return nil
		}
		revision := state.revision
		state.loading++
		i.mu.Unlock()

		channels, err := i.loadSubscriberChannels(ctx, subscriber)

		i.mu.Lock()
		state = i.subscribers[subscriber]
		if state == nil {
			i.mu.Unlock()
			return nil
		}
		state.loading--
		if len(state.sessions) == 0 {
			i.cleanupSubscriberLocked(subscriber, state)
			i.mu.Unlock()
			return nil
		}
		if err != nil {
			i.mu.Unlock()
			return err
		}
		if state.revision != revision {
			i.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}
		i.replaceSubscriberChannelsLocked(state, channels)
		state.loaded = true
		state.dirty = false
		delete(i.dirty, subscriber)
		i.mu.Unlock()
		return nil
	}
}

func (i *persistentSessionIndex) loadSubscriberChannels(ctx context.Context, subscriber store.UserKey) (map[store.UserKey]struct{}, error) {
	if i.load == nil {
		return nil, fmt.Errorf("persistent subscription loader is not configured")
	}
	subscriptions, err := i.load(ctx, subscriber)
	if err != nil {
		return nil, fmt.Errorf("load persistent channel subscriptions for %+v: %w", subscriber, err)
	}
	channels := make(map[store.UserKey]struct{}, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription.DeletedAt != nil || subscription.Channel.Validate() != nil {
			continue
		}
		channels[subscription.Channel] = struct{}{}
	}
	return channels, nil
}

func (i *persistentSessionIndex) ensureSubscriberLocked(key store.UserKey) *persistentSubscriberState {
	state := i.subscribers[key]
	if state == nil {
		state = &persistentSubscriberState{
			sessions: make(map[*clientWSSession]struct{}),
			channels: make(map[store.UserKey]struct{}),
		}
		i.subscribers[key] = state
	}
	return state
}

func (i *persistentSessionIndex) addSessionLocked(state *persistentSubscriberState, sess *clientWSSession) {
	i.all[sess] = struct{}{}
	state.sessions[sess] = struct{}{}
	if permission.IsAdminRole(sess.principal.User.Role) {
		i.admins[sess] = struct{}{}
	}
	for channel := range state.channels {
		i.addChannelSessionLocked(channel, sess)
	}
}

func (i *persistentSessionIndex) replaceSubscriberChannelsLocked(state *persistentSubscriberState, channels map[store.UserKey]struct{}) {
	for channel := range state.channels {
		for sess := range state.sessions {
			i.removeChannelSessionLocked(channel, sess)
		}
	}
	state.channels = channels
	for channel := range state.channels {
		for sess := range state.sessions {
			i.addChannelSessionLocked(channel, sess)
		}
	}
}

func (i *persistentSessionIndex) addChannelSessionLocked(channel store.UserKey, sess *clientWSSession) {
	bucket := i.channels[channel]
	if bucket == nil {
		bucket = make(map[*clientWSSession]struct{})
		i.channels[channel] = bucket
	}
	bucket[sess] = struct{}{}
}

func (i *persistentSessionIndex) removeChannelSessionLocked(channel store.UserKey, sess *clientWSSession) {
	bucket := i.channels[channel]
	if bucket == nil {
		return
	}
	delete(bucket, sess)
	if len(bucket) == 0 {
		delete(i.channels, channel)
	}
}

func (i *persistentSessionIndex) cleanupSubscriberLocked(key store.UserKey, state *persistentSubscriberState) {
	if state != nil && len(state.sessions) == 0 && state.loading == 0 {
		delete(i.subscribers, key)
		delete(i.dirty, key)
	}
}

func clonePersistentSessionSet(source map[*clientWSSession]struct{}) []*clientWSSession {
	sessions := make([]*clientWSSession, 0, len(source))
	for sess := range source {
		sessions = append(sessions, sess)
	}
	return sessions
}
