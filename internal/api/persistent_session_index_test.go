package api

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/tursom/turntf/internal/store"
)

func TestPersistentSessionIndexReturnsOnlyChannelSubscribersAndAdmins(t *testing.T) {
	channel := store.UserKey{NodeID: testNodeID(1), UserID: 9001}
	userA := store.User{NodeID: testNodeID(1), ID: 101, Role: store.RoleUser}
	userB := store.User{NodeID: testNodeID(1), ID: 102, Role: store.RoleUser}
	admin := store.User{NodeID: testNodeID(1), ID: 103, Role: store.RoleAdmin}

	var mu sync.Mutex
	loadCount := make(map[store.UserKey]int)
	index := newPersistentSessionIndex(func(_ context.Context, subscriber store.UserKey) ([]store.Subscription, error) {
		mu.Lock()
		loadCount[subscriber]++
		mu.Unlock()
		if subscriber != userA.Key() {
			return nil, nil
		}
		return []store.Subscription{{Subscriber: subscriber, Channel: channel}}, nil
	})

	sessA1 := persistentIndexTestSession(userA)
	sessA2 := persistentIndexTestSession(userA)
	sessB := persistentIndexTestSession(userB)
	adminSession := persistentIndexTestSession(admin)
	for _, sess := range []*clientWSSession{sessA1, sessA2, sessB, adminSession} {
		if err := index.Register(context.Background(), sess); err != nil {
			t.Fatalf("register persistent session: %v", err)
		}
	}

	channelCandidates, err := index.ChannelCandidates(context.Background(), channel)
	if err != nil {
		t.Fatalf("load channel candidates: %v", err)
	}
	assertPersistentSessionSet(t, channelCandidates, sessA1, sessA2, adminSession)
	assertPersistentSessionSet(t, index.DirectCandidates(userB.Key()), sessB, adminSession)
	assertPersistentSessionSet(t, index.AllCandidates(), sessA1, sessA2, sessB, adminSession)

	mu.Lock()
	if loadCount[userA.Key()] != 1 {
		t.Fatalf("expected subscriptions to load once for a multi-session user, got %d", loadCount[userA.Key()])
	}
	mu.Unlock()

	index.ApplySubscriptionChanges([]store.SubscriptionChange{{
		Subscriber: userA.Key(),
		Channel:    channel,
		Active:     false,
	}})
	index.ApplySubscriptionChanges([]store.SubscriptionChange{{
		Subscriber: userA.Key(),
		Channel:    channel,
		Active:     false,
	}})
	channelCandidates, err = index.ChannelCandidates(context.Background(), channel)
	if err != nil {
		t.Fatalf("reload channel candidates: %v", err)
	}
	assertPersistentSessionSet(t, channelCandidates, adminSession)

	index.Unregister(adminSession)
	index.Unregister(sessA1)
	index.Unregister(sessA2)
	index.Unregister(sessB)
	if index.HasSessions() {
		t.Fatal("expected persistent index to be empty after unregistering every session")
	}
}

func TestPersistentSessionIndexRetriesRegistrationAfterConcurrentSubscriptionChange(t *testing.T) {
	channel := store.UserKey{NodeID: testNodeID(1), UserID: 9101}
	user := store.User{NodeID: testNodeID(1), ID: 111, Role: store.RoleUser}
	firstLoadStarted := make(chan struct{})
	releaseFirstLoad := make(chan struct{})
	var loads atomic.Int32
	index := newPersistentSessionIndex(func(_ context.Context, subscriber store.UserKey) ([]store.Subscription, error) {
		if loads.Add(1) == 1 {
			close(firstLoadStarted)
			<-releaseFirstLoad
			return nil, nil
		}
		return []store.Subscription{{Subscriber: subscriber, Channel: channel}}, nil
	})
	sess := persistentIndexTestSession(user)
	errCh := make(chan error, 1)
	go func() {
		errCh <- index.Register(context.Background(), sess)
	}()
	<-firstLoadStarted
	index.ApplySubscriptionChanges([]store.SubscriptionChange{{
		Subscriber: user.Key(),
		Channel:    channel,
		Active:     true,
	}})
	close(releaseFirstLoad)
	if err := <-errCh; err != nil {
		t.Fatalf("register after concurrent subscription change: %v", err)
	}
	if loads.Load() != 2 {
		t.Fatalf("expected stale subscription load to retry once, got %d loads", loads.Load())
	}
	candidates, err := index.ChannelCandidates(context.Background(), channel)
	if err != nil {
		t.Fatalf("load channel candidates: %v", err)
	}
	assertPersistentSessionSet(t, candidates, sess)
}

func TestPersistentSessionIndexConcurrentRegistrationsObserveSubscriptionChange(t *testing.T) {
	channel := store.UserKey{NodeID: testNodeID(1), UserID: 9151}
	user := store.User{NodeID: testNodeID(1), ID: 116, Role: store.RoleUser}
	initialLoadsStarted := make(chan struct{}, 2)
	releaseInitialLoads := make(chan struct{})
	var loads atomic.Int32
	index := newPersistentSessionIndex(func(_ context.Context, subscriber store.UserKey) ([]store.Subscription, error) {
		if loads.Add(1) <= 2 {
			initialLoadsStarted <- struct{}{}
			<-releaseInitialLoads
			return nil, nil
		}
		return []store.Subscription{{Subscriber: subscriber, Channel: channel}}, nil
	})
	sessA := persistentIndexTestSession(user)
	sessB := persistentIndexTestSession(user)
	errCh := make(chan error, 2)
	for _, sess := range []*clientWSSession{sessA, sessB} {
		go func(sess *clientWSSession) {
			errCh <- index.Register(context.Background(), sess)
		}(sess)
	}
	<-initialLoadsStarted
	<-initialLoadsStarted
	index.ApplySubscriptionChanges([]store.SubscriptionChange{{
		Subscriber: user.Key(),
		Channel:    channel,
		Active:     true,
	}})
	close(releaseInitialLoads)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("register concurrent persistent session: %v", err)
		}
	}

	candidates, err := index.ChannelCandidates(context.Background(), channel)
	if err != nil {
		t.Fatalf("load channel candidates: %v", err)
	}
	assertPersistentSessionSet(t, candidates, sessA, sessB)
}

func TestPersistentSessionIndexReloadsDirtySubscribersBeforeChannelLookup(t *testing.T) {
	channel := store.UserKey{NodeID: testNodeID(1), UserID: 9201}
	user := store.User{NodeID: testNodeID(1), ID: 121, Role: store.RoleUser}
	var active atomic.Bool
	var loadErr atomic.Bool
	index := newPersistentSessionIndex(func(_ context.Context, subscriber store.UserKey) ([]store.Subscription, error) {
		if loadErr.Load() {
			return nil, errors.New("subscription store unavailable")
		}
		if !active.Load() {
			return nil, nil
		}
		return []store.Subscription{{Subscriber: subscriber, Channel: channel}}, nil
	})
	sess := persistentIndexTestSession(user)
	if err := index.Register(context.Background(), sess); err != nil {
		t.Fatalf("register persistent session: %v", err)
	}

	active.Store(true)
	index.ApplySubscriptionChanges([]store.SubscriptionChange{{
		Subscriber: user.Key(),
		Channel:    channel,
		Reload:     true,
	}})
	loadErr.Store(true)
	if _, err := index.ChannelCandidates(context.Background(), channel); err == nil {
		t.Fatal("expected dirty channel lookup to fail closed when subscriptions cannot reload")
	}
	loadErr.Store(false)
	candidates, err := index.ChannelCandidates(context.Background(), channel)
	if err != nil {
		t.Fatalf("reload dirty channel candidates: %v", err)
	}
	assertPersistentSessionSet(t, candidates, sess)
}

func TestPersistentSessionIndexCleansUpUnregisteredSubscriberDuringReload(t *testing.T) {
	channel := store.UserKey{NodeID: testNodeID(1), UserID: 9301}
	user := store.User{NodeID: testNodeID(1), ID: 131, Role: store.RoleUser}
	reloadStarted := make(chan struct{})
	releaseReload := make(chan struct{})
	var loads atomic.Int32
	index := newPersistentSessionIndex(func(_ context.Context, subscriber store.UserKey) ([]store.Subscription, error) {
		if loads.Add(1) == 1 {
			return nil, nil
		}
		close(reloadStarted)
		<-releaseReload
		return []store.Subscription{{Subscriber: subscriber, Channel: channel}}, nil
	})
	sess := persistentIndexTestSession(user)
	if err := index.Register(context.Background(), sess); err != nil {
		t.Fatalf("register persistent session: %v", err)
	}
	index.ApplySubscriptionChanges([]store.SubscriptionChange{{
		Subscriber: user.Key(),
		Channel:    channel,
		Reload:     true,
	}})

	candidatesCh := make(chan []*clientWSSession, 1)
	errCh := make(chan error, 1)
	go func() {
		candidates, err := index.ChannelCandidates(context.Background(), channel)
		candidatesCh <- candidates
		errCh <- err
	}()
	<-reloadStarted
	index.Unregister(sess)
	close(releaseReload)

	if err := <-errCh; err != nil {
		t.Fatalf("load channel candidates after unregister: %v", err)
	}
	assertPersistentSessionSet(t, <-candidatesCh)
	if index.HasSessions() {
		t.Fatal("expected persistent index to be empty after unregister during reload")
	}
	index.mu.RLock()
	defer index.mu.RUnlock()
	if len(index.subscribers) != 0 || len(index.dirty) != 0 {
		t.Fatalf("expected subscriber reload state to be cleaned up: subscribers=%d dirty=%d", len(index.subscribers), len(index.dirty))
	}
}

func persistentIndexTestSession(user store.User) *clientWSSession {
	return &clientWSSession{principal: &requestPrincipal{User: user}}
}

func assertPersistentSessionSet(t *testing.T, got []*clientWSSession, want ...*clientWSSession) {
	t.Helper()
	gotSet := make(map[*clientWSSession]struct{}, len(got))
	for _, sess := range got {
		gotSet[sess] = struct{}{}
	}
	if len(gotSet) != len(want) {
		t.Fatalf("unexpected persistent session count: got=%d want=%d", len(gotSet), len(want))
	}
	for _, sess := range want {
		if _, ok := gotSet[sess]; !ok {
			t.Fatalf("missing persistent session %p from candidates", sess)
		}
	}
}
