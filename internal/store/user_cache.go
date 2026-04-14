package store

import (
	"context"
	"database/sql"
	"sync"
)

type cachedUserRepository struct {
	delegate UserRepository

	mu                sync.RWMutex
	users             map[userCacheKey]User
	broadcastUserKeys []UserKey
}

type userCacheKey struct {
	key            UserKey
	includeDeleted bool
}

func newCachedUserRepository(delegate UserRepository) *cachedUserRepository {
	return &cachedUserRepository{
		delegate: delegate,
		users:    make(map[userCacheKey]User),
	}
}

func (r *cachedUserRepository) GetUser(ctx context.Context, key UserKey, includeDeleted bool) (User, error) {
	if err := key.Validate(); err != nil {
		return User{}, err
	}

	cacheKey := userCacheKey{key: key, includeDeleted: includeDeleted}
	r.mu.RLock()
	user, ok := r.users[cacheKey]
	r.mu.RUnlock()
	if ok {
		return user, nil
	}

	user, err := r.delegate.GetUser(ctx, key, includeDeleted)
	if err != nil {
		return User{}, err
	}

	r.mu.Lock()
	r.users[cacheKey] = user
	r.mu.Unlock()
	return user, nil
}

func (r *cachedUserRepository) GetUserTx(ctx context.Context, tx *sql.Tx, key UserKey, includeDeleted bool) (User, error) {
	return r.delegate.GetUserTx(ctx, tx, key, includeDeleted)
}

func (r *cachedUserRepository) ListBroadcastUserKeys(ctx context.Context) ([]UserKey, error) {
	r.mu.RLock()
	if r.broadcastUserKeys != nil {
		keys := cloneUserKeys(r.broadcastUserKeys)
		r.mu.RUnlock()
		return keys, nil
	}
	r.mu.RUnlock()

	keys, err := r.delegate.ListBroadcastUserKeys(ctx)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	r.broadcastUserKeys = cloneUserKeys(keys)
	r.mu.Unlock()
	return cloneUserKeys(keys), nil
}

func (r *cachedUserRepository) StoreUser(user User) {
	key := user.Key()

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.users, userCacheKey{key: key, includeDeleted: false})
	delete(r.users, userCacheKey{key: key, includeDeleted: true})
	if user.DeletedAt == nil {
		r.users[userCacheKey{key: key, includeDeleted: false}] = user
	}
	r.users[userCacheKey{key: key, includeDeleted: true}] = user
	r.invalidateBroadcastUserKeysLocked()
}

func (r *cachedUserRepository) InvalidateUser(key UserKey) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.users, userCacheKey{key: key, includeDeleted: false})
	delete(r.users, userCacheKey{key: key, includeDeleted: true})
	r.invalidateBroadcastUserKeysLocked()
}

func (r *cachedUserRepository) InvalidateAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.users = make(map[userCacheKey]User)
	r.invalidateBroadcastUserKeysLocked()
}

func (r *cachedUserRepository) invalidateBroadcastUserKeysLocked() {
	r.broadcastUserKeys = nil
}

func cloneUserKeys(keys []UserKey) []UserKey {
	if len(keys) == 0 {
		return []UserKey{}
	}
	cloned := make([]UserKey, len(keys))
	copy(cloned, keys)
	return cloned
}
