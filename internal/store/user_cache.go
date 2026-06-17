package store

import (
	"context"
	"database/sql"
	"sync"
)

// cachedUserRepository 是 UserRepository 的内存缓存装饰器。
// 缓存 GetUser 和 ListBroadcastUserKeys 的结果，写操作时通过 StoreUser/InvalidateUser 主动失效。
type cachedUserRepository struct {
	delegate UserRepository

	mu                sync.RWMutex
	users             map[userCacheKey]User
	broadcastUserKeys []UserKey
}

// userCacheKey 区分 includeDeleted 标记的两个缓存条目。
type userCacheKey struct {
	key            UserKey
	includeDeleted bool
}

// newCachedUserRepository 创建带缓存的 UserRepository 装饰器。
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

// StoreUser 将用户写入缓存，同时失效旧的 includeDeleted 条目。
// 写入时先清除旧条目，再根据 DeletedAt 是否为空决定是否缓存。
// 同时失效广播用户键列表缓存。
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

// InvalidateUser 从缓存中移除指定用户的所有条目（包括 includeDeleted 两个版本）。
// 同时失效广播用户键列表缓存。
func (r *cachedUserRepository) InvalidateUser(key UserKey) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.users, userCacheKey{key: key, includeDeleted: false})
	delete(r.users, userCacheKey{key: key, includeDeleted: true})
	r.invalidateBroadcastUserKeysLocked()
}

// InvalidateAll 清空所有缓存条目，创建新的空 map 并失效广播用户键列表缓存。
func (r *cachedUserRepository) InvalidateAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.users = make(map[userCacheKey]User)
	r.invalidateBroadcastUserKeysLocked()
}

// invalidateBroadcastUserKeysLocked 将广播用户键列表缓存置空，调用方需持有写锁。
func (r *cachedUserRepository) invalidateBroadcastUserKeysLocked() {
	r.broadcastUserKeys = nil
}

// cloneUserKeys 深拷贝 UserKey 切片，防止外部修改影响缓存。
func cloneUserKeys(keys []UserKey) []UserKey {
	if len(keys) == 0 {
		return []UserKey{}
	}
	cloned := make([]UserKey, len(keys))
	copy(cloned, keys)
	return cloned
}
