package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestCachedUserRepositoryCachesNonTransactionalReads(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := UserKey{NodeID: 1, UserID: 2}
	repo := &countingUserRepository{
		users: map[UserKey]User{
			key: {NodeID: key.NodeID, ID: key.UserID, Username: "alice"},
		},
	}
	cached := newCachedUserRepository(repo)

	first, err := cached.GetUser(ctx, key, false)
	if err != nil {
		t.Fatalf("first get user: %v", err)
	}
	second, err := cached.GetUser(ctx, key, false)
	if err != nil {
		t.Fatalf("second get user: %v", err)
	}
	if first.Username != "alice" || second.Username != "alice" {
		t.Fatalf("unexpected cached users: first=%+v second=%+v", first, second)
	}
	if repo.getUserCount != 1 {
		t.Fatalf("expected one delegate get, got %d", repo.getUserCount)
	}

	if _, err := cached.GetUser(ctx, key, true); err != nil {
		t.Fatalf("include-deleted get user: %v", err)
	}
	if repo.getUserCount != 2 {
		t.Fatalf("expected include-deleted reads to use a separate cache key, got %d delegate gets", repo.getUserCount)
	}

	if _, err := cached.GetUserTx(ctx, nil, key, false); err != nil {
		t.Fatalf("transactional get user: %v", err)
	}
	if repo.getUserTxCount != 1 {
		t.Fatalf("expected transactional read to bypass cache, got %d delegate tx gets", repo.getUserTxCount)
	}
}

func TestCachedUserRepositoryInvalidatesUsersAndBroadcastList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := UserKey{NodeID: 1, UserID: 2}
	repo := &countingUserRepository{
		users: map[UserKey]User{
			key: {NodeID: key.NodeID, ID: key.UserID, Username: "alice"},
		},
		broadcastUserKeys: []UserKey{key},
	}
	cached := newCachedUserRepository(repo)

	if _, err := cached.GetUser(ctx, key, false); err != nil {
		t.Fatalf("prime user cache: %v", err)
	}
	keys, err := cached.ListBroadcastUserKeys(ctx)
	if err != nil {
		t.Fatalf("prime broadcast cache: %v", err)
	}
	keys[0].UserID = 999

	keys, err = cached.ListBroadcastUserKeys(ctx)
	if err != nil {
		t.Fatalf("read broadcast cache: %v", err)
	}
	if keys[0] != key {
		t.Fatalf("expected cached broadcast keys to be cloned, got %+v", keys)
	}
	if repo.listBroadcastCount != 1 {
		t.Fatalf("expected one delegate broadcast list, got %d", repo.listBroadcastCount)
	}

	updated := User{NodeID: key.NodeID, ID: key.UserID, Username: "alice-updated"}
	cached.StoreUser(updated)
	loaded, err := cached.GetUser(ctx, key, false)
	if err != nil {
		t.Fatalf("get stored user: %v", err)
	}
	if loaded.Username != "alice-updated" {
		t.Fatalf("expected stored user from cache, got %+v", loaded)
	}
	if repo.getUserCount != 1 {
		t.Fatalf("expected StoreUser to seed cache without delegate get, got %d delegate gets", repo.getUserCount)
	}

	if _, err := cached.ListBroadcastUserKeys(ctx); err != nil {
		t.Fatalf("list broadcast after store: %v", err)
	}
	if repo.listBroadcastCount != 2 {
		t.Fatalf("expected StoreUser to invalidate broadcast list, got %d delegate lists", repo.listBroadcastCount)
	}

	repo.users[key] = User{NodeID: key.NodeID, ID: key.UserID, Username: "alice-reloaded"}
	cached.InvalidateUser(key)
	loaded, err = cached.GetUser(ctx, key, false)
	if err != nil {
		t.Fatalf("get invalidated user: %v", err)
	}
	if loaded.Username != "alice-reloaded" {
		t.Fatalf("expected reloaded user, got %+v", loaded)
	}
	if repo.getUserCount != 2 {
		t.Fatalf("expected invalidate to force delegate get, got %d delegate gets", repo.getUserCount)
	}
}

type countingUserRepository struct {
	users              map[UserKey]User
	broadcastUserKeys  []UserKey
	getUserCount       int
	getUserTxCount     int
	listBroadcastCount int
}

func (r *countingUserRepository) GetUser(_ context.Context, key UserKey, includeDeleted bool) (User, error) {
	r.getUserCount++
	user, ok := r.users[key]
	if !ok || (!includeDeleted && user.DeletedAt != nil) {
		return User{}, ErrNotFound
	}
	return user, nil
}

func (r *countingUserRepository) GetUserTx(_ context.Context, _ *sql.Tx, key UserKey, includeDeleted bool) (User, error) {
	r.getUserTxCount++
	user, ok := r.users[key]
	if !ok || (!includeDeleted && user.DeletedAt != nil) {
		return User{}, ErrNotFound
	}
	return user, nil
}

func (r *countingUserRepository) ListBroadcastUserKeys(context.Context) ([]UserKey, error) {
	r.listBroadcastCount++
	if r.broadcastUserKeys == nil {
		return nil, errors.New("broadcast keys not configured")
	}
	keys := make([]UserKey, len(r.broadcastUserKeys))
	copy(keys, r.broadcastUserKeys)
	return keys, nil
}
