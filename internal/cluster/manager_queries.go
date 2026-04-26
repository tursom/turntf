package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/mesh"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func (m *Manager) QueryLoggedInUsers(ctx context.Context, nodeID int64) ([]app.LoggedInUserSummary, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: cluster manager is not configured", app.ErrServiceUnavailable)
	}
	if nodeID <= 0 {
		return nil, fmt.Errorf("%w: target node id cannot be empty", store.ErrInvalidInput)
	}
	if nodeID == m.cfg.NodeID {
		return m.listLocalLoggedInUsers(ctx)
	}
	if users, err, ok := m.cachedLoggedInUsers(nodeID, time.Now()); ok {
		return users, err
	}

	requestID, resultCh := m.beginLoggedInUsersQuery()
	req := &internalproto.QueryLoggedInUsersRequest{
		RequestId:     requestID,
		TargetNodeId:  nodeID,
		OriginNodeId:  m.cfg.NodeID,
		RemainingHops: defaultLoggedInUsersQueryMaxHops,
	}
	if m.MeshRuntime() == nil {
		m.cancelLoggedInUsersQuery(requestID, mesh.ErrNoRoute)
		err := fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
		m.storeLoggedInUsersError(nodeID, err, time.Now())
		return nil, err
	}
	if err := m.routeMeshLoggedInUsersRequest(ctx, req); err != nil {
		m.cancelLoggedInUsersQuery(requestID, err)
		wrapped := fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
		m.storeLoggedInUsersError(nodeID, wrapped, time.Now())
		return nil, wrapped
	}

	timeoutCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, queryLoggedInUsersTimeout)
		defer cancel()
	}

	select {
	case <-timeoutCtx.Done():
		m.cancelLoggedInUsersQuery(requestID, timeoutCtx.Err())
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: timed out querying node %d logged-in users", app.ErrServiceUnavailable, nodeID)
		}
		return nil, timeoutCtx.Err()
	case result := <-resultCh:
		if result.err != nil {
			if errors.Is(result.err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("%w: timed out querying node %d logged-in users", app.ErrServiceUnavailable, nodeID)
			}
			if errors.Is(result.err, errSessionClosed) {
				wrapped := fmt.Errorf("%w: peer session closed while querying node %d", app.ErrServiceUnavailable, nodeID)
				m.storeLoggedInUsersError(nodeID, wrapped, time.Now())
				return nil, wrapped
			}
			m.storeLoggedInUsersError(nodeID, result.err, time.Now())
			return nil, result.err
		}
		if err := clusterQueryError(result.response); err != nil {
			m.storeLoggedInUsersError(nodeID, err, time.Now())
			return nil, err
		}
		users := loggedInUsersFromCluster(result.response.GetItems())
		m.storeLoggedInUsersResult(nodeID, users, time.Now())
		return users, nil
	}
}

func (m *Manager) listLocalLoggedInUsers(ctx context.Context) ([]app.LoggedInUserSummary, error) {
	m.mu.Lock()
	provider := m.loggedInUsersProvider
	m.mu.Unlock()
	if provider == nil {
		return nil, fmt.Errorf("%w: local logged-in users provider is not configured", app.ErrServiceUnavailable)
	}
	return provider(ctx)
}

func clusterLoggedInUsers(users []app.LoggedInUserSummary) []*internalproto.ClusterLoggedInUser {
	items := make([]*internalproto.ClusterLoggedInUser, 0, len(users))
	for _, user := range users {
		items = append(items, &internalproto.ClusterLoggedInUser{
			NodeId:   user.NodeID,
			UserId:   user.UserID,
			Username: user.Username,
		})
	}
	return items
}

func loggedInUsersFromCluster(users []*internalproto.ClusterLoggedInUser) []app.LoggedInUserSummary {
	items := make([]app.LoggedInUserSummary, 0, len(users))
	for _, user := range users {
		if user == nil {
			continue
		}
		items = append(items, app.LoggedInUserSummary{
			NodeID:   user.NodeId,
			UserID:   user.UserId,
			Username: user.Username,
		})
	}
	return items
}

func clusterQueryError(resp *internalproto.QueryLoggedInUsersResponse) error {
	if resp == nil || strings.TrimSpace(resp.ErrorCode) == "" {
		return nil
	}
	message := strings.TrimSpace(resp.ErrorMessage)
	if message == "" {
		message = "cluster query failed"
	}
	switch resp.ErrorCode {
	case "invalid_request":
		return fmt.Errorf("%w: %s", store.ErrInvalidInput, message)
	case "not_found":
		return fmt.Errorf("%w: %s", store.ErrNotFound, message)
	case "forbidden":
		return fmt.Errorf("%w: %s", store.ErrForbidden, message)
	default:
		return fmt.Errorf("%w: %s", app.ErrServiceUnavailable, message)
	}
}

func (m *Manager) beginLoggedInUsersQuery() (uint64, chan loggedInUsersQueryResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextLoggedInUsersQueryID++
	requestID := m.nextLoggedInUsersQueryID
	ch := make(chan loggedInUsersQueryResult, 1)
	m.pendingLoggedInUsers[requestID] = ch
	return requestID, ch
}

func (m *Manager) cancelLoggedInUsersQuery(requestID uint64, err error) {
	m.mu.Lock()
	ch, ok := m.pendingLoggedInUsers[requestID]
	if ok {
		delete(m.pendingLoggedInUsers, requestID)
	}
	m.mu.Unlock()

	if !ok {
		return
	}
	select {
	case ch <- loggedInUsersQueryResult{err: err}:
	default:
	}
	close(ch)
}

func (m *Manager) resolveLoggedInUsersQuery(requestID uint64, result loggedInUsersQueryResult) bool {
	m.mu.Lock()
	ch, ok := m.pendingLoggedInUsers[requestID]
	if ok {
		delete(m.pendingLoggedInUsers, requestID)
	}
	m.mu.Unlock()

	if !ok {
		return false
	}
	ch <- result
	close(ch)
	return true
}

func (m *Manager) cachedLoggedInUsers(nodeID int64, now time.Time) ([]app.LoggedInUserSummary, error, bool) {
	if m == nil || nodeID <= 0 {
		return nil, nil, false
	}

	m.mu.Lock()
	entry, ok := m.loggedInUsersCache[nodeID]
	if ok && !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
		delete(m.loggedInUsersCache, nodeID)
		ok = false
	}
	m.mu.Unlock()

	if !ok {
		return nil, nil, false
	}
	return cloneLoggedInUsers(entry.users), entry.err, true
}

func (m *Manager) storeLoggedInUsersResult(nodeID int64, users []app.LoggedInUserSummary, now time.Time) {
	if m == nil || nodeID <= 0 {
		return
	}
	m.storeLoggedInUsersCacheEntry(nodeID, loggedInUsersCacheEntry{
		users:     cloneLoggedInUsers(users),
		cachedAt:  now,
		expiresAt: now.Add(loggedInUsersCacheTTL),
	})
}

func (m *Manager) storeLoggedInUsersError(nodeID int64, err error, now time.Time) {
	if m == nil || nodeID <= 0 || err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	m.storeLoggedInUsersCacheEntry(nodeID, loggedInUsersCacheEntry{
		err:       err,
		cachedAt:  now,
		expiresAt: now.Add(loggedInUsersCacheNegativeTTL),
	})
}

func (m *Manager) storeLoggedInUsersCacheEntry(nodeID int64, entry loggedInUsersCacheEntry) {
	if m == nil || nodeID <= 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.loggedInUsersCache == nil {
		m.loggedInUsersCache = make(map[int64]loggedInUsersCacheEntry)
	}
	m.loggedInUsersCache[nodeID] = entry
	if len(m.loggedInUsersCache) <= loggedInUsersCacheMaxEntries {
		return
	}

	var (
		evictNodeID int64
		evictAt     time.Time
		haveEvict   bool
	)
	for candidateNodeID, candidate := range m.loggedInUsersCache {
		if !haveEvict || candidate.cachedAt.Before(evictAt) {
			evictNodeID = candidateNodeID
			evictAt = candidate.cachedAt
			haveEvict = true
		}
	}
	if haveEvict {
		delete(m.loggedInUsersCache, evictNodeID)
	}
}

func cloneLoggedInUsers(users []app.LoggedInUserSummary) []app.LoggedInUserSummary {
	if len(users) == 0 {
		return nil
	}
	cloned := make([]app.LoggedInUserSummary, len(users))
	copy(cloned, users)
	return cloned
}
