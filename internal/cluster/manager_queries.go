package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

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

	requestID, resultCh := m.beginLoggedInUsersQuery()
	req := &internalproto.QueryLoggedInUsersRequest{
		RequestId:     requestID,
		TargetNodeId:  nodeID,
		OriginNodeId:  m.cfg.NodeID,
		RemainingHops: defaultLoggedInUsersQueryMaxHops,
	}
	if m.MeshRuntime() == nil {
		m.cancelLoggedInUsersQuery(requestID, mesh.ErrNoRoute)
		return nil, fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
	}
	if err := m.routeMeshLoggedInUsersRequest(ctx, req); err != nil {
		m.cancelLoggedInUsersQuery(requestID, err)
		return nil, fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
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
				return nil, fmt.Errorf("%w: peer session closed while querying node %d", app.ErrServiceUnavailable, nodeID)
			}
			return nil, result.err
		}
		if err := clusterQueryError(result.response); err != nil {
			return nil, err
		}
		return loggedInUsersFromCluster(result.response.GetItems()), nil
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
