package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/tursom/turntf/internal/app"
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
	users, ok := m.remoteLoggedInUsersSnapshot(nodeID)
	if !ok {
		return nil, meshNoRouteError(nodeID)
	}
	return users, nil
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

func (m *Manager) snapshotLocalLoggedInUsers() ([]app.LoggedInUserSummary, error) {
	if m == nil {
		return nil, nil
	}
	m.mu.Lock()
	provider := m.loggedInUsersProvider
	ctx := m.ctx
	m.mu.Unlock()
	if provider == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	users, err := provider(ctx)
	if err != nil {
		return nil, err
	}
	return normalizeLoggedInUsers(users), nil
}

func (m *Manager) remoteLoggedInUsersSnapshot(nodeID int64) ([]app.LoggedInUserSummary, bool) {
	if m == nil || nodeID <= 0 || nodeID == m.cfg.NodeID {
		return nil, false
	}
	m.mu.Lock()
	users, ok := m.loggedInUsersByNode[nodeID]
	m.mu.Unlock()
	if !ok {
		return nil, false
	}
	return cloneLoggedInUsers(users), true
}

func clusterLoggedInUsers(users []app.LoggedInUserSummary) []*internalproto.ClusterLoggedInUser {
	users = normalizeLoggedInUsers(users)
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
	return normalizeLoggedInUsers(items)
}

func cloneLoggedInUsers(users []app.LoggedInUserSummary) []app.LoggedInUserSummary {
	if len(users) == 0 {
		return nil
	}
	cloned := make([]app.LoggedInUserSummary, len(users))
	copy(cloned, users)
	return cloned
}

func normalizeLoggedInUsers(users []app.LoggedInUserSummary) []app.LoggedInUserSummary {
	if len(users) == 0 {
		return nil
	}
	normalized := cloneLoggedInUsers(users)
	for idx := range normalized {
		normalized[idx].Username = strings.TrimSpace(normalized[idx].Username)
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].NodeID != normalized[j].NodeID {
			return normalized[i].NodeID < normalized[j].NodeID
		}
		if normalized[i].UserID != normalized[j].UserID {
			return normalized[i].UserID < normalized[j].UserID
		}
		return normalized[i].Username < normalized[j].Username
	})
	return normalized
}
