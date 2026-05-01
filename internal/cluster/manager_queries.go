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

// QueryLoggedInUsers 查询指定节点上的已登录用户列表。
// 如果目标节点是本地节点，则直接调用本地提供者；
// 否则从缓存的远程在线状态中返回快照。
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

// listLocalLoggedInUsers 通过配置的提供者列出本节点的已登录用户。
func (m *Manager) listLocalLoggedInUsers(ctx context.Context) ([]app.LoggedInUserSummary, error) {
	m.mu.Lock()
	provider := m.loggedInUsersProvider
	m.mu.Unlock()
	if provider == nil {
		return nil, fmt.Errorf("%w: local logged-in users provider is not configured", app.ErrServiceUnavailable)
	}
	return provider(ctx)
}

// snapshotLocalLoggedInUsers 获取本地已登录用户的快照。
// 返回经过规范化（排序和去空格）的用户列表。
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

// remoteLoggedInUsersSnapshot 返回指定远程节点已登录用户的最新快照副本。
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

// clusterLoggedInUsers 将应用层用户摘要转换为protobuf格式的集群登录用户。
func clusterLoggedInUsers(users []app.LoggedInUserSummary) []*internalproto.ClusterLoggedInUser {
	users = normalizeLoggedInUsers(users)
	items := make([]*internalproto.ClusterLoggedInUser, 0, len(users))
	for _, user := range users {
		items = append(items, &internalproto.ClusterLoggedInUser{
			NodeId:    user.NodeID,
			UserId:    user.UserID,
			Username:  user.Username,
			LoginName: user.LoginName,
		})
	}
	return items
}

// loggedInUsersFromCluster 将protobuf格式的集群登录用户转换为应用层用户摘要。
func loggedInUsersFromCluster(users []*internalproto.ClusterLoggedInUser) []app.LoggedInUserSummary {
	items := make([]app.LoggedInUserSummary, 0, len(users))
	for _, user := range users {
		if user == nil {
			continue
		}
		items = append(items, app.LoggedInUserSummary{
			NodeID:    user.NodeId,
			UserID:    user.UserId,
			Username:  user.Username,
			LoginName: user.LoginName,
		})
	}
	return normalizeLoggedInUsers(items)
}

// cloneLoggedInUsers 创建已登录用户切片的浅拷贝。
func cloneLoggedInUsers(users []app.LoggedInUserSummary) []app.LoggedInUserSummary {
	if len(users) == 0 {
		return nil
	}
	cloned := make([]app.LoggedInUserSummary, len(users))
	copy(cloned, users)
	return cloned
}

// normalizeLoggedInUsers 规范化已登录用户列表：去除用户名和登录名的空白，
// 并按 (NodeID, UserID, Username, LoginName) 排序。
func normalizeLoggedInUsers(users []app.LoggedInUserSummary) []app.LoggedInUserSummary {
	if len(users) == 0 {
		return nil
	}
	normalized := cloneLoggedInUsers(users)
	for idx := range normalized {
		normalized[idx].Username = strings.TrimSpace(normalized[idx].Username)
		normalized[idx].LoginName = strings.TrimSpace(normalized[idx].LoginName)
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].NodeID != normalized[j].NodeID {
			return normalized[i].NodeID < normalized[j].NodeID
		}
		if normalized[i].UserID != normalized[j].UserID {
			return normalized[i].UserID < normalized[j].UserID
		}
		if normalized[i].Username != normalized[j].Username {
			return normalized[i].Username < normalized[j].Username
		}
		return normalized[i].LoginName < normalized[j].LoginName
	})
	return normalized
}
