package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

// userListProfile 用于从用户 Profile JSON 中提取显示名称。
// DisplayName 是标准字段，DisplayNameAlt 是旧版兼容字段。
type userListProfile struct {
	DisplayName    string `json:"display_name"`
	DisplayNameAlt string `json:"displayName"`
}

// ListCommunicableUsers 列出对当前操作者可通讯的活跃用户，并应用可选过滤条件。
func (s *Store) ListCommunicableUsers(ctx context.Context, actor *User, filter UserListFilter) ([]User, error) {
	candidates, err := s.listUserListCandidates(ctx, filter.UID)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return []User{}, nil
	}

	// 标准化名称过滤器（转小写去空格）
	nameFilter := normalizeUserListName(filter.Name)
	// 管理员或未登录用户可直接看到全部用户，跳过通讯性过滤
	if actor == nil || isAdminListUsersRole(actor.Role) {
		return s.filterUsersByName(ctx, actor, candidates, nameFilter)
	}

	blockedByActor, blockedActorByTarget, subscribedChannels, writableChannels, err := s.loadCommunicableUserRelations(ctx, actor)
	if err != nil {
		return nil, err
	}
	hiddenFromOthers, err := s.listUsersHiddenFromOthers(ctx, candidates)
	if err != nil {
		return nil, err
	}

	users := make([]User, 0, len(candidates))
	for _, candidate := range candidates {
		if !userVisibleToActor(actor, candidate, hiddenFromOthers) {
			continue
		}
		if !userCommunicableWithActor(actor, candidate, blockedByActor, blockedActorByTarget, subscribedChannels, writableChannels) {
			continue
		}
		matched, err := s.userMatchesListName(ctx, actor, candidate, nameFilter)
		if err != nil {
			return nil, err
		}
		if matched {
			users = append(users, candidate)
		}
	}
	return users, nil
}

// listUserListCandidates 获取用户列表的候选集合。
// 如果指定了 uid 则只查询指定用户，否则返回全部用户列表。
func (s *Store) listUserListCandidates(ctx context.Context, uid *UserKey) ([]User, error) {
	if uid == nil {
		return s.ListUsers(ctx)
	}
	if err := uid.Validate(); err != nil {
		return nil, err
	}
	user, err := s.GetUser(ctx, *uid)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return []User{}, nil
		}
		return nil, err
	}
	return []User{user}, nil
}

// loadCommunicableUserRelations 为操作者加载通讯性判断所需的关联数据。
// 返回操作者拉黑的用户集合、拉黑操作者的用户集合、已订阅频道集合和可写频道集合。
// 普通用户模式下额外检查对方是否已拉黑操作者（双向拉黑过滤）。
func (s *Store) loadCommunicableUserRelations(ctx context.Context, actor *User) (map[UserKey]struct{}, map[UserKey]struct{}, map[UserKey]struct{}, map[UserKey]struct{}, error) {
	// blockedByActor: 操作者拉黑的用户集合
	blockedByActor := make(map[UserKey]struct{})
	// blockedActorByTarget: 拉黑操作者的用户集合（仅普通用户间双向拉黑检查）
	blockedActorByTarget := make(map[UserKey]struct{})
	// subscribedChannels: 操作者已订阅的频道集合
	subscribedChannels := make(map[UserKey]struct{})
	// writableChannels: 操作者有写入权限的频道集合
	writableChannels := make(map[UserKey]struct{})
	if actor == nil {
		return blockedByActor, blockedActorByTarget, subscribedChannels, writableChannels, nil
	}

	key := actor.Key()
	if actor.CanLogin() {
		blockedEntries, err := s.ListBlockedUsers(ctx, key)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		for _, entry := range blockedEntries {
			blockedByActor[entry.Blocked] = struct{}{}
		}

		subscriptions, err := s.ListChannelSubscriptions(ctx, key)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		for _, item := range subscriptions {
			subscribedChannels[item.Channel] = struct{}{}
		}
	}

	writers, err := s.attachments.ListActiveBySubject(ctx, key, AttachmentTypeChannelWriter)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	for _, attachment := range writers {
		writableChannels[attachment.Owner] = struct{}{}
	}

	// 普通用户模式下，额外查询对方是否拉黑了操作者（检查 AttachmentTypeUserBlacklist）
	if actor.Role == RoleUser {
		blockingAttachments, err := s.attachments.ListActiveBySubject(ctx, key, AttachmentTypeUserBlacklist)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		for _, attachment := range blockingAttachments {
			blockedActorByTarget[attachment.Owner] = struct{}{}
		}
	}
	return blockedByActor, blockedActorByTarget, subscribedChannels, writableChannels, nil
}

// filterUsersByName 从候选用户中按名称模糊过滤。
// nameFilter 为空时返回全部候选用户的副本。
func (s *Store) filterUsersByName(ctx context.Context, actor *User, candidates []User, nameFilter string) ([]User, error) {
	if nameFilter == "" {
		users := make([]User, len(candidates))
		copy(users, candidates)
		return users, nil
	}

	users := make([]User, 0, len(candidates))
	for _, candidate := range candidates {
		matched, err := s.userMatchesListName(ctx, actor, candidate, nameFilter)
		if err != nil {
			return nil, err
		}
		if matched {
			users = append(users, candidate)
		}
	}
	return users, nil
}

// userMatchesListName 检查用户是否匹配名称过滤器。
// 匹配范围包括 username、Profile 中的 displayName（普通用户和 Admin 均可）
// 以及 loginName（仅管理员/未登录时可选）。全部小写不区分大小写。
func (s *Store) userMatchesListName(ctx context.Context, actor *User, user User, nameFilter string) (bool, error) {
	if nameFilter == "" {
		return true, nil
	}
	if strings.Contains(strings.ToLower(user.Username), nameFilter) {
		return true, nil
	}
	if displayName := userProfileDisplayName(user.Profile); displayName != "" && strings.Contains(strings.ToLower(displayName), nameFilter) {
		return true, nil
	}
	if actor == nil || isAdminListUsersRole(actor.Role) {
		loginName, err := s.GetUserLoginName(ctx, user.Key())
		if err != nil {
			return false, err
		}
		if strings.Contains(strings.ToLower(loginName), nameFilter) {
			return true, nil
		}
	}
	return false, nil
}

// userVisibleToActor 判断用户是否对操作者可见。
// 管理员/未登录时所有用户可见。操作者自身和系统保留用户始终可见。
// 普通用户之间检查 system.visible_to_others 元数据标记。
func userVisibleToActor(actor *User, candidate User, hiddenFromOthers map[UserKey]struct{}) bool {
	if actor == nil || isAdminListUsersRole(actor.Role) {
		return true
	}
	if actor.Key() == candidate.Key() || candidate.SystemReserved {
		return true
	}
	_, hidden := hiddenFromOthers[candidate.Key()]
	return !hidden
}

// userCommunicableWithActor 判断操作者是否能与候选用户通讯。
// 角色规则：RoleNode 不可通讯、RoleChannel 需要订阅或写入权限、
// RoleBroadcast 始终可通讯、普通用户间需要双向未拉黑才可通讯。
func userCommunicableWithActor(actor *User, candidate User, blockedByActor, blockedActorByTarget, subscribedChannels, writableChannels map[UserKey]struct{}) bool {
	if actor == nil {
		return true
	}
	if actor.Key() == candidate.Key() {
		return true
	}

	switch candidate.Role {
	case RoleNode:
		return false
	case RoleChannel:
		_, subscribed := subscribedChannels[candidate.Key()]
		_, writable := writableChannels[candidate.Key()]
		return subscribed || writable
	case RoleBroadcast:
		return true
	case RoleUser, RoleAdmin, RoleSuperAdmin:
		_, blocked := blockedByActor[candidate.Key()]
		_, blockedBack := blockedActorByTarget[candidate.Key()]
		return !(blocked && blockedBack)
	default:
		return false
	}
}

// normalizeUserListName 将名称过滤器标准化为全小写去首尾空格。
func normalizeUserListName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

// userProfileDisplayName 从用户 Profile JSON 中提取显示名称，优先使用 DisplayName 字段。
func userProfileDisplayName(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var profile userListProfile
	if err := json.Unmarshal([]byte(raw), &profile); err != nil {
		return ""
	}
	if name := strings.TrimSpace(profile.DisplayName); name != "" {
		return name
	}
	return strings.TrimSpace(profile.DisplayNameAlt)
}

// isAdminListUsersRole 判断角色是否属于管理员（拥有全部用户可见权限）。
func isAdminListUsersRole(role string) bool {
	return role == RoleAdmin || role == RoleSuperAdmin
}
