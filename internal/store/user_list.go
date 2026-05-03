package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

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

	nameFilter := normalizeUserListName(filter.Name)
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

func (s *Store) loadCommunicableUserRelations(ctx context.Context, actor *User) (map[UserKey]struct{}, map[UserKey]struct{}, map[UserKey]struct{}, map[UserKey]struct{}, error) {
	blockedByActor := make(map[UserKey]struct{})
	blockedActorByTarget := make(map[UserKey]struct{})
	subscribedChannels := make(map[UserKey]struct{})
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

func normalizeUserListName(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

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

func isAdminListUsersRole(role string) bool {
	return role == RoleAdmin || role == RoleSuperAdmin
}
