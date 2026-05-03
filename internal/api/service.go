package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/clock"
	"github.com/tursom/turntf/internal/permission"
	"github.com/tursom/turntf/internal/store"
)

// EventSink 事件发布接口。Service 通过它将用户创建/更新/删除、消息创建等事件发布到集群中的其他节点。
type EventSink interface {
	Publish(event store.Event)
}

// TransientPacketRouter 即时包路由接口，负责将即时包从当前节点转发到目标节点（通过 mesh 网络）。
type TransientPacketRouter interface {
	RouteTransientPacket(context.Context, store.TransientPacket) error
}

// TransientPacketReceiver 即时包接收接口。当本地收到来自其他节点的即时包时，通过此接口投递给匹配的客户端会话。
type TransientPacketReceiver interface {
	ReceiveTransientPacket(store.TransientPacket) bool
}

// LoggedInUserProvider 提供本地已登录用户列表。
type LoggedInUserProvider interface {
	ListLoggedInUsers(context.Context) ([]app.LoggedInUserSummary, error)
}

// LoggedInUserQuerier 查询远程节点的已登录用户列表，nodeID 指定目标节点。
type LoggedInUserQuerier interface {
	QueryLoggedInUsers(context.Context, int64) ([]app.LoggedInUserSummary, error)
}

// OnlineSessionRegistry 会话注册接口，用于在集群 mesh 层注册/注销本地客户端会话，以便其他节点发现。
type OnlineSessionRegistry interface {
	RegisterLocalSession(store.OnlineSession)
	UnregisterLocalSession(store.UserKey, store.SessionRef)
}

// OnlinePresenceResolver 在线状态查询接口，查询指定用户当前在哪些节点有活跃会话。
type OnlinePresenceResolver interface {
	QueryOnlineUserPresence(context.Context, store.UserKey) ([]store.OnlineNodePresence, error)
}

// OnlineSessionResolver 在线会话解析接口，查询指定用户当前的详细会话信息。
type OnlineSessionResolver interface {
	ResolveUserSessions(context.Context, store.UserKey) ([]store.OnlineSession, error)
}

// noopEventSink 空实现的事件发布器，当不需要事件复制时使用（例如单节点模式）。
type noopEventSink struct{}

func (noopEventSink) Publish(store.Event) {}

// transientPacketIDNamespace 即时包 ID 命名空间标记位。
// 将客户端/API 即时包的 ID 空间与 mesh 内部控制/数据包分离，确保多节点路径上的转发去重不会将两者误认为重复。
const transientPacketIDNamespace = uint64(1) << 63

// WriteGate 写入门控接口。在集群模式下，只有当前节点为主节点时才允许写入，防止脑裂导致的数据不一致。
type WriteGate interface {
	AllowWrite(context.Context) error
}

// Service 是 API 模块的核心服务层，封装了 store.Store 并增加集群协作能力：
//   - 写入门控（WriteGate）：阻止非主节点写入
//   - 事件发布（EventSink）：将变更事件复制到集群其他节点
//   - 即时包路由（TransientPacketRouter/Receiver）：跨节点投递即时消息
//   - 在线状态与会话管理：注册本地会话、查询用户在线状态
//
// Service 的方法遵循统一模式：先通过 allowWrite 检查写入权限，执行 store 操作，成功后发布事件。
type Service struct {
	store           *store.Store
	eventSink       EventSink
	writeGate       WriteGate
	transientRouter TransientPacketRouter
	localUsers      LoggedInUserProvider
	remoteUsers     LoggedInUserQuerier
	sessionRegistry OnlineSessionRegistry
	presence        OnlinePresenceResolver
	sessions        OnlineSessionResolver
	transientRecvMu sync.RWMutex
	transientRecv   TransientPacketReceiver // 当前活跃的即时包接收器（通常是 HTTP 层）
	nextTransientID atomic.Uint64           // 即时包 ID 自增计数器
	blacklistHits   atomic.Uint64           // 黑名单命中次数统计
}

// New 创建 Service 实例。eventSink 同时作为可选接口的来源：
// 通过类型断言从中提取 WriteGate、TransientPacketRouter、LoggedInUserQuerier、
// OnlineSessionRegistry、OnlinePresenceResolver、OnlineSessionResolver 等可选能力。
func New(st *store.Store, eventSink EventSink) *Service {
	if eventSink == nil {
		eventSink = noopEventSink{}
	}

	var writeGate WriteGate
	if gate, ok := eventSink.(WriteGate); ok {
		writeGate = gate
	}
	var transientRouter TransientPacketRouter
	if router, ok := eventSink.(TransientPacketRouter); ok {
		transientRouter = router
	}
	var remoteUsers LoggedInUserQuerier
	if querier, ok := eventSink.(LoggedInUserQuerier); ok {
		remoteUsers = querier
	}
	var sessionRegistry OnlineSessionRegistry
	if registry, ok := eventSink.(OnlineSessionRegistry); ok {
		sessionRegistry = registry
	}
	var presence OnlinePresenceResolver
	if resolver, ok := eventSink.(OnlinePresenceResolver); ok {
		presence = resolver
	}
	var sessions OnlineSessionResolver
	if resolver, ok := eventSink.(OnlineSessionResolver); ok {
		sessions = resolver
	}
	return &Service{
		store:           st,
		eventSink:       eventSink,
		writeGate:       writeGate,
		transientRouter: transientRouter,
		remoteUsers:     remoteUsers,
		sessionRegistry: sessionRegistry,
		presence:        presence,
		sessions:        sessions,
	}
}

// CreateUser 创建用户。等价于 CreateUserAs(ctx, params, nil)，不指定创建者。
func (s *Service) CreateUser(ctx context.Context, params store.CreateUserParams) (store.User, store.Event, error) {
	return s.CreateUserAs(ctx, params, nil)
}

// CreateUserAs 创建用户并指定创建者。如果创建的是频道用户（RoleChannel），
// 会自动将创建者添加为该频道的管理者和写入者。
func (s *Service) CreateUserAs(ctx context.Context, params store.CreateUserParams, creator *store.UserKey) (store.User, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.User{}, store.Event{}, err
	}
	user, events, err := s.store.CreateUserWithEvents(ctx, params)
	if err != nil {
		return user, store.Event{}, err
	}
	s.publishEvents(events)
	if params.Role == store.RoleChannel && creator != nil {
		for _, attachmentType := range []store.AttachmentType{
			store.AttachmentTypeChannelManager,
			store.AttachmentTypeChannelWriter,
		} {
			if _, _, err := s.UpsertAttachment(ctx, store.UpsertAttachmentParams{
				Owner:      user.Key(),
				Subject:    *creator,
				Type:       attachmentType,
				ConfigJSON: "{}",
			}); err != nil {
				return store.User{}, store.Event{}, err
			}
		}
	}
	return user, primaryStoreUserEvent(events), nil
}

// UpdateUser 更新用户信息。需要写入门控授权。
func (s *Service) UpdateUser(ctx context.Context, params store.UpdateUserParams) (store.User, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.User{}, store.Event{}, err
	}
	user, events, err := s.store.UpdateUserWithEvents(ctx, params)
	if err == nil {
		s.publishEvents(events)
	}
	return user, primaryStoreUserEvent(events), err
}

// DeleteUser 删除指定用户。需要写入门控授权。
func (s *Service) DeleteUser(ctx context.Context, key store.UserKey) (store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Event{}, err
	}
	events, err := s.store.DeleteUserWithEvents(ctx, key)
	if err == nil {
		s.publishEvents(events)
	}
	return primaryStoreUserEvent(events), err
}

// GetUser 获取用户信息（不含敏感字段如密码哈希）。
func (s *Service) GetUser(ctx context.Context, key store.UserKey) (store.User, error) {
	return s.store.GetUser(ctx, key)
}

// AuthenticateUser 验证用户密码，返回完整用户信息。
func (s *Service) AuthenticateUser(ctx context.Context, key store.UserKey, password string) (store.User, error) {
	return s.store.AuthenticateUser(ctx, key, password)
}

// AuthenticateUserByLoginName 通过登录名验证用户密码（而非 nodeID+userID）。
func (s *Service) AuthenticateUserByLoginName(ctx context.Context, loginName, password string) (store.User, error) {
	return s.store.AuthenticateUserByLoginName(ctx, loginName, password)
}

// GetUserLoginName 获取用户的登录名（如果设置了的话）。
func (s *Service) GetUserLoginName(ctx context.Context, key store.UserKey) (string, error) {
	return s.store.GetUserLoginName(ctx, key)
}

// ListUsers 列出系统中所有用户。
func (s *Service) ListUsers(ctx context.Context) ([]store.User, error) {
	return s.store.ListUsers(ctx)
}

// ListCommunicableUsers 列出当前操作者可通讯的用户，并应用可选过滤条件。
func (s *Service) ListCommunicableUsers(ctx context.Context, actor *store.User, filter store.UserListFilter) ([]store.User, error) {
	return s.store.ListCommunicableUsers(ctx, actor, filter)
}

// GetVisibleUserLoginName 根据查看者身份决定是否返回目标用户的登录名。
func (s *Service) GetVisibleUserLoginName(ctx context.Context, viewer *store.User, user store.User) (string, error) {
	if viewer == nil || permission.IsAdminRole(viewer.Role) || viewer.Key() == user.Key() {
		return s.store.GetUserLoginName(ctx, user.Key())
	}
	return "", nil
}

// CreateMessage 创建一条持久化消息。如果发送者被接收者拉黑，返回 ErrBlockedByBlacklist。
func (s *Service) CreateMessage(ctx context.Context, params store.CreateMessageParams) (store.Message, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Message{}, store.Event{}, err
	}
	message, event, err := s.store.CreateMessage(ctx, params)
	if errors.Is(err, store.ErrBlockedByBlacklist) {
		s.recordBlacklistHit()
	}
	if err == nil {
		s.eventSink.Publish(event)
	}
	return message, event, err
}

// SetTransientPacketReceiver 设置即时包接收器。由 HTTP 层在初始化时调用，用于将 transent 包投递到客户端会话。
func (s *Service) SetTransientPacketReceiver(receiver TransientPacketReceiver) {
	s.transientRecvMu.Lock()
	defer s.transientRecvMu.Unlock()
	s.transientRecv = receiver
}

// publishEvents 发布事件列表中所有有效的事件（EventID > 0）到集群。
func (s *Service) publishEvents(events []store.Event) {
	if s == nil || s.eventSink == nil {
		return
	}
	for _, event := range events {
		if event.EventID <= 0 {
			continue
		}
		s.eventSink.Publish(event)
	}
}

// primaryStoreUserEvent 从事件列表中提取第一个用户相关事件（创建/更新/删除），用于返回值。
func primaryStoreUserEvent(events []store.Event) store.Event {
	for _, event := range events {
		switch event.EventType {
		case store.EventTypeUserCreated, store.EventTypeUserUpdated, store.EventTypeUserDeleted:
			return event
		}
	}
	return store.Event{}
}

// SetLoggedInUserProvider 设置本地已登录用户提供者。由 HTTP 层在初始化时注入。
func (s *Service) SetLoggedInUserProvider(provider LoggedInUserProvider) {
	s.localUsers = provider
}

// RegisterLocalSession 向集群注册本地客户端会话，使其他节点可以感知该用户的在线状态。
func (s *Service) RegisterLocalSession(session store.OnlineSession) {
	if s == nil || !session.SessionRef.Valid() || s.sessionRegistry == nil {
		return
	}
	s.sessionRegistry.RegisterLocalSession(session)
}

// UnregisterLocalSession 从集群注销本地客户端会话。
func (s *Service) UnregisterLocalSession(user store.UserKey, sessionRef store.SessionRef) {
	if s == nil || !sessionRef.Valid() || s.sessionRegistry == nil {
		return
	}
	s.sessionRegistry.UnregisterLocalSession(user, sessionRef)
}

// DispatchTransientPacket 发送一条即时消息（不持久化），不指定目标会话。
func (s *Service) DispatchTransientPacket(ctx context.Context, recipient store.UserKey, sender store.UserKey, body []byte, mode store.DeliveryMode) (store.TransientPacket, error) {
	return s.DispatchTransientPacketTo(ctx, recipient, sender, body, mode, store.SessionRef{})
}

// DispatchTransientPacketTo 发送一条即时消息到指定的目标会话。
// 如果 targetSession 有效，消息直接路由到该会话；否则根据在线状态广播到该用户的所有在线节点。
func (s *Service) DispatchTransientPacketTo(ctx context.Context, recipient store.UserKey, sender store.UserKey, body []byte, mode store.DeliveryMode, targetSession store.SessionRef) (store.TransientPacket, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.TransientPacket{}, err
	}
	if err := recipient.Validate(); err != nil {
		return store.TransientPacket{}, err
	}
	if err := sender.Validate(); err != nil || len(body) == 0 {
		return store.TransientPacket{}, store.ErrInvalidInput
	}
	recipientUser, err := s.store.GetUser(ctx, recipient)
	if err != nil {
		return store.TransientPacket{}, err
	}
	if !recipientUser.CanLogin() {
		return store.TransientPacket{}, fmt.Errorf("%w: transient recipient must be a login user", store.ErrInvalidInput)
	}
	blocked, err := s.store.IsBlockedByRecipient(ctx, recipient, sender)
	if err != nil {
		return store.TransientPacket{}, err
	}
	if blocked {
		s.recordBlacklistHit()
		return store.TransientPacket{}, store.ErrBlockedByBlacklist
	}
	if targetSession.Valid() {
		if s.sessions != nil {
			sessions, err := s.sessions.ResolveUserSessions(ctx, recipient)
			if err != nil {
				return store.TransientPacket{}, err
			}
			if !containsTargetSession(sessions, targetSession) {
				return store.TransientPacket{}, store.ErrNotFound
			}
		}
		packet := s.newTransientPacket(recipient, sender, body, mode, targetSession.ServingNodeID, targetSession)
		if err := s.dispatchTransientPackets(ctx, []store.TransientPacket{packet}); err != nil {
			return store.TransientPacket{}, err
		}
		return packet, nil
	}

	targetNodeIDs := []int64{recipient.NodeID}
	if s.presence != nil {
		presence, err := s.presence.QueryOnlineUserPresence(ctx, recipient)
		if err != nil {
			return store.TransientPacket{}, err
		}
		targetNodeIDs = uniqueServingNodeIDs(presence)
	}
	if len(targetNodeIDs) == 0 && s.sessions != nil {
		sessions, err := s.sessions.ResolveUserSessions(ctx, recipient)
		if err != nil {
			return store.TransientPacket{}, err
		}
		targetNodeIDs = uniqueServingNodeIDsFromSessions(sessions)
	}
	if len(targetNodeIDs) == 0 {
		return store.TransientPacket{}, store.ErrNotFound
	}

	packets := make([]store.TransientPacket, 0, len(targetNodeIDs))
	for _, targetNodeID := range targetNodeIDs {
		if targetNodeID <= 0 {
			continue
		}
		packets = append(packets, s.newTransientPacket(recipient, sender, body, mode, targetNodeID, store.SessionRef{}))
	}
	if len(packets) == 0 {
		return store.TransientPacket{}, store.ErrNotFound
	}
	if err := s.dispatchTransientPackets(ctx, packets); err != nil {
		return store.TransientPacket{}, err
	}
	return packets[0], nil
}

// deliverTransientPacket 将即时包投递给本地接收器（如果设置了的话）。
func (s *Service) deliverTransientPacket(packet store.TransientPacket) bool {
	s.transientRecvMu.RLock()
	receiver := s.transientRecv
	s.transientRecvMu.RUnlock()
	if receiver == nil {
		return false
	}
	return receiver.ReceiveTransientPacket(packet)
}

// QueryOnlineUserPresence 查询指定用户当前在哪些节点有活跃会话（在线存在性）。
func (s *Service) QueryOnlineUserPresence(ctx context.Context, user store.UserKey) ([]store.OnlineNodePresence, error) {
	if s == nil {
		return nil, nil
	}
	if err := user.Validate(); err != nil {
		return nil, err
	}
	if s.presence == nil {
		return nil, nil
	}
	return s.presence.QueryOnlineUserPresence(ctx, user)
}

// ResolveUserSessions 查询指定用户当前的所有在线会话详情。
func (s *Service) ResolveUserSessions(ctx context.Context, user store.UserKey) ([]store.OnlineSession, error) {
	if s == nil {
		return nil, nil
	}
	if err := user.Validate(); err != nil {
		return nil, err
	}
	if s.sessions == nil {
		return nil, nil
	}
	return s.sessions.ResolveUserSessions(ctx, user)
}

func (s *Service) newTransientPacket(recipient store.UserKey, sender store.UserKey, body []byte, mode store.DeliveryMode, targetNodeID int64, targetSession store.SessionRef) store.TransientPacket {
	return store.TransientPacket{
		// Keep client/API transient packets in a separate packet-id space from
		// mesh runtime control/data packets so forwarding deduplication does not
		// treat them as duplicates on multi-node paths.
		PacketID:      s.nextTransientPacketID(),
		SourceNodeID:  s.store.NodeID(),
		TargetNodeID:  targetNodeID,
		Recipient:     recipient,
		Sender:        sender,
		Body:          body,
		DeliveryMode:  mode,
		TTLHops:       8,
		TargetSession: targetSession,
	}
}

// dispatchTransientPackets 分发即时包列表：本地包直接投递，远程包通过 transientRouter 路由。
func (s *Service) dispatchTransientPackets(ctx context.Context, packets []store.TransientPacket) error {
	for _, packet := range packets {
		if packet.TargetNodeID == s.store.NodeID() {
			s.deliverTransientPacket(packet)
			continue
		}
		if s.transientRouter == nil {
			continue
		}
		if err := s.transientRouter.RouteTransientPacket(ctx, packet); err != nil {
			return err
		}
	}
	return nil
}

// uniqueServingNodeIDs 从在线节点存在性列表中提取去重后的 ServingNodeID。
func uniqueServingNodeIDs(items []store.OnlineNodePresence) []int64 {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(items))
	out := make([]int64, 0, len(items))
	for _, item := range items {
		if item.ServingNodeID <= 0 {
			continue
		}
		if _, ok := seen[item.ServingNodeID]; ok {
			continue
		}
		seen[item.ServingNodeID] = struct{}{}
		out = append(out, item.ServingNodeID)
	}
	return out
}

// uniqueServingNodeIDsFromSessions 从在线会话列表中提取去重后的 ServingNodeID。
func uniqueServingNodeIDsFromSessions(items []store.OnlineSession) []int64 {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(items))
	out := make([]int64, 0, len(items))
	for _, item := range items {
		if item.SessionRef.ServingNodeID <= 0 {
			continue
		}
		if _, ok := seen[item.SessionRef.ServingNodeID]; ok {
			continue
		}
		seen[item.SessionRef.ServingNodeID] = struct{}{}
		out = append(out, item.SessionRef.ServingNodeID)
	}
	return out
}

// containsTargetSession 检查会话列表中是否包含指定的目标会话引用。
func containsTargetSession(items []store.OnlineSession, target store.SessionRef) bool {
	for _, item := range items {
		if item.SessionRef == target {
			return true
		}
	}
	return false
}

// ListMessagesByUser 列出指定用户收到的消息，按时间降序排列，最多返回 limit 条。
func (s *Service) ListMessagesByUser(ctx context.Context, key store.UserKey, limit int) ([]store.Message, error) {
	return s.store.ListMessagesByUser(ctx, key, limit)
}

// ListMessagesBySession 列出指定 session（会话双方）之间的消息，按时间降序排列。
func (s *Service) ListMessagesBySession(ctx context.Context, session []byte, requester store.UserKey, limit int) ([]store.Message, error) {
	return s.store.ListMessagesBySession(ctx, session, requester, limit)
}

// UpsertAttachment 创建或更新附件（如频道管理、订阅关系、黑名单等）。
func (s *Service) UpsertAttachment(ctx context.Context, params store.UpsertAttachmentParams) (store.Attachment, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Attachment{}, store.Event{}, err
	}
	attachment, event, err := s.store.UpsertAttachment(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return attachment, event, err
}

// DeleteAttachment 删除附件及其关联关系。
func (s *Service) DeleteAttachment(ctx context.Context, params store.DeleteAttachmentParams) (store.Attachment, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Attachment{}, store.Event{}, err
	}
	attachment, event, err := s.store.DeleteAttachment(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return attachment, event, err
}

// ListUserAttachments 列出用户的指定类型附件。
func (s *Service) ListUserAttachments(ctx context.Context, owner store.UserKey, attachmentType store.AttachmentType) ([]store.Attachment, error) {
	return s.store.ListUserAttachments(ctx, owner, attachmentType)
}

// UpsertUserMetadata 创建或更新用户自定义元数据键值对。
func (s *Service) UpsertUserMetadata(ctx context.Context, params store.UpsertUserMetadataParams) (store.UserMetadata, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.UserMetadata{}, store.Event{}, err
	}
	metadata, event, err := s.store.UpsertUserMetadata(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return metadata, event, err
}

// GetUserMetadata 获取用户的一条自定义元数据。
func (s *Service) GetUserMetadata(ctx context.Context, owner store.UserKey, key string) (store.UserMetadata, error) {
	return s.store.GetUserMetadata(ctx, owner, key)
}

// DeleteUserMetadata 删除用户的一条自定义元数据（软删除）。
func (s *Service) DeleteUserMetadata(ctx context.Context, params store.DeleteUserMetadataParams) (store.UserMetadata, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.UserMetadata{}, store.Event{}, err
	}
	metadata, event, err := s.store.DeleteUserMetadata(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return metadata, event, err
}

// ScanUserMetadata 按前缀扫描用户元数据，支持分页游标。
func (s *Service) ScanUserMetadata(ctx context.Context, params store.ScanUserMetadataParams) (store.UserMetadataScanResult, error) {
	return s.store.ScanUserMetadata(ctx, params)
}

// IsChannelManager 检查 subject 是否为 channel 的管理员。
func (s *Service) IsChannelManager(ctx context.Context, channel, subject store.UserKey) (bool, error) {
	return s.store.IsChannelManager(ctx, channel, subject)
}

// IsChannelWriter 检查 subject 是否有权向 channel 写入消息。
func (s *Service) IsChannelWriter(ctx context.Context, channel, subject store.UserKey) (bool, error) {
	return s.store.IsChannelWriter(ctx, channel, subject)
}

// SubscribeChannel 订阅频道，使订阅者能接收频道的持久化消息推送。
func (s *Service) SubscribeChannel(ctx context.Context, params store.ChannelSubscriptionParams) (store.Subscription, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Subscription{}, store.Event{}, err
	}
	subscription, event, err := s.store.SubscribeChannel(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return subscription, event, err
}

// UnsubscribeChannel 取消频道订阅。
func (s *Service) UnsubscribeChannel(ctx context.Context, params store.ChannelSubscriptionParams) (store.Subscription, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Subscription{}, store.Event{}, err
	}
	subscription, event, err := s.store.UnsubscribeChannel(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return subscription, event, err
}

// ListChannelSubscriptions 列出用户的所有频道订阅。
func (s *Service) ListChannelSubscriptions(ctx context.Context, key store.UserKey) ([]store.Subscription, error) {
	return s.store.ListChannelSubscriptions(ctx, key)
}

// IsSubscribedToChannel 检查用户是否已订阅指定频道。
func (s *Service) IsSubscribedToChannel(ctx context.Context, subscriber, channel store.UserKey) (bool, error) {
	return s.store.IsSubscribedToChannel(ctx, subscriber, channel)
}

// ListEvents 查询事件日志，返回序列号大于 afterSequence 的事件，最多 limit 条。
func (s *Service) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]store.Event, error) {
	return s.store.ListEvents(ctx, afterSequence, limit)
}

// LastEventSequence 返回当前最新的（已提交的）事件序列号。
func (s *Service) LastEventSequence(ctx context.Context) (int64, error) {
	return s.store.LastEventSequence(ctx)
}

// BlockUser 将目标用户加入黑名单，屏蔽其发送的消息。
func (s *Service) BlockUser(ctx context.Context, params store.BlacklistParams) (store.BlacklistEntry, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.BlacklistEntry{}, store.Event{}, err
	}
	entry, event, err := s.store.BlockUser(ctx, params)
	if err == nil && event.EventID != 0 {
		s.eventSink.Publish(event)
	}
	return entry, event, err
}

// UnblockUser 将用户从黑名单移除。
func (s *Service) UnblockUser(ctx context.Context, params store.BlacklistParams) (store.BlacklistEntry, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.BlacklistEntry{}, store.Event{}, err
	}
	entry, event, err := s.store.UnblockUser(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return entry, event, err
}

// ListBlockedUsers 列出用户的黑名单。
func (s *Service) ListBlockedUsers(ctx context.Context, owner store.UserKey) ([]store.BlacklistEntry, error) {
	return s.store.ListBlockedUsers(ctx, owner)
}

// IsMessageBlockedByBlacklist 检查 owner 在 createdAt 时间点是否已拉黑 sender。
func (s *Service) IsMessageBlockedByBlacklist(ctx context.Context, owner, sender store.UserKey, createdAt clock.Timestamp) (bool, error) {
	return s.store.IsMessageHiddenByBlacklist(ctx, owner, sender, createdAt)
}

// IsBlockedByRecipient 检查 recipient 当前是否已拉黑 sender。
func (s *Service) IsBlockedByRecipient(ctx context.Context, recipient, sender store.UserKey) (bool, error) {
	return s.store.IsBlockedByRecipient(ctx, recipient, sender)
}

// BlacklistHitsTotal 返回黑名单命中次数（用于监控指标）。
func (s *Service) BlacklistHitsTotal() uint64 {
	return s.blacklistHits.Load()
}

// RecordBlacklistHit 记录一次黑名单命中（公开方法，供外部进行指标统计）。
func (s *Service) RecordBlacklistHit() {
	s.recordBlacklistHit()
}

// allowWrite 检查写入权限。若未设置 writeGate 则默认允许。
func (s *Service) allowWrite(ctx context.Context) error {
	if s.writeGate == nil {
		return nil
	}
	return s.writeGate.AllowWrite(ctx)
}

// recordBlacklistHit 自增黑名单命中计数器（线程安全）。
func (s *Service) recordBlacklistHit() {
	s.blacklistHits.Add(1)
}

// nextTransientPacketID 生成下一个即时包 ID，高位带命名空间标记位以确保与 mesh 内部包 ID 不冲突。
func (s *Service) nextTransientPacketID() uint64 {
	return transientPacketIDNamespace | s.nextTransientID.Add(1)
}
