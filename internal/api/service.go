package api

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/clock"
	"github.com/tursom/turntf/internal/store"
)

type EventSink interface {
	Publish(event store.Event)
}

type TransientPacketRouter interface {
	RouteTransientPacket(context.Context, store.TransientPacket) error
}

type TransientPacketReceiver interface {
	ReceiveTransientPacket(store.TransientPacket) bool
}

type LoggedInUserProvider interface {
	ListLoggedInUsers(context.Context) ([]app.LoggedInUserSummary, error)
}

type LoggedInUserQuerier interface {
	QueryLoggedInUsers(context.Context, int64) ([]app.LoggedInUserSummary, error)
}

type OnlineSessionRegistry interface {
	RegisterLocalSession(store.OnlineSession)
	UnregisterLocalSession(store.UserKey, store.SessionRef)
}

type OnlinePresenceResolver interface {
	QueryOnlineUserPresence(context.Context, store.UserKey) ([]store.OnlineNodePresence, error)
}

type OnlineSessionResolver interface {
	ResolveUserSessions(context.Context, store.UserKey) ([]store.OnlineSession, error)
}

type noopEventSink struct{}

func (noopEventSink) Publish(store.Event) {}

const transientPacketIDNamespace = uint64(1) << 63

type WriteGate interface {
	AllowWrite(context.Context) error
}

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
	transientRecv   TransientPacketReceiver
	nextTransientID atomic.Uint64
	blacklistHits   atomic.Uint64
}

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

func (s *Service) CreateUser(ctx context.Context, params store.CreateUserParams) (store.User, store.Event, error) {
	return s.CreateUserAs(ctx, params, nil)
}

func (s *Service) CreateUserAs(ctx context.Context, params store.CreateUserParams, creator *store.UserKey) (store.User, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.User{}, store.Event{}, err
	}
	user, event, err := s.store.CreateUser(ctx, params)
	if err != nil {
		return user, event, err
	}
	s.eventSink.Publish(event)
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
	return user, event, err
}

func (s *Service) UpdateUser(ctx context.Context, params store.UpdateUserParams) (store.User, store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.User{}, store.Event{}, err
	}
	user, event, err := s.store.UpdateUser(ctx, params)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return user, event, err
}

func (s *Service) DeleteUser(ctx context.Context, key store.UserKey) (store.Event, error) {
	if err := s.allowWrite(ctx); err != nil {
		return store.Event{}, err
	}
	event, err := s.store.DeleteUser(ctx, key)
	if err == nil {
		s.eventSink.Publish(event)
	}
	return event, err
}

func (s *Service) GetUser(ctx context.Context, key store.UserKey) (store.User, error) {
	return s.store.GetUser(ctx, key)
}

func (s *Service) AuthenticateUser(ctx context.Context, key store.UserKey, password string) (store.User, error) {
	return s.store.AuthenticateUser(ctx, key, password)
}

func (s *Service) ListUsers(ctx context.Context) ([]store.User, error) {
	return s.store.ListUsers(ctx)
}

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

func (s *Service) SetTransientPacketReceiver(receiver TransientPacketReceiver) {
	s.transientRecvMu.Lock()
	defer s.transientRecvMu.Unlock()
	s.transientRecv = receiver
}

func (s *Service) SetLoggedInUserProvider(provider LoggedInUserProvider) {
	s.localUsers = provider
}

func (s *Service) RegisterLocalSession(session store.OnlineSession) {
	if s == nil || !session.SessionRef.Valid() || s.sessionRegistry == nil {
		return
	}
	s.sessionRegistry.RegisterLocalSession(session)
}

func (s *Service) UnregisterLocalSession(user store.UserKey, sessionRef store.SessionRef) {
	if s == nil || !sessionRef.Valid() || s.sessionRegistry == nil {
		return
	}
	s.sessionRegistry.UnregisterLocalSession(user, sessionRef)
}

func (s *Service) DispatchTransientPacket(ctx context.Context, recipient store.UserKey, sender store.UserKey, body []byte, mode store.DeliveryMode) (store.TransientPacket, error) {
	return s.DispatchTransientPacketTo(ctx, recipient, sender, body, mode, store.SessionRef{})
}

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

func (s *Service) deliverTransientPacket(packet store.TransientPacket) bool {
	s.transientRecvMu.RLock()
	receiver := s.transientRecv
	s.transientRecvMu.RUnlock()
	if receiver == nil {
		return false
	}
	return receiver.ReceiveTransientPacket(packet)
}

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
		Body:          append([]byte(nil), body...),
		DeliveryMode:  mode,
		TTLHops:       8,
		TargetSession: targetSession,
	}
}

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

func containsTargetSession(items []store.OnlineSession, target store.SessionRef) bool {
	for _, item := range items {
		if item.SessionRef == target {
			return true
		}
	}
	return false
}

func (s *Service) ListMessagesByUser(ctx context.Context, key store.UserKey, limit int) ([]store.Message, error) {
	return s.store.ListMessagesByUser(ctx, key, limit)
}

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

func (s *Service) ListUserAttachments(ctx context.Context, owner store.UserKey, attachmentType store.AttachmentType) ([]store.Attachment, error) {
	return s.store.ListUserAttachments(ctx, owner, attachmentType)
}

func (s *Service) IsChannelManager(ctx context.Context, channel, subject store.UserKey) (bool, error) {
	return s.store.IsChannelManager(ctx, channel, subject)
}

func (s *Service) IsChannelWriter(ctx context.Context, channel, subject store.UserKey) (bool, error) {
	return s.store.IsChannelWriter(ctx, channel, subject)
}

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

func (s *Service) ListChannelSubscriptions(ctx context.Context, key store.UserKey) ([]store.Subscription, error) {
	return s.store.ListChannelSubscriptions(ctx, key)
}

func (s *Service) IsSubscribedToChannel(ctx context.Context, subscriber, channel store.UserKey) (bool, error) {
	return s.store.IsSubscribedToChannel(ctx, subscriber, channel)
}

func (s *Service) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]store.Event, error) {
	return s.store.ListEvents(ctx, afterSequence, limit)
}

func (s *Service) LastEventSequence(ctx context.Context) (int64, error) {
	return s.store.LastEventSequence(ctx)
}

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

func (s *Service) ListBlockedUsers(ctx context.Context, owner store.UserKey) ([]store.BlacklistEntry, error) {
	return s.store.ListBlockedUsers(ctx, owner)
}

func (s *Service) IsMessageBlockedByBlacklist(ctx context.Context, owner, sender store.UserKey, createdAt clock.Timestamp) (bool, error) {
	return s.store.IsMessageHiddenByBlacklist(ctx, owner, sender, createdAt)
}

func (s *Service) IsBlockedByRecipient(ctx context.Context, recipient, sender store.UserKey) (bool, error) {
	return s.store.IsBlockedByRecipient(ctx, recipient, sender)
}

func (s *Service) BlacklistHitsTotal() uint64 {
	return s.blacklistHits.Load()
}

func (s *Service) RecordBlacklistHit() {
	s.recordBlacklistHit()
}

func (s *Service) allowWrite(ctx context.Context) error {
	if s.writeGate == nil {
		return nil
	}
	return s.writeGate.AllowWrite(ctx)
}

func (s *Service) recordBlacklistHit() {
	s.blacklistHits.Add(1)
}

func (s *Service) nextTransientPacketID() uint64 {
	return transientPacketIDNamespace | s.nextTransientID.Add(1)
}
