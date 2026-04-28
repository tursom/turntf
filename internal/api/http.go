package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/store"
)

const clientSessionShardCount = 256

type clientSessionShard struct {
	mu       sync.RWMutex
	sessions map[store.UserKey]map[string]*clientWSSession
}

type onlineUserState struct {
	Username     string
	SessionCount int
}

type HTTP struct {
	service          *Service
	mux              *http.ServeMux
	nodeID           int64
	signer           *auth.Signer
	tokenTTL         time.Duration
	sessionShards    [clientSessionShardCount]clientSessionShard
	persistentMu     sync.RWMutex
	persistent       map[*clientWSSession]struct{}
	persistentAdmin  map[*clientWSSession]struct{}
	onlineUsersMu    sync.RWMutex
	onlineUsers      map[store.UserKey]onlineUserState
	onlineUserCount  atomic.Int64
	sessionRegistry  OnlineSessionRegistry
	sessionSequence  atomic.Uint64
	targetRoleMu     sync.Mutex
	targetRoleCache  map[store.UserKey]clientRoleCacheEntry
	dispatcherMu     sync.Mutex
	dispatcherCancel context.CancelFunc
	closeOnce        sync.Once
}

type HTTPOptions struct {
	NodeID   int64
	Signer   *auth.Signer
	TokenTTL time.Duration
}

type createUserRequest struct {
	Username string          `json:"username"`
	Password string          `json:"password"`
	Profile  json.RawMessage `json:"profile,omitempty"`
	Role     string          `json:"role,omitempty"`
}

type updateUserRequest struct {
	Username *string          `json:"username,omitempty"`
	Password *string          `json:"password,omitempty"`
	Profile  *json.RawMessage `json:"profile,omitempty"`
	Role     *string          `json:"role,omitempty"`
}

type createMessageRequest struct {
	Body         []byte `json:"body"`
	DeliveryKind string `json:"delivery_kind,omitempty"`
	DeliveryMode string `json:"delivery_mode,omitempty"`
	SyncMode     string `json:"sync_mode,omitempty"`
}

type subscriptionRequest struct {
	ChannelNodeID int64 `json:"channel_node_id"`
	ChannelUserID int64 `json:"channel_user_id"`
}

type blacklistRequest struct {
	BlockedNodeID int64 `json:"blocked_node_id"`
	BlockedUserID int64 `json:"blocked_user_id"`
}

type attachmentRequest struct {
	ConfigJSON json.RawMessage `json:"config_json"`
}

type loginRequest struct {
	NodeID   int64  `json:"node_id"`
	UserID   int64  `json:"user_id"`
	Password string `json:"password"`
}

type requestPrincipal struct {
	User   store.User
	Claims auth.Claims
}

type deliveryKind string

const (
	deliveryKindPersistent deliveryKind = "persistent"
	deliveryKindTransient  deliveryKind = "transient"
)

func normalizeDeliveryKind(raw string) (deliveryKind, error) {
	switch deliveryKind(strings.TrimSpace(raw)) {
	case "", deliveryKindPersistent:
		return deliveryKindPersistent, nil
	case deliveryKindTransient:
		return deliveryKindTransient, nil
	default:
		return "", fmt.Errorf("%w: unsupported delivery kind %q", store.ErrInvalidInput, raw)
	}
}

func NewHTTP(service *Service, opts ...HTTPOptions) *HTTP {
	var resolved HTTPOptions
	if len(opts) > 0 {
		resolved = opts[0]
	}
	tokenTTL := resolved.TokenTTL
	if tokenTTL <= 0 {
		tokenTTL = 24 * time.Hour
	}
	h := &HTTP{
		service:         service,
		mux:             http.NewServeMux(),
		nodeID:          resolved.NodeID,
		signer:          resolved.Signer,
		tokenTTL:        tokenTTL,
		persistent:      make(map[*clientWSSession]struct{}),
		persistentAdmin: make(map[*clientWSSession]struct{}),
		onlineUsers:     make(map[store.UserKey]onlineUserState),
		targetRoleCache: make(map[store.UserKey]clientRoleCacheEntry),
	}
	if service != nil && service.sessionRegistry != nil {
		h.sessionRegistry = service.sessionRegistry
	}
	for idx := range h.sessionShards {
		h.sessionShards[idx].sessions = make(map[store.UserKey]map[string]*clientWSSession)
	}
	if service != nil {
		service.SetTransientPacketReceiver(h)
		service.SetLoggedInUserProvider(h)
	}
	h.routes()
	return h
}

func (h *HTTP) Handler() http.Handler {
	return h.mux
}

func (h *HTTP) Close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		if h.dispatcherCancel != nil {
			h.dispatcherCancel()
		}
	})
	return nil
}

func (h *HTTP) routes() {
	h.mux.HandleFunc("GET /healthz", h.handleHealth)
	h.mux.HandleFunc("GET "+clientWSPath, h.handleClientWebSocket)
	h.mux.HandleFunc("GET "+clientRealtimeWSPath, h.handleRealtimeWebSocket)
	h.mux.HandleFunc("POST /auth/login", h.handleLogin)
	h.mux.HandleFunc("POST /users", h.handleCreateUser)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}", h.handleGetUser)
	h.mux.HandleFunc("PATCH /nodes/{node_id}/users/{user_id}", h.handleUpdateUser)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}", h.handleDeleteUser)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/messages", h.handleListMessagesByUser)
	h.mux.HandleFunc("POST /nodes/{node_id}/users/{user_id}/messages", h.handleCreateMessage)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/attachments", h.handleListUserAttachments)
	h.mux.HandleFunc("PUT /nodes/{node_id}/users/{user_id}/attachments/{attachment_type}/{subject_node_id}/{subject_user_id}", h.handleUpsertUserAttachment)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}/attachments/{attachment_type}/{subject_node_id}/{subject_user_id}", h.handleDeleteUserAttachment)
	h.mux.HandleFunc("GET /events", h.handleListEvents)
	h.mux.HandleFunc("GET /cluster/nodes", h.handleClusterNodes)
	h.mux.HandleFunc("GET /cluster/nodes/{node_id}/logged-in-users", h.handleNodeLoggedInUsers)
	h.mux.HandleFunc("GET /ops/status", h.handleOpsStatus)
	h.mux.HandleFunc("GET /metrics", h.handleMetrics)
}

func (h *HTTP) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *HTTP) handleLogin(w http.ResponseWriter, r *http.Request) {
	if h.signer == nil {
		writeError(w, http.StatusServiceUnavailable, "authentication is not configured")
		return
	}

	var req loginRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	key := store.UserKey{NodeID: req.NodeID, UserID: req.UserID}
	user, err := h.service.AuthenticateUser(r.Context(), key, req.Password)
	if err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeStoreError(w, err)
			return
		}
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}

	now := time.Now().UTC()
	expiresAt := now.Add(h.tokenTTL)
	token, err := h.signer.Sign(auth.Claims{
		Subject:   formatUserSubject(user.Key()),
		Issuer:    strconv.FormatInt(h.nodeID, 10),
		IssuedAt:  now.Unix(),
		ExpiresAt: expiresAt.Unix(),
		Metadata: map[string]string{
			"role": user.Role,
		},
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to sign token")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"expires_at": expiresAt.Format(time.RFC3339),
		"user":       userResponseFromStore(user),
	})
}

func (h *HTTP) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAdmin(w, r)
	if !ok {
		return
	}

	var req createUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	profile, err := normalizeJSONValue(req.Profile, "{}")
	if err != nil {
		writeError(w, http.StatusBadRequest, "profile must be valid JSON")
		return
	}
	passwordHash := ""
	if strings.TrimSpace(req.Role) != store.RoleChannel {
		var err error
		passwordHash, err = auth.HashPassword(req.Password)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	var creator *store.UserKey
	if principal != nil {
		key := principal.User.Key()
		creator = &key
	}
	user, _, err := h.service.CreateUserAs(r.Context(), store.CreateUserParams{
		Username:     req.Username,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	}, creator)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateTargetRoleCache(user.Key())

	writeJSON(w, http.StatusCreated, userResponseFromStore(user))
}

func (h *HTTP) handleGetUser(w http.ResponseWriter, r *http.Request) {
	key, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, key); !ok {
		return
	}

	user, err := h.service.GetUser(r.Context(), key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateTargetRoleCache(user.Key())

	writeJSON(w, http.StatusOK, userResponseFromStore(user))
}

func (h *HTTP) handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	key, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	target, err := h.service.GetUser(r.Context(), key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := h.authorizeManageUser(r.Context(), principal, target, true); err != nil {
		writeStoreError(w, err)
		return
	}

	var req updateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var profile *string
	var passwordHash *string
	if req.Profile != nil {
		normalized, err := normalizeJSONValue(*req.Profile, "{}")
		if err != nil {
			writeError(w, http.StatusBadRequest, "profile must be valid JSON")
			return
		}
		profile = &normalized
	}
	if req.Password != nil {
		hashed, err := auth.HashPassword(*req.Password)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		passwordHash = &hashed
	}

	if principal != nil && !isAdminRole(principal.User.Role) && target.Role == store.RoleChannel {
		if req.Password != nil || req.Role != nil {
			writeStoreError(w, store.ErrForbidden)
			return
		}
	}

	user, _, err := h.service.UpdateUser(r.Context(), store.UpdateUserParams{
		Key:          key,
		Username:     req.Username,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, userResponseFromStore(user))
}

func (h *HTTP) handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	key, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	target, err := h.service.GetUser(r.Context(), key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := h.authorizeManageUser(r.Context(), principal, target, false); err != nil {
		writeStoreError(w, err)
		return
	}

	if _, err := h.service.DeleteUser(r.Context(), key); err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateTargetRoleCache(key)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "deleted",
		"node_id": key.NodeID,
		"user_id": key.UserID,
	})
}

func (h *HTTP) handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	key, pathOK := parsePathUserKey(w, r)
	if !pathOK {
		return
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}

	var req createMessageRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.authorizeCreateMessage(r.Context(), principal, key); err != nil {
		writeStoreError(w, err)
		return
	}
	sender, err := messageSenderFromPrincipal(principal)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	deliveryKind, err := normalizeDeliveryKind(req.DeliveryKind)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if deliveryKind == deliveryKindTransient {
		if strings.TrimSpace(req.SyncMode) != "" {
			writeStoreError(w, fmt.Errorf("%w: sync_mode is only allowed for persistent messages", store.ErrInvalidInput))
			return
		}
		mode, err := store.NormalizeDeliveryMode(req.DeliveryMode)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		packet, err := h.service.DispatchTransientPacket(r.Context(), key, sender, req.Body, mode)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, transientPacketAcceptedResponse(packet))
		return
	}
	if strings.TrimSpace(req.DeliveryMode) != "" {
		writeStoreError(w, fmt.Errorf("%w: delivery_mode is only allowed for transient messages", store.ErrInvalidInput))
		return
	}
	syncMode, err := store.NormalizePebbleMessageSyncMode(req.SyncMode)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	message, _, err := h.service.CreateMessage(r.Context(), store.CreateMessageParams{
		UserKey:               key,
		Sender:                sender,
		Body:                  req.Body,
		PebbleMessageSyncMode: syncMode,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, messageResponseFromStore(message))
}

func (h *HTTP) authorizeCreateMessage(ctx context.Context, principal *requestPrincipal, key store.UserKey) error {
	if principal == nil || isAdminRole(principal.User.Role) || principal.User.Key() == key {
		return nil
	}
	target, err := h.service.GetUser(ctx, key)
	if err != nil {
		return err
	}
	if target.CanLogin() {
		return nil
	}
	if target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	allowed, err := h.service.IsChannelWriter(ctx, key, principal.User.Key())
	if err != nil {
		return err
	}
	if !allowed {
		return store.ErrForbidden
	}
	return nil
}

func (h *HTTP) handleListMessagesByUser(w http.ResponseWriter, r *http.Request) {
	key, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	target, err := h.service.GetUser(r.Context(), key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if target.CanLogin() {
		if _, ok := h.requireSelfOrAdmin(w, r, key); !ok {
			return
		}
	} else if _, ok := h.requireAdmin(w, r); !ok {
		return
	}

	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		limit = parsed
	}

	messages, err := h.service.ListMessagesByUser(r.Context(), key, limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	items := make([]messageResponse, 0, len(messages))
	for _, message := range messages {
		items = append(items, messageResponseFromStore(message))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

func (h *HTTP) handleListUserAttachments(w http.ResponseWriter, r *http.Request) {
	owner, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	attachmentType, err := attachmentTypeFromQuery(r)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := h.authorizeAttachmentOwnerAccess(r.Context(), principal, owner, attachmentType); err != nil {
		writeStoreError(w, err)
		return
	}
	attachments, err := h.service.ListUserAttachments(r.Context(), owner, attachmentType)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	items := make([]attachmentResponse, 0, len(attachments))
	for _, attachment := range attachments {
		items = append(items, attachmentResponseFromStore(attachment))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

func (h *HTTP) handleUpsertUserAttachment(w http.ResponseWriter, r *http.Request) {
	owner, attachmentType, subject, principal, ok := h.parseAttachmentWriteRequest(w, r)
	if !ok {
		return
	}
	if err := h.authorizeAttachmentAccess(r.Context(), principal, owner, attachmentType); err != nil {
		writeStoreError(w, err)
		return
	}

	var req attachmentRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	attachment, _, err := h.service.UpsertAttachment(r.Context(), store.UpsertAttachmentParams{
		Owner:      owner,
		Subject:    subject,
		Type:       attachmentType,
		ConfigJSON: string(req.ConfigJSON),
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateAttachmentCaches(attachment)
	writeJSON(w, http.StatusCreated, attachmentResponseFromStore(attachment))
}

func (h *HTTP) handleDeleteUserAttachment(w http.ResponseWriter, r *http.Request) {
	owner, attachmentType, subject, principal, ok := h.parseAttachmentWriteRequest(w, r)
	if !ok {
		return
	}
	if err := h.authorizeAttachmentAccess(r.Context(), principal, owner, attachmentType); err != nil {
		writeStoreError(w, err)
		return
	}
	attachment, _, err := h.service.DeleteAttachment(r.Context(), store.DeleteAttachmentParams{
		Owner:   owner,
		Subject: subject,
		Type:    attachmentType,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateAttachmentCaches(attachment)
	writeJSON(w, http.StatusOK, attachmentResponseFromStore(attachment))
}

func (h *HTTP) handleSubscribeChannel(w http.ResponseWriter, r *http.Request) {
	subscriber, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, subscriber); !ok {
		return
	}

	var req subscriptionRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	channel := store.UserKey{NodeID: req.ChannelNodeID, UserID: req.ChannelUserID}
	subscription, _, err := h.service.SubscribeChannel(r.Context(), store.ChannelSubscriptionParams{
		Subscriber: subscriber,
		Channel:    channel,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateUserChannelSubscriptionCache(subscriber, channel)
	writeJSON(w, http.StatusCreated, subscriptionResponseFromStore(subscription))
}

func (h *HTTP) handleUnsubscribeChannel(w http.ResponseWriter, r *http.Request) {
	subscriber, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, subscriber); !ok {
		return
	}
	channelNodeID, ok := parsePositivePathInt(w, r, "channel_node_id")
	if !ok {
		return
	}
	channelUserID, ok := parsePositivePathInt(w, r, "channel_user_id")
	if !ok {
		return
	}
	subscription, _, err := h.service.UnsubscribeChannel(r.Context(), store.ChannelSubscriptionParams{
		Subscriber: subscriber,
		Channel:    store.UserKey{NodeID: channelNodeID, UserID: channelUserID},
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateUserChannelSubscriptionCache(subscriber, subscription.Channel)
	writeJSON(w, http.StatusOK, subscriptionResponseFromStore(subscription))
}

func (h *HTTP) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subscriber, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, subscriber); !ok {
		return
	}
	subscriptions, err := h.service.ListChannelSubscriptions(r.Context(), subscriber)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	items := make([]subscriptionResponse, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		items = append(items, subscriptionResponseFromStore(subscription))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

func (h *HTTP) handleBlockUser(w http.ResponseWriter, r *http.Request) {
	owner, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, owner); !ok {
		return
	}

	var req blacklistRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	entry, _, err := h.service.BlockUser(r.Context(), store.BlacklistParams{
		Owner:   owner,
		Blocked: store.UserKey{NodeID: req.BlockedNodeID, UserID: req.BlockedUserID},
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateUserBlacklistCache(owner, entry.Blocked)
	writeJSON(w, http.StatusCreated, blacklistResponseFromStore(entry))
}

func (h *HTTP) handleUnblockUser(w http.ResponseWriter, r *http.Request) {
	owner, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, owner); !ok {
		return
	}
	blockedNodeID, ok := parsePositivePathInt(w, r, "blocked_node_id")
	if !ok {
		return
	}
	blockedUserID, ok := parsePositivePathInt(w, r, "blocked_user_id")
	if !ok {
		return
	}
	entry, _, err := h.service.UnblockUser(r.Context(), store.BlacklistParams{
		Owner:   owner,
		Blocked: store.UserKey{NodeID: blockedNodeID, UserID: blockedUserID},
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateUserBlacklistCache(owner, entry.Blocked)
	writeJSON(w, http.StatusOK, blacklistResponseFromStore(entry))
}

func (h *HTTP) handleListBlockedUsers(w http.ResponseWriter, r *http.Request) {
	owner, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, owner); !ok {
		return
	}
	entries, err := h.service.ListBlockedUsers(r.Context(), owner)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	items := make([]blacklistResponse, 0, len(entries))
	for _, entry := range entries {
		items = append(items, blacklistResponseFromStore(entry))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

func (h *HTTP) handleListEvents(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.requireAdmin(w, r); !ok {
		return
	}

	after := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("after")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "after must be an integer")
			return
		}
		after = parsed
	}

	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "limit must be an integer")
			return
		}
		limit = parsed
	}

	events, err := h.service.ListEvents(r.Context(), after, limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	items := make([]eventResponse, 0, len(events))
	for _, event := range events {
		items = append(items, eventResponseFromStore(event))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

func (h *HTTP) handleOpsStatus(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.requireAdmin(w, r); !ok {
		return
	}

	status, err := h.service.OperationsStatus(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *HTTP) handleClusterNodes(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.requireAuthenticated(w, r); !ok {
		return
	}

	nodes, err := h.service.ClusterNodes(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

func (h *HTTP) handleNodeLoggedInUsers(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.requireAuthenticated(w, r); !ok {
		return
	}
	nodeID, ok := parsePositivePathInt(w, r, "node_id")
	if !ok {
		return
	}
	users, err := h.service.ListNodeLoggedInUsers(r.Context(), nodeID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, users)
}

func (h *HTTP) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if _, ok := h.requireAdmin(w, r); !ok {
		return
	}

	metrics, err := h.service.Metrics(r.Context())
	if err != nil {
		writeStoreError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(metrics))
}

type userResponse struct {
	NodeID         int64           `json:"node_id"`
	UserID         int64           `json:"user_id"`
	ID             int64           `json:"id,omitempty"`
	Username       string          `json:"username"`
	Profile        json.RawMessage `json:"profile"`
	Role           string          `json:"role"`
	SystemReserved bool            `json:"system_reserved"`
	CreatedAt      string          `json:"created_at"`
	UpdatedAt      string          `json:"updated_at"`
	OriginNodeID   int64           `json:"origin_node_id"`
}

type messageResponse struct {
	Recipient store.UserKey `json:"recipient"`
	NodeID    int64         `json:"node_id"`
	Seq       int64         `json:"seq"`
	Sender    store.UserKey `json:"sender"`
	Body      []byte        `json:"body"`
	CreatedAt string        `json:"created_at"`
}

type transientPacketResponse struct {
	Mode         string        `json:"mode"`
	PacketID     uint64        `json:"packet_id"`
	SourceNodeID int64         `json:"source_node_id"`
	TargetNodeID int64         `json:"target_node_id"`
	Recipient    store.UserKey `json:"recipient"`
	DeliveryMode string        `json:"delivery_mode"`
}

type attachmentResponse struct {
	Owner          store.UserKey   `json:"owner"`
	Subject        store.UserKey   `json:"subject"`
	AttachmentType string          `json:"attachment_type"`
	ConfigJSON     json.RawMessage `json:"config_json"`
	AttachedAt     string          `json:"attached_at"`
	DeletedAt      string          `json:"deleted_at,omitempty"`
	OriginNodeID   int64           `json:"origin_node_id"`
}

type subscriptionResponse struct {
	Subscriber   store.UserKey `json:"subscriber"`
	Channel      store.UserKey `json:"channel"`
	SubscribedAt string        `json:"subscribed_at"`
	DeletedAt    string        `json:"deleted_at,omitempty"`
	OriginNodeID int64         `json:"origin_node_id"`
}

type blacklistResponse struct {
	Owner        store.UserKey `json:"owner"`
	Blocked      store.UserKey `json:"blocked"`
	BlockedAt    string        `json:"blocked_at"`
	DeletedAt    string        `json:"deleted_at,omitempty"`
	OriginNodeID int64         `json:"origin_node_id"`
}

type eventResponse struct {
	Sequence        int64           `json:"sequence"`
	EventID         int64           `json:"event_id"`
	EventType       store.EventType `json:"event_type"`
	Aggregate       string          `json:"aggregate"`
	AggregateNodeID int64           `json:"aggregate_node_id"`
	AggregateID     int64           `json:"aggregate_id"`
	HLC             string          `json:"hlc"`
	OriginNodeID    int64           `json:"origin_node_id"`
	Event           any             `json:"event"`
}

func userResponseFromStore(user store.User) userResponse {
	return userResponse{
		NodeID:         user.NodeID,
		UserID:         user.ID,
		ID:             user.ID,
		Username:       user.Username,
		Profile:        json.RawMessage(user.Profile),
		Role:           user.Role,
		SystemReserved: user.SystemReserved,
		CreatedAt:      user.CreatedAt.String(),
		UpdatedAt:      user.UpdatedAt.String(),
		OriginNodeID:   user.OriginNodeID,
	}
}

func messageResponseFromStore(message store.Message) messageResponse {
	return messageResponse{
		Recipient: message.Recipient,
		NodeID:    message.NodeID,
		Seq:       message.Seq,
		Sender:    message.Sender,
		Body:      message.Body,
		CreatedAt: message.CreatedAt.String(),
	}
}

func transientPacketAcceptedResponse(packet store.TransientPacket) transientPacketResponse {
	return transientPacketResponse{
		Mode:         "transient",
		PacketID:     packet.PacketID,
		SourceNodeID: packet.SourceNodeID,
		TargetNodeID: packet.TargetNodeID,
		Recipient:    packet.Recipient,
		DeliveryMode: string(packet.DeliveryMode),
	}
}

func attachmentResponseFromStore(attachment store.Attachment) attachmentResponse {
	response := attachmentResponse{
		Owner:          attachment.Owner,
		Subject:        attachment.Subject,
		AttachmentType: string(attachment.Type),
		ConfigJSON:     json.RawMessage(attachment.ConfigJSON),
		AttachedAt:     attachment.AttachedAt.String(),
		OriginNodeID:   attachment.OriginNodeID,
	}
	if attachment.DeletedAt != nil {
		response.DeletedAt = attachment.DeletedAt.String()
	}
	return response
}

func clientSessionShardIndex(key store.UserKey) int {
	hash := uint64(key.NodeID)*11400714819323198485 ^ (uint64(key.UserID) + 0x9e3779b97f4a7c15)
	return int(hash % clientSessionShardCount)
}

func (h *HTTP) sessionShard(key store.UserKey) *clientSessionShard {
	if h == nil {
		return nil
	}
	return &h.sessionShards[clientSessionShardIndex(key)]
}

func (h *HTTP) registerClientSession(key store.UserKey, sess *clientWSSession) {
	if h == nil || sess == nil {
		return
	}
	shard := h.sessionShard(key)
	if shard == nil {
		return
	}
	shard.mu.Lock()
	bucket := shard.sessions[key]
	if bucket == nil {
		bucket = make(map[string]*clientWSSession)
		shard.sessions[key] = bucket
	}
	if sess.sessionRef.SessionID == "" {
		shard.mu.Unlock()
		return
	}
	if _, exists := bucket[sess.sessionRef.SessionID]; exists {
		shard.mu.Unlock()
		return
	}
	bucket[sess.sessionRef.SessionID] = sess
	shard.mu.Unlock()

	h.onlineUsersMu.Lock()
	state := h.onlineUsers[key]
	if state.SessionCount == 0 {
		h.onlineUserCount.Add(1)
	}
	if sess.principal != nil && sess.principal.User.Username != "" {
		state.Username = sess.principal.User.Username
	}
	state.SessionCount++
	h.onlineUsers[key] = state
	h.onlineUsersMu.Unlock()

	if h.service != nil {
		h.service.RegisterLocalSession(sess.onlineSession())
	}
	if sess.requiresPersistentPush() {
		h.registerPersistentSession(sess)
	}
}

func (h *HTTP) unregisterClientSession(key store.UserKey, sess *clientWSSession) {
	if h == nil || sess == nil {
		return
	}
	shard := h.sessionShard(key)
	if shard == nil {
		return
	}
	shard.mu.Lock()
	bucket := shard.sessions[key]
	if bucket == nil {
		shard.mu.Unlock()
		return
	}
	if _, exists := bucket[sess.sessionRef.SessionID]; !exists {
		shard.mu.Unlock()
		return
	}
	delete(bucket, sess.sessionRef.SessionID)
	if len(bucket) == 0 {
		delete(shard.sessions, key)
	}
	shard.mu.Unlock()

	h.onlineUsersMu.Lock()
	state, ok := h.onlineUsers[key]
	if ok {
		if state.SessionCount <= 1 {
			delete(h.onlineUsers, key)
			h.onlineUserCount.Add(-1)
		} else {
			state.SessionCount--
			h.onlineUsers[key] = state
		}
	}
	h.onlineUsersMu.Unlock()

	if h.service != nil {
		h.service.UnregisterLocalSession(key, sess.sessionRef)
	}
	if sess.requiresPersistentPush() {
		h.unregisterPersistentSession(sess)
	}
}

func (h *HTTP) ReceiveTransientPacket(packet store.TransientPacket) bool {
	blocked, err := h.service.IsBlockedByRecipient(context.Background(), packet.Recipient, packet.Sender)
	if err == nil && blocked {
		h.service.RecordBlacklistHit()
		return false
	}
	shard := h.sessionShard(packet.Recipient)
	if shard == nil {
		return false
	}
	if packet.TargetSession.Valid() {
		if packet.TargetSession.ServingNodeID != h.nodeID {
			return false
		}
		shard.mu.RLock()
		bucket := shard.sessions[packet.Recipient]
		sess := bucket[packet.TargetSession.SessionID]
		shard.mu.RUnlock()
		if sess == nil {
			return false
		}
		return sess.pushPacket(packet) == nil
	}
	shard.mu.RLock()
	bucket := shard.sessions[packet.Recipient]
	sessions := make([]*clientWSSession, 0, len(bucket))
	for _, sess := range bucket {
		sessions = append(sessions, sess)
	}
	shard.mu.RUnlock()
	if len(sessions) == 0 {
		return false
	}
	delivered := false
	for _, sess := range sessions {
		if err := sess.pushPacket(packet); err == nil {
			delivered = true
		}
	}
	return delivered
}

func (h *HTTP) ListLocalUserSessions(_ context.Context, key store.UserKey) ([]store.OnlineSession, error) {
	if h == nil {
		return nil, nil
	}
	shard := h.sessionShard(key)
	if shard == nil {
		return nil, nil
	}
	shard.mu.RLock()
	bucket := shard.sessions[key]
	items := make([]store.OnlineSession, 0, len(bucket))
	for _, sess := range bucket {
		items = append(items, sess.onlineSession())
	}
	shard.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i].SessionRef.SessionID < items[j].SessionRef.SessionID
	})
	return items, nil
}

func (h *HTTP) newSessionRef() store.SessionRef {
	if h == nil {
		return store.SessionRef{}
	}
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err == nil {
		return store.SessionRef{
			ServingNodeID: h.nodeID,
			SessionID:     hex.EncodeToString(raw),
		}
	}
	return store.SessionRef{
		ServingNodeID: h.nodeID,
		SessionID:     fmt.Sprintf("%016x", h.sessionSequence.Add(1)),
	}
}

func (h *HTTP) ListLoggedInUsers(context.Context) ([]app.LoggedInUserSummary, error) {
	if h == nil {
		return nil, nil
	}
	h.onlineUsersMu.RLock()
	users := make([]app.LoggedInUserSummary, 0, int(h.onlineUserCount.Load()))
	for key, state := range h.onlineUsers {
		users = append(users, app.LoggedInUserSummary{
			NodeID:   key.NodeID,
			UserID:   key.UserID,
			Username: state.Username,
		})
	}
	h.onlineUsersMu.RUnlock()

	sort.Slice(users, func(i, j int) bool {
		if users[i].NodeID != users[j].NodeID {
			return users[i].NodeID < users[j].NodeID
		}
		return users[i].UserID < users[j].UserID
	})
	return users, nil
}

func subscriptionResponseFromStore(subscription store.Subscription) subscriptionResponse {
	response := subscriptionResponse{
		Subscriber:   subscription.Subscriber,
		Channel:      subscription.Channel,
		SubscribedAt: subscription.SubscribedAt.String(),
		OriginNodeID: subscription.OriginNodeID,
	}
	if subscription.DeletedAt != nil {
		response.DeletedAt = subscription.DeletedAt.String()
	}
	return response
}

func blacklistResponseFromStore(entry store.BlacklistEntry) blacklistResponse {
	response := blacklistResponse{
		Owner:        entry.Owner,
		Blocked:      entry.Blocked,
		BlockedAt:    entry.BlockedAt.String(),
		OriginNodeID: entry.OriginNodeID,
	}
	if entry.DeletedAt != nil {
		response.DeletedAt = entry.DeletedAt.String()
	}
	return response
}

func eventResponseFromStore(event store.Event) eventResponse {
	return eventResponse{
		Sequence:        event.Sequence,
		EventID:         event.EventID,
		EventType:       event.EventType,
		Aggregate:       event.Aggregate,
		AggregateNodeID: event.AggregateNodeID,
		AggregateID:     event.AggregateID,
		HLC:             event.HLC.String(),
		OriginNodeID:    event.OriginNodeID,
		Event:           event.Body,
	}
}

func decodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain a single json object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, app.ErrClockNotSynchronized):
		writeError(w, http.StatusServiceUnavailable, app.ErrClockNotSynchronized.Error())
	case errors.Is(err, app.ErrServiceUnavailable):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, store.ErrBlockedByBlacklist):
		writeError(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, store.ErrForbidden):
		writeError(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, store.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, "resource conflict")
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "resource not found")
	default:
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}

func parsePathUserKey(w http.ResponseWriter, r *http.Request) (store.UserKey, bool) {
	nodeID, ok := parsePositivePathInt(w, r, "node_id")
	if !ok {
		return store.UserKey{}, false
	}
	userID, ok := parsePositivePathInt(w, r, "user_id")
	if !ok {
		return store.UserKey{}, false
	}
	return store.UserKey{NodeID: nodeID, UserID: userID}, true
}

func parsePositivePathInt(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := strings.TrimSpace(r.PathValue(name))
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		writeError(w, http.StatusBadRequest, name+" must be a positive integer")
		return 0, false
	}
	return value, true
}

func normalizeJSONValue(raw json.RawMessage, defaultValue string) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return defaultValue, nil
	}
	if !json.Valid(trimmed) {
		return "", fmt.Errorf("invalid json payload")
	}
	return string(trimmed), nil
}

func attachmentTypeFromQuery(r *http.Request) (store.AttachmentType, error) {
	if r == nil {
		return "", nil
	}
	raw := strings.TrimSpace(r.URL.Query().Get("attachment_type"))
	if raw == "" {
		return "", nil
	}
	return store.NormalizeAttachmentType(raw)
}

func attachmentTypeFromPath(w http.ResponseWriter, r *http.Request) (store.AttachmentType, bool) {
	raw := strings.TrimSpace(r.PathValue("attachment_type"))
	attachmentType, err := store.NormalizeAttachmentType(raw)
	if err != nil {
		writeStoreError(w, err)
		return "", false
	}
	return attachmentType, true
}

func (h *HTTP) parseAttachmentWriteRequest(w http.ResponseWriter, r *http.Request) (store.UserKey, store.AttachmentType, store.UserKey, *requestPrincipal, bool) {
	owner, ok := parsePathUserKey(w, r)
	if !ok {
		return store.UserKey{}, "", store.UserKey{}, nil, false
	}
	attachmentType, ok := attachmentTypeFromPath(w, r)
	if !ok {
		return store.UserKey{}, "", store.UserKey{}, nil, false
	}
	subjectNodeID, ok := parsePositivePathInt(w, r, "subject_node_id")
	if !ok {
		return store.UserKey{}, "", store.UserKey{}, nil, false
	}
	subjectUserID, ok := parsePositivePathInt(w, r, "subject_user_id")
	if !ok {
		return store.UserKey{}, "", store.UserKey{}, nil, false
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return store.UserKey{}, "", store.UserKey{}, nil, false
	}
	return owner, attachmentType, store.UserKey{NodeID: subjectNodeID, UserID: subjectUserID}, principal, true
}

func (h *HTTP) authorizeAttachmentAccess(ctx context.Context, principal *requestPrincipal, owner store.UserKey, attachmentType store.AttachmentType) error {
	if principal == nil || isAdminRole(principal.User.Role) {
		return nil
	}
	switch attachmentType {
	case store.AttachmentTypeChannelManager, store.AttachmentTypeChannelWriter:
		allowed, err := h.service.IsChannelManager(ctx, owner, principal.User.Key())
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}
		return store.ErrForbidden
	case store.AttachmentTypeChannelSubscription, store.AttachmentTypeUserBlacklist:
		if principal.User.Key() == owner {
			return nil
		}
		return store.ErrForbidden
	default:
		return store.ErrInvalidInput
	}
}

func (h *HTTP) authorizeAttachmentOwnerAccess(ctx context.Context, principal *requestPrincipal, owner store.UserKey, attachmentType store.AttachmentType) error {
	if principal == nil || isAdminRole(principal.User.Role) {
		return nil
	}
	if attachmentType != "" {
		return h.authorizeAttachmentAccess(ctx, principal, owner, attachmentType)
	}
	ownerUser, err := h.service.GetUser(ctx, owner)
	if err != nil {
		return err
	}
	if ownerUser.Role == store.RoleChannel {
		allowed, err := h.service.IsChannelManager(ctx, owner, principal.User.Key())
		if err != nil {
			return err
		}
		if allowed {
			return nil
		}
		return store.ErrForbidden
	}
	if principal.User.Key() == owner {
		return nil
	}
	return store.ErrForbidden
}

func (h *HTTP) authorizeManageUser(ctx context.Context, principal *requestPrincipal, target store.User, update bool) error {
	if principal == nil || isAdminRole(principal.User.Role) {
		return nil
	}
	if target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	allowed, err := h.service.IsChannelManager(ctx, target.Key(), principal.User.Key())
	if err != nil {
		return err
	}
	if !allowed {
		return store.ErrForbidden
	}
	return nil
}

func (h *HTTP) invalidateAttachmentCaches(attachment store.Attachment) {
	switch attachment.Type {
	case store.AttachmentTypeChannelSubscription:
		h.invalidateUserChannelSubscriptionCache(attachment.Owner, attachment.Subject)
	case store.AttachmentTypeUserBlacklist:
		h.invalidateUserBlacklistCache(attachment.Owner, attachment.Subject)
	}
}

func (h *HTTP) requireAuthenticated(w http.ResponseWriter, r *http.Request) (*requestPrincipal, bool) {
	if h.signer == nil {
		return nil, true
	}
	principal, err := h.authenticateRequest(r.Context(), r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return nil, false
	}
	return principal, true
}

func (h *HTTP) requireAdmin(w http.ResponseWriter, r *http.Request) (*requestPrincipal, bool) {
	if h.signer == nil {
		return nil, true
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return nil, false
	}
	if !isAdminRole(principal.User.Role) {
		writeError(w, http.StatusForbidden, "forbidden")
		return nil, false
	}
	return principal, true
}

func (h *HTTP) requireSelfOrAdmin(w http.ResponseWriter, r *http.Request, key store.UserKey) (*requestPrincipal, bool) {
	if h.signer == nil {
		return nil, true
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return nil, false
	}
	if isAdminRole(principal.User.Role) || principal.User.Key() == key {
		return principal, true
	}
	writeError(w, http.StatusForbidden, "forbidden")
	return nil, false
}

func (h *HTTP) authenticateRequest(ctx context.Context, r *http.Request) (*requestPrincipal, error) {
	if h.signer == nil {
		return nil, errors.New("auth disabled")
	}
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(header, "Bearer ") {
		return nil, errors.New("missing bearer token")
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	if token == "" {
		return nil, errors.New("missing bearer token")
	}
	claims, err := h.signer.Verify(token)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC().Unix()
	if claims.ExpiresAt <= 0 || now >= claims.ExpiresAt {
		return nil, errors.New("token expired")
	}
	key, err := parseUserSubject(claims.Subject)
	if err != nil {
		return nil, errors.New("invalid subject")
	}
	user, err := h.service.GetUser(ctx, key)
	if err != nil {
		return nil, err
	}
	return &requestPrincipal{User: user, Claims: claims}, nil
}

func isAdminRole(role string) bool {
	return role == store.RoleSuperAdmin || role == store.RoleAdmin
}

func messageSenderFromPrincipal(principal *requestPrincipal) (store.UserKey, error) {
	if principal == nil {
		return store.UserKey{}, fmt.Errorf("%w: authentication is required to derive sender", store.ErrForbidden)
	}
	return principal.User.Key(), nil
}

func formatUserSubject(key store.UserKey) string {
	return strconv.FormatInt(key.NodeID, 10) + ":" + strconv.FormatInt(key.UserID, 10)
}

func parseUserSubject(subject string) (store.UserKey, error) {
	parts := strings.Split(strings.TrimSpace(subject), ":")
	if len(parts) != 2 {
		return store.UserKey{}, fmt.Errorf("invalid subject")
	}
	nodeID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return store.UserKey{}, err
	}
	userID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return store.UserKey{}, err
	}
	key := store.UserKey{NodeID: nodeID, UserID: userID}
	if err := key.Validate(); err != nil {
		return store.UserKey{}, err
	}
	return key, nil
}
