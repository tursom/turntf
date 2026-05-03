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
	"github.com/tursom/turntf/internal/permission"
	"github.com/tursom/turntf/internal/store"
)

// clientSessionShardCount 会话分片数量（必须是 2 的幂），用于减少并发竞争。
const clientSessionShardCount = 256

// clientSessionBucket 同一用户的所有会话集合，按 sessionID 索引，并持有快照供高效遍历。
type clientSessionBucket struct {
	bySessionID map[string]*clientWSSession
	snapshot    []*clientWSSession
}

// clientSessionShard 一个会话分片，受独立互斥锁保护以降低锁竞争。
type clientSessionShard struct {
	mu       sync.RWMutex
	sessions map[store.UserKey]*clientSessionBucket
}

// onlineUserState 在线用户的聚合状态（用于 /cluster/nodes/:id/logged-in-users）。
type onlineUserState struct {
	Username     string
	LoginName    string
	SessionCount int
}

// HTTP 是 REST API 的核心处理器，管理 HTTP 路由、JWT 认证、客户端会话分片、
// 持久化事件分发和在线用户统计。它同时实现了 TransientPacketReceiver 和
// LoggedInUserProvider 接口，桥接 Service 层与集群 mesh 层。
type HTTP struct {
	service          *Service
	authorizer       *permission.Authorizer
	mux              *http.ServeMux
	nodeID           int64
	signer           *auth.Signer                                // JWT 签名器，nil 表示禁用认证
	tokenTTL         time.Duration                               // JWT 有效期
	sessionShards    [clientSessionShardCount]clientSessionShard // 分片会话存储
	persistentMu     sync.RWMutex
	persistent       map[*clientWSSession]struct{} // 需要持久化推送的会话集合
	persistentAdmin  map[*clientWSSession]struct{} // 管理员会话（接收所有消息）
	onlineUsersMu    sync.RWMutex
	onlineUsers      map[store.UserKey]onlineUserState // 在线用户聚合状态
	onlineUserCount  atomic.Int64
	sessionRegistry  OnlineSessionRegistry // 集群会话注册器
	sessionSequence  atomic.Uint64         // 会话 ID 自增序列
	targetRoleMu     sync.Mutex
	targetRoleCache  map[store.UserKey]clientRoleCacheEntry // 用户角色缓存
	dispatcherMu     sync.Mutex
	dispatcherCancel context.CancelFunc // 持久化分发器取消函数
	closeOnce        sync.Once          // 确保 Close 只执行一次
}

// HTTPOptions 配置 HTTP 服务的可选参数。
type HTTPOptions struct {
	NodeID   int64
	Signer   *auth.Signer
	TokenTTL time.Duration
}

// -- HTTP JSON 请求类型 --

type createUserRequest struct {
	Username  string          `json:"username"`
	LoginName string          `json:"login_name,omitempty"`
	Password  string          `json:"password"`
	Profile   json.RawMessage `json:"profile,omitempty"`
	Role      string          `json:"role,omitempty"`
}

type updateUserRequest struct {
	Username  *string          `json:"username,omitempty"`
	LoginName *string          `json:"login_name,omitempty"`
	Password  *string          `json:"password,omitempty"`
	Profile   *json.RawMessage `json:"profile,omitempty"`
	Role      *string          `json:"role,omitempty"`
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

type userMetadataRequest struct {
	Value      *[]byte             `json:"value,omitempty"`
	TypedValue *metadataTypedValue `json:"typed_value,omitempty"`
	ExpiresAt  *string             `json:"expires_at,omitempty"`
}

type loginRequest struct {
	NodeID    int64  `json:"node_id"`
	UserID    int64  `json:"user_id"`
	LoginName string `json:"login_name,omitempty"`
	Password  string `json:"password"`
}

// requestPrincipal 表示通过认证的请求主体，包含用户信息和 JWT Claims。
type requestPrincipal struct {
	User   store.User
	Claims auth.Claims
}

// actorFromPrincipal 从 requestPrincipal 中提取 store.User 指针（用于权限检查）。
func actorFromPrincipal(principal *requestPrincipal) *store.User {
	if principal == nil {
		return nil
	}
	return &principal.User
}

// deliveryKind 消息投递类型：persistent（持久化）或 transient（即时）。
type deliveryKind string

const (
	deliveryKindPersistent deliveryKind = "persistent"
	deliveryKindTransient  deliveryKind = "transient"
)

// normalizeDeliveryKind 将字符串规范化为 deliveryKind 枚举值。
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

// NewHTTP 创建 HTTP 服务实例。初始化路由、会话分片、缓存，并将自身注入为 Service 的 TransientPacketReceiver 和 LoggedInUserProvider。
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
		authorizer:      permission.NewAuthorizer(service, resolved.Signer != nil),
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
		h.sessionShards[idx].sessions = make(map[store.UserKey]*clientSessionBucket)
	}
	if service != nil {
		service.SetTransientPacketReceiver(h)
		service.SetLoggedInUserProvider(h)
	}
	h.routes()
	return h
}

// Handler 返回 http.Handler，用于挂载到 HTTP 服务器。
func (h *HTTP) Handler() http.Handler {
	return h.mux
}

// Close 优雅关闭 HTTP 服务：取消持久化事件分发器。
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

// routes 注册所有 REST API 路由（使用 Go 1.22+ 模式匹配语法）。
func (h *HTTP) routes() {
	h.mux.HandleFunc("GET /healthz", h.handleHealth)
	h.mux.HandleFunc("GET "+clientWSPath, h.handleClientWebSocket)
	h.mux.HandleFunc("GET "+clientRealtimeWSPath, h.handleRealtimeWebSocket)
	h.mux.HandleFunc("POST /auth/login", h.handleLogin)
	h.mux.HandleFunc("GET /users", h.handleListUsers)
	h.mux.HandleFunc("POST /users", h.handleCreateUser)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}", h.handleGetUser)
	h.mux.HandleFunc("PATCH /nodes/{node_id}/users/{user_id}", h.handleUpdateUser)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}", h.handleDeleteUser)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/messages", h.handleListMessagesByUser)
	h.mux.HandleFunc("POST /nodes/{node_id}/users/{user_id}/messages", h.handleCreateMessage)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/metadata", h.handleScanUserMetadata)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/metadata/{key}", h.handleGetUserMetadata)
	h.mux.HandleFunc("PUT /nodes/{node_id}/users/{user_id}/metadata/{key}", h.handleUpsertUserMetadata)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}/metadata/{key}", h.handleDeleteUserMetadata)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/attachments", h.handleListUserAttachments)
	h.mux.HandleFunc("PUT /nodes/{node_id}/users/{user_id}/attachments/{attachment_type}/{subject_node_id}/{subject_user_id}", h.handleUpsertUserAttachment)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}/attachments/{attachment_type}/{subject_node_id}/{subject_user_id}", h.handleDeleteUserAttachment)
	h.mux.HandleFunc("POST /nodes/{node_id}/users/{user_id}/subscriptions", h.handleSubscribeChannel)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}/subscriptions/{channel_node_id}/{channel_user_id}", h.handleUnsubscribeChannel)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/subscriptions", h.handleListSubscriptions)
	h.mux.HandleFunc("POST /nodes/{node_id}/users/{user_id}/blacklist", h.handleBlockUser)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}/blacklist/{blocked_node_id}/{blocked_user_id}", h.handleUnblockUser)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/blacklist", h.handleListBlockedUsers)
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

	loginName := strings.TrimSpace(req.LoginName)
	hasIDSelector := req.NodeID > 0 || req.UserID > 0
	hasLoginNameSelector := loginName != ""
	if hasIDSelector == hasLoginNameSelector {
		writeError(w, http.StatusBadRequest, "exactly one of (node_id,user_id) or login_name must be provided")
		return
	}

	var user store.User
	var err error
	if hasLoginNameSelector {
		user, err = h.service.AuthenticateUserByLoginName(r.Context(), loginName, req.Password)
	} else {
		key := store.UserKey{NodeID: req.NodeID, UserID: req.UserID}
		user, err = h.service.AuthenticateUser(r.Context(), key, req.Password)
	}
	if err != nil {
		if errors.Is(err, store.ErrInvalidInput) {
			writeStoreError(w, err)
			return
		}
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	loginName, err = h.service.GetUserLoginName(r.Context(), user.Key())
	if err != nil {
		writeStoreError(w, err)
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
		"user":       userResponseFromStore(user, loginName),
	})
}

func (h *HTTP) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}

	var req createUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.authorizer.CreateUser(actorFromPrincipal(principal), req.Role); err != nil {
		writeStoreError(w, err)
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
		LoginName:    req.LoginName,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	}, creator)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateTargetRoleCache(user.Key())

	resp, err := h.buildUserResponse(r.Context(), actorFromPrincipal(principal), user)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (h *HTTP) handleGetUser(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	key, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if err := h.authorizer.ViewUser(actorFromPrincipal(principal), key); err != nil {
		writeStoreError(w, err)
		return
	}

	user, err := h.service.GetUser(r.Context(), key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	h.invalidateTargetRoleCache(user.Key())

	resp, err := h.buildUserResponse(r.Context(), actorFromPrincipal(principal), user)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *HTTP) handleListUsers(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	uid, err := userListUIDFromQuery(r)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	users, err := h.service.ListCommunicableUsers(r.Context(), actorFromPrincipal(principal), store.UserListFilter{
		Name: strings.TrimSpace(r.URL.Query().Get("name")),
		UID:  uid,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

	resp := make([]userResponse, 0, len(users))
	for _, user := range users {
		item, err := h.buildUserResponse(r.Context(), actorFromPrincipal(principal), user)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		resp = append(resp, item)
	}
	writeJSON(w, http.StatusOK, resp)
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
	var req updateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.authorizer.UpdateUser(r.Context(), actorFromPrincipal(principal), target, req.Role, req.Password != nil, req.LoginName != nil); err != nil {
		writeStoreError(w, err)
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

	user, _, err := h.service.UpdateUser(r.Context(), store.UpdateUserParams{
		Key:          key,
		Username:     req.Username,
		LoginName:    req.LoginName,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

	resp, err := h.buildUserResponse(r.Context(), actorFromPrincipal(principal), user)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
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
	if err := h.authorizer.DeleteUser(r.Context(), actorFromPrincipal(principal), target); err != nil {
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
	if err := h.authorizer.CreateMessage(r.Context(), actorFromPrincipal(principal), key); err != nil {
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

func (h *HTTP) handleListMessagesByUser(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	key, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	target, err := h.service.GetUser(r.Context(), key)
	if err != nil {
		writeStoreError(w, err)
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

	var messages []store.Message
	peerNodeIDRaw := r.URL.Query().Get("peer_node_id")
	peerUserIDRaw := r.URL.Query().Get("peer_user_id")
	if peerNodeIDRaw != "" && peerUserIDRaw != "" {
		peerNodeID, err := strconv.ParseInt(peerNodeIDRaw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "peer_node_id must be an integer")
			return
		}
		peerUserID, err := strconv.ParseInt(peerUserIDRaw, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "peer_user_id must be an integer")
			return
		}
		peer := store.UserKey{NodeID: peerNodeID, UserID: peerUserID}
		if err := peer.Validate(); err != nil {
			writeError(w, http.StatusBadRequest, "invalid peer user")
			return
		}
		actor := actorFromPrincipal(principal)
		// 会话查询允许 target 或 peer 任一方作为请求者（管理员也可）
		if err := h.authorizer.ListMessages(actor, target); err != nil {
			if actor.Key() != peer {
				writeStoreError(w, err)
				return
			}
		}
		session := store.MessageSession(key, peer)
		messages, err = h.service.ListMessagesBySession(r.Context(), session, actor.Key(), limit)
	} else {
		if err := h.authorizer.ListMessages(actorFromPrincipal(principal), target); err != nil {
			writeStoreError(w, err)
			return
		}
		messages, err = h.service.ListMessagesByUser(r.Context(), key, limit)
	}
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	attachmentType, err := attachmentTypeFromQuery(r)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	owner, usingCurrent, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if usingCurrent && !allowCurrentUserSentinelForAttachmentList(attachmentType) {
		writeStoreError(w, fmt.Errorf("%w: /nodes/0/users/0/attachments only supports empty, channel_subscription, or user_blacklist attachment_type", store.ErrInvalidInput))
		return
	}
	if err := h.authorizer.ListAttachment(r.Context(), actorFromPrincipal(principal), owner, attachmentType); err != nil {
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

func (h *HTTP) handleGetUserMetadata(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	owner, key, ok := h.parseUserMetadataKeyRequest(w, r, principal)
	if !ok {
		return
	}
	ownerUser, err := h.service.GetUser(r.Context(), owner)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := h.authorizer.ReadUserMetadata(r.Context(), actorFromPrincipal(principal), ownerUser); err != nil {
		writeStoreError(w, err)
		return
	}
	metadata, err := h.service.GetUserMetadata(r.Context(), owner, key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, userMetadataResponseFromStore(metadata))
}

func (h *HTTP) handleUpsertUserMetadata(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	owner, key, ok := h.parseUserMetadataKeyRequest(w, r, principal)
	if !ok {
		return
	}
	ownerUser, err := h.service.GetUser(r.Context(), owner)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := h.authorizer.WriteUserMetadata(r.Context(), actorFromPrincipal(principal), ownerUser); err != nil {
		writeStoreError(w, err)
		return
	}

	var req userMetadataRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	expiresAt, err := parseOptionalMetadataExpiresAt(req.ExpiresAt)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	value, err := metadataRawValueFromRequest(req)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	metadata, _, err := h.service.UpsertUserMetadata(r.Context(), store.UpsertUserMetadataParams{
		Owner:     owner,
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, userMetadataResponseFromStore(metadata))
}

func (h *HTTP) handleDeleteUserMetadata(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	owner, key, ok := h.parseUserMetadataKeyRequest(w, r, principal)
	if !ok {
		return
	}
	ownerUser, err := h.service.GetUser(r.Context(), owner)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := h.authorizer.WriteUserMetadata(r.Context(), actorFromPrincipal(principal), ownerUser); err != nil {
		writeStoreError(w, err)
		return
	}
	metadata, _, err := h.service.DeleteUserMetadata(r.Context(), store.DeleteUserMetadataParams{
		Owner: owner,
		Key:   key,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, userMetadataResponseFromStore(metadata))
}

func (h *HTTP) handleScanUserMetadata(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	owner, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	ownerUser, err := h.service.GetUser(r.Context(), owner)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := h.authorizer.ReadUserMetadata(r.Context(), actorFromPrincipal(principal), ownerUser); err != nil {
		writeStoreError(w, err)
		return
	}
	limit, err := metadataScanLimitFromQuery(r)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	result, err := h.service.ScanUserMetadata(r.Context(), store.ScanUserMetadataParams{
		Owner:  owner,
		Prefix: strings.TrimSpace(r.URL.Query().Get("prefix")),
		After:  strings.TrimSpace(r.URL.Query().Get("after")),
		Limit:  limit,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	items := make([]userMetadataResponse, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, userMetadataResponseFromStore(item))
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":      items,
		"count":      len(items),
		"next_after": result.NextAfter,
	})
}

func (h *HTTP) handleUpsertUserAttachment(w http.ResponseWriter, r *http.Request) {
	owner, attachmentType, subject, principal, ok := h.parseAttachmentWriteRequest(w, r)
	if !ok {
		return
	}
	if err := h.authorizer.ManageAttachment(r.Context(), actorFromPrincipal(principal), owner, attachmentType); err != nil {
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
	if err := h.authorizer.ManageAttachment(r.Context(), actorFromPrincipal(principal), owner, attachmentType); err != nil {
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	subscriber, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if err := h.authorizer.ManageSubscription(actorFromPrincipal(principal), subscriber); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	subscriber, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if err := h.authorizer.ManageSubscription(actorFromPrincipal(principal), subscriber); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	subscriber, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if err := h.authorizer.ListSubscription(actorFromPrincipal(principal), subscriber); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	owner, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if err := h.authorizer.ManageBlacklist(actorFromPrincipal(principal), owner); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	owner, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if err := h.authorizer.ManageBlacklist(actorFromPrincipal(principal), owner); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	owner, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return
	}
	if err := h.authorizer.ListBlacklist(actorFromPrincipal(principal), owner); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	if err := h.authorizer.ListEvents(actorFromPrincipal(principal)); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	if err := h.authorizer.ReadOpsStatus(actorFromPrincipal(principal)); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	if err := h.authorizer.ListClusterNodes(actorFromPrincipal(principal)); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	if err := h.authorizer.ListLoggedInUsers(actorFromPrincipal(principal)); err != nil {
		writeStoreError(w, err)
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
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}
	if err := h.authorizer.ReadMetrics(actorFromPrincipal(principal)); err != nil {
		writeStoreError(w, err)
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
	LoginName      string          `json:"login_name"`
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

type userMetadataResponse struct {
	Owner        store.UserKey       `json:"owner"`
	Key          string              `json:"key"`
	Value        []byte              `json:"value"`
	TypedValue   *metadataTypedValue `json:"typed_value,omitempty"`
	UpdatedAt    string              `json:"updated_at"`
	DeletedAt    string              `json:"deleted_at,omitempty"`
	ExpiresAt    string              `json:"expires_at,omitempty"`
	OriginNodeID int64               `json:"origin_node_id"`
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

func userResponseFromStore(user store.User, loginName string) userResponse {
	return userResponse{
		NodeID:         user.NodeID,
		UserID:         user.ID,
		ID:             user.ID,
		Username:       user.Username,
		LoginName:      loginName,
		Profile:        json.RawMessage(user.Profile),
		Role:           user.Role,
		SystemReserved: user.SystemReserved,
		CreatedAt:      user.CreatedAt.String(),
		UpdatedAt:      user.UpdatedAt.String(),
		OriginNodeID:   user.OriginNodeID,
	}
}

func (h *HTTP) buildUserResponse(ctx context.Context, viewer *store.User, user store.User) (userResponse, error) {
	loginName := ""
	if h != nil && h.service != nil {
		var err error
		loginName, err = h.service.GetVisibleUserLoginName(ctx, viewer, user)
		if err != nil {
			return userResponse{}, err
		}
	}
	return userResponseFromStore(user, loginName), nil
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

func userMetadataResponseFromStore(metadata store.UserMetadata) userMetadataResponse {
	response := userMetadataResponse{
		Owner:        metadata.Owner,
		Key:          metadata.Key,
		Value:        append([]byte(nil), metadata.Value...),
		TypedValue:   metadataTypedValueFromRaw(metadata.Value),
		UpdatedAt:    metadata.UpdatedAt.String(),
		OriginNodeID: metadata.OriginNodeID,
	}
	if metadata.DeletedAt != nil {
		response.DeletedAt = metadata.DeletedAt.String()
	}
	if metadata.ExpiresAt != nil {
		response.ExpiresAt = store.FormatUserMetadataExpiresAt(*metadata.ExpiresAt)
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

func newClientSessionBucket() *clientSessionBucket {
	return &clientSessionBucket{
		bySessionID: make(map[string]*clientWSSession),
	}
}

func appendClientSessionSnapshot(snapshot []*clientWSSession, sess *clientWSSession) []*clientWSSession {
	next := make([]*clientWSSession, len(snapshot), len(snapshot)+1)
	copy(next, snapshot)
	return append(next, sess)
}

func removeClientSessionSnapshot(snapshot []*clientWSSession, sessionID string) []*clientWSSession {
	for idx, sess := range snapshot {
		if sess == nil || sess.sessionRef.SessionID != sessionID {
			continue
		}
		next := make([]*clientWSSession, 0, len(snapshot)-1)
		next = append(next, snapshot[:idx]...)
		next = append(next, snapshot[idx+1:]...)
		return next
	}
	return snapshot
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
		bucket = newClientSessionBucket()
		shard.sessions[key] = bucket
	}
	if sess.sessionRef.SessionID == "" {
		shard.mu.Unlock()
		return
	}
	if _, exists := bucket.bySessionID[sess.sessionRef.SessionID]; exists {
		shard.mu.Unlock()
		return
	}
	bucket.bySessionID[sess.sessionRef.SessionID] = sess
	bucket.snapshot = appendClientSessionSnapshot(bucket.snapshot, sess)
	shard.mu.Unlock()

	h.onlineUsersMu.Lock()
	state := h.onlineUsers[key]
	if state.SessionCount == 0 {
		h.onlineUserCount.Add(1)
	}
	if sess.principal != nil && sess.principal.User.Username != "" {
		state.Username = sess.principal.User.Username
	}
	if sess.loginName != "" {
		state.LoginName = sess.loginName
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
	if _, exists := bucket.bySessionID[sess.sessionRef.SessionID]; !exists {
		shard.mu.Unlock()
		return
	}
	delete(bucket.bySessionID, sess.sessionRef.SessionID)
	bucket.snapshot = removeClientSessionSnapshot(bucket.snapshot, sess.sessionRef.SessionID)
	if len(bucket.bySessionID) == 0 {
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

// ReceiveTransientPacket 实现 TransientPacketReceiver 接口。
// 检查黑名单后，将即时包投递到目标用户在本节点的匹配客户端会话。
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
		var sess *clientWSSession
		if bucket != nil {
			sess = bucket.bySessionID[packet.TargetSession.SessionID]
		}
		shard.mu.RUnlock()
		if sess == nil {
			return false
		}
		return sess.pushPacket(packet) == nil
	}
	shard.mu.RLock()
	bucket := shard.sessions[packet.Recipient]
	var sessions []*clientWSSession
	if bucket != nil {
		sessions = bucket.snapshot
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

// ListLocalUserSessions 返回指定用户在本节点的所有活跃客户端会话。
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
	size := 0
	if bucket != nil {
		size = len(bucket.snapshot)
	}
	items := make([]store.OnlineSession, 0, size)
	if bucket != nil {
		for _, sess := range bucket.snapshot {
			items = append(items, sess.onlineSession())
		}
	}
	shard.mu.RUnlock()
	sort.Slice(items, func(i, j int) bool {
		return items[i].SessionRef.SessionID < items[j].SessionRef.SessionID
	})
	return items, nil
}

// newSessionRef 生成一个新的全局唯一会话引用。优先使用加密随机 ID，失败时回退到自增序列。
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

// ListLoggedInUsers 实现 LoggedInUserProvider 接口。返回本节点所有已登录用户的摘要列表。
func (h *HTTP) ListLoggedInUsers(context.Context) ([]app.LoggedInUserSummary, error) {
	if h == nil {
		return nil, nil
	}
	h.onlineUsersMu.RLock()
	users := make([]app.LoggedInUserSummary, 0, int(h.onlineUserCount.Load()))
	for key, state := range h.onlineUsers {
		users = append(users, app.LoggedInUserSummary{
			NodeID:    key.NodeID,
			UserID:    key.UserID,
			Username:  state.Username,
			LoginName: state.LoginName,
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

// decodeJSON 解码 JSON 请求体，拒绝未知字段以防止客户端拼写错误。
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

// writeJSON 向 HTTP 响应写入 JSON 编码的数据。
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeError 写入 JSON 格式的 HTTP 错误响应。
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// writeStoreError 将 store 层或 app 层错误映射为对应的 HTTP 状态码和错误响应。
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

// parsePathUserKey 从 URL 路径中提取 node_id 和 user_id，组合为 store.UserKey。
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

// parseNonNegativePathInt 从 URL 路径中解析非负整数参数。
func parseNonNegativePathInt(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := strings.TrimSpace(r.PathValue(name))
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		writeError(w, http.StatusBadRequest, name+" must be a non-negative integer")
		return 0, false
	}
	return value, true
}

// parsePositivePathInt 从 URL 路径中解析正整数参数。
func parsePositivePathInt(w http.ResponseWriter, r *http.Request, name string) (int64, bool) {
	raw := strings.TrimSpace(r.PathValue(name))
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		writeError(w, http.StatusBadRequest, name+" must be a positive integer")
		return 0, false
	}
	return value, true
}

// normalizeJSONValue 规范化 JSON 字节：空值返回默认值，非法 JSON 返回错误。
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

// parseOptionalMetadataExpiresAt 解析可选的元数据过期时间字符串。
func parseOptionalMetadataExpiresAt(raw *string) (*time.Time, error) {
	if raw == nil {
		return nil, nil
	}
	return store.ParseUserMetadataExpiresAt(*raw)
}

func metadataScanLimitFromQuery(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: limit must be an integer", store.ErrInvalidInput)
	}
	return limit, nil
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

func userListUIDFromQuery(r *http.Request) (*store.UserKey, error) {
	if r == nil {
		return nil, nil
	}
	raw := strings.TrimSpace(r.URL.Query().Get("uid"))
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("%w: uid must be in node_id:user_id format", store.ErrInvalidInput)
	}
	nodeID, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || nodeID <= 0 {
		return nil, fmt.Errorf("%w: uid node_id must be a positive integer", store.ErrInvalidInput)
	}
	userID, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || userID <= 0 {
		return nil, fmt.Errorf("%w: uid user_id must be a positive integer", store.ErrInvalidInput)
	}
	key := &store.UserKey{NodeID: nodeID, UserID: userID}
	return key, nil
}

func userMetadataKeyFromPath(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.PathValue("key")
	if key == "" {
		writeStoreError(w, fmt.Errorf("%w: key cannot be empty", store.ErrInvalidInput))
		return "", false
	}
	return key, true
}

func (h *HTTP) parseUserMetadataKeyRequest(w http.ResponseWriter, r *http.Request, principal *requestPrincipal) (store.UserKey, string, bool) {
	owner, _, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return store.UserKey{}, "", false
	}
	key, ok := userMetadataKeyFromPath(w, r)
	if !ok {
		return store.UserKey{}, "", false
	}
	return owner, key, true
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
	owner, usingCurrent, ok := h.parsePathUserKeyOrCurrent(w, r, principal)
	if !ok {
		return store.UserKey{}, "", store.UserKey{}, nil, false
	}
	if usingCurrent && !allowCurrentUserSentinelForAttachmentWrite(attachmentType) {
		writeStoreError(w, fmt.Errorf("%w: /nodes/0/users/0/attachments only supports channel_subscription or user_blacklist for attachment write routes", store.ErrInvalidInput))
		return store.UserKey{}, "", store.UserKey{}, nil, false
	}
	return owner, attachmentType, store.UserKey{NodeID: subjectNodeID, UserID: subjectUserID}, principal, true
}

// parsePathUserKeyOrCurrent 解析用户路径。
// 显式正整数路径返回目标用户；仅 node_id=0 且 user_id=0 时回退到当前登录用户。
func (h *HTTP) parsePathUserKeyOrCurrent(w http.ResponseWriter, r *http.Request, principal *requestPrincipal) (store.UserKey, bool, bool) {
	nodeID, ok := parseNonNegativePathInt(w, r, "node_id")
	if !ok {
		return store.UserKey{}, false, false
	}
	userID, ok := parseNonNegativePathInt(w, r, "user_id")
	if !ok {
		return store.UserKey{}, false, false
	}
	switch {
	case nodeID == 0 && userID == 0:
		if principal == nil {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return store.UserKey{}, false, false
		}
		return principal.User.Key(), true, true
	case nodeID == 0 || userID == 0:
		writeError(w, http.StatusBadRequest, "node_id and user_id must both be 0 or both be positive integers")
		return store.UserKey{}, false, false
	default:
		return store.UserKey{NodeID: nodeID, UserID: userID}, false, true
	}
}

func allowCurrentUserSentinelForAttachmentWrite(attachmentType store.AttachmentType) bool {
	return attachmentType == store.AttachmentTypeChannelSubscription || attachmentType == store.AttachmentTypeUserBlacklist
}

func allowCurrentUserSentinelForAttachmentList(attachmentType store.AttachmentType) bool {
	return attachmentType == "" || allowCurrentUserSentinelForAttachmentWrite(attachmentType)
}

// invalidateAttachmentCaches 当附件变更时使相关客户端缓存失效（频道订阅或黑名单）。
func (h *HTTP) invalidateAttachmentCaches(attachment store.Attachment) {
	switch attachment.Type {
	case store.AttachmentTypeChannelSubscription:
		h.invalidateUserChannelSubscriptionCache(attachment.Owner, attachment.Subject)
	case store.AttachmentTypeUserBlacklist:
		h.invalidateUserBlacklistCache(attachment.Owner, attachment.Subject)
	}
}

// requireAuthenticated 验证请求的 Bearer Token 并返回认证主体。如果认证未配置则跳过。
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

// authenticateRequest 解析并验证 Bearer Token，返回对应的用户身份。
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

func messageSenderFromPrincipal(principal *requestPrincipal) (store.UserKey, error) {
	if principal == nil {
		return store.UserKey{}, fmt.Errorf("%w: authentication is required to derive sender", store.ErrForbidden)
	}
	return principal.User.Key(), nil
}

// formatUserSubject 将 UserKey 格式化 JWT subject 字符串："nodeID:userID"。
func formatUserSubject(key store.UserKey) string {
	return strconv.FormatInt(key.NodeID, 10) + ":" + strconv.FormatInt(key.UserID, 10)
}

// parseUserSubject 从 JWT subject 字符串 "nodeID:userID" 中解析 store.UserKey。
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
