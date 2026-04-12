package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/store"
)

type HTTP struct {
	service    *Service
	mux        *http.ServeMux
	nodeID     int64
	signer     *auth.Signer
	tokenTTL   time.Duration
	sessionsMu sync.RWMutex
	sessions   map[store.UserKey]map[*clientWSSession]struct{}
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
	Sender       string `json:"sender"`
	Body         []byte `json:"body"`
	DeliveryKind string `json:"delivery_kind,omitempty"`
	DeliveryMode string `json:"delivery_mode,omitempty"`
}

type subscriptionRequest struct {
	ChannelNodeID int64 `json:"channel_node_id"`
	ChannelUserID int64 `json:"channel_user_id"`
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
		service:  service,
		mux:      http.NewServeMux(),
		nodeID:   resolved.NodeID,
		signer:   resolved.Signer,
		tokenTTL: tokenTTL,
		sessions: make(map[store.UserKey]map[*clientWSSession]struct{}),
	}
	if service != nil {
		service.SetTransientPacketReceiver(h)
	}
	h.routes()
	return h
}

func (h *HTTP) Handler() http.Handler {
	return h.mux
}

func (h *HTTP) routes() {
	h.mux.HandleFunc("GET /healthz", h.handleHealth)
	h.mux.HandleFunc("GET /ws/client", h.handleClientWebSocket)
	h.mux.HandleFunc("POST /auth/login", h.handleLogin)
	h.mux.HandleFunc("POST /users", h.handleCreateUser)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}", h.handleGetUser)
	h.mux.HandleFunc("PATCH /nodes/{node_id}/users/{user_id}", h.handleUpdateUser)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}", h.handleDeleteUser)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/messages", h.handleListMessagesByUser)
	h.mux.HandleFunc("POST /nodes/{node_id}/users/{user_id}/messages", h.handleCreateMessage)
	h.mux.HandleFunc("GET /nodes/{node_id}/users/{user_id}/subscriptions", h.handleListSubscriptions)
	h.mux.HandleFunc("POST /nodes/{node_id}/users/{user_id}/subscriptions", h.handleSubscribeChannel)
	h.mux.HandleFunc("DELETE /nodes/{node_id}/users/{user_id}/subscriptions/{channel_node_id}/{channel_user_id}", h.handleUnsubscribeChannel)
	h.mux.HandleFunc("GET /events", h.handleListEvents)
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
	if _, ok := h.requireAdmin(w, r); !ok {
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

	user, _, err := h.service.CreateUser(r.Context(), store.CreateUserParams{
		Username:     req.Username,
		PasswordHash: passwordHash,
		Profile:      profile,
		Role:         req.Role,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

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

	writeJSON(w, http.StatusOK, userResponseFromStore(user))
}

func (h *HTTP) handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	key, ok := parsePathUserKey(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireAdmin(w, r); !ok {
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
	if _, ok := h.requireAdmin(w, r); !ok {
		return
	}

	if _, err := h.service.DeleteUser(r.Context(), key); err != nil {
		writeStoreError(w, err)
		return
	}

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
	deliveryKind, err := normalizeDeliveryKind(req.DeliveryKind)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if deliveryKind == deliveryKindTransient {
		mode, err := store.NormalizeDeliveryMode(req.DeliveryMode)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		packet, err := h.service.DispatchTransientPacket(r.Context(), key, req.Sender, req.Body, mode)
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

	message, _, err := h.service.CreateMessage(r.Context(), store.CreateMessageParams{
		UserKey: key,
		Sender:  req.Sender,
		Body:    req.Body,
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
	if target.Role != store.RoleChannel {
		return store.ErrForbidden
	}
	subscribed, err := h.service.IsSubscribedToChannel(ctx, principal.User.Key(), key)
	if err != nil {
		return err
	}
	if !subscribed {
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
	UserNodeID int64  `json:"user_node_id"`
	UserID     int64  `json:"user_id"`
	NodeID     int64  `json:"node_id"`
	Seq        int64  `json:"seq"`
	Sender     string `json:"sender"`
	Body       []byte `json:"body"`
	CreatedAt  string `json:"created_at"`
}

type transientPacketResponse struct {
	Mode         string        `json:"mode"`
	PacketID     uint64        `json:"packet_id"`
	SourceNodeID int64         `json:"source_node_id"`
	TargetNodeID int64         `json:"target_node_id"`
	Recipient    store.UserKey `json:"recipient"`
	DeliveryMode string        `json:"delivery_mode"`
}

type subscriptionResponse struct {
	SubscriberNodeID int64  `json:"subscriber_node_id"`
	SubscriberUserID int64  `json:"subscriber_user_id"`
	ChannelNodeID    int64  `json:"channel_node_id"`
	ChannelUserID    int64  `json:"channel_user_id"`
	SubscribedAt     string `json:"subscribed_at"`
	DeletedAt        string `json:"deleted_at,omitempty"`
	OriginNodeID     int64  `json:"origin_node_id"`
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
		UserNodeID: message.UserNodeID,
		UserID:     message.UserID,
		NodeID:     message.NodeID,
		Seq:        message.Seq,
		Sender:     message.Sender,
		Body:       message.Body,
		CreatedAt:  message.CreatedAt.String(),
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

func (h *HTTP) registerClientSession(key store.UserKey, sess *clientWSSession) {
	if h == nil || sess == nil {
		return
	}
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	bucket := h.sessions[key]
	if bucket == nil {
		bucket = make(map[*clientWSSession]struct{})
		h.sessions[key] = bucket
	}
	bucket[sess] = struct{}{}
}

func (h *HTTP) unregisterClientSession(key store.UserKey, sess *clientWSSession) {
	if h == nil || sess == nil {
		return
	}
	h.sessionsMu.Lock()
	defer h.sessionsMu.Unlock()
	bucket := h.sessions[key]
	if bucket == nil {
		return
	}
	delete(bucket, sess)
	if len(bucket) == 0 {
		delete(h.sessions, key)
	}
}

func (h *HTTP) ReceiveTransientPacket(packet store.TransientPacket) bool {
	h.sessionsMu.RLock()
	bucket := h.sessions[packet.Recipient]
	sessions := make([]*clientWSSession, 0, len(bucket))
	for sess := range bucket {
		sessions = append(sessions, sess)
	}
	h.sessionsMu.RUnlock()
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

func subscriptionResponseFromStore(subscription store.Subscription) subscriptionResponse {
	response := subscriptionResponse{
		SubscriberNodeID: subscription.Subscriber.NodeID,
		SubscriberUserID: subscription.Subscriber.UserID,
		ChannelNodeID:    subscription.Channel.NodeID,
		ChannelUserID:    subscription.Channel.UserID,
		SubscribedAt:     subscription.SubscribedAt.String(),
		OriginNodeID:     subscription.OriginNodeID,
	}
	if subscription.DeletedAt != nil {
		response.DeletedAt = subscription.DeletedAt.String()
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
