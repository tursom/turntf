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
	"time"

	"notifier/internal/app"
	"notifier/internal/auth"
	"notifier/internal/store"
)

type HTTP struct {
	service  *Service
	mux      *http.ServeMux
	nodeID   string
	signer   *auth.Signer
	tokenTTL time.Duration
}

type HTTPOptions struct {
	NodeID   string
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
	UserID   int64           `json:"user_id"`
	Sender   string          `json:"sender"`
	Body     string          `json:"body"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

type loginRequest struct {
	UserID   int64  `json:"user_id"`
	Password string `json:"password"`
}

type requestPrincipal struct {
	User   store.User
	Claims auth.Claims
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
		nodeID:   strings.TrimSpace(resolved.NodeID),
		signer:   resolved.Signer,
		tokenTTL: tokenTTL,
	}
	h.routes()
	return h
}

func (h *HTTP) Handler() http.Handler {
	return h.mux
}

func (h *HTTP) routes() {
	h.mux.HandleFunc("GET /healthz", h.handleHealth)
	h.mux.HandleFunc("POST /auth/login", h.handleLogin)
	h.mux.HandleFunc("POST /users", h.handleCreateUser)
	h.mux.HandleFunc("GET /users/{id}", h.handleGetUser)
	h.mux.HandleFunc("PATCH /users/{id}", h.handleUpdateUser)
	h.mux.HandleFunc("DELETE /users/{id}", h.handleDeleteUser)
	h.mux.HandleFunc("GET /users/{id}/messages", h.handleListMessagesByUser)
	h.mux.HandleFunc("POST /messages", h.handleCreateMessage)
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

	user, err := h.service.AuthenticateUser(r.Context(), req.UserID, req.Password)
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
		Subject:   strconv.FormatInt(user.ID, 10),
		Issuer:    h.nodeID,
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
	passwordHash, err := auth.HashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
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
	userID, ok := parsePathID(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, userID); !ok {
		return
	}

	user, err := h.service.GetUser(r.Context(), userID)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, userResponseFromStore(user))
}

func (h *HTTP) handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	userID, ok := parsePathID(w, r)
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
		UserID:       userID,
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
	userID, ok := parsePathID(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireAdmin(w, r); !ok {
		return
	}

	if _, err := h.service.DeleteUser(r.Context(), userID); err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "deleted",
		"id":     userID,
	})
}

func (h *HTTP) handleCreateMessage(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return
	}

	var req createMessageRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if principal != nil && !isAdminRole(principal.User.Role) && req.UserID != principal.User.ID {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	metadata, err := normalizeJSONValue(req.Metadata, "")
	if err != nil {
		writeError(w, http.StatusBadRequest, "metadata must be valid JSON")
		return
	}

	message, _, err := h.service.CreateMessage(r.Context(), store.CreateMessageParams{
		UserID:   req.UserID,
		Sender:   req.Sender,
		Body:     req.Body,
		Metadata: metadata,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, messageResponseFromStore(message))
}

func (h *HTTP) handleListMessagesByUser(w http.ResponseWriter, r *http.Request) {
	userID, ok := parsePathID(w, r)
	if !ok {
		return
	}
	if _, ok := h.requireSelfOrAdmin(w, r, userID); !ok {
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

	messages, err := h.service.ListMessagesByUser(r.Context(), userID, limit)
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
	ID             int64           `json:"id"`
	Username       string          `json:"username"`
	Profile        json.RawMessage `json:"profile"`
	Role           string          `json:"role"`
	SystemReserved bool            `json:"system_reserved"`
	CreatedAt      string          `json:"created_at"`
	UpdatedAt      string          `json:"updated_at"`
	OriginNodeID   string          `json:"origin_node_id"`
}

type messageResponse struct {
	UserID    int64           `json:"user_id"`
	NodeID    string          `json:"node_id"`
	Seq       int64           `json:"seq"`
	Sender    string          `json:"sender"`
	Body      string          `json:"body"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt string          `json:"created_at"`
}

type eventResponse struct {
	Sequence     int64           `json:"sequence"`
	EventID      int64           `json:"event_id"`
	Kind         string          `json:"kind"`
	Aggregate    string          `json:"aggregate"`
	AggregateID  int64           `json:"aggregate_id"`
	HLC          string          `json:"hlc"`
	OriginNodeID string          `json:"origin_node_id"`
	Payload      json.RawMessage `json:"payload"`
}

func userResponseFromStore(user store.User) userResponse {
	return userResponse{
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
	var metadata json.RawMessage
	if strings.TrimSpace(message.Metadata) != "" {
		metadata = json.RawMessage(message.Metadata)
	}
	return messageResponse{
		UserID:    message.UserID,
		NodeID:    message.NodeID,
		Seq:       message.Seq,
		Sender:    message.Sender,
		Body:      message.Body,
		Metadata:  metadata,
		CreatedAt: message.CreatedAt.String(),
	}
}

func eventResponseFromStore(event store.Event) eventResponse {
	return eventResponse{
		Sequence:     event.Sequence,
		EventID:      event.EventID,
		Kind:         event.Kind,
		Aggregate:    event.Aggregate,
		AggregateID:  event.AggregateID,
		HLC:          event.HLC.String(),
		OriginNodeID: event.OriginNodeID,
		Payload:      json.RawMessage(event.Payload),
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

func parsePathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	raw := strings.TrimSpace(r.PathValue("id"))
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value <= 0 {
		writeError(w, http.StatusBadRequest, "id must be a positive integer")
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

func (h *HTTP) requireSelfOrAdmin(w http.ResponseWriter, r *http.Request, userID int64) (*requestPrincipal, bool) {
	if h.signer == nil {
		return nil, true
	}
	principal, ok := h.requireAuthenticated(w, r)
	if !ok {
		return nil, false
	}
	if isAdminRole(principal.User.Role) || principal.User.ID == userID {
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
	userID, err := strconv.ParseInt(strings.TrimSpace(claims.Subject), 10, 64)
	if err != nil || userID <= 0 {
		return nil, errors.New("invalid subject")
	}
	user, err := h.service.GetUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	return &requestPrincipal{User: user, Claims: claims}, nil
}

func isAdminRole(role string) bool {
	return role == store.RoleSuperAdmin || role == store.RoleAdmin
}
