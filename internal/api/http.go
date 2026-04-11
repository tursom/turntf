package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"notifier/internal/app"
	"notifier/internal/store"
)

type HTTP struct {
	service *Service
	mux     *http.ServeMux
}

type createUserRequest struct {
	Username     string          `json:"username"`
	PasswordHash string          `json:"password_hash"`
	Profile      json.RawMessage `json:"profile,omitempty"`
}

type updateUserRequest struct {
	Username     *string          `json:"username,omitempty"`
	PasswordHash *string          `json:"password_hash,omitempty"`
	Profile      *json.RawMessage `json:"profile,omitempty"`
}

type createMessageRequest struct {
	UserID   int64           `json:"user_id"`
	Sender   string          `json:"sender"`
	Body     string          `json:"body"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

func NewHTTP(service *Service) *HTTP {
	h := &HTTP{
		service: service,
		mux:     http.NewServeMux(),
	}
	h.routes()
	return h
}

func (h *HTTP) Handler() http.Handler {
	return h.mux
}

func (h *HTTP) routes() {
	h.mux.HandleFunc("GET /healthz", h.handleHealth)
	h.mux.HandleFunc("POST /users", h.handleCreateUser)
	h.mux.HandleFunc("GET /users/{id}", h.handleGetUser)
	h.mux.HandleFunc("PATCH /users/{id}", h.handleUpdateUser)
	h.mux.HandleFunc("DELETE /users/{id}", h.handleDeleteUser)
	h.mux.HandleFunc("GET /users/{id}/messages", h.handleListMessagesByUser)
	h.mux.HandleFunc("POST /messages", h.handleCreateMessage)
	h.mux.HandleFunc("GET /events", h.handleListEvents)
}

func (h *HTTP) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *HTTP) handleCreateUser(w http.ResponseWriter, r *http.Request) {
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

	user, _, err := h.service.CreateUser(r.Context(), store.CreateUserParams{
		Username:     req.Username,
		PasswordHash: req.PasswordHash,
		Profile:      profile,
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

	var req updateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var profile *string
	if req.Profile != nil {
		normalized, err := normalizeJSONValue(*req.Profile, "{}")
		if err != nil {
			writeError(w, http.StatusBadRequest, "profile must be valid JSON")
			return
		}
		profile = &normalized
	}

	user, _, err := h.service.UpdateUser(r.Context(), store.UpdateUserParams{
		UserID:       userID,
		Username:     req.Username,
		PasswordHash: req.PasswordHash,
		Profile:      profile,
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
	var req createMessageRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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

type userResponse struct {
	ID           int64           `json:"id"`
	Username     string          `json:"username"`
	Profile      json.RawMessage `json:"profile"`
	CreatedAt    string          `json:"created_at"`
	UpdatedAt    string          `json:"updated_at"`
	OriginNodeID string          `json:"origin_node_id"`
}

type messageResponse struct {
	ID           int64           `json:"id"`
	UserID       int64           `json:"user_id"`
	Sender       string          `json:"sender"`
	Body         string          `json:"body"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
	CreatedAt    string          `json:"created_at"`
	OriginNodeID string          `json:"origin_node_id"`
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
		ID:           user.ID,
		Username:     user.Username,
		Profile:      json.RawMessage(user.Profile),
		CreatedAt:    user.CreatedAt.String(),
		UpdatedAt:    user.UpdatedAt.String(),
		OriginNodeID: user.OriginNodeID,
	}
}

func messageResponseFromStore(message store.Message) messageResponse {
	var metadata json.RawMessage
	if strings.TrimSpace(message.Metadata) != "" {
		metadata = json.RawMessage(message.Metadata)
	}
	return messageResponse{
		ID:           message.ID,
		UserID:       message.UserID,
		Sender:       message.Sender,
		Body:         message.Body,
		Metadata:     metadata,
		CreatedAt:    message.CreatedAt.String(),
		OriginNodeID: message.OriginNodeID,
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
