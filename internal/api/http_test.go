package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"

	"notifier/internal/app"
	"notifier/internal/store"
)

func TestUserAndMessageHTTPAPI(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t)

	createUserBody := map[string]any{
		"username":      "alice",
		"password_hash": "hash-1",
		"profile": map[string]any{
			"display_name": "Alice",
		},
	}

	var createdUser struct {
		ID       int64             `json:"id"`
		Username string            `json:"username"`
		Profile  map[string]string `json:"profile"`
	}
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/users", createUserBody, http.StatusCreated), &createdUser)
	if createdUser.ID == 0 {
		t.Fatalf("expected created user id")
	}
	if createdUser.Username != "alice" {
		t.Fatalf("unexpected created user: %+v", createdUser)
	}

	var loadedUser struct {
		ID       int64             `json:"id"`
		Username string            `json:"username"`
		Profile  map[string]string `json:"profile"`
	}
	mustJSON(t, doJSON(t, handler, http.MethodGet, "/users/"+strconv.FormatInt(createdUser.ID, 10), nil, http.StatusOK), &loadedUser)
	if loadedUser.ID != createdUser.ID {
		t.Fatalf("unexpected loaded user: %+v", loadedUser)
	}

	updateBody := map[string]any{
		"username":      "alice-updated",
		"password_hash": "hash-2",
		"profile": map[string]any{
			"display_name": "Alice Updated",
		},
	}
	var updatedUser struct {
		Username string            `json:"username"`
		Profile  map[string]string `json:"profile"`
	}
	mustJSON(t, doJSON(t, handler, http.MethodPatch, "/users/"+strconv.FormatInt(createdUser.ID, 10), updateBody, http.StatusOK), &updatedUser)
	if updatedUser.Username != "alice-updated" || updatedUser.Profile["display_name"] != "Alice Updated" {
		t.Fatalf("unexpected updated user: %+v", updatedUser)
	}

	createMessageBody := map[string]any{
		"user_id": createdUser.ID,
		"sender":  "orders",
		"body":    "package shipped",
		"metadata": map[string]any{
			"order_id": "A1001",
		},
	}
	var createdMessage struct {
		ID       int64             `json:"id"`
		UserID   int64             `json:"user_id"`
		Metadata map[string]string `json:"metadata"`
	}
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/messages", createMessageBody, http.StatusCreated), &createdMessage)
	if createdMessage.ID == 0 || createdMessage.UserID != createdUser.ID {
		t.Fatalf("unexpected created message: %+v", createdMessage)
	}

	var listMessages struct {
		Count int `json:"count"`
		Items []struct {
			ID   int64  `json:"id"`
			Body string `json:"body"`
		} `json:"items"`
	}
	mustJSON(t, doJSON(t, handler, http.MethodGet, "/users/"+strconv.FormatInt(createdUser.ID, 10)+"/messages?limit=10", nil, http.StatusOK), &listMessages)
	if listMessages.Count != 1 || len(listMessages.Items) != 1 || listMessages.Items[0].Body != "package shipped" {
		t.Fatalf("unexpected messages: %+v", listMessages)
	}

	var listEvents struct {
		Count int `json:"count"`
		Items []struct {
			Kind string `json:"kind"`
		} `json:"items"`
	}
	mustJSON(t, doJSON(t, handler, http.MethodGet, "/events?after=0&limit=10", nil, http.StatusOK), &listEvents)
	if listEvents.Count != 3 {
		t.Fatalf("expected 3 events, got %+v", listEvents)
	}

	body := doJSON(t, handler, http.MethodDelete, "/users/"+strconv.FormatInt(createdUser.ID, 10), nil, http.StatusOK)
	var deleteResp struct {
		Status string `json:"status"`
	}
	mustJSON(t, body, &deleteResp)
	if deleteResp.Status != "deleted" {
		t.Fatalf("unexpected delete response: %+v", deleteResp)
	}

	doJSON(t, handler, http.MethodGet, "/users/"+strconv.FormatInt(createdUser.ID, 10), nil, http.StatusNotFound)
}

func TestCreateUserAllowsDuplicateUsername(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t)

	createUserBody := map[string]any{
		"username":      "alice",
		"password_hash": "hash-1",
	}
	doJSON(t, handler, http.MethodPost, "/users", createUserBody, http.StatusCreated)
	doJSON(t, handler, http.MethodPost, "/users", createUserBody, http.StatusCreated)
}

func TestUpdateUserAllowsDuplicateUsername(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t)

	var first struct {
		ID int64 `json:"id"`
	}
	var second struct {
		ID int64 `json:"id"`
	}

	mustJSON(t, doJSON(t, handler, http.MethodPost, "/users", map[string]any{
		"username":      "alice",
		"password_hash": "hash-1",
	}, http.StatusCreated), &first)
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/users", map[string]any{
		"username":      "bob",
		"password_hash": "hash-2",
	}, http.StatusCreated), &second)

	doJSON(t, handler, http.MethodPatch, "/users/"+strconv.FormatInt(second.ID, 10), map[string]any{
		"username": "alice",
	}, http.StatusOK)
}

type gatingSink struct {
	allow bool
}

func (s gatingSink) Publish(store.Event) {}

func (s gatingSink) AllowWrite(context.Context) error {
	if s.allow {
		return nil
	}
	return app.ErrClockNotSynchronized
}

func TestWriteEndpointsReturn503WhenClockIsNotSynchronized(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "api-gated.db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:   "node-a",
		NodeSlot: 1,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	user, _, err := st.CreateUser(context.Background(), store.CreateUserParams{
		Username:     "existing-user",
		PasswordHash: "hash-1",
	})
	if err != nil {
		t.Fatalf("seed user: %v", err)
	}

	handler := NewHTTP(New(st, gatingSink{})).Handler()

	for _, tc := range []struct {
		method string
		path   string
		body   any
	}{
		{
			method: http.MethodPost,
			path:   "/users",
			body: map[string]any{
				"username":      "alice",
				"password_hash": "hash-1",
			},
		},
		{
			method: http.MethodPost,
			path:   "/messages",
			body: map[string]any{
				"user_id": user.ID,
				"sender":  "orders",
				"body":    "package shipped",
			},
		},
		{
			method: http.MethodPatch,
			path:   "/users/" + strconv.FormatInt(user.ID, 10),
			body: map[string]any{
				"username": "renamed-user",
			},
		},
		{
			method: http.MethodDelete,
			path:   "/users/" + strconv.FormatInt(user.ID, 10),
			body:   nil,
		},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			data := doJSON(t, handler, tc.method, tc.path, tc.body, http.StatusServiceUnavailable)
			var payload map[string]string
			mustJSON(t, data, &payload)
			if payload["error"] != app.ErrClockNotSynchronized.Error() {
				t.Fatalf("unexpected error payload: %+v", payload)
			}
		})
	}
}

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "api.db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:   "node-a",
		NodeSlot: 1,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})

	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	return NewHTTP(New(st, nil)).Handler()
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any, wantStatus int) []byte {
	t.Helper()

	var reqBody *bytes.Reader
	if body == nil {
		reqBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reqBody = bytes.NewReader(payload)
	}

	req := httptest.NewRequest(method, path, reqBody)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != wantStatus {
		t.Fatalf("unexpected status for %s %s: got=%d want=%d body=%s", method, path, rr.Code, wantStatus, rr.Body.String())
	}
	return rr.Body.Bytes()
}

func mustJSON(t *testing.T, data []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("unmarshal json: %v body=%s", err, string(data))
	}
}
