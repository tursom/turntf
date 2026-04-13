package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/store"
)

func TestUserAndMessageHTTPAPI(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")

	createUserBody := map[string]any{
		"username": "alice",
		"password": "password-1",
		"profile": map[string]any{
			"display_name": "Alice",
		},
	}

	var createdUser struct {
		NodeID   int64             `json:"node_id"`
		UserID   int64             `json:"user_id"`
		Username string            `json:"username"`
		Profile  map[string]string `json:"profile"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/users", createUserBody, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &createdUser)
	if createdUser.NodeID == 0 || createdUser.UserID == 0 {
		t.Fatalf("expected created user id")
	}
	if createdUser.Username != "alice" {
		t.Fatalf("unexpected created user: %+v", createdUser)
	}

	var loadedUser struct {
		NodeID   int64             `json:"node_id"`
		UserID   int64             `json:"user_id"`
		Username string            `json:"username"`
		Profile  map[string]string `json:"profile"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userPath(createdUser.NodeID, createdUser.UserID), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &loadedUser)
	if loadedUser.NodeID != createdUser.NodeID || loadedUser.UserID != createdUser.UserID {
		t.Fatalf("unexpected loaded user: %+v", loadedUser)
	}

	updateBody := map[string]any{
		"username": "alice-updated",
		"password": "password-2",
		"profile": map[string]any{
			"display_name": "Alice Updated",
		},
	}
	var updatedUser struct {
		Username string            `json:"username"`
		Profile  map[string]string `json:"profile"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPatch, userPath(createdUser.NodeID, createdUser.UserID), updateBody, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &updatedUser)
	if updatedUser.Username != "alice-updated" || updatedUser.Profile["display_name"] != "Alice Updated" {
		t.Fatalf("unexpected updated user: %+v", updatedUser)
	}

	createMessageBody := map[string]any{
		"body": []byte("package shipped"),
	}
	var createdMessage struct {
		UserNodeID int64 `json:"user_node_id"`
		UserID     int64 `json:"user_id"`
		NodeID     int64 `json:"node_id"`
		Seq        int64 `json:"seq"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, userMessagesPath(createdUser.NodeID, createdUser.UserID), createMessageBody, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &createdMessage)
	if createdMessage.UserNodeID != createdUser.NodeID || createdMessage.UserID != createdUser.UserID || createdMessage.NodeID != testNodeID(1) || createdMessage.Seq != 1 {
		t.Fatalf("unexpected created message: %+v", createdMessage)
	}

	var listMessages struct {
		Count int `json:"count"`
		Items []struct {
			UserNodeID int64  `json:"user_node_id"`
			UserID     int64  `json:"user_id"`
			NodeID     int64  `json:"node_id"`
			Seq        int64  `json:"seq"`
			Body       []byte `json:"body"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMessagesPath(createdUser.NodeID, createdUser.UserID)+"?limit=10", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &listMessages)
	if listMessages.Count != 1 || len(listMessages.Items) != 1 ||
		listMessages.Items[0].UserNodeID != createdUser.NodeID ||
		listMessages.Items[0].UserID != createdUser.UserID ||
		listMessages.Items[0].NodeID != testNodeID(1) ||
		listMessages.Items[0].Seq != 1 ||
		string(listMessages.Items[0].Body) != "package shipped" {
		t.Fatalf("unexpected messages: %+v", listMessages)
	}

	var listEvents struct {
		Count int `json:"count"`
		Items []struct {
			EventType string `json:"event_type"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/events?after=0&limit=10", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &listEvents)
	if listEvents.Count < 6 {
		t.Fatalf("expected bootstrap and test events, got %+v", listEvents)
	}
	if len(listEvents.Items) != listEvents.Count ||
		listEvents.Items[len(listEvents.Items)-3].EventType != "user_created" ||
		listEvents.Items[len(listEvents.Items)-2].EventType != "user_updated" ||
		listEvents.Items[len(listEvents.Items)-1].EventType != "message_created" {
		t.Fatalf("unexpected events payload: %+v", listEvents)
	}

	var opsStatus struct {
		NodeID            int64 `json:"node_id"`
		LastEventSequence int64 `json:"last_event_sequence"`
		ConflictTotal     int64 `json:"conflict_total"`
		MessageTrim       struct {
			TrimmedTotal int64 `json:"trimmed_total"`
		} `json:"message_trim"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/ops/status", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &opsStatus)
	if opsStatus.NodeID != testNodeID(1) || opsStatus.LastEventSequence < 6 {
		t.Fatalf("unexpected ops status: %+v", opsStatus)
	}

	metrics := doPlain(t, handler, http.MethodGet, "/metrics", map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	if !strings.Contains(metrics, `notifier_event_log_last_sequence{node_id="4096"} `) {
		t.Fatalf("metrics missing last sequence: %s", metrics)
	}

	body := doJSONWithHeaders(t, handler, http.MethodDelete, userPath(createdUser.NodeID, createdUser.UserID), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	var deleteResp struct {
		Status string `json:"status"`
	}
	mustJSON(t, body, &deleteResp)
	if deleteResp.Status != "deleted" {
		t.Fatalf("unexpected delete response: %+v", deleteResp)
	}

	doJSONWithHeaders(t, handler, http.MethodGet, userPath(createdUser.NodeID, createdUser.UserID), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusNotFound)
}

func TestCreateUserAllowsDuplicateUsername(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t)

	createUserBody := map[string]any{
		"username": "alice",
		"password": "password-1",
	}
	doJSON(t, handler, http.MethodPost, "/users", createUserBody, http.StatusCreated)
	doJSON(t, handler, http.MethodPost, "/users", createUserBody, http.StatusCreated)
}

func TestUpdateUserAllowsDuplicateUsername(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(t)

	var first struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	var second struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}

	mustJSON(t, doJSON(t, handler, http.MethodPost, "/users", map[string]any{
		"username": "alice",
		"password": "password-1",
	}, http.StatusCreated), &first)
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/users", map[string]any{
		"username": "bob",
		"password": "password-2",
	}, http.StatusCreated), &second)

	doJSON(t, handler, http.MethodPatch, userPath(second.NodeID, second.UserID), map[string]any{
		"username": "alice",
	}, http.StatusOK)
}

func TestListEventsReturnsTypedEventJSON(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")

	var createdUser struct {
		NodeID   int64  `json:"node_id"`
		UserID   int64  `json:"user_id"`
		Username string `json:"username"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/users", map[string]any{
		"username": "alice",
		"password": "password-1",
		"profile": map[string]any{
			"display_name": "Alice",
		},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &createdUser)

	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, userMessagesPath(createdUser.NodeID, createdUser.UserID), map[string]any{
		"body": []byte("package shipped"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &struct{}{})

	var listEvents struct {
		Count int `json:"count"`
		Items []struct {
			EventType string          `json:"event_type"`
			Event     json.RawMessage `json:"event"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/events?after=0&limit=10", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &listEvents)
	if listEvents.Count < 5 || len(listEvents.Items) != listEvents.Count {
		t.Fatalf("unexpected events payload: %+v", listEvents)
	}

	var userEvent struct {
		NodeID       int64  `json:"node_id"`
		UserID       int64  `json:"user_id"`
		Username     string `json:"username"`
		Profile      string `json:"profile"`
		CreatedAtHLC string `json:"created_at_hlc"`
	}
	userEventIndex := len(listEvents.Items) - 2
	if listEvents.Items[userEventIndex].EventType != "user_created" {
		t.Fatalf("unexpected user_created event type: %+v", listEvents.Items[userEventIndex])
	}
	mustJSON(t, listEvents.Items[userEventIndex].Event, &userEvent)
	if userEvent.NodeID != createdUser.NodeID || userEvent.UserID != createdUser.UserID || userEvent.Username != "alice" || userEvent.Profile != `{"display_name":"Alice"}` || userEvent.CreatedAtHLC == "" {
		t.Fatalf("unexpected user_created event json: %+v", userEvent)
	}

	var messageEvent struct {
		UserNodeID   int64  `json:"user_node_id"`
		UserID       int64  `json:"user_id"`
		NodeID       int64  `json:"node_id"`
		Seq          int64  `json:"seq"`
		SenderNodeID int64  `json:"sender_node_id"`
		SenderUserID int64  `json:"sender_user_id"`
		Body         []byte `json:"body"`
	}
	messageEventIndex := len(listEvents.Items) - 1
	if listEvents.Items[messageEventIndex].EventType != "message_created" {
		t.Fatalf("unexpected message_created event type: %+v", listEvents.Items[messageEventIndex])
	}
	mustJSON(t, listEvents.Items[messageEventIndex].Event, &messageEvent)
	if messageEvent.UserNodeID != createdUser.NodeID || messageEvent.UserID != createdUser.UserID || messageEvent.NodeID != testNodeID(1) || messageEvent.Seq != 1 || messageEvent.SenderNodeID != adminKey.NodeID || messageEvent.SenderUserID != adminKey.UserID || string(messageEvent.Body) != "package shipped" {
		t.Fatalf("unexpected message_created event json: %+v", messageEvent)
	}
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
		NodeID: testNodeID(1),
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

	signer, err := auth.NewSigner("gating-secret")
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	if err := st.EnsureBootstrapAdmin(context.Background(), store.BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: mustHashPassword(t, "root-password"),
	}); err != nil {
		t.Fatalf("bootstrap admin: %v", err)
	}
	handler := NewHTTP(New(st, gatingSink{}), HTTPOptions{
		NodeID:   testNodeID(1),
		Signer:   signer,
		TokenTTL: 0,
	}).Handler()
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")

	for _, tc := range []struct {
		method string
		path   string
		body   any
	}{
		{
			method: http.MethodPost,
			path:   "/users",
			body: map[string]any{
				"username": "alice",
				"password": "password-1",
			},
		},
		{
			method: http.MethodPost,
			path:   userMessagesPath(user.NodeID, user.ID),
			body: map[string]any{
				"body": []byte("package shipped"),
			},
		},
		{
			method: http.MethodPatch,
			path:   userPath(user.NodeID, user.ID),
			body: map[string]any{
				"username": "renamed-user",
			},
		},
		{
			method: http.MethodDelete,
			path:   userPath(user.NodeID, user.ID),
			body:   nil,
		},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			data := doJSONWithHeaders(t, handler, tc.method, tc.path, tc.body, map[string]string{
				"Authorization": "Bearer " + adminToken,
			}, http.StatusServiceUnavailable)
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
		NodeID: testNodeID(1),
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

func userPath(nodeID, userID int64) string {
	return "/nodes/" + strconv.FormatInt(nodeID, 10) + "/users/" + strconv.FormatInt(userID, 10)
}

func userMessagesPath(nodeID, userID int64) string {
	return userPath(nodeID, userID) + "/messages"
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

func doPlain(t *testing.T, handler http.Handler, method, path string, headers map[string]string, wantStatus int) string {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != wantStatus {
		t.Fatalf("unexpected status for %s %s: got=%d want=%d body=%s", method, path, rr.Code, wantStatus, rr.Body.String())
	}
	if wantStatus == http.StatusOK && !strings.HasPrefix(rr.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("unexpected content type: %s", rr.Header().Get("Content-Type"))
	}
	return rr.Body.String()
}
