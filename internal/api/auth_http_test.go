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
	"time"

	"notifier/internal/auth"
	"notifier/internal/store"
)

type authenticatedTestAPI struct {
	handler http.Handler
}

func TestAuthenticatedHTTPLoginAndAuthorization(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "unauthorized",
		"password": "unauthorized-password",
	}, nil, http.StatusUnauthorized)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"user_id":  store.BootstrapAdminUserID,
		"password": "wrong",
	}, nil, http.StatusUnauthorized)

	adminToken := loginToken(t, testAPI.handler, store.BootstrapAdminUserID, "root-password")
	aliceID := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	aliceToken := loginToken(t, testAPI.handler, aliceID, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/status", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/status", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/metrics", nil, nil, http.StatusUnauthorized)
	metrics := doPlain(t, testAPI.handler, http.MethodGet, "/metrics", map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	if !strings.Contains(metrics, "notifier_write_gate_ready") {
		t.Fatalf("metrics missing write gate gauge: %s", metrics)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/users/"+strconv.FormatInt(aliceID, 10), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK)

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/users/"+strconv.FormatInt(store.BootstrapAdminUserID, 10), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "bob",
		"password": "bob-password",
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/messages", map[string]any{
		"user_id": aliceID,
		"sender":  "alice",
		"body":    "hello",
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/messages", map[string]any{
		"user_id": store.BootstrapAdminUserID,
		"sender":  "alice",
		"body":    "forbidden",
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
}

func newAuthenticatedTestAPI(t *testing.T) authenticatedTestAPI {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "auth-api.db")
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
	if err := st.EnsureBootstrapAdmin(context.Background(), store.BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: mustHashPassword(t, "root-password"),
	}); err != nil {
		t.Fatalf("ensure bootstrap admin: %v", err)
	}

	signer, err := auth.NewSigner("token-secret")
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}

	return authenticatedTestAPI{
		handler: NewHTTP(New(st, nil), HTTPOptions{
			NodeID:   testNodeID(1),
			Signer:   signer,
			TokenTTL: time.Hour,
		}).Handler(),
	}
}

func loginToken(t *testing.T, handler http.Handler, userID int64, password string) string {
	t.Helper()

	var response struct {
		Token string `json:"token"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/auth/login", map[string]any{
		"user_id":  userID,
		"password": password,
	}, nil, http.StatusOK), &response)
	if response.Token == "" {
		t.Fatalf("expected login token")
	}
	return response.Token
}

func createUserAs(t *testing.T, handler http.Handler, token, username, password, role string) int64 {
	t.Helper()

	var response struct {
		ID int64 `json:"id"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/users", map[string]any{
		"username": username,
		"password": password,
		"role":     role,
	}, map[string]string{
		"Authorization": "Bearer " + token,
	}, http.StatusCreated), &response)
	if response.ID == 0 {
		t.Fatalf("expected created user id")
	}
	return response.ID
}

func mustHashPassword(t *testing.T, password string) string {
	t.Helper()

	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return hash
}

func doJSONWithHeaders(t *testing.T, handler http.Handler, method, path string, body any, headers map[string]string, wantStatus int) []byte {
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
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != wantStatus {
		t.Fatalf("unexpected status for %s %s: got=%d want=%d body=%s", method, path, rr.Code, wantStatus, rr.Body.String())
	}
	return rr.Body.Bytes()
}
