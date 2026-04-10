package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"notifier/internal/security"
	"notifier/internal/store"
)

func TestRegisterLoginPushAndListNotifications(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	serviceToken, err := security.GenerateOpaqueToken("ntfsk_")
	if err != nil {
		t.Fatalf("generate service token: %v", err)
	}
	if _, err := st.CreateServiceKey(context.Background(), "orders-service", security.HashToken(serviceToken), "ntfsk_"); err != nil {
		t.Fatalf("create service key: %v", err)
	}

	handler := New(st, time.Hour).Handler()

	registerBody := map[string]string{"username": "alice", "password": "password123"}
	var registered store.User
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/api/v1/users/register", registerBody, nil, http.StatusCreated), &registered)
	if registered.PushID == "" {
		t.Fatalf("expected push id")
	}
	if registered.Role != store.UserRoleAdmin {
		t.Fatalf("expected bootstrap user to be admin, got %q", registered.Role)
	}

	loginResp := loginUser(t, handler, "alice", "password123")
	if loginResp.Token == "" {
		t.Fatalf("expected login token")
	}
	if loginResp.User.Role != store.UserRoleAdmin {
		t.Fatalf("expected admin role in login response, got %q", loginResp.User.Role)
	}

	var me store.User
	mustJSON(t, doJSON(t, handler, http.MethodGet, "/api/v1/users/me", nil, bearerHeaders(loginResp.Token), http.StatusOK), &me)
	if me.Role != store.UserRoleAdmin {
		t.Fatalf("expected /me to include admin role, got %q", me.Role)
	}

	pushBody := map[string]any{
		"push_id": registered.PushID,
		"sender":  "orders-service",
		"title":   "new order",
		"body":    "your order has been shipped",
		"metadata": map[string]any{
			"order_id": "A1001",
		},
	}
	pushHeaders := map[string]string{"X-API-Key": serviceToken}
	doJSON(t, handler, http.MethodPost, "/api/v1/push", pushBody, pushHeaders, http.StatusAccepted)

	respBody := doJSON(t, handler, http.MethodGet, "/api/v1/notifications?limit=10", nil, bearerHeaders(loginResp.Token), http.StatusOK)
	var listResp struct {
		Count int                  `json:"count"`
		Items []store.Notification `json:"items"`
	}
	mustJSON(t, respBody, &listResp)
	if listResp.Count != 1 || len(listResp.Items) != 1 {
		t.Fatalf("expected one notification, got %+v", listResp)
	}
	if listResp.Items[0].Title != "new order" {
		t.Fatalf("unexpected notification title: %+v", listResp.Items[0])
	}
}

func TestSecondPublicRegistrationRejected(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	handler := New(st, time.Hour).Handler()
	registerBody := map[string]string{"username": "alice", "password": "password123"}

	doJSON(t, handler, http.MethodPost, "/api/v1/users/register", registerBody, nil, http.StatusCreated)

	body := doJSON(t, handler, http.MethodPost, "/api/v1/users/register", map[string]string{
		"username": "bob",
		"password": "password456",
	}, nil, http.StatusForbidden)
	assertErrorMessage(t, body, "public registration is closed")
}

func TestAdminUserManagement(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	handler := New(st, time.Hour).Handler()

	doJSON(t, handler, http.MethodPost, "/api/v1/users/register", map[string]string{
		"username": "alice",
		"password": "password123",
	}, nil, http.StatusCreated)
	adminLogin := loginUser(t, handler, "alice", "password123")
	adminHeaders := bearerHeaders(adminLogin.Token)

	var bob store.User
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/api/v1/admin/users", map[string]string{
		"username": "bob",
		"password": "password456",
		"role":     store.UserRoleUser,
	}, adminHeaders, http.StatusCreated), &bob)
	if bob.Role != store.UserRoleUser {
		t.Fatalf("expected bob to be user, got %q", bob.Role)
	}

	var carol store.User
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/api/v1/admin/users", map[string]string{
		"username": "carol",
		"password": "password789",
		"role":     store.UserRoleAdmin,
	}, adminHeaders, http.StatusCreated), &carol)
	if carol.Role != store.UserRoleAdmin {
		t.Fatalf("expected carol to be admin, got %q", carol.Role)
	}

	var listResp struct {
		Count int          `json:"count"`
		Items []store.User `json:"items"`
	}
	mustJSON(t, doJSON(t, handler, http.MethodGet, "/api/v1/admin/users", nil, adminHeaders, http.StatusOK), &listResp)
	if listResp.Count != 3 {
		t.Fatalf("expected 3 users, got %d", listResp.Count)
	}

	var updatedBob store.User
	mustJSON(t, doJSON(t, handler, http.MethodPatch, "/api/v1/admin/users/"+itoa(bob.ID), map[string]string{
		"role":     store.UserRoleAdmin,
		"password": "newpassword456",
	}, adminHeaders, http.StatusOK), &updatedBob)
	if updatedBob.Role != store.UserRoleAdmin {
		t.Fatalf("expected bob to become admin, got %q", updatedBob.Role)
	}

	assertErrorMessage(t, doJSON(t, handler, http.MethodPost, "/api/v1/users/login", map[string]string{
		"username": "bob",
		"password": "password456",
	}, nil, http.StatusUnauthorized), "invalid auth: invalid username or password")

	bobLogin := loginUser(t, handler, "bob", "newpassword456")
	if bobLogin.User.Role != store.UserRoleAdmin {
		t.Fatalf("expected bob login role to be admin, got %q", bobLogin.User.Role)
	}

	doJSON(t, handler, http.MethodDelete, "/api/v1/admin/users/"+itoa(carol.ID), nil, adminHeaders, http.StatusOK)

	mustJSON(t, doJSON(t, handler, http.MethodGet, "/api/v1/admin/users", nil, adminHeaders, http.StatusOK), &listResp)
	if listResp.Count != 2 {
		t.Fatalf("expected 2 users after delete, got %d", listResp.Count)
	}
}

func TestNonAdminForbiddenOnAdminEndpoints(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	handler := New(st, time.Hour).Handler()

	doJSON(t, handler, http.MethodPost, "/api/v1/users/register", map[string]string{
		"username": "alice",
		"password": "password123",
	}, nil, http.StatusCreated)
	adminLogin := loginUser(t, handler, "alice", "password123")

	var bob store.User
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/api/v1/admin/users", map[string]string{
		"username": "bob",
		"password": "password456",
		"role":     store.UserRoleUser,
	}, bearerHeaders(adminLogin.Token), http.StatusCreated), &bob)

	bobLogin := loginUser(t, handler, "bob", "password456")
	body := doJSON(t, handler, http.MethodGet, "/api/v1/admin/users", nil, bearerHeaders(bobLogin.Token), http.StatusForbidden)
	assertErrorMessage(t, body, "admin access required")
}

func TestLastAdminCannotBeModifiedOrDeleted(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	handler := New(st, time.Hour).Handler()

	var alice store.User
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/api/v1/users/register", map[string]string{
		"username": "alice",
		"password": "password123",
	}, nil, http.StatusCreated), &alice)

	adminLogin := loginUser(t, handler, "alice", "password123")
	headers := bearerHeaders(adminLogin.Token)

	assertErrorMessage(t, doJSON(t, handler, http.MethodPatch, "/api/v1/admin/users/"+itoa(alice.ID), map[string]string{
		"role": store.UserRoleUser,
	}, headers, http.StatusConflict), "cannot modify the last admin")

	assertErrorMessage(t, doJSON(t, handler, http.MethodDelete, "/api/v1/admin/users/"+itoa(alice.ID), nil, headers, http.StatusConflict), "cannot modify the last admin")
}

func TestWebUILoginAndDashboard(t *testing.T) {
	t.Parallel()

	st := openTestStore(t)
	defer st.Close()

	passwordHash, err := security.HashPassword("password123")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	user, err := st.CreateUser(context.Background(), "alice", passwordHash, store.UserRoleAdmin)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if _, err := st.CreateNotification(context.Background(), user.ID, user.PushID, "orders-service", "welcome", "hello from dashboard", `{"kind":"welcome"}`); err != nil {
		t.Fatalf("create notification: %v", err)
	}

	handler := New(st, time.Hour).Handler()

	loginForm := url.Values{
		"username": {"alice"},
		"password": {"password123"},
	}
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(loginForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("unexpected login status: %d, body=%s", rr.Code, rr.Body.String())
	}
	if location := rr.Header().Get("Location"); location != "/dashboard" {
		t.Fatalf("unexpected redirect location: %s", location)
	}
	cookies := rr.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatalf("expected session cookie")
	}

	dashboardReq := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	for _, cookie := range cookies {
		dashboardReq.AddCookie(cookie)
	}
	dashboardRR := httptest.NewRecorder()
	handler.ServeHTTP(dashboardRR, dashboardReq)
	if dashboardRR.Code != http.StatusOK {
		t.Fatalf("unexpected dashboard status: %d, body=%s", dashboardRR.Code, dashboardRR.Body.String())
	}

	body := dashboardRR.Body.String()
	if !strings.Contains(body, "欢迎，alice") {
		t.Fatalf("dashboard missing username: %s", body)
	}
	if !strings.Contains(body, user.PushID) {
		t.Fatalf("dashboard missing push id: %s", body)
	}
	if !strings.Contains(body, "welcome") {
		t.Fatalf("dashboard missing notification title: %s", body)
	}
	if !strings.Contains(body, "当前角色：管理员") {
		t.Fatalf("dashboard missing role label: %s", body)
	}
}

func openTestStore(t *testing.T) *store.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "notifier.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		st.Close()
		t.Fatalf("init store: %v", err)
	}
	return st
}

func loginUser(t *testing.T, handler http.Handler, username, password string) loginResponse {
	t.Helper()

	var resp loginResponse
	mustJSON(t, doJSON(t, handler, http.MethodPost, "/api/v1/users/login", map[string]string{
		"username": username,
		"password": password,
	}, nil, http.StatusOK), &resp)
	return resp
}

func bearerHeaders(token string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token}
}

func assertErrorMessage(t *testing.T, body []byte, want string) {
	t.Helper()

	var resp map[string]string
	mustJSON(t, body, &resp)
	if resp["error"] != want {
		t.Fatalf("unexpected error message %q, want %q", resp["error"], want)
	}
}

func itoa(value int64) string {
	return strconv.FormatInt(value, 10)
}

func doJSON(t *testing.T, handler http.Handler, method, path string, body any, headers map[string]string, wantStatus int) []byte {
	t.Helper()

	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(payload))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != wantStatus {
		t.Fatalf("unexpected status %d, want %d, body=%s", rr.Code, wantStatus, rr.Body.String())
	}
	return rr.Body.Bytes()
}

func mustJSON(t *testing.T, data []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("unmarshal response: %v, body=%s", err, string(data))
	}
}
