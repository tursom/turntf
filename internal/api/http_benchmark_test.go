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
	"time"

	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/testutil/benchroot"
)

const apiBenchmarkWarmupPasses = 1

func BenchmarkHTTPCreateMessageAuthenticated(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, engine := range []string{store.EngineSQLite, store.EnginePebble} {
				b.Run(engine, func(b *testing.B) {
					benchmarkHTTPCreateMessageAuthenticated(b, mode, engine)
				})
			}
		})
	}
}

func BenchmarkHTTPListMessagesByUserAuthenticated(b *testing.B) {
	for _, mode := range benchroot.Modes(b) {
		mode := mode
		b.Run(mode.Name(), func(b *testing.B) {
			for _, engine := range []string{store.EngineSQLite, store.EnginePebble} {
				b.Run(engine, func(b *testing.B) {
					benchmarkHTTPListMessagesByUserAuthenticated(b, mode, engine)
				})
			}
		})
	}
}

func benchmarkHTTPCreateMessageAuthenticated(b *testing.B, mode benchroot.Mode, engine string) {
	testAPI, closeAPI := openBenchmarkAuthenticatedTestAPI(b, mode, engine)
	b.Cleanup(closeAPI)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := benchmarkLoginToken(b, testAPI.handler, adminKey, "root-password")
	userKey := benchmarkCreateUserAs(b, testAPI.handler, adminToken, "bench-create-message", "bench-password", store.RoleUser)
	headers := map[string]string{"Authorization": "Bearer " + adminToken}
	payload := bytes.Repeat([]byte("m"), 256)

	runAPIBenchmarkWarmup(b, func() {
		mustBenchmarkHTTPCreateMessageAuthenticatedOnce(b, testAPI.handler, headers, userKey, payload)
	})
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		mustBenchmarkHTTPCreateMessageAuthenticatedOnce(b, testAPI.handler, headers, userKey, payload)
	}
}

func benchmarkHTTPListMessagesByUserAuthenticated(b *testing.B, mode benchroot.Mode, engine string) {
	const history = 100
	const limit = 50

	ctx := context.Background()
	testAPI, closeAPI := openBenchmarkAuthenticatedTestAPI(b, mode, engine)
	b.Cleanup(closeAPI)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := benchmarkLoginToken(b, testAPI.handler, adminKey, "root-password")
	userKey := benchmarkCreateUserAs(b, testAPI.handler, adminToken, "bench-list-message", "bench-password", store.RoleUser)
	headers := map[string]string{"Authorization": "Bearer " + adminToken}

	expectedBodies := make([][]byte, 0, history)
	for i := 0; i < history; i++ {
		body := []byte("http-bench-message-" + strconv.Itoa(i))
		if _, _, err := testAPI.http.service.CreateMessage(ctx, store.CreateMessageParams{
			UserKey: userKey,
			Sender:  userKey,
			Body:    body,
		}); err != nil {
			b.Fatalf("seed list message %d: %v", i, err)
		}
		expectedBodies = append(expectedBodies, body)
	}
	wantFirst := expectedBodies[len(expectedBodies)-1]
	wantLast := expectedBodies[len(expectedBodies)-limit]

	runAPIBenchmarkWarmup(b, func() {
		mustBenchmarkHTTPListMessagesByUserAuthenticatedOnce(b, testAPI.handler, headers, userKey, limit, wantFirst, wantLast)
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustBenchmarkHTTPListMessagesByUserAuthenticatedOnce(b, testAPI.handler, headers, userKey, limit, wantFirst, wantLast)
	}
}

func openBenchmarkAuthenticatedTestAPI(tb testing.TB, mode benchroot.Mode, engine string) (authenticatedTestAPI, func()) {
	tb.Helper()

	dir, cleanupDir := mode.MkdirTemp(tb, "turntf-api-bench-*")
	opts := store.Options{NodeID: testNodeID(1), Engine: engine}
	if engine == store.EnginePebble {
		opts.PebblePath = filepath.Join(dir, "api.pebble")
	}
	st, err := store.Open(filepath.Join(dir, "api.db"), opts)
	if err != nil {
		cleanupDir()
		tb.Fatalf("open benchmark api store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("init benchmark api store: %v", err)
	}
	if err := st.EnsureBootstrapAdmin(context.Background(), store.BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: benchmarkMustHashPassword(tb, "root-password"),
	}); err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("ensure bootstrap admin: %v", err)
	}

	signer, err := auth.NewSigner("token-secret")
	if err != nil {
		_ = st.Close()
		cleanupDir()
		tb.Fatalf("new signer: %v", err)
	}

	httpAPI := NewHTTP(New(st, nil), HTTPOptions{
		NodeID:   testNodeID(1),
		Signer:   signer,
		TokenTTL: time.Hour,
	})
	return authenticatedTestAPI{
			handler: httpAPI.Handler(),
			http:    httpAPI,
		}, func() {
			_ = st.Close()
			cleanupDir()
		}
}

func benchmarkLoginToken(tb testing.TB, handler http.Handler, key store.UserKey, password string) string {
	tb.Helper()

	var response struct {
		Token string `json:"token"`
	}
	benchmarkMustJSON(tb, benchmarkDoJSONWithHeaders(tb, handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  key.NodeID,
		"user_id":  key.UserID,
		"password": password,
	}, nil, http.StatusOK), &response)
	if response.Token == "" {
		tb.Fatalf("expected login token")
	}
	return response.Token
}

func benchmarkCreateUserAs(tb testing.TB, handler http.Handler, token, username, password, role string) store.UserKey {
	tb.Helper()

	var response struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	benchmarkMustJSON(tb, benchmarkDoJSONWithHeaders(tb, handler, http.MethodPost, "/users", map[string]any{
		"username": username,
		"password": password,
		"role":     role,
	}, map[string]string{
		"Authorization": "Bearer " + token,
	}, http.StatusCreated), &response)
	key := store.UserKey{NodeID: response.NodeID, UserID: response.UserID}
	if err := key.Validate(); err != nil {
		tb.Fatalf("expected created user id")
	}
	return key
}

func benchmarkDoJSONWithHeaders(tb testing.TB, handler http.Handler, method, path string, body any, headers map[string]string, wantStatus int) []byte {
	tb.Helper()

	var reqBody *bytes.Reader
	if body == nil {
		reqBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			tb.Fatalf("marshal body: %v", err)
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
		tb.Fatalf("unexpected status for %s %s: got=%d want=%d body=%s", method, path, rr.Code, wantStatus, rr.Body.String())
	}
	return rr.Body.Bytes()
}

func benchmarkMustJSON(tb testing.TB, data []byte, dst any) {
	tb.Helper()
	if err := json.Unmarshal(data, dst); err != nil {
		tb.Fatalf("unmarshal json: %v body=%s", err, string(data))
	}
}

func benchmarkMustHashPassword(tb testing.TB, password string) string {
	tb.Helper()
	hash, err := auth.HashPassword(password)
	if err != nil {
		tb.Fatalf("hash password: %v", err)
	}
	return hash
}

func runAPIBenchmarkWarmup(tb testing.TB, fn func()) {
	tb.Helper()
	for i := 0; i < apiBenchmarkWarmupPasses; i++ {
		fn()
	}
}

func mustBenchmarkHTTPCreateMessageAuthenticatedOnce(tb testing.TB, handler http.Handler, headers map[string]string, userKey store.UserKey, payload []byte) {
	tb.Helper()

	var response struct {
		Recipient store.UserKey `json:"recipient"`
		NodeID    int64         `json:"node_id"`
		Seq       int64         `json:"seq"`
	}
	benchmarkMustJSON(tb, benchmarkDoJSONWithHeaders(tb, handler, http.MethodPost, userMessagesPath(userKey.NodeID, userKey.UserID), map[string]any{
		"body": payload,
	}, headers, http.StatusCreated), &response)
	if response.Recipient != userKey || response.NodeID != testNodeID(1) || response.Seq <= 0 {
		tb.Fatalf("unexpected create message response: %+v", response)
	}
}

func mustBenchmarkHTTPListMessagesByUserAuthenticatedOnce(tb testing.TB, handler http.Handler, headers map[string]string, userKey store.UserKey, limit int, wantFirst, wantLast []byte) {
	tb.Helper()

	var response struct {
		Count int `json:"count"`
		Items []struct {
			Body []byte `json:"body"`
		} `json:"items"`
	}
	benchmarkMustJSON(tb, benchmarkDoJSONWithHeaders(tb, handler, http.MethodGet, userMessagesPath(userKey.NodeID, userKey.UserID)+"?limit="+strconv.Itoa(limit), nil, headers, http.StatusOK), &response)
	if response.Count != limit || len(response.Items) != limit {
		tb.Fatalf("unexpected list message count: %+v", response)
	}
	if !bytes.Equal(response.Items[0].Body, wantFirst) || !bytes.Equal(response.Items[len(response.Items)-1].Body, wantLast) {
		tb.Fatalf("unexpected list message ordering: first=%q last=%q", response.Items[0].Body, response.Items[len(response.Items)-1].Body)
	}
}
