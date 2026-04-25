package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/store"
)

func BenchmarkHTTPCreateMessageAuthenticated(b *testing.B) {
	for _, engine := range []string{store.EngineSQLite, store.EnginePebble} {
		b.Run(engine, func(b *testing.B) {
			benchmarkHTTPCreateMessageAuthenticated(b, engine)
		})
	}
}

func BenchmarkHTTPListMessagesByUserAuthenticated(b *testing.B) {
	for _, engine := range []string{store.EngineSQLite, store.EnginePebble} {
		b.Run(engine, func(b *testing.B) {
			benchmarkHTTPListMessagesByUserAuthenticated(b, engine)
		})
	}
}

func benchmarkHTTPCreateMessageAuthenticated(b *testing.B, engine string) {
	testAPI, closeAPI := openBenchmarkAuthenticatedTestAPI(b, engine)
	b.Cleanup(closeAPI)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := benchmarkLoginToken(b, testAPI.handler, adminKey, "root-password")
	userKey := benchmarkCreateUserAs(b, testAPI.handler, adminToken, "bench-create-message", "bench-password", store.RoleUser)
	headers := map[string]string{"Authorization": "Bearer " + adminToken}
	payload := bytes.Repeat([]byte("m"), 256)

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var response struct {
			Recipient store.UserKey `json:"recipient"`
			NodeID    int64         `json:"node_id"`
			Seq       int64         `json:"seq"`
		}
		benchmarkMustJSON(b, benchmarkDoJSONWithHeaders(b, testAPI.handler, http.MethodPost, userMessagesPath(userKey.NodeID, userKey.UserID), map[string]any{
			"body": payload,
		}, headers, http.StatusCreated), &response)
		if response.Recipient != userKey || response.NodeID != testNodeID(1) || response.Seq <= 0 {
			b.Fatalf("unexpected create message response: %+v", response)
		}
	}
}

func benchmarkHTTPListMessagesByUserAuthenticated(b *testing.B, engine string) {
	const history = 100
	const limit = 50

	ctx := context.Background()
	testAPI, closeAPI := openBenchmarkAuthenticatedTestAPI(b, engine)
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var response struct {
			Count int `json:"count"`
			Items []struct {
				Body []byte `json:"body"`
			} `json:"items"`
		}
		benchmarkMustJSON(b, benchmarkDoJSONWithHeaders(b, testAPI.handler, http.MethodGet, userMessagesPath(userKey.NodeID, userKey.UserID)+"?limit=50", nil, headers, http.StatusOK), &response)
		if response.Count != limit || len(response.Items) != limit {
			b.Fatalf("unexpected list message count: %+v", response)
		}
		if !bytes.Equal(response.Items[0].Body, wantFirst) || !bytes.Equal(response.Items[len(response.Items)-1].Body, wantLast) {
			b.Fatalf("unexpected list message ordering: first=%q last=%q", response.Items[0].Body, response.Items[len(response.Items)-1].Body)
		}
	}
}

func openBenchmarkAuthenticatedTestAPI(tb testing.TB, engine string) (authenticatedTestAPI, func()) {
	tb.Helper()

	dir, err := os.MkdirTemp("", "turntf-api-bench-*")
	if err != nil {
		tb.Fatalf("create benchmark api temp dir: %v", err)
	}
	opts := store.Options{NodeID: testNodeID(1), Engine: engine}
	if engine == store.EnginePebble {
		opts.PebblePath = filepath.Join(dir, "api.pebble")
	}
	st, err := store.Open(filepath.Join(dir, "api.db"), opts)
	if err != nil {
		_ = os.RemoveAll(dir)
		tb.Fatalf("open benchmark api store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		_ = st.Close()
		_ = os.RemoveAll(dir)
		tb.Fatalf("init benchmark api store: %v", err)
	}
	if err := st.EnsureBootstrapAdmin(context.Background(), store.BootstrapAdminConfig{
		Username:     "root",
		PasswordHash: benchmarkMustHashPassword(tb, "root-password"),
	}); err != nil {
		_ = st.Close()
		_ = os.RemoveAll(dir)
		tb.Fatalf("ensure bootstrap admin: %v", err)
	}

	signer, err := auth.NewSigner("token-secret")
	if err != nil {
		_ = st.Close()
		_ = os.RemoveAll(dir)
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
			_ = os.RemoveAll(dir)
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
