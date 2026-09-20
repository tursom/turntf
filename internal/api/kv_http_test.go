package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tursom/turntf/internal/kv"
	"github.com/tursom/turntf/internal/store"
)

// 捕获通过 HTTP 认证后传入 Manager 的身份；未实现的方法不能意外成为测试依赖。
type kvIdentitySink struct {
	KVService
	principal kv.Principal
	owner     string
	target    string
}

func (*kvIdentitySink) Publish(store.Event) {}
func (s *kvIdentitySink) KVDatabases(_ context.Context, p kv.Principal, owner string) ([]kv.DatabaseInfo, uint64, error) {
	s.principal, s.owner = p, owner
	return []kv.DatabaseInfo{{Name: "config", Owner: owner}}, 9007199254740993, nil
}
func (s *kvIdentitySink) KVAccess(_ context.Context, p kv.Principal, database string) (kv.DatabaseAccess, uint64, error) {
	s.principal = p
	return kv.DatabaseAccess{Name: database, Owner: "1:2", ACL: map[string]kv.Permission{"1:2": 7}}, 1, nil
}
func (s *kvIdentitySink) KVTxn(_ context.Context, p kv.Principal, database string, compare []kv.Compare, puts []kv.Put, deletes []string) (kv.Result, error) {
	s.principal = p
	return kv.Result{Revision: 9007199254740993, Succeeded: false}, nil
}

func (s *kvIdentitySink) KVGrant(_ context.Context, p kv.Principal, database, target string, permission kv.Permission) (kv.Result, error) {
	s.principal, s.target = p, target
	return kv.Result{Revision: 1, Succeeded: true}, nil
}

func TestKVHTTPGrantValidatesAndNormalizesSubject(t *testing.T) {
	sink := &kvIdentitySink{}
	api := newAuthenticatedTestAPIWithSink(t, sink)
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	token := loginToken(t, api.handler, key, "root-password")
	headers := map[string]string{"Authorization": "Bearer " + token}
	for _, tc := range []struct{ input, want string }{{"01:002", "1:2"}, {"*", "*"}, {"1:9223372036854775807", "1:9223372036854775807"}} {
		doJSONWithHeaders(t, api.handler, http.MethodPut, "/kv/config/acl/"+tc.input, map[string]any{"permission": 3}, headers, http.StatusOK)
		if sink.target != tc.want {
			t.Fatalf("grant target=%q want=%q", sink.target, tc.want)
		}
	}
	for _, target := range []string{"0:2", "1:0", "1:9223372036854775808", "not-a-user"} {
		doJSONWithHeaders(t, api.handler, http.MethodPut, "/kv/config/acl/"+target, map[string]any{"permission": 1}, headers, http.StatusBadRequest)
	}
	doJSONWithHeaders(t, api.handler, http.MethodPut, "/kv/config/acl/1:2", map[string]any{"permission": 8}, headers, http.StatusBadRequest)
}

func TestKVHTTPDerivesSystemAdminFromAuthenticatedUser(t *testing.T) {
	sink := &kvIdentitySink{}
	api := newAuthenticatedTestAPIWithSink(t, sink)
	rootKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	rootToken := loginToken(t, api.handler, rootKey, "root-password")
	adminKey := createUserAs(t, api.handler, rootToken, "kv-admin", "admin-password", store.RoleAdmin)
	userKey := createUserAs(t, api.handler, rootToken, "kv-user", "user-password", store.RoleUser)
	for _, tc := range []struct {
		name, token string
		key         store.UserKey
		admin       bool
	}{
		{"super_admin", rootToken, rootKey, true},
		{"admin", loginToken(t, api.handler, adminKey, "admin-password"), adminKey, true},
		{"user", loginToken(t, api.handler, userKey, "user-password"), userKey, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 请求中的伪造身份标志不能覆盖认证结果。
			req := httptest.NewRequest(http.MethodGet, "/kv/databases?owner="+formatUserSubject(userKey)+"&system_admin=true", nil)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			req.Header.Set("X-System-Admin", "true")
			w := httptest.NewRecorder()
			api.handler.ServeHTTP(w, req)
			if w.Code != http.StatusOK || sink.principal.Subject != formatUserSubject(tc.key) || sink.principal.SystemAdmin != tc.admin {
				t.Fatalf("status=%d identity=%+v body=%s", w.Code, sink.principal, w.Body)
			}
			if sink.owner != formatUserSubject(userKey) || !strings.Contains(w.Body.String(), "9007199254740993") {
				t.Fatalf("owner/revision lost: %s", w.Body)
			}
		})
	}
	for _, path := range []string{"/kv/databases", "/kv/config/acl"} {
		w := httptest.NewRecorder()
		api.handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated %s: %d", path, w.Code)
		}
	}
	doJSONWithHeaders(t, api.handler, http.MethodGet, "/kv/databases?owner=invalid", nil, map[string]string{"Authorization": "Bearer " + rootToken}, http.StatusBadRequest)
	doJSONWithHeaders(t, api.handler, http.MethodGet, "/kv/config/acl", nil, map[string]string{"Authorization": "Bearer " + rootToken}, http.StatusOK)
	if !sink.principal.SystemAdmin {
		t.Fatal("ACL route lost administrator identity")
	}
}

func TestKVHTTPCompareFailurePreservesExistingResponseContract(t *testing.T) {
	sink := &kvIdentitySink{}
	api := newAuthenticatedTestAPIWithSink(t, sink)
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	token := loginToken(t, api.handler, key, "root-password")
	req := httptest.NewRequest(http.MethodPost, "/kv/config/txn", strings.NewReader(`{"compare":[{"key":"a","revision":9007199254740993}],"puts":[{"key":"a","value":"eA=="}]}`))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	api.handler.ServeHTTP(w, req)
	var result kv.Result
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK || result.Succeeded || result.Revision != 9007199254740993 {
		t.Fatalf("unexpected compare result: %d %+v", w.Code, result)
	}
}

func TestKVHTTPAuthenticationDisabledDoesNotPanic(t *testing.T) {
	api := newAuthenticatedTestAPI(t)
	api.http.signer = nil
	w := httptest.NewRecorder()
	api.handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/kv/databases", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
}
