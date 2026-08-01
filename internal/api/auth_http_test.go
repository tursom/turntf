package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	gproto "google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/auth"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type authenticatedTestAPI struct {
	handler http.Handler
	http    *HTTP
}

type fakeClusterStatusSink struct {
	status app.ClusterStatus
}

func (s fakeClusterStatusSink) Publish(store.Event) {}

func (s fakeClusterStatusSink) Status(context.Context) (app.ClusterStatus, error) {
	return s.status, nil
}

func (s fakeClusterStatusSink) ConfiguredPeerNodeIDs() []int64 {
	ids := make([]int64, 0, len(s.status.Peers))
	for _, peer := range s.status.Peers {
		if peer.NodeID > 0 {
			ids = append(ids, peer.NodeID)
		}
	}
	return ids
}

type fakeLoggedInUsersSink struct {
	fakeClusterStatusSink
	query func(context.Context, int64) ([]app.LoggedInUserSummary, error)
}

func (s fakeLoggedInUsersSink) QueryLoggedInUsers(ctx context.Context, nodeID int64) ([]app.LoggedInUserSummary, error) {
	if s.query == nil {
		return nil, nil
	}
	return s.query(ctx, nodeID)
}

func TestAuthenticatedHTTPLoginAndAuthorization(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "unauthorized",
		"password": "unauthorized-password",
	}, nil, http.StatusUnauthorized)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  testNodeID(1),
		"user_id":  store.BootstrapAdminUserID,
		"password": "wrong",
	}, nil, http.StatusUnauthorized)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

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

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK)

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userPath(adminKey.NodeID, adminKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "bob",
		"password": "bob-password",
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("hello"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(adminKey.NodeID, adminKey.UserID), map[string]any{
		"body": []byte("forbidden"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var channel struct {
		NodeID int64  `json:"node_id"`
		UserID int64  `json:"user_id"`
		Role   string `json:"role"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "alerts",
		"role":     store.RoleChannel,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &channel)
	if channel.Role != store.RoleChannel {
		t.Fatalf("expected channel role, got %+v", channel)
	}
	channelKey := store.UserKey{NodeID: channel.NodeID, UserID: channel.UserID}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  channel.NodeID,
		"user_id":  channel.UserID,
		"password": "anything",
	}, nil, http.StatusUnauthorized)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(channel.NodeID, channel.UserID), map[string]any{
		"body": []byte("not subscribed"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(aliceKey, store.AttachmentTypeChannelSubscription, channelKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(channel.NodeID, channel.UserID), map[string]any{
		"body": []byte("channel message"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	broadcastPath := userMessagesPath(testNodeID(1), store.BroadcastUserID)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, broadcastPath, map[string]any{
		"body": []byte("ordinary broadcast"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, broadcastPath, map[string]any{
		"body": []byte("admin broadcast"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	var aliceMessages struct {
		Items []authMessageItem `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID)+"?limit=20", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &aliceMessages)
	if !responseMessagesContainBody(aliceMessages.Items, "channel message") || !responseMessagesContainBody(aliceMessages.Items, "admin broadcast") {
		t.Fatalf("expected alice message list to include channel and broadcast: %+v", aliceMessages)
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(channel.NodeID, channel.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(channel.NodeID, channel.UserID), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
}

func TestAuthenticatedHTTPListUsersSupportsCommunicableFilteringAndSearch(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")
	aliceKey := createUserAsWithOptions(t, handler, adminToken, "alice", "alice-password", store.RoleUser, "alice.login", map[string]any{
		"display_name": "Alice Display",
	})
	bobKey := createUserAsWithOptions(t, handler, adminToken, "bob", "bob-password", store.RoleUser, "bob.login", map[string]any{
		"display_name": "Bob Hidden",
	})
	carolKey := createUserAsWithOptions(t, handler, adminToken, "carol", "carol-password", store.RoleUser, "carol.login", map[string]any{
		"display_name": "Carol Visible",
	})
	ordersKey := createUserAsWithOptions(t, handler, adminToken, "orders", "", store.RoleChannel, "", map[string]any{
		"display_name": "Orders Channel",
	})
	writersKey := createUserAsWithOptions(t, handler, adminToken, "writers", "", store.RoleChannel, "", map[string]any{
		"display_name": "Writers Channel",
	})
	hiddenChannelKey := createUserAsWithOptions(t, handler, adminToken, "hidden", "", store.RoleChannel, "", map[string]any{
		"display_name": "Hidden Channel",
	})

	aliceToken := loginToken(t, handler, aliceKey, "alice-password")
	bobToken := loginToken(t, handler, bobKey, "bob-password")

	doJSONWithHeaders(t, handler, http.MethodPost, subscriptionsPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"channel_node_id": ordersKey.NodeID,
		"channel_user_id": ordersKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(writersKey, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPost, blacklistPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"blocked_node_id": bobKey.NodeID,
		"blocked_user_id": bobKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPost, blacklistPath(bobKey.NodeID, bobKey.UserID), map[string]any{
		"blocked_node_id": aliceKey.NodeID,
		"blocked_user_id": aliceKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPost, blacklistPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"blocked_node_id": carolKey.NodeID,
		"blocked_user_id": carolKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var users []authUserItem
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &users)
	if !responseUsersContainKey(users, aliceKey) || !responseUsersContainKey(users, adminKey) || !responseUsersContainKey(users, carolKey) {
		t.Fatalf("expected communicable users to include self/admin/carol: %+v", users)
	}
	if !responseUsersContainKey(users, ordersKey) || !responseUsersContainKey(users, writersKey) {
		t.Fatalf("expected communicable channels to include subscribed and writable channels: %+v", users)
	}
	broadcastKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BroadcastUserID}
	if !responseUsersContainKey(users, broadcastKey) {
		t.Fatalf("expected broadcast user in communicable list: %+v", users)
	}
	nodeKey := store.UserKey{NodeID: testNodeID(1), UserID: store.NodeIngressUserID}
	if responseUsersContainKey(users, bobKey) || responseUsersContainKey(users, hiddenChannelKey) || responseUsersContainKey(users, nodeKey) {
		t.Fatalf("unexpected hidden users/channels in communicable list: %+v", users)
	}
	aliceItem, ok := responseUserByKey(users, aliceKey)
	if !ok || aliceItem.LoginName != "alice.login" {
		t.Fatalf("expected self login_name to remain visible: %+v", aliceItem)
	}
	carolItem, ok := responseUserByKey(users, carolKey)
	if !ok || carolItem.LoginName != "" {
		t.Fatalf("expected other user's login_name to be hidden: %+v", carolItem)
	}

	var filtered []authUserItem
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users?name=carol%20visible", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &filtered)
	if len(filtered) != 1 || filtered[0].UserID != carolKey.UserID {
		t.Fatalf("expected name filter to match carol display name: %+v", filtered)
	}

	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users?name=carol.login", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &filtered)
	if len(filtered) != 0 {
		t.Fatalf("expected non-admin login_name search to return empty: %+v", filtered)
	}

	carolUID := strconv.FormatInt(carolKey.NodeID, 10) + ":" + strconv.FormatInt(carolKey.UserID, 10)
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users?uid="+url.QueryEscape(carolUID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &filtered)
	if len(filtered) != 1 || filtered[0].UserID != carolKey.UserID {
		t.Fatalf("expected uid filter to return only carol: %+v", filtered)
	}

	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users?name=carol&uid="+url.QueryEscape(carolUID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &filtered)
	if len(filtered) != 1 || filtered[0].UserID != carolKey.UserID {
		t.Fatalf("expected combined name+uid filter to keep carol: %+v", filtered)
	}

	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users?name=nomatch&uid="+url.QueryEscape(carolUID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &filtered)
	if len(filtered) != 0 {
		t.Fatalf("expected combined name+uid filter to apply AND semantics: %+v", filtered)
	}

	doJSONWithHeaders(t, handler, http.MethodGet, "/users?uid=1025", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)

	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users?name=carol.login", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &filtered)
	if len(filtered) != 1 || filtered[0].UserID != carolKey.UserID || filtered[0].LoginName != "carol.login" {
		t.Fatalf("expected admin login_name search to return full user info: %+v", filtered)
	}
}

func TestHTTPAdminAndSuperAdminPermissionSeparation(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	superAdminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	superAdminToken := loginToken(t, testAPI.handler, superAdminKey, "root-password")

	adminKey := createUserAs(t, testAPI.handler, superAdminToken, "ops-admin", "ops-admin-password", store.RoleAdmin)
	adminToken := loginToken(t, testAPI.handler, adminKey, "ops-admin-password")

	managedUserKey := createUserAs(t, testAPI.handler, adminToken, "managed-user", "managed-user-password", store.RoleUser)
	_ = createUserAs(t, testAPI.handler, adminToken, "managed-channel", "", store.RoleChannel)

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/events?after=0&limit=10", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/status", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	metrics := doPlain(t, testAPI.handler, http.MethodGet, "/metrics", map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	if !strings.Contains(metrics, "notifier_write_gate_ready") {
		t.Fatalf("metrics missing write gate gauge: %s", metrics)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, "/users", map[string]any{
		"username": "blocked-admin",
		"password": "blocked-admin-password",
		"role":     store.RoleAdmin,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(managedUserKey.NodeID, managedUserKey.UserID), map[string]any{
		"role": store.RoleAdmin,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusForbidden)

	var promoted struct {
		NodeID int64  `json:"node_id"`
		UserID int64  `json:"user_id"`
		Role   string `json:"role"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(managedUserKey.NodeID, managedUserKey.UserID), map[string]any{
		"role": store.RoleAdmin,
	}, map[string]string{
		"Authorization": "Bearer " + superAdminToken,
	}, http.StatusOK), &promoted)
	if promoted.Role != store.RoleAdmin {
		t.Fatalf("expected promoted admin role, got %+v", promoted)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(promoted.NodeID, promoted.UserID), map[string]any{
		"username": "blocked-admin-rename",
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodDelete, userPath(promoted.NodeID, promoted.UserID), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusForbidden)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(superAdminKey.NodeID, superAdminKey.UserID), map[string]any{
		"password": "new-root-password",
	}, map[string]string{
		"Authorization": "Bearer " + superAdminToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(testNodeID(1), store.BroadcastUserID), map[string]any{
		"username": "blocked-broadcast",
	}, map[string]string{
		"Authorization": "Bearer " + superAdminToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodDelete, userPath(testNodeID(1), store.NodeIngressUserID), nil, map[string]string{
		"Authorization": "Bearer " + superAdminToken,
	}, http.StatusForbidden)

	var demoted struct {
		Role string `json:"role"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPatch, userPath(promoted.NodeID, promoted.UserID), map[string]any{
		"role": store.RoleUser,
	}, map[string]string{
		"Authorization": "Bearer " + superAdminToken,
	}, http.StatusOK), &demoted)
	if demoted.Role != store.RoleUser {
		t.Fatalf("expected demoted user role, got %+v", demoted)
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodDelete, userPath(promoted.NodeID, promoted.UserID), nil, map[string]string{
		"Authorization": "Bearer " + superAdminToken,
	}, http.StatusOK)
}

func TestBlacklistHTTPAPIRejectsDirectMessagesButKeepsChannelVisibility(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")
	aliceKey := createUserAs(t, handler, adminToken, "alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, handler, adminToken, "bob", "bob-password", store.RoleUser)
	channelKey := createUserAs(t, handler, adminToken, "orders", "", store.RoleChannel)

	aliceToken := loginToken(t, handler, aliceKey, "alice-password")
	bobToken := loginToken(t, handler, bobKey, "bob-password")

	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(aliceKey, store.AttachmentTypeChannelSubscription, channelKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(bobKey, store.AttachmentTypeChannelSubscription, channelKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelWriter, bobKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	if _, _, err := testAPI.http.service.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: aliceKey,
		Sender:  bobKey,
		Body:    []byte("before blacklist"),
	}); err != nil {
		t.Fatalf("create direct message before blacklist: %v", err)
	}

	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(aliceKey, store.AttachmentTypeUserBlacklist, bobKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var blockedList struct {
		Items []struct {
			Subject store.UserKey `json:"subject"`
		} `json:"items"`
		Count int `json:"count"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, attachmentsPath(aliceKey)+"?attachment_type="+string(store.AttachmentTypeUserBlacklist), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &blockedList)
	if blockedList.Count != 1 || blockedList.Items[0].Subject != bobKey {
		t.Fatalf("unexpected blocked users list: %+v", blockedList)
	}

	if _, _, err := testAPI.http.service.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: aliceKey,
		Sender:  bobKey,
		Body:    []byte("after blacklist"),
	}); !errors.Is(err, store.ErrBlockedByBlacklist) {
		t.Fatalf("expected service direct message to be blocked, got %v", err)
	}

	doJSONWithHeaders(t, handler, http.MethodPost, userMessagesPath(channelKey.NodeID, channelKey.UserID), map[string]any{
		"body": []byte("channel after blacklist"),
	}, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusCreated)

	var aliceMessages struct {
		Items []authMessageItem `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID)+"?limit=20", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &aliceMessages)
	if !responseMessagesContainBody(aliceMessages.Items, "before blacklist") || !responseMessagesContainBody(aliceMessages.Items, "channel after blacklist") {
		t.Fatalf("expected history and channel message to remain visible: %+v", aliceMessages)
	}
	if responseMessagesContainBody(aliceMessages.Items, "after blacklist") {
		t.Fatalf("unexpected blocked direct message in list: %+v", aliceMessages)
	}

	doJSONWithHeaders(t, handler, http.MethodDelete, attachmentPath(aliceKey, store.AttachmentTypeUserBlacklist, bobKey), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	if _, _, err := testAPI.http.service.CreateMessage(context.Background(), store.CreateMessageParams{
		UserKey: aliceKey,
		Sender:  bobKey,
		Body:    []byte("after unblock"),
	}); err != nil {
		t.Fatalf("create direct message after unblock: %v", err)
	}

	metrics := doPlain(t, handler, http.MethodGet, "/metrics", map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	if !strings.Contains(metrics, "notifier_blacklist_rejected_total") {
		t.Fatalf("metrics missing blacklist counter: %s", metrics)
	}
}

func TestHTTPSelfScopedCurrentUserSentinelSupportsUserMessagesAttachmentsSubscriptionsAndBlacklist(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")
	aliceKey := createUserAs(t, handler, adminToken, "self-http-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, handler, adminToken, "self-http-bob", "bob-password", store.RoleUser)
	channelKey := createUserAs(t, handler, adminToken, "self-http-orders", "", store.RoleChannel)

	aliceToken := loginToken(t, handler, aliceKey, "alice-password")
	bobToken := loginToken(t, handler, bobKey, "bob-password")
	currentUser := store.UserKey{}

	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("self-sentinel-message"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var selfUser struct {
		NodeID   int64  `json:"node_id"`
		UserID   int64  `json:"user_id"`
		Username string `json:"username"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userPath(0, 0), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &selfUser)
	if selfUser.NodeID != aliceKey.NodeID || selfUser.UserID != aliceKey.UserID || selfUser.Username != "self-http-alice" {
		t.Fatalf("unexpected self user response: %+v", selfUser)
	}

	var selfMessages struct {
		Count int               `json:"count"`
		Items []authMessageItem `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMessagesPath(0, 0)+"?limit=10", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &selfMessages)
	if selfMessages.Count != 1 || !responseMessagesContainBody(selfMessages.Items, "self-sentinel-message") {
		t.Fatalf("unexpected self message list: %+v", selfMessages)
	}

	doJSONWithHeaders(t, handler, http.MethodPost, subscriptionsPath(0, 0), map[string]any{
		"channel_node_id": channelKey.NodeID,
		"channel_user_id": channelKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var subscriptions struct {
		Count int `json:"count"`
		Items []struct {
			Subscriber store.UserKey `json:"subscriber"`
			Channel    store.UserKey `json:"channel"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, subscriptionsPath(0, 0), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &subscriptions)
	if subscriptions.Count != 1 || subscriptions.Items[0].Subscriber != aliceKey || subscriptions.Items[0].Channel != channelKey {
		t.Fatalf("unexpected self subscriptions response: %+v", subscriptions)
	}

	doJSONWithHeaders(t, handler, http.MethodDelete, subscriptionPath(0, 0, channelKey), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK)

	doJSONWithHeaders(t, handler, http.MethodPost, blacklistPath(0, 0), map[string]any{
		"blocked_node_id": bobKey.NodeID,
		"blocked_user_id": bobKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var blockedUsers struct {
		Count int `json:"count"`
		Items []struct {
			Owner   store.UserKey `json:"owner"`
			Blocked store.UserKey `json:"blocked"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, blacklistPath(0, 0), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &blockedUsers)
	if blockedUsers.Count != 1 || blockedUsers.Items[0].Owner != aliceKey || blockedUsers.Items[0].Blocked != bobKey {
		t.Fatalf("unexpected self blacklist response: %+v", blockedUsers)
	}

	doJSONWithHeaders(t, handler, http.MethodDelete, blockedUserPath(0, 0, bobKey), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK)

	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(currentUser, store.AttachmentTypeChannelSubscription, channelKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(currentUser, store.AttachmentTypeUserBlacklist, bobKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var selfAttachments struct {
		Count int `json:"count"`
		Items []struct {
			Owner          store.UserKey `json:"owner"`
			Subject        store.UserKey `json:"subject"`
			AttachmentType string        `json:"attachment_type"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, attachmentsPath(currentUser), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &selfAttachments)
	if selfAttachments.Count != 2 {
		t.Fatalf("unexpected self attachments response: %+v", selfAttachments)
	}

	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(currentUser, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)
	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(currentUser, store.AttachmentTypeChannelManager, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)
	doJSONWithHeaders(t, handler, http.MethodGet, attachmentsPath(currentUser)+"?attachment_type="+string(store.AttachmentTypeChannelWriter), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)

	doJSONWithHeaders(t, handler, http.MethodGet, userPath(0, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)
	doJSONWithHeaders(t, handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, 0), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)

	doJSONWithHeaders(t, handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, handler, http.MethodGet, subscriptionsPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusForbidden)
	doJSONWithHeaders(t, handler, http.MethodGet, blacklistPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusForbidden)
}

func TestClusterNodesHTTPRequiresAuthenticationAndAllowsUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeClusterStatusSink{
		status: app.ClusterStatus{
			NodeID: testNodeID(1),
			Peers: []app.ClusterPeerStatus{
				{NodeID: testNodeID(2), ConfiguredURL: "ws://127.0.0.1:9081/internal/cluster/ws", Connected: true},
				{NodeID: testNodeID(3), ConfiguredURL: "ws://127.0.0.1:9082/internal/cluster/ws", Connected: false},
				{ConfiguredURL: "ws://127.0.0.1:9083/internal/cluster/ws", Connected: true},
			},
		},
	})

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes", nil, nil, http.StatusUnauthorized)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	var nodes struct {
		Nodes []struct {
			NodeID        int64  `json:"node_id"`
			IsLocal       bool   `json:"is_local"`
			ConfiguredURL string `json:"configured_url"`
		} `json:"nodes"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &nodes)
	if len(nodes.Nodes) != 2 {
		t.Fatalf("unexpected cluster nodes: %+v", nodes)
	}
	if !nodes.Nodes[0].IsLocal || nodes.Nodes[0].NodeID != testNodeID(1) || nodes.Nodes[0].ConfiguredURL != "" {
		t.Fatalf("unexpected local cluster node: %+v", nodes.Nodes[0])
	}
	if nodes.Nodes[1].IsLocal || nodes.Nodes[1].NodeID != testNodeID(2) || nodes.Nodes[1].ConfiguredURL != "ws://127.0.0.1:9081/internal/cluster/ws" {
		t.Fatalf("unexpected peer cluster node: %+v", nodes.Nodes[1])
	}
}

func TestNodeLoggedInUsersHTTPRequiresAuthenticationAndReturnsDeduplicatedUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes/4096/logged-in-users", nil, nil, http.StatusUnauthorized)

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "bob", "bob-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	connA1 := dialClientWebSocket(t, server.URL)
	defer connA1.Close()
	loginClientWebSocket(t, connA1, aliceKey, "alice-password")

	connA2 := dialClientWebSocket(t, server.URL)
	defer connA2.Close()
	loginClientWebSocket(t, connA2, aliceKey, "alice-password")

	connB := dialClientWebSocket(t, server.URL)
	defer connB.Close()
	loginClientWebSocket(t, connB, bobKey, "bob-password")

	var resp struct {
		TargetNodeID int64 `json:"target_node_id"`
		Count        int   `json:"count"`
		Items        []struct {
			NodeID   int64  `json:"node_id"`
			UserID   int64  `json:"user_id"`
			Username string `json:"username"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes/4096/logged-in-users", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &resp)
	if resp.TargetNodeID != testNodeID(1) || resp.Count != 2 || len(resp.Items) != 2 {
		t.Fatalf("unexpected logged-in users response: %+v", resp)
	}
	if resp.Items[0].NodeID != aliceKey.NodeID || resp.Items[0].UserID != aliceKey.UserID || resp.Items[0].Username != "alice" {
		t.Fatalf("unexpected first logged-in user: %+v", resp.Items[0])
	}
	if resp.Items[1].NodeID != bobKey.NodeID || resp.Items[1].UserID != bobKey.UserID || resp.Items[1].Username != "bob" {
		t.Fatalf("unexpected second logged-in user: %+v", resp.Items[1])
	}
}

func TestNodeLoggedInUsersHTTPReturns503WhenRemoteNodeUnavailable(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeLoggedInUsersSink{
		query: func(context.Context, int64) ([]app.LoggedInUserSummary, error) {
			return nil, fmt.Errorf("%w: node 8192 is not connected", app.ErrServiceUnavailable)
		},
	})

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/cluster/nodes/8192/logged-in-users", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusServiceUnavailable)
}

func newAuthenticatedTestAPI(t *testing.T) authenticatedTestAPI {
	t.Helper()
	return newAuthenticatedTestAPIWithSinkAndStoreOptions(t, nil, store.Options{})
}

func newAuthenticatedTestAPIWithSink(t *testing.T, sink EventSink) authenticatedTestAPI {
	t.Helper()
	return newAuthenticatedTestAPIWithSinkAndStoreOptions(t, sink, store.Options{})
}

func newAuthenticatedTestAPIWithStoreOptions(t *testing.T, opts store.Options) authenticatedTestAPI {
	t.Helper()
	return newAuthenticatedTestAPIWithSinkAndStoreOptions(t, nil, opts)
}

func newAuthenticatedTestAPIWithSinkAndStoreOptions(t *testing.T, sink EventSink, opts store.Options) authenticatedTestAPI {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "auth-api.db")
	if opts.NodeID == 0 {
		opts.NodeID = testNodeID(1)
	}
	if opts.Engine == store.EnginePebble && strings.TrimSpace(opts.PebblePath) == "" {
		opts.PebblePath = filepath.Join(t.TempDir(), "auth-api.pebble")
	}
	st, err := store.Open(dbPath, opts)
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

	httpAPI := NewHTTP(New(st, sink), HTTPOptions{
		NodeID:   testNodeID(1),
		Signer:   signer,
		TokenTTL: time.Hour,
	})
	t.Cleanup(func() {
		_ = httpAPI.Close()
	})
	return authenticatedTestAPI{
		handler: httpAPI.Handler(),
		http:    httpAPI,
	}
}

func TestClientWebSocketLoginAndPushesBytesMessages(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	body := []byte{0xff, 0x00, 'x'}
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": body,
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password:        "alice-password",
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})

	loginResp := readServerEnvelope(t, conn).GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != aliceKey.UserID || loginResp.ProtocolVersion != internalproto.ClientProtocolVersion {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}
	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || !senderMatchesRef(pushed.Message.GetSender(), adminKey) || string(pushed.Message.GetBody()) != string(body) {
		t.Fatalf("unexpected pushed message: %+v", pushed)
	}
}

func TestClientWebSocketTransientOnlyLoginSkipsPersistentCatchupButStillReceivesPackets(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("persistent-history"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocketWithOptions(t, conn, aliceKey, "alice-password", true)

	if !testAPI.http.ReceiveTransientPacket(store.TransientPacket{
		PacketID:     1,
		SourceNodeID: adminKey.NodeID,
		TargetNodeID: aliceKey.NodeID,
		Recipient:    aliceKey,
		Sender:       adminKey,
		Body:         []byte("transient"),
		DeliveryMode: store.DeliveryModeBestEffort,
	}) {
		t.Fatal("expected transient-only session to receive local packet")
	}

	packet := readServerEnvelope(t, conn).GetPacketPushed()
	if packet == nil || packet.Packet == nil || string(packet.Packet.GetBody()) != "transient" || !senderMatchesRef(packet.Packet.GetSender(), adminKey) {
		t.Fatalf("unexpected transient packet push: %+v", packet)
	}
}

func TestClientWebSocketLoginStartsRealtimePushFromLoginWatermark(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("after-login"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "after-login" || !senderMatchesRef(pushed.Message.GetSender(), adminKey) {
		t.Fatalf("unexpected pushed message after login watermark: %+v", pushed)
	}
}

func TestRealtimeWebSocketAllowsPresenceAndTransientOnlyTraffic(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientRealtimeWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListNodeLoggedInUsers{
			ListNodeLoggedInUsers: &internalproto.ListNodeLoggedInUsersRequest{
				RequestId: 201,
				NodeId:    testNodeID(1),
			},
		},
	})
	usersResp := readServerEnvelope(t, conn).GetListNodeLoggedInUsersResponse()
	if usersResp == nil || usersResp.RequestId != 201 || usersResp.Count != 1 || usersResp.Items[0].GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected realtime logged-in users response: %+v", usersResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 202,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("persistent"),
			},
		},
	})
	rpcErr := readServerEnvelope(t, conn).GetError()
	if rpcErr == nil || rpcErr.RequestId != 202 || rpcErr.Code != "invalid_request" {
		t.Fatalf("unexpected realtime persistent rejection: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId:    203,
				Target:       &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:         []byte("ephemeral"),
				DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
				DeliveryMode: internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT,
			},
		},
	})
	var (
		gotSendResp bool
		gotPacket   bool
	)
	for range 2 {
		envelope := readServerEnvelope(t, conn)
		if sendResp := envelope.GetSendMessageResponse(); sendResp != nil {
			if sendResp.RequestId != 203 || sendResp.GetTransientAccepted() == nil {
				t.Fatalf("unexpected realtime transient send response: %+v", sendResp)
			}
			gotSendResp = true
			continue
		}
		if packet := envelope.GetPacketPushed(); packet != nil {
			if packet.Packet == nil || string(packet.Packet.GetBody()) != "ephemeral" || !senderMatchesRef(packet.Packet.GetSender(), aliceKey) {
				t.Fatalf("unexpected realtime transient packet push: %+v", packet)
			}
			gotPacket = true
			continue
		}
		t.Fatalf("unexpected realtime websocket envelope: %+v", envelope)
	}
	if !gotSendResp || !gotPacket {
		t.Fatalf("expected realtime transient response and packet push, got response=%v packet=%v", gotSendResp, gotPacket)
	}
}

func TestClientWebSocketSubscribedChannelReceivesRealtimePush(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	channelKey := createUserAs(t, testAPI.handler, adminToken, "orders", "", store.RoleChannel)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(aliceKey, store.AttachmentTypeChannelSubscription, channelKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(channelKey.NodeID, channelKey.UserID), map[string]any{
		"body": []byte("channel-live"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "channel-live" || pushed.Message.GetRecipient().GetUserId() != channelKey.UserID {
		t.Fatalf("unexpected channel pushed message: %+v", pushed)
	}
}

func TestClientWebSocketAdminReceivesDirectMessagePushFromSharedDispatcher(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, adminKey, "root-password")

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("admin-audit"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	pushed := readServerEnvelope(t, conn).GetMessagePushed()
	if pushed == nil || pushed.Message == nil || string(pushed.Message.GetBody()) != "admin-audit" || pushed.Message.GetRecipient().GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected admin pushed message: %+v", pushed)
	}
}

func TestClientWebSocketListClusterNodesAllowsUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeClusterStatusSink{
		status: app.ClusterStatus{
			NodeID: testNodeID(1),
			Peers: []app.ClusterPeerStatus{
				{NodeID: testNodeID(2), ConfiguredURL: "ws://127.0.0.1:9081/internal/cluster/ws", Connected: true},
				{NodeID: testNodeID(3), ConfiguredURL: "ws://127.0.0.1:9082/internal/cluster/ws", Connected: false},
			},
		},
	})
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListClusterNodes{
			ListClusterNodes: &internalproto.ListClusterNodesRequest{RequestId: 99},
		},
	})
	resp := readServerEnvelope(t, conn).GetListClusterNodesResponse()
	if resp == nil || resp.RequestId != 99 || resp.Count != 2 {
		t.Fatalf("unexpected list cluster nodes response: %+v", resp)
	}
	if resp.Items[0].GetNodeId() != testNodeID(1) || !resp.Items[0].GetIsLocal() || resp.Items[0].GetConfiguredUrl() != "" {
		t.Fatalf("unexpected local cluster node: %+v", resp.Items[0])
	}
	if resp.Items[1].GetNodeId() != testNodeID(2) || resp.Items[1].GetIsLocal() || resp.Items[1].GetConfiguredUrl() != "ws://127.0.0.1:9081/internal/cluster/ws" {
		t.Fatalf("unexpected peer cluster node: %+v", resp.Items[1])
	}
}

func TestClientWebSocketListNodeLoggedInUsersAllowsUsers(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "bob", "bob-password", store.RoleUser)

	connAlice := dialClientWebSocket(t, server.URL)
	defer connAlice.Close()
	loginClientWebSocket(t, connAlice, aliceKey, "alice-password")

	connAliceDup := dialClientWebSocket(t, server.URL)
	defer connAliceDup.Close()
	loginClientWebSocket(t, connAliceDup, aliceKey, "alice-password")

	connBob := dialClientWebSocket(t, server.URL)
	defer connBob.Close()
	loginClientWebSocket(t, connBob, bobKey, "bob-password")

	writeClientEnvelope(t, connAlice, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListNodeLoggedInUsers{
			ListNodeLoggedInUsers: &internalproto.ListNodeLoggedInUsersRequest{
				RequestId: 77,
				NodeId:    testNodeID(1),
			},
		},
	})
	resp := readServerEnvelope(t, connAlice).GetListNodeLoggedInUsersResponse()
	if resp == nil || resp.RequestId != 77 || resp.TargetNodeId != testNodeID(1) || resp.Count != 2 {
		t.Fatalf("unexpected list node logged-in users response: %+v", resp)
	}
	if resp.Items[0].GetUserId() != aliceKey.UserID || resp.Items[0].GetUsername() != "alice" {
		t.Fatalf("unexpected first logged-in user: %+v", resp.Items[0])
	}
	if resp.Items[1].GetUserId() != bobKey.UserID || resp.Items[1].GetUsername() != "bob" {
		t.Fatalf("unexpected second logged-in user: %+v", resp.Items[1])
	}
}

func TestClientWebSocketListNodeLoggedInUsersReturnsErrorWhenRemoteNodeUnavailable(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeLoggedInUsersSink{
		query: func(context.Context, int64) ([]app.LoggedInUserSummary, error) {
			return nil, fmt.Errorf("%w: node 8192 is not connected", app.ErrServiceUnavailable)
		},
	})
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListNodeLoggedInUsers{
			ListNodeLoggedInUsers: &internalproto.ListNodeLoggedInUsersRequest{
				RequestId: 88,
				NodeId:    testNodeID(2),
			},
		},
	})
	resp := readServerEnvelope(t, conn).GetError()
	if resp == nil || resp.RequestId != 88 || resp.Code != "service_unavailable" {
		t.Fatalf("unexpected websocket error response: %+v", resp)
	}
}

func TestClientWebSocketSeenCursorAndSendMessage(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	var created struct {
		NodeID int64 `json:"node_id"`
		Seq    int64 `json:"seq"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body": []byte("already seen"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated), &created)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password:        "alice-password",
				ProtocolVersion: internalproto.ClientProtocolVersion,
				SeenMessages: []*internalproto.MessageCursor{{
					NodeId: created.NodeID,
					Seq:    created.Seq,
				}},
			},
		},
	})
	if loginResp := readServerEnvelope(t, conn).GetLoginResponse(); loginResp == nil {
		t.Fatalf("expected login response")
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 42,
				Target: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
				Body: []byte{0x00, 0x01, 0xfe},
			},
		},
	})
	sendResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendResp == nil || sendResp.RequestId != 42 || string(sendResp.GetMessage().GetBody()) != string([]byte{0x00, 0x01, 0xfe}) {
		t.Fatalf("unexpected send response: %+v", sendResp)
	}
	if !senderMatchesRef(sendResp.GetMessage().GetSender(), aliceKey) {
		t.Fatalf("unexpected send response sender: %+v", sendResp)
	}
}

func TestClientWebSocketTransientSendMessage(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId:    43,
				Target:       &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:         []byte("ephemeral"),
				DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
				DeliveryMode: internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT,
			},
		},
	})
	var (
		gotSendResp bool
		gotPacket   bool
	)
	for range 2 {
		envelope := readServerEnvelope(t, conn)
		if sendResp := envelope.GetSendMessageResponse(); sendResp != nil {
			if sendResp.RequestId != 43 || sendResp.GetTransientAccepted() == nil {
				t.Fatalf("unexpected transient send response: %+v", sendResp)
			}
			gotSendResp = true
			continue
		}
		if packet := envelope.GetPacketPushed(); packet != nil {
			if packet.Packet == nil || string(packet.Packet.GetBody()) != "ephemeral" || packet.Packet.GetRecipient().GetUserId() != aliceKey.UserID || !senderMatchesRef(packet.Packet.GetSender(), aliceKey) {
				t.Fatalf("unexpected transient packet push: %+v", packet)
			}
			gotPacket = true
			continue
		}
		t.Fatalf("unexpected websocket envelope: %+v", envelope)
	}
	if !gotSendResp || !gotPacket {
		t.Fatalf("expected transient send response and packet push, got response=%v packet=%v", gotSendResp, gotPacket)
	}

	var listed struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"count"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + loginToken(t, testAPI.handler, aliceKey, "alice-password"),
	}, http.StatusOK), &listed)
	if listed.Count != 0 {
		t.Fatalf("expected transient ws send to avoid persistence, got %+v", listed)
	}
}

func TestClientWebSocketResolveUserSessionsReturnsLocalSessionRefs(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	connA := dialClientWebSocket(t, server.URL)
	defer connA.Close()
	loginA := loginClientWebSocketAndRead(t, connA, aliceKey, "alice-password", false)

	connB := dialClientWebSocket(t, server.URL)
	defer connB.Close()
	loginB := loginClientWebSocketAndRead(t, connB, aliceKey, "alice-password", false)

	writeClientEnvelope(t, connA, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ResolveUserSessions{
			ResolveUserSessions: &internalproto.ResolveUserSessionsRequest{
				RequestId: 501,
				User:      &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
			},
		},
	})
	resp := readServerEnvelope(t, connA).GetResolveUserSessionsResponse()
	if resp == nil || resp.RequestId != 501 || resp.Count != 2 {
		t.Fatalf("unexpected resolve sessions response: %+v", resp)
	}
	if len(resp.Presence) != 1 || resp.Presence[0].GetServingNodeId() != aliceKey.NodeID || resp.Presence[0].GetSessionCount() != 2 {
		t.Fatalf("unexpected resolve sessions presence: %+v", resp.GetPresence())
	}
	got := map[string]struct{}{}
	for _, item := range resp.Items {
		if item == nil || item.GetSession() == nil || item.GetSession().GetServingNodeId() != aliceKey.NodeID {
			t.Fatalf("unexpected resolved session item: %+v", item)
		}
		got[item.GetSession().GetSessionId()] = struct{}{}
	}
	if _, ok := got[loginA.GetSessionRef().GetSessionId()]; !ok {
		t.Fatalf("missing first session ref in response: %+v", resp)
	}
	if _, ok := got[loginB.GetSessionRef().GetSessionId()]; !ok {
		t.Fatalf("missing second session ref in response: %+v", resp)
	}
}

func TestClientWebSocketTransientTargetSessionDeliversOnlySelectedSession(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	connA := dialClientWebSocket(t, server.URL)
	defer connA.Close()
	loginClientWebSocketAndRead(t, connA, aliceKey, "alice-password", false)

	connB := dialClientWebSocket(t, server.URL)
	defer connB.Close()
	loginB := loginClientWebSocketAndRead(t, connB, aliceKey, "alice-password", false)

	writeClientEnvelope(t, connA, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId:     502,
				Target:        &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:          []byte("targeted"),
				DeliveryKind:  internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
				DeliveryMode:  internalproto.ClientDeliveryMode_CLIENT_DELIVERY_MODE_BEST_EFFORT,
				TargetSession: loginB.GetSessionRef(),
			},
		},
	})
	sendResp := readServerEnvelope(t, connA).GetSendMessageResponse()
	if sendResp == nil || sendResp.RequestId != 502 || sendResp.GetTransientAccepted() == nil {
		t.Fatalf("unexpected targeted transient send response: %+v", sendResp)
	}
	if sendResp.GetTransientAccepted().GetTargetSession().GetSessionId() != loginB.GetSessionRef().GetSessionId() {
		t.Fatalf("unexpected targeted transient accepted session: %+v", sendResp.GetTransientAccepted())
	}

	packet := readServerEnvelope(t, connB).GetPacketPushed()
	if packet == nil || packet.Packet == nil || string(packet.Packet.GetBody()) != "targeted" {
		t.Fatalf("unexpected targeted packet push: %+v", packet)
	}
	if packet.Packet.GetTargetSession().GetSessionId() != loginB.GetSessionRef().GetSessionId() {
		t.Fatalf("unexpected targeted packet session: %+v", packet.Packet)
	}

	expectNoServerEnvelopeWithin(t, connA, 200*time.Millisecond)
}

func TestTransientHTTPAndWebSocketPacket(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Password:        "alice-password",
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})
	if loginResp := readServerEnvelope(t, conn).GetLoginResponse(); loginResp == nil {
		t.Fatalf("expected login response")
	}

	var accepted struct {
		Mode         string `json:"mode"`
		PacketID     uint64 `json:"packet_id"`
		TargetNodeID int64  `json:"target_node_id"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body":          []byte("transient"),
		"delivery_kind": string(deliveryKindTransient),
		"delivery_mode": string(store.DeliveryModeBestEffort),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusAccepted), &accepted)
	if accepted.Mode != "transient" || accepted.PacketID == 0 || accepted.TargetNodeID != aliceKey.NodeID {
		t.Fatalf("unexpected accepted response: %+v", accepted)
	}

	packet := readServerEnvelope(t, conn).GetPacketPushed()
	if packet == nil || packet.Packet == nil || string(packet.Packet.GetBody()) != "transient" || packet.Packet.GetRecipient().GetUserId() != aliceKey.UserID || !senderMatchesRef(packet.Packet.GetSender(), aliceKey) {
		t.Fatalf("unexpected packet push: %+v", packet)
	}

	var listed struct {
		Items []map[string]any `json:"items"`
		Count int              `json:"count"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &listed)
	if listed.Count != 0 {
		t.Fatalf("expected transient packet to avoid persistence, got %+v", listed)
	}
}

func TestPersistentHTTPCreateMessageAcceptsSyncModeOnPebble(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithStoreOptions(t, store.Options{
		Engine:                store.EnginePebble,
		PebbleMessageSyncMode: store.PebbleMessageSyncModeNoSync,
	})
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	for _, syncMode := range []string{"force_sync", "no_sync"} {
		doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
			"body":      []byte("message-" + syncMode),
			"sync_mode": syncMode,
		}, map[string]string{
			"Authorization": "Bearer " + adminToken,
		}, http.StatusCreated)
	}

	var listed struct {
		Items []authMessageItem `json:"items"`
		Count int               `json:"count"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, userMessagesPath(aliceKey.NodeID, aliceKey.UserID)+"?limit=10", nil, map[string]string{
		"Authorization": "Bearer " + loginToken(t, testAPI.handler, aliceKey, "alice-password"),
	}, http.StatusOK), &listed)
	if listed.Count != 2 || !responseMessagesContainBody(listed.Items, "message-force_sync") || !responseMessagesContainBody(listed.Items, "message-no_sync") {
		t.Fatalf("unexpected persisted messages after sync_mode HTTP writes: %+v", listed)
	}
}

func TestPersistentHTTPCreateMessageIgnoresSyncModeOnSQLite(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body":      []byte("sqlite-sync-mode"),
		"sync_mode": "force_sync",
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)
}

func TestTransientHTTPRejectsSyncMode(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"body":          []byte("transient"),
		"delivery_kind": string(deliveryKindTransient),
		"sync_mode":     "force_sync",
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusBadRequest)
}

func TestTransientRequiresLoginRecipient(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	channelKey := createUserAs(t, testAPI.handler, adminToken, "orders", "", store.RoleChannel)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(channelKey.NodeID, channelKey.UserID), map[string]any{
		"body":          []byte("transient"),
		"delivery_kind": string(deliveryKindTransient),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusBadRequest)
}

func TestClientWebSocketAdminRPCProvidesFullHTTPCapabilities(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, adminKey, "root-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_CreateUser{
			CreateUser: &internalproto.CreateUserRequest{
				RequestId:   11,
				Username:    "rpc-alice",
				Password:    "rpc-password",
				ProfileJson: []byte(`{"display_name":"RPC Alice"}`),
				Role:        store.RoleUser,
			},
		},
	})
	createResp := readServerEnvelope(t, conn).GetCreateUserResponse()
	if createResp == nil || createResp.RequestId != 11 || createResp.User.GetUserId() == 0 || string(createResp.User.GetProfileJson()) != `{"display_name":"RPC Alice"}` {
		t.Fatalf("unexpected create user response: %+v", createResp)
	}
	createdKey := store.UserKey{NodeID: createResp.User.GetNodeId(), UserID: createResp.User.GetUserId()}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpdateUser{
			UpdateUser: &internalproto.UpdateUserRequest{
				RequestId: 12,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
				Username:  &internalproto.StringField{Value: "rpc-alice-updated"},
				ProfileJson: &internalproto.BytesField{
					Value: []byte(`{"display_name":"RPC Alice Updated"}`),
				},
			},
		},
	})
	updateResp := readServerEnvelope(t, conn).GetUpdateUserResponse()
	if updateResp == nil || updateResp.RequestId != 12 || updateResp.User.GetUsername() != "rpc-alice-updated" {
		t.Fatalf("unexpected update user response: %+v", updateResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 13,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
			},
		},
	})
	getResp := readServerEnvelope(t, conn).GetGetUserResponse()
	if getResp == nil || getResp.RequestId != 13 || getResp.User.GetUsername() != "rpc-alice-updated" {
		t.Fatalf("unexpected get user response: %+v", getResp)
	}

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, userMessagesPath(createdKey.NodeID, createdKey.UserID), map[string]any{
		"body": []byte("rpc hello"),
	}, map[string]string{
		"Authorization": "Bearer " + loginToken(t, testAPI.handler, adminKey, "root-password"),
	}, http.StatusCreated)

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListMessages{
			ListMessages: &internalproto.ListMessagesRequest{
				RequestId: 14,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
				Limit:     10,
			},
		},
	})
	listMessages := readServerEnvelope(t, conn).GetListMessagesResponse()
	if listMessages == nil || listMessages.RequestId != 14 || listMessages.Count != 1 || string(listMessages.Items[0].GetBody()) != "rpc hello" {
		t.Fatalf("unexpected list messages response: %+v", listMessages)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListEvents{
			ListEvents: &internalproto.ListEventsRequest{
				RequestId: 15,
				After:     0,
				Limit:     20,
			},
		},
	})
	listEvents := readServerEnvelope(t, conn).GetListEventsResponse()
	if listEvents == nil || listEvents.RequestId != 15 || listEvents.Count < 3 || len(listEvents.Items[0].GetEventJson()) == 0 {
		t.Fatalf("unexpected list events response: %+v", listEvents)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_OperationsStatus{
			OperationsStatus: &internalproto.OperationsStatusRequest{RequestId: 16},
		},
	})
	opsResp := readServerEnvelope(t, conn).GetOperationsStatusResponse()
	if opsResp == nil || opsResp.RequestId != 16 || opsResp.Status.GetNodeId() != testNodeID(1) {
		t.Fatalf("unexpected operations status response: %+v", opsResp)
	}
	if opsResp.Status.GetEventLogTrim() == nil {
		t.Fatalf("expected event log trim status in operations response: %+v", opsResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Metrics{
			Metrics: &internalproto.MetricsRequest{RequestId: 17},
		},
	})
	metricsResp := readServerEnvelope(t, conn).GetMetricsResponse()
	if metricsResp == nil || metricsResp.RequestId != 17 || !strings.Contains(metricsResp.Text, "notifier_write_gate_ready") {
		t.Fatalf("unexpected metrics response: %+v", metricsResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUser{
			DeleteUser: &internalproto.DeleteUserRequest{
				RequestId: 18,
				User:      &internalproto.UserRef{NodeId: createdKey.NodeID, UserId: createdKey.UserID},
			},
		},
	})
	deleteResp := readServerEnvelope(t, conn).GetDeleteUserResponse()
	if deleteResp == nil || deleteResp.RequestId != 18 || deleteResp.Status != "deleted" {
		t.Fatalf("unexpected delete user response: %+v", deleteResp)
	}
}

func TestClientWebSocketRPCRespectsUserAuthorizationAndSubscriptions(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	channelKey := createUserAs(t, testAPI.handler, adminToken, "orders", "", store.RoleChannel)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 21,
				User:      &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
			},
		},
	})
	getResp := readServerEnvelope(t, conn).GetGetUserResponse()
	if getResp == nil || getResp.RequestId != 21 || getResp.User.GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected self get user response: %+v", getResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 22,
				User:      &internalproto.UserRef{NodeId: adminKey.NodeID, UserId: adminKey.UserID},
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 22 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected get user forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 230,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("seed"),
			},
		},
	})
	sendSelfResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendSelfResp == nil || sendSelfResp.RequestId != 230 || string(sendSelfResp.GetMessage().GetBody()) != "seed" {
		t.Fatalf("unexpected self send response: %+v", sendSelfResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListMessages{
			ListMessages: &internalproto.ListMessagesRequest{
				RequestId: 23,
				User:      &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Limit:     10,
			},
		},
	})
	listMessages := readServerEnvelope(t, conn).GetListMessagesResponse()
	if listMessages == nil || listMessages.RequestId != 23 || listMessages.Count != 1 || string(listMessages.Items[0].GetBody()) != "seed" {
		t.Fatalf("unexpected self list messages response: %+v", listMessages)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListEvents{
			ListEvents: &internalproto.ListEventsRequest{RequestId: 24, After: 0, Limit: 10},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 24 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected list events forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserAttachment{
			UpsertUserAttachment: &internalproto.UpsertUserAttachmentRequest{
				RequestId: 25,
				Owner: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
				Subject: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
				ConfigJson:     []byte("{}"),
			},
		},
	})
	upsertResp := readServerEnvelope(t, conn).GetUpsertUserAttachmentResponse()
	if upsertResp == nil || upsertResp.RequestId != 25 || upsertResp.Attachment.GetSubject().GetUserId() != channelKey.UserID {
		t.Fatalf("unexpected attachment upsert response: %+v", upsertResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUserAttachments{
			ListUserAttachments: &internalproto.ListUserAttachmentsRequest{
				RequestId: 26,
				Owner: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
			},
		},
	})
	listAttachments := readServerEnvelope(t, conn).GetListUserAttachmentsResponse()
	if listAttachments == nil || listAttachments.RequestId != 26 || listAttachments.Count != 1 || listAttachments.Items[0].GetSubject().GetUserId() != channelKey.UserID {
		t.Fatalf("unexpected list attachments response: %+v", listAttachments)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 27,
				Target: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
				Body: []byte("channel payload"),
			},
		},
	})
	sendResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendResp == nil || sendResp.RequestId != 27 || string(sendResp.GetMessage().GetBody()) != "channel payload" {
		t.Fatalf("unexpected send to channel response: %+v", sendResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUserAttachment{
			DeleteUserAttachment: &internalproto.DeleteUserAttachmentRequest{
				RequestId: 28,
				Owner: &internalproto.UserRef{
					NodeId: aliceKey.NodeID,
					UserId: aliceKey.UserID,
				},
				Subject: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
			},
		},
	})
	deleteResp := readServerEnvelope(t, conn).GetDeleteUserAttachmentResponse()
	if deleteResp == nil || deleteResp.RequestId != 28 || deleteResp.Attachment.GetDeletedAt() == "" {
		t.Fatalf("unexpected attachment delete response: %+v", deleteResp)
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodDelete, attachmentPath(channelKey, store.AttachmentTypeChannelWriter, aliceKey), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 29,
				Target: &internalproto.UserRef{
					NodeId: channelKey.NodeID,
					UserId: channelKey.UserID,
				},
				Body: []byte("forbidden after unsubscribe"),
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 29 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected send forbidden error: %+v", rpcErr)
	}
}

func TestClientWebSocketListUsersSupportsFiltersAndVisibility(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAsWithOptions(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser, "alice.login", map[string]any{
		"display_name": "Alice Display",
	})
	bobKey := createUserAsWithOptions(t, testAPI.handler, adminToken, "bob", "bob-password", store.RoleUser, "bob.login", map[string]any{
		"display_name": "Bob Hidden",
	})
	carolKey := createUserAsWithOptions(t, testAPI.handler, adminToken, "carol", "carol-password", store.RoleUser, "carol.login", map[string]any{
		"display_name": "Carol Visible",
	})
	ordersKey := createUserAsWithOptions(t, testAPI.handler, adminToken, "orders", "", store.RoleChannel, "", map[string]any{
		"display_name": "Orders Channel",
	})
	writersKey := createUserAsWithOptions(t, testAPI.handler, adminToken, "writers", "", store.RoleChannel, "", map[string]any{
		"display_name": "Writers Channel",
	})
	hiddenChannelKey := createUserAsWithOptions(t, testAPI.handler, adminToken, "hidden", "", store.RoleChannel, "", map[string]any{
		"display_name": "Hidden Channel",
	})

	aliceToken := loginToken(t, testAPI.handler, aliceKey, "alice-password")
	bobToken := loginToken(t, testAPI.handler, bobKey, "bob-password")
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, subscriptionsPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"channel_node_id": ordersKey.NodeID,
		"channel_user_id": ordersKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(writersKey, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, blacklistPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"blocked_node_id": bobKey.NodeID,
		"blocked_user_id": bobKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, blacklistPath(bobKey.NodeID, bobKey.UserID), map[string]any{
		"blocked_node_id": aliceKey.NodeID,
		"blocked_user_id": aliceKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, blacklistPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"blocked_node_id": carolKey.NodeID,
		"blocked_user_id": carolKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{RequestId: 330},
		},
	})
	listResp := readServerEnvelope(t, conn).GetListUsersResponse()
	if listResp == nil || listResp.RequestId != 330 {
		t.Fatalf("unexpected websocket list users response: %+v", listResp)
	}
	if !protoUsersContainKey(listResp.Items, aliceKey) || !protoUsersContainKey(listResp.Items, adminKey) || !protoUsersContainKey(listResp.Items, carolKey) {
		t.Fatalf("expected websocket communicable list to include self/admin/carol: %+v", listResp)
	}
	if !protoUsersContainKey(listResp.Items, ordersKey) || !protoUsersContainKey(listResp.Items, writersKey) {
		t.Fatalf("expected websocket communicable list to include subscribed/writable channels: %+v", listResp)
	}
	broadcastKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BroadcastUserID}
	if !protoUsersContainKey(listResp.Items, broadcastKey) {
		t.Fatalf("expected websocket communicable list to include broadcast: %+v", listResp)
	}
	nodeKey := store.UserKey{NodeID: testNodeID(1), UserID: store.NodeIngressUserID}
	if protoUsersContainKey(listResp.Items, bobKey) || protoUsersContainKey(listResp.Items, hiddenChannelKey) || protoUsersContainKey(listResp.Items, nodeKey) {
		t.Fatalf("unexpected websocket hidden users/channels in list: %+v", listResp)
	}
	aliceProto, ok := protoUserByKey(listResp.Items, aliceKey)
	if !ok || aliceProto.GetLoginName() != "alice.login" {
		t.Fatalf("expected websocket self login_name to remain visible: %+v", aliceProto)
	}
	carolProto, ok := protoUserByKey(listResp.Items, carolKey)
	if !ok || carolProto.GetLoginName() != "" {
		t.Fatalf("expected websocket other user's login_name to be hidden: %+v", carolProto)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{RequestId: 331, Name: "carol visible"},
		},
	})
	nameResp := readServerEnvelope(t, conn).GetListUsersResponse()
	if nameResp == nil || nameResp.RequestId != 331 || nameResp.Count != 1 || !protoUsersContainKey(nameResp.Items, carolKey) {
		t.Fatalf("expected websocket name filter to match carol: %+v", nameResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{RequestId: 332, Name: "carol.login"},
		},
	})
	hiddenResp := readServerEnvelope(t, conn).GetListUsersResponse()
	if hiddenResp == nil || hiddenResp.RequestId != 332 || hiddenResp.Count != 0 {
		t.Fatalf("expected websocket non-admin login_name search to return empty: %+v", hiddenResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{
				RequestId: 333,
				Uid:       &internalproto.UserRef{NodeId: carolKey.NodeID},
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 333 || rpcErr.Code != "invalid_request" {
		t.Fatalf("unexpected websocket invalid uid error: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{
				RequestId: 334,
				Uid:       &internalproto.UserRef{NodeId: carolKey.NodeID, UserId: carolKey.UserID},
			},
		},
	})
	uidResp := readServerEnvelope(t, conn).GetListUsersResponse()
	if uidResp == nil || uidResp.RequestId != 334 || uidResp.Count != 1 || !protoUsersContainKey(uidResp.Items, carolKey) {
		t.Fatalf("expected websocket uid filter to return only carol: %+v", uidResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{
				RequestId: 335,
				Name:      "nomatch",
				Uid:       &internalproto.UserRef{NodeId: carolKey.NodeID, UserId: carolKey.UserID},
			},
		},
	})
	combinedResp := readServerEnvelope(t, conn).GetListUsersResponse()
	if combinedResp == nil || combinedResp.RequestId != 335 || combinedResp.Count != 0 {
		t.Fatalf("expected websocket name+uid filter to apply AND semantics: %+v", combinedResp)
	}

	adminConn := dialClientWebSocket(t, server.URL)
	defer adminConn.Close()
	loginClientWebSocket(t, adminConn, adminKey, "root-password")
	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{RequestId: 336, Name: "carol.login"},
		},
	})
	adminResp := readServerEnvelope(t, adminConn).GetListUsersResponse()
	if adminResp == nil || adminResp.RequestId != 336 || adminResp.Count != 1 {
		t.Fatalf("expected admin websocket login_name search to return carol: %+v", adminResp)
	}
	adminCarol, ok := protoUserByKey(adminResp.Items, carolKey)
	if !ok || adminCarol.GetLoginName() != "carol.login" {
		t.Fatalf("expected admin websocket response to include login_name: %+v", adminCarol)
	}

	realtimeConn := dialClientRealtimeWebSocket(t, server.URL)
	defer realtimeConn.Close()
	loginClientWebSocketWithOptions(t, realtimeConn, aliceKey, "alice-password", true)
	writeClientEnvelope(t, realtimeConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{RequestId: 337},
		},
	})
	if rpcErr := readServerEnvelope(t, realtimeConn).GetError(); rpcErr == nil || rpcErr.RequestId != 337 || rpcErr.Code != "invalid_request" {
		t.Fatalf("unexpected realtime websocket list_users error: %+v", rpcErr)
	}
}

func TestClientWebSocketSelfScopedRPCSupportsImplicitCurrentUser(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "implicit-alice", "alice-password", store.RoleUser)
	channelKey := createUserAs(t, testAPI.handler, adminToken, "implicit-orders", "", store.RoleChannel)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelWriter, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	adminConn := dialClientWebSocket(t, server.URL)
	defer adminConn.Close()
	loginClientWebSocket(t, adminConn, adminKey, "root-password")
	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 300,
			},
		},
	})
	adminGetResp := readServerEnvelope(t, adminConn).GetGetUserResponse()
	if adminGetResp == nil || adminGetResp.RequestId != 300 || !senderMatchesRef(&internalproto.UserRef{NodeId: adminGetResp.User.GetNodeId(), UserId: adminGetResp.User.GetUserId()}, adminKey) {
		t.Fatalf("unexpected implicit admin get user response: %+v", adminGetResp)
	}

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 301,
			},
		},
	})
	getNilResp := readServerEnvelope(t, conn).GetGetUserResponse()
	if getNilResp == nil || getNilResp.RequestId != 301 || getNilResp.User.GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected implicit nil get user response: %+v", getNilResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 302,
				User:      &internalproto.UserRef{},
			},
		},
	})
	getZeroResp := readServerEnvelope(t, conn).GetGetUserResponse()
	if getZeroResp == nil || getZeroResp.RequestId != 302 || getZeroResp.User.GetUserId() != aliceKey.UserID {
		t.Fatalf("unexpected implicit zero get user response: %+v", getZeroResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUser{
			GetUser: &internalproto.GetUserRequest{
				RequestId: 303,
				User:      &internalproto.UserRef{NodeId: aliceKey.NodeID},
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 303 || rpcErr.Code != "invalid_request" {
		t.Fatalf("unexpected partial get user error: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 304,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("implicit-seed"),
			},
		},
	})
	sendResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if sendResp == nil || sendResp.RequestId != 304 || string(sendResp.GetMessage().GetBody()) != "implicit-seed" {
		t.Fatalf("unexpected implicit self send response: %+v", sendResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListMessages{
			ListMessages: &internalproto.ListMessagesRequest{
				RequestId: 305,
				Limit:     10,
			},
		},
	})
	listNilResp := readServerEnvelope(t, conn).GetListMessagesResponse()
	if listNilResp == nil || listNilResp.RequestId != 305 || listNilResp.Count != 1 || string(listNilResp.Items[0].GetBody()) != "implicit-seed" {
		t.Fatalf("unexpected implicit nil list messages response: %+v", listNilResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListMessages{
			ListMessages: &internalproto.ListMessagesRequest{
				RequestId: 306,
				User:      &internalproto.UserRef{},
				Limit:     10,
			},
		},
	})
	listZeroResp := readServerEnvelope(t, conn).GetListMessagesResponse()
	if listZeroResp == nil || listZeroResp.RequestId != 306 || listZeroResp.Count != 1 || string(listZeroResp.Items[0].GetBody()) != "implicit-seed" {
		t.Fatalf("unexpected implicit zero list messages response: %+v", listZeroResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListMessages{
			ListMessages: &internalproto.ListMessagesRequest{
				RequestId: 307,
				User:      &internalproto.UserRef{NodeId: adminKey.NodeID, UserId: adminKey.UserID},
				Limit:     10,
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 307 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected explicit other list messages error: %+v", rpcErr)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserAttachment{
			UpsertUserAttachment: &internalproto.UpsertUserAttachmentRequest{
				RequestId:      308,
				Subject:        &internalproto.UserRef{NodeId: channelKey.NodeID, UserId: channelKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
				ConfigJson:     []byte("{}"),
			},
		},
	})
	subscribeResp := readServerEnvelope(t, conn).GetUpsertUserAttachmentResponse()
	if subscribeResp == nil || subscribeResp.RequestId != 308 || !senderMatchesRef(subscribeResp.Attachment.GetOwner(), aliceKey) || !senderMatchesRef(subscribeResp.Attachment.GetSubject(), channelKey) {
		t.Fatalf("unexpected implicit subscribe response: %+v", subscribeResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUserAttachments{
			ListUserAttachments: &internalproto.ListUserAttachmentsRequest{
				RequestId:      309,
				Owner:          &internalproto.UserRef{},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
			},
		},
	})
	listSubscriptionResp := readServerEnvelope(t, conn).GetListUserAttachmentsResponse()
	if listSubscriptionResp == nil || listSubscriptionResp.RequestId != 309 || listSubscriptionResp.Count != 1 || !senderMatchesRef(listSubscriptionResp.Items[0].GetSubject(), channelKey) {
		t.Fatalf("unexpected implicit subscription list response: %+v", listSubscriptionResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUserAttachments{
			ListUserAttachments: &internalproto.ListUserAttachmentsRequest{
				RequestId: 310,
			},
		},
	})
	listAllResp := readServerEnvelope(t, conn).GetListUserAttachmentsResponse()
	if listAllResp == nil || listAllResp.RequestId != 310 || listAllResp.Count != 1 || listAllResp.Items[0].GetAttachmentType() != internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION {
		t.Fatalf("unexpected implicit all attachments response: %+v", listAllResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUserAttachment{
			DeleteUserAttachment: &internalproto.DeleteUserAttachmentRequest{
				RequestId:      311,
				Owner:          &internalproto.UserRef{},
				Subject:        &internalproto.UserRef{NodeId: channelKey.NodeID, UserId: channelKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
			},
		},
	})
	deleteResp := readServerEnvelope(t, conn).GetDeleteUserAttachmentResponse()
	if deleteResp == nil || deleteResp.RequestId != 311 || deleteResp.Attachment.GetDeletedAt() == "" {
		t.Fatalf("unexpected implicit unsubscribe response: %+v", deleteResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserAttachment{
			UpsertUserAttachment: &internalproto.UpsertUserAttachmentRequest{
				RequestId:      312,
				Subject:        &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_CHANNEL_WRITER,
				ConfigJson:     []byte("{}"),
			},
		},
	})
	if rpcErr := readServerEnvelope(t, conn).GetError(); rpcErr == nil || rpcErr.RequestId != 312 || rpcErr.Code != "invalid_request" {
		t.Fatalf("unexpected implicit channel writer owner error: %+v", rpcErr)
	}
}

func TestClientWebSocketAdminAndSuperAdminPermissionSeparation(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	superAdminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	superAdminToken := loginToken(t, testAPI.handler, superAdminKey, "root-password")
	adminKey := createUserAs(t, testAPI.handler, superAdminToken, "rpc-admin", "rpc-admin-password", store.RoleAdmin)
	managedKey := createUserAs(t, testAPI.handler, superAdminToken, "rpc-managed", "rpc-managed-password", store.RoleUser)

	adminConn := dialClientWebSocket(t, server.URL)
	defer adminConn.Close()
	loginClientWebSocket(t, adminConn, adminKey, "rpc-admin-password")

	superConn := dialClientWebSocket(t, server.URL)
	defer superConn.Close()
	loginClientWebSocket(t, superConn, superAdminKey, "root-password")

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_CreateUser{
			CreateUser: &internalproto.CreateUserRequest{
				RequestId: 200,
				Username:  "rpc-ordinary",
				Password:  "rpc-ordinary-password",
				Role:      store.RoleUser,
			},
		},
	})
	if createResp := readServerEnvelope(t, adminConn).GetCreateUserResponse(); createResp == nil || createResp.RequestId != 200 || createResp.User.GetRole() != store.RoleUser {
		t.Fatalf("unexpected admin create user response: %+v", createResp)
	}

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_CreateUser{
			CreateUser: &internalproto.CreateUserRequest{
				RequestId: 201,
				Username:  "rpc-blocked-admin",
				Password:  "rpc-blocked-admin-password",
				Role:      store.RoleAdmin,
			},
		},
	})
	if rpcErr := readServerEnvelope(t, adminConn).GetError(); rpcErr == nil || rpcErr.RequestId != 201 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected create admin forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListEvents{
			ListEvents: &internalproto.ListEventsRequest{RequestId: 202, After: 0, Limit: 10},
		},
	})
	if listEvents := readServerEnvelope(t, adminConn).GetListEventsResponse(); listEvents == nil || listEvents.RequestId != 202 {
		t.Fatalf("unexpected admin list events response: %+v", listEvents)
	}

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_OperationsStatus{
			OperationsStatus: &internalproto.OperationsStatusRequest{RequestId: 203},
		},
	})
	if opsResp := readServerEnvelope(t, adminConn).GetOperationsStatusResponse(); opsResp == nil || opsResp.RequestId != 203 {
		t.Fatalf("unexpected admin ops response: %+v", opsResp)
	}

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Metrics{
			Metrics: &internalproto.MetricsRequest{RequestId: 204},
		},
	})
	metricsResp := readServerEnvelope(t, adminConn).GetMetricsResponse()
	if metricsResp == nil || metricsResp.RequestId != 204 || !strings.Contains(metricsResp.Text, "notifier_write_gate_ready") {
		t.Fatalf("unexpected admin metrics response: %+v", metricsResp)
	}

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpdateUser{
			UpdateUser: &internalproto.UpdateUserRequest{
				RequestId: 205,
				User:      &internalproto.UserRef{NodeId: managedKey.NodeID, UserId: managedKey.UserID},
				Role:      &internalproto.StringField{Value: store.RoleAdmin},
			},
		},
	})
	if rpcErr := readServerEnvelope(t, adminConn).GetError(); rpcErr == nil || rpcErr.RequestId != 205 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected promote forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, superConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpdateUser{
			UpdateUser: &internalproto.UpdateUserRequest{
				RequestId: 206,
				User:      &internalproto.UserRef{NodeId: managedKey.NodeID, UserId: managedKey.UserID},
				Role:      &internalproto.StringField{Value: store.RoleAdmin},
			},
		},
	})
	updateResp := readServerEnvelope(t, superConn).GetUpdateUserResponse()
	if updateResp == nil || updateResp.RequestId != 206 || updateResp.User.GetRole() != store.RoleAdmin {
		t.Fatalf("unexpected super admin promote response: %+v", updateResp)
	}

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpdateUser{
			UpdateUser: &internalproto.UpdateUserRequest{
				RequestId: 207,
				User:      &internalproto.UserRef{NodeId: managedKey.NodeID, UserId: managedKey.UserID},
				Username:  &internalproto.StringField{Value: "blocked-admin-update"},
			},
		},
	})
	if rpcErr := readServerEnvelope(t, adminConn).GetError(); rpcErr == nil || rpcErr.RequestId != 207 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected update admin forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUser{
			DeleteUser: &internalproto.DeleteUserRequest{
				RequestId: 208,
				User:      &internalproto.UserRef{NodeId: managedKey.NodeID, UserId: managedKey.UserID},
			},
		},
	})
	if rpcErr := readServerEnvelope(t, adminConn).GetError(); rpcErr == nil || rpcErr.RequestId != 208 || rpcErr.Code != "forbidden" {
		t.Fatalf("unexpected delete admin forbidden error: %+v", rpcErr)
	}

	writeClientEnvelope(t, superConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpdateUser{
			UpdateUser: &internalproto.UpdateUserRequest{
				RequestId: 209,
				User:      &internalproto.UserRef{NodeId: managedKey.NodeID, UserId: managedKey.UserID},
				Role:      &internalproto.StringField{Value: store.RoleUser},
			},
		},
	})
	if demoteResp := readServerEnvelope(t, superConn).GetUpdateUserResponse(); demoteResp == nil || demoteResp.RequestId != 209 || demoteResp.User.GetRole() != store.RoleUser {
		t.Fatalf("unexpected super admin demote response: %+v", demoteResp)
	}

	writeClientEnvelope(t, superConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUser{
			DeleteUser: &internalproto.DeleteUserRequest{
				RequestId: 210,
				User:      &internalproto.UserRef{NodeId: managedKey.NodeID, UserId: managedKey.UserID},
			},
		},
	})
	if deleteResp := readServerEnvelope(t, superConn).GetDeleteUserResponse(); deleteResp == nil || deleteResp.RequestId != 210 || deleteResp.Status != "deleted" {
		t.Fatalf("unexpected super admin delete response: %+v", deleteResp)
	}
}

func TestClientWebSocketPersistentSendMessageAcceptsSyncMode(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithStoreOptions(t, store.Options{
		Engine:                store.EnginePebble,
		PebbleMessageSyncMode: store.PebbleMessageSyncModeForceSync,
	})
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 301,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("ws-no-sync"),
				SyncMode:  internalproto.ClientMessageSyncMode_CLIENT_MESSAGE_SYNC_MODE_NO_SYNC,
			},
		},
	})
	firstResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if firstResp == nil || firstResp.RequestId != 301 || string(firstResp.GetMessage().GetBody()) != "ws-no-sync" {
		t.Fatalf("unexpected websocket send response with explicit no_sync: %+v", firstResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId: 302,
				Target:    &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:      []byte("ws-default-sync"),
			},
		},
	})
	secondResp := readServerEnvelope(t, conn).GetSendMessageResponse()
	if secondResp == nil || secondResp.RequestId != 302 || string(secondResp.GetMessage().GetBody()) != "ws-default-sync" {
		t.Fatalf("unexpected websocket send response with default sync mode: %+v", secondResp)
	}
}

func TestClientWebSocketTransientSendMessageRejectsSyncMode(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_SendMessage{
			SendMessage: &internalproto.SendMessageRequest{
				RequestId:    303,
				Target:       &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Body:         []byte("ephemeral"),
				DeliveryKind: internalproto.ClientDeliveryKind_CLIENT_DELIVERY_KIND_TRANSIENT,
				SyncMode:     internalproto.ClientMessageSyncMode_CLIENT_MESSAGE_SYNC_MODE_FORCE_SYNC,
			},
		},
	})
	rpcErr := readServerEnvelope(t, conn).GetError()
	if rpcErr == nil || rpcErr.RequestId != 303 || rpcErr.Code != "invalid_request" {
		t.Fatalf("unexpected transient sync_mode websocket error: %+v", rpcErr)
	}
}

func TestClientWebSocketAttachmentRPC(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "bob", "bob-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserAttachment{
			UpsertUserAttachment: &internalproto.UpsertUserAttachmentRequest{
				RequestId:      30,
				Owner:          &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Subject:        &internalproto.UserRef{NodeId: bobKey.NodeID, UserId: bobKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
				ConfigJson:     []byte("{}"),
			},
		},
	})
	blockResp := readServerEnvelope(t, conn).GetUpsertUserAttachmentResponse()
	if blockResp == nil || blockResp.RequestId != 30 || !senderMatchesRef(blockResp.Attachment.GetSubject(), bobKey) {
		t.Fatalf("unexpected attachment upsert response: %+v", blockResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUserAttachments{
			ListUserAttachments: &internalproto.ListUserAttachmentsRequest{
				RequestId:      31,
				Owner:          &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
			},
		},
	})
	listResp := readServerEnvelope(t, conn).GetListUserAttachmentsResponse()
	if listResp == nil || listResp.RequestId != 31 || listResp.Count != 1 || !senderMatchesRef(listResp.Items[0].GetSubject(), bobKey) {
		t.Fatalf("unexpected list attachments response: %+v", listResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUserAttachment{
			DeleteUserAttachment: &internalproto.DeleteUserAttachmentRequest{
				RequestId:      32,
				Owner:          &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Subject:        &internalproto.UserRef{NodeId: bobKey.NodeID, UserId: bobKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
			},
		},
	})
	unblockResp := readServerEnvelope(t, conn).GetDeleteUserAttachmentResponse()
	if unblockResp == nil || unblockResp.RequestId != 32 || unblockResp.Attachment.GetDeletedAt() == "" {
		t.Fatalf("unexpected attachment delete response: %+v", unblockResp)
	}
}

func TestClientWebSocketBlacklistAttachmentSupportsImplicitCurrentUserOwner(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "implicit-blacklist-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "implicit-blacklist-bob", "bob-password", store.RoleUser)

	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, aliceKey, "alice-password")

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserAttachment{
			UpsertUserAttachment: &internalproto.UpsertUserAttachmentRequest{
				RequestId:      330,
				Subject:        &internalproto.UserRef{NodeId: bobKey.NodeID, UserId: bobKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
				ConfigJson:     []byte("{}"),
			},
		},
	})
	blockResp := readServerEnvelope(t, conn).GetUpsertUserAttachmentResponse()
	if blockResp == nil || blockResp.RequestId != 330 || !senderMatchesRef(blockResp.Attachment.GetOwner(), aliceKey) || !senderMatchesRef(blockResp.Attachment.GetSubject(), bobKey) {
		t.Fatalf("unexpected implicit blacklist upsert response: %+v", blockResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUserAttachments{
			ListUserAttachments: &internalproto.ListUserAttachmentsRequest{
				RequestId:      331,
				Owner:          &internalproto.UserRef{},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
			},
		},
	})
	listResp := readServerEnvelope(t, conn).GetListUserAttachmentsResponse()
	if listResp == nil || listResp.RequestId != 331 || listResp.Count != 1 || !senderMatchesRef(listResp.Items[0].GetSubject(), bobKey) {
		t.Fatalf("unexpected implicit blacklist list response: %+v", listResp)
	}

	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUserAttachment{
			DeleteUserAttachment: &internalproto.DeleteUserAttachmentRequest{
				RequestId:      332,
				Subject:        &internalproto.UserRef{NodeId: bobKey.NodeID, UserId: bobKey.UserID},
				AttachmentType: internalproto.AttachmentType_ATTACHMENT_TYPE_USER_BLACKLIST,
			},
		},
	})
	unblockResp := readServerEnvelope(t, conn).GetDeleteUserAttachmentResponse()
	if unblockResp == nil || unblockResp.RequestId != 332 || unblockResp.Attachment.GetDeletedAt() == "" {
		t.Fatalf("unexpected implicit blacklist delete response: %+v", unblockResp)
	}
}

func loginClientWebSocket(t *testing.T, conn *websocket.Conn, key store.UserKey, password string) {
	t.Helper()
	loginClientWebSocketWithOptions(t, conn, key, password, false)
}

func loginClientWebSocketAndRead(t *testing.T, conn *websocket.Conn, key store.UserKey, password string, transientOnly bool) *internalproto.LoginResponse {
	t.Helper()
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_Login{
			Login: &internalproto.LoginRequest{
				User:            &internalproto.UserRef{NodeId: key.NodeID, UserId: key.UserID},
				Password:        password,
				TransientOnly:   transientOnly,
				ProtocolVersion: internalproto.ClientProtocolVersion,
			},
		},
	})
	loginResp := readServerEnvelope(t, conn).GetLoginResponse()
	if loginResp == nil || loginResp.User.GetUserId() != key.UserID || loginResp.GetSessionRef() == nil || loginResp.GetSessionRef().GetSessionId() == "" {
		t.Fatalf("unexpected login response: %+v", loginResp)
	}
	return loginResp
}

func loginClientWebSocketWithOptions(t *testing.T, conn *websocket.Conn, key store.UserKey, password string, transientOnly bool) {
	t.Helper()
	loginClientWebSocketAndRead(t, conn, key, password, transientOnly)
}

func expectNoServerEnvelopeWithin(t *testing.T, conn *websocket.Conn, timeout time.Duration) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("expected no websocket server envelope")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected websocket timeout, got %v", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear read deadline: %v", err)
	}
}

func senderMatchesRef(ref *internalproto.UserRef, key store.UserKey) bool {
	return ref != nil && ref.GetNodeId() == key.NodeID && ref.GetUserId() == key.UserID
}

func loginToken(t *testing.T, handler http.Handler, key store.UserKey, password string) string {
	t.Helper()

	var response struct {
		Token string `json:"token"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/auth/login", map[string]any{
		"node_id":  key.NodeID,
		"user_id":  key.UserID,
		"password": password,
	}, nil, http.StatusOK), &response)
	if response.Token == "" {
		t.Fatalf("expected login token")
	}
	return response.Token
}

func createUserAs(t *testing.T, handler http.Handler, token, username, password, role string) store.UserKey {
	t.Helper()
	return createUserAsWithOptions(t, handler, token, username, password, role, "", nil)
}

func createUserAsWithOptions(t *testing.T, handler http.Handler, token, username, password, role, loginName string, profile any) store.UserKey {
	t.Helper()

	var response struct {
		NodeID int64 `json:"node_id"`
		UserID int64 `json:"user_id"`
	}
	payload := map[string]any{
		"username": username,
		"password": password,
		"role":     role,
	}
	if loginName != "" {
		payload["login_name"] = loginName
	}
	if profile != nil {
		payload["profile"] = profile
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPost, "/users", payload, map[string]string{
		"Authorization": "Bearer " + token,
	}, http.StatusCreated), &response)
	key := store.UserKey{NodeID: response.NodeID, UserID: response.UserID}
	if err := key.Validate(); err != nil {
		t.Fatalf("expected created user id")
	}
	return key
}

func attachmentsPath(owner store.UserKey) string {
	return userPath(owner.NodeID, owner.UserID) + "/attachments"
}

func attachmentPath(owner store.UserKey, attachmentType store.AttachmentType, subject store.UserKey) string {
	return attachmentsPath(owner) + "/" + string(attachmentType) + "/" + strconv.FormatInt(subject.NodeID, 10) + "/" + strconv.FormatInt(subject.UserID, 10)
}

func subscriptionsPath(nodeID, userID int64) string {
	return userPath(nodeID, userID) + "/subscriptions"
}

func subscriptionPath(nodeID, userID int64, channel store.UserKey) string {
	return subscriptionsPath(nodeID, userID) + "/" + strconv.FormatInt(channel.NodeID, 10) + "/" + strconv.FormatInt(channel.UserID, 10)
}

func blacklistPath(nodeID, userID int64) string {
	return userPath(nodeID, userID) + "/blacklist"
}

func blockedUserPath(nodeID, userID int64, blocked store.UserKey) string {
	return blacklistPath(nodeID, userID) + "/" + strconv.FormatInt(blocked.NodeID, 10) + "/" + strconv.FormatInt(blocked.UserID, 10)
}

type authUserItem struct {
	NodeID    int64  `json:"node_id"`
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	LoginName string `json:"login_name"`
}

type authMessageItem struct {
	Body []byte `json:"body"`
}

func responseUsersContainKey(users []authUserItem, key store.UserKey) bool {
	_, ok := responseUserByKey(users, key)
	return ok
}

func responseUserByKey(users []authUserItem, key store.UserKey) (authUserItem, bool) {
	for _, user := range users {
		if user.NodeID == key.NodeID && user.UserID == key.UserID {
			return user, true
		}
	}
	return authUserItem{}, false
}

func responseMessagesContainBody(messages []authMessageItem, body string) bool {
	for _, message := range messages {
		if string(message.Body) == body {
			return true
		}
	}
	return false
}

func protoUsersContainKey(users []*internalproto.User, key store.UserKey) bool {
	_, ok := protoUserByKey(users, key)
	return ok
}

func protoUserByKey(users []*internalproto.User, key store.UserKey) (*internalproto.User, bool) {
	for _, user := range users {
		if user != nil && user.GetNodeId() == key.NodeID && user.GetUserId() == key.UserID {
			return user, true
		}
	}
	return nil, false
}

func dialClientWebSocket(t *testing.T, serverURL string) *websocket.Conn {
	t.Helper()
	return dialClientWebSocketPath(t, serverURL, clientWSPath)
}

func dialClientRealtimeWebSocket(t *testing.T, serverURL string) *websocket.Conn {
	t.Helper()
	return dialClientWebSocketPath(t, serverURL, clientRealtimeWSPath)
}

func dialClientWebSocketPath(t *testing.T, serverURL, path string) *websocket.Conn {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server url: %v", err)
	}
	parsed.Scheme = "ws"
	parsed.Path = path
	conn, _, err := websocket.DefaultDialer.Dial(parsed.String(), nil)
	if err != nil {
		t.Fatalf("dial client websocket: %v", err)
	}
	return conn
}

func writeClientEnvelope(t *testing.T, conn *websocket.Conn, envelope *internalproto.ClientEnvelope) {
	t.Helper()
	data, err := gproto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal client envelope: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write client envelope: %v", err)
	}
}

func readServerEnvelope(t *testing.T, conn *websocket.Conn) *internalproto.ServerEnvelope {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read server envelope: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("unexpected websocket message type: %d", messageType)
	}
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal server envelope: %v", err)
	}
	return &envelope
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

func newIPv4TestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on ipv4 loopback: %v", err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: handler},
	}
	server.Start()
	return server
}
