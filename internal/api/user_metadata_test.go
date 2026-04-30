package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/tursom/turntf/internal/store"
	internalproto "github.com/tursom/turntf/internal/proto"
)

func TestHTTPUserMetadataCRUDAndScan(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")
	aliceKey := createUserAs(t, handler, adminToken, "meta-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, handler, adminToken, "meta-bob", "bob-password", store.RoleUser)
	aliceToken := loginToken(t, handler, aliceKey, "alice-password")
	bobToken := loginToken(t, handler, bobKey, "bob-password")

	var upserted struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "session:web:1"), map[string]any{
		"value": []byte{0xff, 0x00, 'x'},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated), &upserted)
	if upserted.Key != "session:web:1" || string(upserted.Value) != string([]byte{0xff, 0x00, 'x'}) {
		t.Fatalf("unexpected upserted metadata: %+v", upserted)
	}

	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "session:web:2"), map[string]any{
		"value": []byte("second"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "draft:chat:1"), map[string]any{
		"value": []byte("draft"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	var loaded struct {
		Key   string `json:"key"`
		Value []byte `json:"value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(aliceKey, "session:web:1"), nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &loaded)
	if loaded.Key != "session:web:1" || string(loaded.Value) != string([]byte{0xff, 0x00, 'x'}) {
		t.Fatalf("unexpected loaded metadata: %+v", loaded)
	}

	doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(aliceKey, "session:web:1"), nil, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusForbidden)

	var firstPage struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
		NextAfter string `json:"next_after"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMetadataScanPath(aliceKey)+"?prefix=session:&limit=1", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &firstPage)
	if len(firstPage.Items) != 1 || firstPage.Items[0].Key != "session:web:1" || firstPage.NextAfter != "session:web:1" {
		t.Fatalf("unexpected first scan page: %+v", firstPage)
	}

	var secondPage struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
		NextAfter string `json:"next_after"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMetadataScanPath(aliceKey)+"?prefix=session:&after="+firstPage.NextAfter+"&limit=1", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &secondPage)
	if len(secondPage.Items) != 1 || secondPage.Items[0].Key != "session:web:2" || secondPage.NextAfter != "" {
		t.Fatalf("unexpected second scan page: %+v", secondPage)
	}

	var deleted struct {
		Key       string `json:"key"`
		DeletedAt string `json:"deleted_at"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodDelete, userMetadataPath(aliceKey, "session:web:1"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &deleted)
	if deleted.Key != "session:web:1" || deleted.DeletedAt == "" {
		t.Fatalf("unexpected delete response: %+v", deleted)
	}
	doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(aliceKey, "session:web:1"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusNotFound)

	expiredAt := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "session:web:expired"), map[string]any{
		"value":      []byte("expired"),
		"expires_at": expiredAt,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(aliceKey, "session:web:expired"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusNotFound)
}

func TestClientWebSocketUserMetadataRPC(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "rpc-meta-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "rpc-meta-bob", "bob-password", store.RoleUser)

	aliceConn := dialClientWebSocket(t, server.URL)
	defer aliceConn.Close()
	loginClientWebSocket(t, aliceConn, aliceKey, "alice-password")

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserMetadata{
			UpsertUserMetadata: &internalproto.UpsertUserMetadataRequest{
				RequestId: 41,
				Owner:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Key:       "session:web:1",
				Value:     []byte{0x00, 0xfe},
			},
		},
	})
	upsertResp := readServerEnvelope(t, aliceConn).GetUpsertUserMetadataResponse()
	if upsertResp == nil || upsertResp.RequestId != 41 || upsertResp.Metadata.GetKey() != "session:web:1" || string(upsertResp.Metadata.GetValue()) != string([]byte{0x00, 0xfe}) {
		t.Fatalf("unexpected upsert metadata response: %+v", upsertResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 42,
				Owner:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Key:       "session:web:1",
			},
		},
	})
	getResp := readServerEnvelope(t, aliceConn).GetGetUserMetadataResponse()
	if getResp == nil || getResp.RequestId != 42 || string(getResp.Metadata.GetValue()) != string([]byte{0x00, 0xfe}) {
		t.Fatalf("unexpected get metadata response: %+v", getResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ScanUserMetadata{
			ScanUserMetadata: &internalproto.ScanUserMetadataRequest{
				RequestId: 43,
				Owner:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Prefix:    "session:",
				Limit:     10,
			},
		},
	})
	scanResp := readServerEnvelope(t, aliceConn).GetScanUserMetadataResponse()
	if scanResp == nil || scanResp.RequestId != 43 || scanResp.Count != 1 || scanResp.Items[0].GetKey() != "session:web:1" {
		t.Fatalf("unexpected scan metadata response: %+v", scanResp)
	}

	bobConn := dialClientWebSocket(t, server.URL)
	defer bobConn.Close()
	loginClientWebSocket(t, bobConn, bobKey, "bob-password")
	writeClientEnvelope(t, bobConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 44,
				Owner:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Key:       "session:web:1",
			},
		},
	})
	forbidden := readServerEnvelope(t, bobConn).GetError()
	if forbidden == nil || forbidden.RequestId != 44 || forbidden.Code != "forbidden" {
		t.Fatalf("unexpected forbidden metadata response: %+v", forbidden)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUserMetadata{
			DeleteUserMetadata: &internalproto.DeleteUserMetadataRequest{
				RequestId: 45,
				Owner:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Key:       "session:web:1",
			},
		},
	})
	deleteResp := readServerEnvelope(t, aliceConn).GetDeleteUserMetadataResponse()
	if deleteResp == nil || deleteResp.RequestId != 45 || deleteResp.Metadata.GetDeletedAt() == "" {
		t.Fatalf("unexpected delete metadata response: %+v", deleteResp)
	}
	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 46,
				Owner:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Key:       "session:web:1",
			},
		},
	})
	notFound := readServerEnvelope(t, aliceConn).GetError()
	if notFound == nil || notFound.RequestId != 46 || notFound.Code != "not_found" {
		t.Fatalf("unexpected not found metadata response: %+v", notFound)
	}
}

func userMetadataScanPath(owner store.UserKey) string {
	return userPath(owner.NodeID, owner.UserID) + "/metadata"
}

func userMetadataPath(owner store.UserKey, key string) string {
	return userMetadataScanPath(owner) + "/" + key
}
