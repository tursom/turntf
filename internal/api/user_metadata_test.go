package api

import (
	"net/http"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
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

func TestHTTPSelfScopedUserMetadataSupportsCurrentUserSentinel(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")
	aliceKey := createUserAs(t, handler, adminToken, "self-meta-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, handler, adminToken, "self-meta-bob", "bob-password", store.RoleUser)
	aliceToken := loginToken(t, handler, aliceKey, "alice-password")
	bobToken := loginToken(t, handler, bobKey, "bob-password")

	currentUser := store.UserKey{}

	var upserted struct {
		Owner store.UserKey `json:"owner"`
		Key   string        `json:"key"`
		Value []byte        `json:"value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(currentUser, "session:self"), map[string]any{
		"value": []byte("self-current"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated), &upserted)
	if upserted.Owner != aliceKey || upserted.Key != "session:self" || string(upserted.Value) != "self-current" {
		t.Fatalf("unexpected self sentinel metadata upsert: %+v", upserted)
	}

	var loaded struct {
		Owner store.UserKey `json:"owner"`
		Key   string        `json:"key"`
		Value []byte        `json:"value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(currentUser, "session:self"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &loaded)
	if loaded.Owner != aliceKey || loaded.Key != "session:self" || string(loaded.Value) != "self-current" {
		t.Fatalf("unexpected self sentinel metadata get: %+v", loaded)
	}

	var scanned struct {
		Items []struct {
			Owner store.UserKey `json:"owner"`
			Key   string        `json:"key"`
		} `json:"items"`
		Count int `json:"count"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMetadataScanPath(currentUser)+"?prefix=session:&limit=10", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &scanned)
	if scanned.Count != 1 || len(scanned.Items) != 1 || scanned.Items[0].Owner != aliceKey || scanned.Items[0].Key != "session:self" {
		t.Fatalf("unexpected self sentinel metadata scan: %+v", scanned)
	}

	var deleted struct {
		Owner     store.UserKey `json:"owner"`
		Key       string        `json:"key"`
		DeletedAt string        `json:"deleted_at"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodDelete, userMetadataPath(currentUser, "session:self"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &deleted)
	if deleted.Owner != aliceKey || deleted.Key != "session:self" || deleted.DeletedAt == "" {
		t.Fatalf("unexpected self sentinel metadata delete: %+v", deleted)
	}

	doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(currentUser, "session:self"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusNotFound)
	doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(store.UserKey{UserID: aliceKey.UserID}, "session:self"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)
	doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(store.UserKey{NodeID: aliceKey.NodeID}, "session:self"), nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)
	doJSONWithHeaders(t, handler, http.MethodGet, userMetadataPath(aliceKey, "session:self"), nil, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusForbidden)
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

func TestClientWebSocketUserMetadataSupportsImplicitCurrentUserOwner(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "implicit-meta-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "implicit-meta-bob", "bob-password", store.RoleUser)

	aliceConn := dialClientWebSocket(t, server.URL)
	defer aliceConn.Close()
	loginClientWebSocket(t, aliceConn, aliceKey, "alice-password")

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserMetadata{
			UpsertUserMetadata: &internalproto.UpsertUserMetadataRequest{
				RequestId: 61,
				Key:       "session:nil",
				Value:     []byte("nil-owner"),
			},
		},
	})
	upsertNilResp := readServerEnvelope(t, aliceConn).GetUpsertUserMetadataResponse()
	if upsertNilResp == nil || upsertNilResp.RequestId != 61 || upsertNilResp.Metadata.GetKey() != "session:nil" || string(upsertNilResp.Metadata.GetValue()) != "nil-owner" {
		t.Fatalf("unexpected nil-owner upsert metadata response: %+v", upsertNilResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserMetadata{
			UpsertUserMetadata: &internalproto.UpsertUserMetadataRequest{
				RequestId: 62,
				Owner:     &internalproto.UserRef{},
				Key:       "session:zero",
				Value:     []byte("zero-owner"),
			},
		},
	})
	upsertZeroResp := readServerEnvelope(t, aliceConn).GetUpsertUserMetadataResponse()
	if upsertZeroResp == nil || upsertZeroResp.RequestId != 62 || upsertZeroResp.Metadata.GetKey() != "session:zero" || string(upsertZeroResp.Metadata.GetValue()) != "zero-owner" {
		t.Fatalf("unexpected zero-owner upsert metadata response: %+v", upsertZeroResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 63,
				Key:       "session:nil",
			},
		},
	})
	getNilResp := readServerEnvelope(t, aliceConn).GetGetUserMetadataResponse()
	if getNilResp == nil || getNilResp.RequestId != 63 || string(getNilResp.Metadata.GetValue()) != "nil-owner" {
		t.Fatalf("unexpected nil-owner get metadata response: %+v", getNilResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 64,
				Owner:     &internalproto.UserRef{},
				Key:       "session:zero",
			},
		},
	})
	getZeroResp := readServerEnvelope(t, aliceConn).GetGetUserMetadataResponse()
	if getZeroResp == nil || getZeroResp.RequestId != 64 || string(getZeroResp.Metadata.GetValue()) != "zero-owner" {
		t.Fatalf("unexpected zero-owner get metadata response: %+v", getZeroResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ScanUserMetadata{
			ScanUserMetadata: &internalproto.ScanUserMetadataRequest{
				RequestId: 65,
				Prefix:    "session:",
				Limit:     10,
			},
		},
	})
	scanNilResp := readServerEnvelope(t, aliceConn).GetScanUserMetadataResponse()
	if scanNilResp == nil || scanNilResp.RequestId != 65 || scanNilResp.Count != 2 {
		t.Fatalf("unexpected nil-owner scan metadata response: %+v", scanNilResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ScanUserMetadata{
			ScanUserMetadata: &internalproto.ScanUserMetadataRequest{
				RequestId: 66,
				Owner:     &internalproto.UserRef{},
				Prefix:    "session:",
				Limit:     10,
			},
		},
	})
	scanZeroResp := readServerEnvelope(t, aliceConn).GetScanUserMetadataResponse()
	if scanZeroResp == nil || scanZeroResp.RequestId != 66 || scanZeroResp.Count != 2 {
		t.Fatalf("unexpected zero-owner scan metadata response: %+v", scanZeroResp)
	}

	bobConn := dialClientWebSocket(t, server.URL)
	defer bobConn.Close()
	loginClientWebSocket(t, bobConn, bobKey, "bob-password")
	writeClientEnvelope(t, bobConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 67,
				Owner:     &internalproto.UserRef{NodeId: aliceKey.NodeID, UserId: aliceKey.UserID},
				Key:       "session:nil",
			},
		},
	})
	forbidden := readServerEnvelope(t, bobConn).GetError()
	if forbidden == nil || forbidden.RequestId != 67 || forbidden.Code != "forbidden" {
		t.Fatalf("unexpected explicit other metadata error: %+v", forbidden)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUserMetadata{
			DeleteUserMetadata: &internalproto.DeleteUserMetadataRequest{
				RequestId: 68,
				Key:       "session:nil",
			},
		},
	})
	deleteNilResp := readServerEnvelope(t, aliceConn).GetDeleteUserMetadataResponse()
	if deleteNilResp == nil || deleteNilResp.RequestId != 68 || deleteNilResp.Metadata.GetDeletedAt() == "" {
		t.Fatalf("unexpected nil-owner delete metadata response: %+v", deleteNilResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_DeleteUserMetadata{
			DeleteUserMetadata: &internalproto.DeleteUserMetadataRequest{
				RequestId: 69,
				Owner:     &internalproto.UserRef{},
				Key:       "session:zero",
			},
		},
	})
	deleteZeroResp := readServerEnvelope(t, aliceConn).GetDeleteUserMetadataResponse()
	if deleteZeroResp == nil || deleteZeroResp.RequestId != 69 || deleteZeroResp.Metadata.GetDeletedAt() == "" {
		t.Fatalf("unexpected zero-owner delete metadata response: %+v", deleteZeroResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 70,
				Key:       "session:nil",
			},
		},
	})
	notFoundNil := readServerEnvelope(t, aliceConn).GetError()
	if notFoundNil == nil || notFoundNil.RequestId != 70 || notFoundNil.Code != "not_found" {
		t.Fatalf("unexpected nil-owner not found metadata response: %+v", notFoundNil)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 71,
				Owner:     &internalproto.UserRef{},
				Key:       "session:zero",
			},
		},
	})
	notFoundZero := readServerEnvelope(t, aliceConn).GetError()
	if notFoundZero == nil || notFoundZero.RequestId != 71 || notFoundZero.Code != "not_found" {
		t.Fatalf("unexpected zero-owner not found metadata response: %+v", notFoundZero)
	}
}

func userMetadataScanPath(owner store.UserKey) string {
	return userPath(owner.NodeID, owner.UserID) + "/metadata"
}

func userMetadataPath(owner store.UserKey, key string) string {
	return userMetadataScanPath(owner) + "/" + key
}
