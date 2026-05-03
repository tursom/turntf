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

func TestHTTPUserMetadataTypedValueAndChannelManagerAccess(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")
	aliceKey := createUserAs(t, handler, adminToken, "typed-meta-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, handler, adminToken, "typed-meta-bob", "bob-password", store.RoleUser)
	channelKey := createUserAs(t, handler, adminToken, "typed-meta-channel", "", store.RoleChannel)
	aliceToken := loginToken(t, handler, aliceKey, "alice-password")
	bobToken := loginToken(t, handler, bobKey, "bob-password")

	doJSONWithHeaders(t, handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelManager, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	var boolMetadata struct {
		Key        string `json:"key"`
		Value      []byte `json:"value"`
		TypedValue struct {
			Kind      string `json:"kind"`
			BoolValue bool   `json:"bool_value"`
		} `json:"typed_value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, store.UserMetadataKeyVisibleToOthers), map[string]any{
		"typed_value": map[string]any{
			"kind":       "bool",
			"bool_value": false,
		},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated), &boolMetadata)
	if boolMetadata.Key != store.UserMetadataKeyVisibleToOthers || string(boolMetadata.Value) != "false" || boolMetadata.TypedValue.Kind != "bool" || boolMetadata.TypedValue.BoolValue {
		t.Fatalf("unexpected bool metadata response: %+v", boolMetadata)
	}

	var jsonMetadata struct {
		Key        string `json:"key"`
		Value      []byte `json:"value"`
		TypedValue struct {
			Kind      string         `json:"kind"`
			JSONValue map[string]any `json:"json_value"`
		} `json:"typed_value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(channelKey, "channel.config"), map[string]any{
		"typed_value": map[string]any{
			"kind":       "json",
			"json_value": map[string]any{"theme": "blue"},
		},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated), &jsonMetadata)
	if jsonMetadata.TypedValue.Kind != "json" || jsonMetadata.TypedValue.JSONValue["theme"] != "blue" {
		t.Fatalf("unexpected json metadata response: %+v", jsonMetadata)
	}

	var stringMetadata struct {
		Value      []byte `json:"value"`
		TypedValue struct {
			Kind        string `json:"kind"`
			StringValue string `json:"string_value"`
		} `json:"typed_value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "profile.nickname"), map[string]any{
		"typed_value": map[string]any{
			"kind":         "string",
			"string_value": "Alice",
		},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated), &stringMetadata)
	if string(stringMetadata.Value) != "\"Alice\"" || stringMetadata.TypedValue.Kind != "string" || stringMetadata.TypedValue.StringValue != "Alice" {
		t.Fatalf("unexpected string metadata response: %+v", stringMetadata)
	}

	var numberMetadata struct {
		Value      []byte `json:"value"`
		TypedValue struct {
			Kind        string  `json:"kind"`
			NumberValue float64 `json:"number_value"`
		} `json:"typed_value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "profile.score"), map[string]any{
		"typed_value": map[string]any{
			"kind":         "number",
			"number_value": 7.5,
		},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated), &numberMetadata)
	if string(numberMetadata.Value) != "7.5" || numberMetadata.TypedValue.Kind != "number" || numberMetadata.TypedValue.NumberValue != 7.5 {
		t.Fatalf("unexpected number metadata response: %+v", numberMetadata)
	}

	var bytesMetadata struct {
		Key        string `json:"key"`
		Value      []byte `json:"value"`
		TypedValue any    `json:"typed_value"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "avatar.raw"), map[string]any{
		"typed_value": map[string]any{
			"kind":        "bytes",
			"bytes_value": "AAE=",
		},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated), &bytesMetadata)
	if string(bytesMetadata.Value) != string([]byte{0x00, 0x01}) || bytesMetadata.TypedValue != nil {
		t.Fatalf("unexpected bytes metadata response: %+v", bytesMetadata)
	}

	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, "invalid.payload"), map[string]any{
		"value": []byte("one"),
		"typed_value": map[string]any{
			"kind":       "bool",
			"bool_value": true,
		},
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)

	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(aliceKey, store.UserMetadataKeyVisibleToOthers), map[string]any{
		"typed_value": map[string]any{
			"kind":       "bool",
			"bool_value": true,
		},
		"expires_at": time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusBadRequest)

	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(channelKey, "channel.config"), map[string]any{
		"value": []byte("forbidden"),
	}, map[string]string{
		"Authorization": "Bearer " + bobToken,
	}, http.StatusForbidden)

	var scanned struct {
		Items []struct {
			Key        string `json:"key"`
			TypedValue struct {
				Kind      string `json:"kind"`
				BoolValue bool   `json:"bool_value"`
			} `json:"typed_value"`
		} `json:"items"`
	}
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, userMetadataScanPath(aliceKey)+"?prefix=system.&limit=10", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &scanned)
	if len(scanned.Items) != 1 || scanned.Items[0].Key != store.UserMetadataKeyVisibleToOthers || scanned.Items[0].TypedValue.Kind != "bool" || scanned.Items[0].TypedValue.BoolValue {
		t.Fatalf("unexpected system metadata scan response: %+v", scanned)
	}
}

func TestClientWebSocketUserMetadataSupportsChannelOwnerAndVisibilityBytes(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "ws-meta-alice", "alice-password", store.RoleUser)
	channelKey := createUserAs(t, testAPI.handler, adminToken, "ws-meta-channel", "", store.RoleChannel)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, attachmentPath(channelKey, store.AttachmentTypeChannelManager, aliceKey), map[string]any{
		"config_json": map[string]any{},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	aliceConn := dialClientWebSocket(t, server.URL)
	defer aliceConn.Close()
	loginClientWebSocket(t, aliceConn, aliceKey, "alice-password")

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_UpsertUserMetadata{
			UpsertUserMetadata: &internalproto.UpsertUserMetadataRequest{
				RequestId: 81,
				Owner:     &internalproto.UserRef{NodeId: channelKey.NodeID, UserId: channelKey.UserID},
				Key:       store.UserMetadataKeyVisibleToOthers,
				Value:     []byte("false"),
			},
		},
	})
	upsertResp := readServerEnvelope(t, aliceConn).GetUpsertUserMetadataResponse()
	if upsertResp == nil || upsertResp.RequestId != 81 || upsertResp.Metadata.GetKey() != store.UserMetadataKeyVisibleToOthers || string(upsertResp.Metadata.GetValue()) != "false" {
		t.Fatalf("unexpected websocket visibility upsert response: %+v", upsertResp)
	}

	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_GetUserMetadata{
			GetUserMetadata: &internalproto.GetUserMetadataRequest{
				RequestId: 82,
				Owner:     &internalproto.UserRef{NodeId: channelKey.NodeID, UserId: channelKey.UserID},
				Key:       store.UserMetadataKeyVisibleToOthers,
			},
		},
	})
	getResp := readServerEnvelope(t, aliceConn).GetGetUserMetadataResponse()
	if getResp == nil || getResp.RequestId != 82 || string(getResp.Metadata.GetValue()) != "false" {
		t.Fatalf("unexpected websocket visibility get response: %+v", getResp)
	}
}

func TestHTTPListUsersRespectsVisibilityMetadata(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	handler := testAPI.handler

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, handler, adminKey, "root-password")
	aliceKey := createUserAs(t, handler, adminToken, "visible-http-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, handler, adminToken, "visible-http-bob", "bob-password", store.RoleUser)
	channelKey := createUserAs(t, handler, adminToken, "visible-http-channel", "", store.RoleChannel)
	aliceToken := loginToken(t, handler, aliceKey, "alice-password")

	doJSONWithHeaders(t, handler, http.MethodPost, subscriptionsPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"channel_node_id": channelKey.NodeID,
		"channel_user_id": channelKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)

	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(bobKey, store.UserMetadataKeyVisibleToOthers), map[string]any{
		"typed_value": map[string]any{
			"kind":       "bool",
			"bool_value": false,
		},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, handler, http.MethodPut, userMetadataPath(channelKey, store.UserMetadataKeyVisibleToOthers), map[string]any{
		"typed_value": map[string]any{
			"kind":       "bool",
			"bool_value": false,
		},
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	var users []authUserItem
	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users", nil, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusOK), &users)
	if responseUsersContainKey(users, bobKey) || responseUsersContainKey(users, channelKey) {
		t.Fatalf("expected hidden users to disappear from /users: %+v", users)
	}

	mustJSON(t, doJSONWithHeaders(t, handler, http.MethodGet, "/users", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &users)
	if !responseUsersContainKey(users, bobKey) || !responseUsersContainKey(users, channelKey) {
		t.Fatalf("expected admin /users to keep hidden entries visible: %+v", users)
	}

	doJSONWithHeaders(t, handler, http.MethodPost, userMessagesPath(bobKey.NodeID, bobKey.UserID), map[string]any{
		"body": []byte("hidden direct message"),
	}, map[string]string{
		"Authorization": "Bearer " + aliceToken,
	}, http.StatusCreated)
}

func TestClientWebSocketListUsersRespectsVisibilityMetadata(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	aliceKey := createUserAs(t, testAPI.handler, adminToken, "visible-ws-alice", "alice-password", store.RoleUser)
	bobKey := createUserAs(t, testAPI.handler, adminToken, "visible-ws-bob", "bob-password", store.RoleUser)
	channelKey := createUserAs(t, testAPI.handler, adminToken, "visible-ws-channel", "", store.RoleChannel)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, subscriptionsPath(aliceKey.NodeID, aliceKey.UserID), map[string]any{
		"channel_node_id": channelKey.NodeID,
		"channel_user_id": channelKey.UserID,
	}, map[string]string{
		"Authorization": "Bearer " + loginToken(t, testAPI.handler, aliceKey, "alice-password"),
	}, http.StatusCreated)

	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, userMetadataPath(bobKey, store.UserMetadataKeyVisibleToOthers), map[string]any{
		"value": []byte("false"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPut, userMetadataPath(channelKey, store.UserMetadataKeyVisibleToOthers), map[string]any{
		"value": []byte("false"),
	}, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusCreated)

	aliceConn := dialClientWebSocket(t, server.URL)
	defer aliceConn.Close()
	loginClientWebSocket(t, aliceConn, aliceKey, "alice-password")
	writeClientEnvelope(t, aliceConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{RequestId: 91},
		},
	})
	aliceResp := readServerEnvelope(t, aliceConn).GetListUsersResponse()
	if aliceResp == nil || protoUsersContainKey(aliceResp.Items, bobKey) || protoUsersContainKey(aliceResp.Items, channelKey) {
		t.Fatalf("expected websocket list_users to hide invisible entries: %+v", aliceResp)
	}

	adminConn := dialClientWebSocket(t, server.URL)
	defer adminConn.Close()
	loginClientWebSocket(t, adminConn, adminKey, "root-password")
	writeClientEnvelope(t, adminConn, &internalproto.ClientEnvelope{
		Body: &internalproto.ClientEnvelope_ListUsers{
			ListUsers: &internalproto.ListUsersRequest{RequestId: 92},
		},
	})
	adminResp := readServerEnvelope(t, adminConn).GetListUsersResponse()
	if adminResp == nil || !protoUsersContainKey(adminResp.Items, bobKey) || !protoUsersContainKey(adminResp.Items, channelKey) {
		t.Fatalf("expected admin websocket list_users to keep hidden entries visible: %+v", adminResp)
	}
}

func userMetadataScanPath(owner store.UserKey) string {
	return userPath(owner.NodeID, owner.UserID) + "/metadata"
}

func userMetadataPath(owner store.UserKey, key string) string {
	return userMetadataScanPath(owner) + "/" + key
}
