package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
	"github.com/tursom/turntf/internal/trace"
)

func TestTracedMessageAPIDoesNotExposeUntracedBodies(t *testing.T) {
	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	path := fmt.Sprintf("/nodes/%d/users/%d/messages", adminKey.NodeID, adminKey.UserID)
	var created messageResponse
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, map[string]any{
		"body": []byte("diagnostic body must not be retained"), "trace_requested": true,
	}, map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusCreated), &created)
	if !trace.ValidID(created.TraceID) {
		t.Fatalf("missing trace id in create response: %+v", created)
	}
	var result struct {
		Events []trace.Event `json:"events"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/traces/"+created.TraceID, nil,
		map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusOK), &result)
	if len(result.Events) != 1 || result.Events[0].Stage != "stored" || result.Events[0].MessageSeq != created.Seq {
		t.Fatalf("unexpected persistent trace: %+v", result.Events)
	}
	if result.Events[0].MessageNodeID != created.NodeID {
		t.Fatalf("message node mismatch: %+v", result.Events[0])
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/traces/"+created.TraceID, nil, nil, http.StatusUnauthorized)
	user := createUserAs(t, testAPI.handler, adminToken, "trace-reader", "reader-password", store.RoleUser)
	userToken := loginToken(t, testAPI.handler, user, "reader-password")
	doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/traces/"+created.TraceID, nil,
		map[string]string{"Authorization": "Bearer " + userToken}, http.StatusForbidden)

	var plain messageResponse
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, map[string]any{
		"body": []byte("plain"),
	}, map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusCreated), &plain)
	if plain.TraceID != "" {
		t.Fatalf("untraced message gained trace id: %q", plain.TraceID)
	}
}

func TestTracedPersistentEventSurvivesPebbleWrite(t *testing.T) {
	testAPI := newAuthenticatedTestAPIWithStoreOptions(t, store.Options{Engine: store.EnginePebble})
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	token := loginToken(t, testAPI.handler, key, "root-password")
	var result messageResponse
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost,
		fmt.Sprintf("/nodes/%d/users/%d/messages", key.NodeID, key.UserID),
		map[string]any{"body": []byte("pebble trace"), "trace_requested": true},
		map[string]string{"Authorization": "Bearer " + token}, http.StatusCreated), &result)
	events, err := testAPI.http.service.store.ListEvents(context.Background(), 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if message, ok := event.Body.(*internalproto.MessageCreatedEvent); ok && message.GetSeq() == result.Seq {
			if !trace.ValidID(result.TraceID) || message.GetTraceId() != result.TraceID {
				t.Fatalf("pebble event lost trace id: %+v", message)
			}
			return
		}
	}
	t.Fatal("traced message creation event missing from Pebble log")
}

func TestLocalTransientTraceIsNotAnEndUserAck(t *testing.T) {
	testAPI := newAuthenticatedTestAPI(t)
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	packet, err := testAPI.http.service.DispatchTransientPacketToTraced(context.Background(), key, key,
		[]byte("test"), store.DeliveryModeBestEffort, store.SessionRef{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !trace.ValidID(packet.TraceID) {
		t.Fatalf("invalid trace id: %q", packet.TraceID)
	}
	events := testAPI.http.service.TraceEvents(packet.TraceID)
	if len(events) != 2 || events[0].Stage != "delivery_missed" || events[1].Stage != "accepted" {
		t.Fatalf("acceptance is not delivery: %+v", events)
	}
}

func TestWebSocketSendReturnsTraceID(t *testing.T) {
	testAPI := newAuthenticatedTestAPI(t)
	server := newIPv4TestServer(t, testAPI.handler)
	defer server.Close()
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	conn := dialClientWebSocket(t, server.URL)
	defer conn.Close()
	loginClientWebSocket(t, conn, key, "root-password")
	writeClientEnvelope(t, conn, &internalproto.ClientEnvelope{Body: &internalproto.ClientEnvelope_SendMessage{
		SendMessage: &internalproto.SendMessageRequest{RequestId: 111, Target: &internalproto.UserRef{
			NodeId: key.NodeID, UserId: key.UserID}, Body: []byte("ws traced message"), TraceRequested: true},
	}})
	response := readServerEnvelope(t, conn).GetSendMessageResponse()
	if response == nil || response.RequestId != 111 || !trace.ValidID(response.GetTraceId()) {
		t.Fatalf("missing trace id in websocket response: %+v", response)
	}
	if events := testAPI.http.service.TraceEvents(response.GetTraceId()); len(events) == 0 || events[0].Stage != "stored" {
		t.Fatalf("traced websocket message not recorded: %+v", events)
	}
}

type testProbeSink struct {
	calls  int
	target int64
}

func (*testProbeSink) Publish(store.Event) {}
func (sink *testProbeSink) ProbeRoute(_ context.Context, target int64) (string, error) {
	sink.calls++
	sink.target = target
	return "aabbccddeeff00112233445566778899", nil
}

func TestOpsProbeRequiresAdminAndBoundsRequests(t *testing.T) {
	sink := &testProbeSink{}
	testAPI := newAuthenticatedTestAPIWithSink(t, sink)
	key := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, key, "root-password")
	user := createUserAs(t, testAPI.handler, adminToken, "probe-reader", "reader-password", store.RoleUser)
	userToken := loginToken(t, testAPI.handler, user, "reader-password")
	target := testNodeID(2)
	body := map[string]any{"target_node_id": target}
	path := "/ops/probes"
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, body, nil, http.StatusUnauthorized)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, body,
		map[string]string{"Authorization": "Bearer " + userToken}, http.StatusForbidden)
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, map[string]any{"target_node_id": 0},
		map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusBadRequest)
	var response struct {
		TraceID      string `json:"trace_id"`
		SourceNodeID int64  `json:"source_node_id"`
		Status       string `json:"status"`
	}
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, body,
		map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusAccepted), &response)
	if !trace.ValidID(response.TraceID) || response.SourceNodeID != key.NodeID || response.Status != "dispatched" || sink.calls != 1 || sink.target != target {
		t.Fatalf("unexpected probe response: %+v sink=%+v", response, sink)
	}
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, body,
		map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusTooManyRequests)
	if sink.calls != 1 {
		t.Fatal("rate-limited request started another probe")
	}
	testAPI.http.probeMu.Lock()
	testAPI.http.lastProbe = time.Now().Add(-2 * time.Second)
	testAPI.http.probeMu.Unlock()
	doJSONWithHeaders(t, testAPI.handler, http.MethodPost, path, map[string]any{"target_node_id": target, "body": "not allowed"},
		map[string]string{"Authorization": "Bearer " + adminToken}, http.StatusBadRequest)
}
