package api

import (
	"context"
	"errors"
	"sync"
	"testing"

	gproto "google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type transientPacketCaptureConn struct {
	mu      sync.Mutex
	packets []*internalproto.Packet
}

func (c *transientPacketCaptureConn) Send(_ context.Context, payload []byte) error {
	var envelope internalproto.ServerEnvelope
	if err := gproto.Unmarshal(payload, &envelope); err != nil {
		return err
	}
	packetPushed := envelope.GetPacketPushed()
	if packetPushed == nil || packetPushed.Packet == nil {
		return nil
	}
	c.mu.Lock()
	c.packets = append(c.packets, packetPushed.Packet)
	c.mu.Unlock()
	return nil
}

func (c *transientPacketCaptureConn) Receive(context.Context) ([]byte, error) {
	return nil, errors.New("receive is not implemented for transient packet capture")
}

func (c *transientPacketCaptureConn) Close() error { return nil }

func (c *transientPacketCaptureConn) RemoteAddr() string { return "capture" }

func (c *transientPacketCaptureConn) Transport() string { return "capture" }

func (c *transientPacketCaptureConn) packetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.packets)
}

func newTransientCaptureSession(httpAPI *HTTP, user store.User, sessionID string, conn *transientPacketCaptureConn) *clientWSSession {
	return &clientWSSession{
		http:              httpAPI,
		conn:              conn,
		protocol:          "capture",
		remoteAddr:        "capture",
		sessionRef:        store.SessionRef{ServingNodeID: user.NodeID, SessionID: sessionID},
		principal:         &requestPrincipal{User: user},
		transientOnly:     true,
		seen:              make(map[clientMessageCursor]struct{}),
		blacklistCache:    make(map[store.UserKey]clientBoolCacheEntry),
		subscriptionCache: make(map[store.UserKey]clientBoolCacheEntry),
	}
}

func TestHTTPReceiveTransientPacketStableSnapshotAndTargetSession(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPI(t)
	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")
	recipientKey := createUserAs(t, testAPI.handler, adminToken, "recipient", "recipient-password", store.RoleUser)
	recipientUser, err := testAPI.http.service.GetUser(context.Background(), recipientKey)
	if err != nil {
		t.Fatalf("get recipient user: %v", err)
	}

	connA := &transientPacketCaptureConn{}
	connB := &transientPacketCaptureConn{}
	sessA := newTransientCaptureSession(testAPI.http, recipientUser, "sess-a", connA)
	sessB := newTransientCaptureSession(testAPI.http, recipientUser, "sess-b", connB)
	testAPI.http.registerClientSession(recipientKey, sessA)
	testAPI.http.registerClientSession(recipientKey, sessB)
	t.Cleanup(func() {
		testAPI.http.unregisterClientSession(recipientKey, sessA)
		testAPI.http.unregisterClientSession(recipientKey, sessB)
	})

	shard := testAPI.http.sessionShard(recipientKey)
	if shard == nil {
		t.Fatal("expected session shard")
	}
	shard.mu.RLock()
	bucket := shard.sessions[recipientKey]
	shard.mu.RUnlock()
	if bucket == nil || len(bucket.snapshot) != 2 {
		t.Fatalf("expected stable snapshot with 2 sessions, got %+v", bucket)
	}

	broadcast := store.TransientPacket{
		PacketID:     71,
		SourceNodeID: adminKey.NodeID,
		TargetNodeID: recipientKey.NodeID,
		Recipient:    recipientKey,
		Sender:       adminKey,
		Body:         []byte("broadcast"),
		DeliveryMode: store.DeliveryModeBestEffort,
		TTLHops:      8,
	}
	if !testAPI.http.ReceiveTransientPacket(broadcast) {
		t.Fatal("expected broadcast transient packet to be delivered")
	}
	if connA.packetCount() != 1 || connB.packetCount() != 1 {
		t.Fatalf("expected broadcast to reach both sessions, got a=%d b=%d", connA.packetCount(), connB.packetCount())
	}

	targeted := broadcast
	targeted.PacketID = 72
	targeted.TargetSession = sessB.sessionRef
	targeted.Body = []byte("targeted")
	if !testAPI.http.ReceiveTransientPacket(targeted) {
		t.Fatal("expected targeted transient packet to be delivered")
	}
	if connA.packetCount() != 1 || connB.packetCount() != 2 {
		t.Fatalf("expected targeted delivery to only hit sess-b, got a=%d b=%d", connA.packetCount(), connB.packetCount())
	}
}
