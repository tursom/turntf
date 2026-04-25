package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/tursom/turntf/internal/auth"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

var clusterLogCaptureMu sync.Mutex

type clusterLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *clusterLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *clusterLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureClusterLogs(t *testing.T) *clusterLogBuffer {
	t.Helper()
	clusterLogCaptureMu.Lock()
	originalLogger := log.Logger
	logOutput := &clusterLogBuffer{}
	log.Logger = zerolog.New(logOutput)
	t.Cleanup(func() {
		log.Logger = originalLogger
		clusterLogCaptureMu.Unlock()
	})
	return logOutput
}

func newHandshakeTestManager(t *testing.T) *Manager {
	t.Helper()
	mgr, err := NewManager(Config{
		NodeID:            testNodeID(1),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: store.DefaultMessageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
		Peers: []Peer{
			{URL: "ws://127.0.0.1:9081/internal/cluster/ws"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	return mgr
}

func newReplicationTestStore(t *testing.T, nodeID string, slot uint16) *store.Store {
	t.Helper()
	return newReplicationTestStoreWithWindow(t, nodeID, slot, store.DefaultMessageWindowSize)
}

func newReplicationTestStoreWithRetention(t *testing.T, nodeID string, slot uint16, messageWindowSize int, maxEventsPerOrigin int) *store.Store {
	t.Helper()
	_ = nodeID
	dbPath := t.TempDir() + "/cluster.db"
	st, err := store.Open(dbPath, store.Options{
		NodeID:                     testNodeID(slot),
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	return st
}

func newReplicationTestStoreWithWindow(t *testing.T, nodeID string, slot uint16, messageWindowSize int) *store.Store {
	t.Helper()
	_ = nodeID
	dbPath := t.TempDir() + "/cluster.db"
	st, err := store.Open(dbPath, store.Options{
		NodeID:            testNodeID(slot),
		MessageWindowSize: messageWindowSize,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	return st
}

func newReplicationTestManager(t *testing.T, st *store.Store) *Manager {
	t.Helper()
	return newReplicationTestManagerWithWindow(t, st, store.DefaultMessageWindowSize)
}

func newReplicationTestManagerWithWindow(t *testing.T, st *store.Store, messageWindowSize int) *Manager {
	t.Helper()
	mgr, err := NewManager(Config{
		NodeID:            testNodeID(2),
		AdvertisePath:     websocketPath,
		ClusterSecret:     "secret",
		MessageWindowSize: messageWindowSize,
		MaxClockSkewMs:    DefaultMaxClockSkewMs,
		DiscoveryDisabled: true,
		Peers: []Peer{
			{URL: "ws://127.0.0.1:9080/internal/cluster/ws"},
		},
	}, st)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	t.Cleanup(func() {
		if mgr.cancel != nil {
			mgr.cancel()
		}
	})
	return mgr
}

func mustHashPassword(t *testing.T, password string) string {
	t.Helper()
	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return hash
}

func mustJSON(t *testing.T, data []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("unmarshal json: %v body=%s", err, string(data))
	}
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fn() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

func mustHelloEnvelope(t *testing.T, envelopeNodeID int64, hello *internalproto.Hello) *internalproto.Envelope {
	t.Helper()
	if hello != nil && hello.SnapshotVersion == "" {
		hello.SnapshotVersion = internalproto.SnapshotVersion
	}
	return &internalproto.Envelope{
		NodeId: envelopeNodeID,
		Body: &internalproto.Envelope_Hello{
			Hello: hello,
		},
	}
}

func snapshotDigestEnvelope(nodeID int64, digest *internalproto.SnapshotDigest) *internalproto.Envelope {
	return &internalproto.Envelope{
		NodeId: nodeID,
		Body: &internalproto.Envelope_SnapshotDigest{
			SnapshotDigest: digest,
		},
	}
}

func assertSnapshotRequest(t *testing.T, sess *session, partition string) {
	t.Helper()
	select {
	case envelope := <-sess.send:
		chunk := envelope.GetSnapshotChunk()
		if chunk == nil {
			t.Fatalf("expected snapshot chunk request, got %+v", envelope)
		}
		if !chunk.Request {
			t.Fatalf("expected snapshot chunk request flag")
		}
		if chunk.Partition != partition {
			t.Fatalf("unexpected snapshot partition: got=%q want=%q", chunk.Partition, partition)
		}
	default:
		t.Fatalf("expected snapshot chunk request for %s", partition)
	}
}

func mustListen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln
}

func newClusterHTTPTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Listener = mustListen(t)
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func doJSON(t *testing.T, baseURL, method, path string, body any, wantStatus int) []byte {
	t.Helper()
	data, err := doJSONRequestWithHeaders(baseURL, method, path, body, wantStatus, nil)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return data
}

func doJSONRequestWithHeaders(baseURL, method, path string, body any, wantStatus int, headers map[string]string) ([]byte, error) {
	var requestBody *bytes.Reader
	if body == nil {
		requestBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		requestBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequest(method, baseURL+path, requestBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data := new(bytes.Buffer)
	if _, err := data.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	if resp.StatusCode != wantStatus {
		return nil, errors.New("unexpected status for " + method + " " + path + ": got=" + strconv.Itoa(resp.StatusCode) + " want=" + strconv.Itoa(wantStatus) + " body=" + data.String())
	}
	return data.Bytes(), nil
}

func readySnapshotTestSession(mgr *Manager, peerID int64, remoteMessageWindowSize int) *session {
	sess := &session{
		manager:                 mgr,
		conn:                    testLegacyTransportConn{},
		peerID:                  peerID,
		connectionID:            1,
		remoteSnapshotVersion:   internalproto.SnapshotVersion,
		remoteMessageWindowSize: remoteMessageWindowSize,
		send:                    make(chan *internalproto.Envelope, 4),
	}
	sess.markReplicationReady()
	mgr.mu.Lock()
	peer := mgr.peers[peerID]
	if peer == nil {
		peer = &peerState{
			clockState: clockStateTrusted,
			sessions:   make(map[uint64]*session),
		}
		mgr.peers[peerID] = peer
	}
	now := time.Now().UTC()
	peer.active = sess
	peer.trustedSession = sess
	peer.clockState = clockStateTrusted
	peer.lastClockSync = now
	peer.lastCredibleClockSync = now
	peer.sessions[sess.connectionID] = sess
	mgr.lastTrustedClockSync = now
	mgr.refreshNodeClockStateLocked()
	mgr.mu.Unlock()
	return sess
}

type testLegacyTransportConn struct{}

func (testLegacyTransportConn) Send(context.Context, []byte) error { return nil }

func (testLegacyTransportConn) Receive(context.Context) ([]byte, error) { return nil, io.EOF }

func (testLegacyTransportConn) Close() error { return nil }

func (testLegacyTransportConn) LocalAddr() string { return "local" }

func (testLegacyTransportConn) RemoteAddr() string { return "remote" }

func (testLegacyTransportConn) Direction() string { return "outbound" }

func (testLegacyTransportConn) Transport() string { return transportWebSocket }
