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
	"path/filepath"
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

func silenceClusterLogs(tb testing.TB) {
	tb.Helper()
	clusterLogCaptureMu.Lock()
	originalLogger := log.Logger
	log.Logger = zerolog.New(io.Discard)
	tb.Cleanup(func() {
		log.Logger = originalLogger
		clusterLogCaptureMu.Unlock()
	})
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

func newReplicationTestStore(tb testing.TB, nodeID string, slot uint16) *store.Store {
	tb.Helper()
	return newReplicationTestStoreWithWindow(tb, nodeID, slot, store.DefaultMessageWindowSize)
}

func newReplicationTestStoreWithRetention(tb testing.TB, nodeID string, slot uint16, messageWindowSize int, maxEventsPerOrigin int) *store.Store {
	tb.Helper()
	dbPath := filepath.Join(tb.TempDir(), nodeID+".db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:                     testNodeID(slot),
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	})
	if err != nil {
		tb.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		tb.Fatalf("init store: %v", err)
	}
	tb.Cleanup(func() {
		_ = st.Close()
	})
	return st
}

func newReplicationTestStoreWithWindow(tb testing.TB, nodeID string, slot uint16, messageWindowSize int) *store.Store {
	tb.Helper()
	dbPath := filepath.Join(tb.TempDir(), nodeID+".db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:            testNodeID(slot),
		MessageWindowSize: messageWindowSize,
	})
	if err != nil {
		tb.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		tb.Fatalf("init store: %v", err)
	}
	tb.Cleanup(func() {
		_ = st.Close()
	})
	return st
}

func newPebbleReplicationTestStore(tb testing.TB, nodeID string, slot uint16) *store.Store {
	tb.Helper()
	return newPebbleReplicationTestStoreWithWindow(tb, nodeID, slot, store.DefaultMessageWindowSize)
}

func newPebbleReplicationTestStoreWithWindow(tb testing.TB, nodeID string, slot uint16, messageWindowSize int) *store.Store {
	tb.Helper()
	return newPebbleReplicationTestStoreWithRetention(tb, nodeID, slot, messageWindowSize, store.DefaultEventLogMaxEventsPerOrigin)
}

func newPebbleReplicationTestStoreWithRetention(tb testing.TB, nodeID string, slot uint16, messageWindowSize int, maxEventsPerOrigin int) *store.Store {
	tb.Helper()
	dir := tb.TempDir()
	st, err := store.Open(filepath.Join(dir, nodeID+".db"), store.Options{
		NodeID:                     testNodeID(slot),
		Engine:                     store.EnginePebble,
		PebblePath:                 filepath.Join(dir, nodeID+".pebble"),
		MessageWindowSize:          messageWindowSize,
		EventLogMaxEventsPerOrigin: maxEventsPerOrigin,
	})
	if err != nil {
		tb.Fatalf("open pebble store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		tb.Fatalf("init pebble store: %v", err)
	}
	tb.Cleanup(func() {
		if err := st.Close(); err != nil {
			tb.Fatalf("close pebble store: %v", err)
		}
	})
	return st
}

func newReplicationTestManager(tb testing.TB, st *store.Store) *Manager {
	tb.Helper()
	return newReplicationTestManagerWithWindow(tb, st, store.DefaultMessageWindowSize)
}

func newReplicationTestManagerWithWindow(tb testing.TB, st *store.Store, messageWindowSize int) *Manager {
	tb.Helper()
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
		tb.Fatalf("new manager: %v", err)
	}
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	tb.Cleanup(func() {
		if mgr.cancel != nil {
			mgr.cancel()
		}
	})
	return mgr
}

func mustHashPassword(tb testing.TB, password string) string {
	tb.Helper()
	hash, err := auth.HashPassword(password)
	if err != nil {
		tb.Fatalf("hash password: %v", err)
	}
	return hash
}

func mustJSON(tb testing.TB, data []byte, dst any) {
	tb.Helper()
	if err := json.Unmarshal(data, dst); err != nil {
		tb.Fatalf("unmarshal json: %v body=%s", err, string(data))
	}
}

func waitFor(tb testing.TB, timeout time.Duration, fn func() bool) {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fn() {
		tb.Fatalf("condition not met within %s", timeout)
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

func assertSnapshotRequest(tb testing.TB, sess *session, partition string) {
	tb.Helper()
	select {
	case envelope := <-sess.send:
		chunk := envelope.GetSnapshotChunk()
		if chunk == nil {
			tb.Fatalf("expected snapshot chunk request, got %+v", envelope)
		}
		if !chunk.Request {
			tb.Fatalf("expected snapshot chunk request flag")
		}
		if chunk.Partition != partition {
			tb.Fatalf("unexpected snapshot partition: got=%q want=%q", chunk.Partition, partition)
		}
	default:
		tb.Fatalf("expected snapshot chunk request for %s", partition)
	}
}

func mustListen(tb testing.TB) net.Listener {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen: %v", err)
	}
	return ln
}

func newClusterHTTPTestServer(tb testing.TB, handler http.Handler) *httptest.Server {
	tb.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Listener = mustListen(tb)
	server.Start()
	tb.Cleanup(server.Close)
	return server
}

func doJSON(tb testing.TB, baseURL, method, path string, body any, wantStatus int) []byte {
	tb.Helper()
	data, err := doJSONRequestWithHeaders(baseURL, method, path, body, wantStatus, nil)
	if err != nil {
		tb.Fatalf("request failed: %v", err)
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
