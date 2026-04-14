package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/api"
	"github.com/tursom/turntf/internal/auth"
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const clusterTestAdminPassword = "root-password"

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

type clusterTestNode struct {
	id             int64
	name           string
	store          *store.Store
	service        *api.Service
	manager        *Manager
	server         *http.Server
	ln             net.Listener
	baseURL        string
	timeSyncScript *clusterTestTimeSyncScript

	startMu        sync.Mutex
	managerStarted bool
	serverStarted  bool
}

type clusterTestNodeConfig struct {
	Name                        string
	Slot                        uint16
	Peers                       []Peer
	MessageWindowSize           int
	ClockSkewMs                 int64
	MaxClockSkewMs              int64
	ClockRejectAfterSkewSamples int
	TimeSyncSamples             []timeSyncSample
	DisableRouting              bool
	DiscoveryEnabled            bool
}

type clusterTestNodeSpec struct {
	Name                        string
	Slot                        uint16
	PeerNames                   []string
	PeerLinks                   []clusterTestPeerLinkSpec
	MessageWindowSize           int
	ClockSkewMs                 int64
	MaxClockSkewMs              int64
	ClockRejectAfterSkewSamples int
	TimeSyncSamples             []timeSyncSample
	DisableRouting              bool
	DiscoveryEnabled            bool
}

type clusterTestPeerLinkSpec struct {
	Alias            string
	Target           string
	InitiallyBlocked bool
}

type clusterPeerExpectation struct {
	node   *clusterTestNode
	peerID int64
}

type clusterTestTimeSyncScript struct {
	mu       sync.Mutex
	samples  []timeSyncSample
	fallback timeSyncSample
}

func newClusterTestTimeSyncScript(samples []timeSyncSample) *clusterTestTimeSyncScript {
	script := &clusterTestTimeSyncScript{
		fallback: timeSyncSample{offsetMs: 0, rttMs: 1},
	}
	script.Set(samples...)
	return script
}

func (s *clusterTestTimeSyncScript) Set(samples ...timeSyncSample) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	s.samples = append([]timeSyncSample(nil), samples...)
	if len(samples) > 0 {
		s.fallback = samples[len(samples)-1]
	}
}

func (s *clusterTestTimeSyncScript) Next(*session) (timeSyncSample, error) {
	if s == nil {
		return timeSyncSample{offsetMs: 0, rttMs: 1}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.samples) == 0 {
		return s.fallback, nil
	}
	next := s.samples[0]
	s.samples = s.samples[1:]
	s.fallback = next
	if next.rttMs <= 0 {
		next.rttMs = 1
	}
	return next, nil
}

func newClusterTestNodeFixture(t *testing.T, cfg clusterTestNodeConfig) *clusterTestNode {
	t.Helper()
	return newClusterTestNodeFixtureWithListener(t, cfg, mustListen(t))
}

func newClusterTestNodes(t *testing.T, specs ...clusterTestNodeSpec) map[string]*clusterTestNode {
	t.Helper()

	if len(specs) == 0 {
		t.Fatalf("cluster test topology must not be empty")
	}

	listeners := make(map[string]net.Listener, len(specs))
	for _, spec := range specs {
		if spec.Name == "" {
			t.Fatalf("cluster test node name must not be empty")
		}
		if _, exists := listeners[spec.Name]; exists {
			t.Fatalf("duplicate cluster test node name: %s", spec.Name)
		}
		listeners[spec.Name] = mustListen(t)
	}

	nodes := make(map[string]*clusterTestNode, len(specs))
	for _, spec := range specs {
		peers := make([]Peer, 0, len(spec.PeerNames))
		for _, peerName := range spec.PeerNames {
			peerListener, ok := listeners[peerName]
			if !ok {
				t.Fatalf("unknown cluster test peer %q for node %q", peerName, spec.Name)
			}
			peers = append(peers, Peer{URL: wsURL(peerListener)})
		}

		cfg := clusterTestNodeConfig{
			Name:                        spec.Name,
			Slot:                        spec.Slot,
			Peers:                       peers,
			MessageWindowSize:           spec.MessageWindowSize,
			ClockSkewMs:                 spec.ClockSkewMs,
			MaxClockSkewMs:              spec.MaxClockSkewMs,
			ClockRejectAfterSkewSamples: spec.ClockRejectAfterSkewSamples,
			TimeSyncSamples:             spec.TimeSyncSamples,
			DisableRouting:              spec.DisableRouting,
			DiscoveryEnabled:            spec.DiscoveryEnabled,
		}
		if cfg.MessageWindowSize == 0 {
			cfg.MessageWindowSize = store.DefaultMessageWindowSize
		}
		if cfg.MaxClockSkewMs == 0 {
			cfg.MaxClockSkewMs = DefaultMaxClockSkewMs
		}

		node := newClusterTestNodeFixtureWithListener(t, cfg, listeners[spec.Name])
		nodes[spec.Name] = node
	}

	return nodes
}

func newClusterTestNodeFixtureWithListener(t *testing.T, cfg clusterTestNodeConfig, ln net.Listener) *clusterTestNode {
	t.Helper()

	if cfg.Name == "" {
		t.Fatalf("cluster test node name must not be empty")
	}
	if cfg.Slot == 0 {
		t.Fatalf("cluster test node slot must not be zero")
	}
	if cfg.MessageWindowSize == 0 {
		cfg.MessageWindowSize = store.DefaultMessageWindowSize
	}
	if cfg.MaxClockSkewMs < 0 {
		cfg.MaxClockSkewMs = 0
	} else if cfg.MaxClockSkewMs == 0 {
		cfg.MaxClockSkewMs = DefaultMaxClockSkewMs
	}

	numericNodeID := testNodeID(cfg.Slot)
	sharedClock := clock.NewClockWithSource(numericNodeID, func() int64 {
		return time.Now().UTC().UnixMilli() + cfg.ClockSkewMs
	})

	dbPath := filepath.Join(t.TempDir(), cfg.Name+".db")
	st, err := store.Open(dbPath, store.Options{
		NodeID:            numericNodeID,
		MessageWindowSize: cfg.MessageWindowSize,
		Clock:             sharedClock,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	manager, err := NewManager(Config{
		NodeID:                      numericNodeID,
		AdvertisePath:               websocketPath,
		ClusterSecret:               "secret",
		Peers:                       cfg.Peers,
		DiscoveryDisabled:           !cfg.DiscoveryEnabled,
		MessageWindowSize:           cfg.MessageWindowSize,
		MaxClockSkewMs:              cfg.MaxClockSkewMs,
		ClockRejectAfterSkewSamples: cfg.ClockRejectAfterSkewSamples,
	}, st)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if cfg.DisableRouting {
		manager.setSupportsRouting(false)
	}
	var timeSyncScript *clusterTestTimeSyncScript
	if len(cfg.TimeSyncSamples) > 0 {
		timeSyncScript = newClusterTestTimeSyncScript(cfg.TimeSyncSamples)
		manager.timeSyncer = timeSyncScript.Next
	}

	svc := api.New(st, manager)
	httpAPI := api.NewHTTP(svc)
	manager.SetTransientHandler(httpAPI.ReceiveTransientPacket)
	manager.SetLoggedInUsersProvider(httpAPI.ListLoggedInUsers)

	rootMux := http.NewServeMux()
	rootMux.Handle("/", httpAPI.Handler())
	rootMux.Handle(manager.AdvertisePath(), manager.Handler())

	node := &clusterTestNode{
		id:             numericNodeID,
		name:           cfg.Name,
		store:          st,
		service:        svc,
		manager:        manager,
		server:         &http.Server{Handler: rootMux},
		ln:             ln,
		baseURL:        "http://" + ln.Addr().String(),
		timeSyncScript: timeSyncScript,
	}

	t.Cleanup(func() {
		_ = node.manager.Close()
		_ = node.server.Close()
		_ = node.ln.Close()
		_ = node.store.Close()
	})

	return node
}

func startClusterTestNodes(t *testing.T, nodes ...*clusterTestNode) {
	t.Helper()
	for _, node := range nodes {
		node.Start(t)
	}
}

func expectClusterPeer(node *clusterTestNode, peerID int64) clusterPeerExpectation {
	return clusterPeerExpectation{node: node, peerID: peerID}
}

func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	if timeout < 10*time.Second {
		timeout = 10 * time.Second
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	eventually(t, timeout, fn)
}

func waitForPeersActive(t *testing.T, timeout time.Duration, expectations ...clusterPeerExpectation) {
	t.Helper()
	eventually(t, timeout, func() bool {
		for _, expectation := range expectations {
			if !expectation.node.ActivePeer(expectation.peerID) {
				return false
			}
		}
		return true
	})
}

func waitForReplicatedUser(t *testing.T, timeout time.Duration, node *clusterTestNode, key store.UserKey) {
	t.Helper()
	eventually(t, timeout, func() bool {
		_, err := node.store.GetUser(context.Background(), key)
		return err == nil
	})
}

func waitForReplicatedMessages(t *testing.T, timeout time.Duration, node *clusterTestNode, key store.UserKey, limit int, match func([]store.Message) bool) {
	t.Helper()
	eventually(t, timeout, func() bool {
		messages, err := node.store.ListMessagesByUser(context.Background(), key, limit)
		return err == nil && match(messages)
	})
}

func waitForMessagesEqual(t *testing.T, timeout time.Duration, node *clusterTestNode, key store.UserKey, limit int, expectedBodies ...string) {
	t.Helper()
	waitForReplicatedMessages(t, timeout, node, key, limit, func(messages []store.Message) bool {
		if len(messages) != len(expectedBodies) {
			return false
		}
		for i, expectedBody := range expectedBodies {
			if string(messages[i].Body) != expectedBody {
				return false
			}
		}
		return true
	})
}

func waitForUsersConverged(t *testing.T, timeout time.Duration, key store.UserKey, expectedUsername, expectedProfile string, nodes ...*clusterTestNode) {
	t.Helper()
	eventually(t, timeout, func() bool {
		for _, node := range nodes {
			user, err := node.store.GetUser(context.Background(), key)
			if err != nil {
				return false
			}
			if user.Username != expectedUsername || user.Profile != expectedProfile {
				return false
			}
		}
		return true
	})
}

func waitForUserDeletedEverywhere(t *testing.T, timeout time.Duration, key store.UserKey, nodes ...*clusterTestNode) {
	t.Helper()
	eventually(t, timeout, func() bool {
		for _, node := range nodes {
			if _, err := node.store.GetUser(context.Background(), key); !errors.Is(err, store.ErrNotFound) {
				return false
			}
		}
		return true
	})
}

func waitForEventCount(t *testing.T, timeout time.Duration, node *clusterTestNode, afterID int64, limit, expectedCount int) {
	t.Helper()
	eventually(t, timeout, func() bool {
		events, err := node.store.ListEvents(context.Background(), afterID, limit)
		return err == nil && len(events) == expectedCount
	})
}

func waitForPeerAckReached(t *testing.T, timeout time.Duration, node *clusterTestNode, peerID int64, minAck uint64) {
	t.Helper()
	eventually(t, timeout, func() bool {
		return node.LastAck(peerID) >= minAck
	})
}

func assertPeerAckReached(t *testing.T, timeout time.Duration, node *clusterTestNode, peerID int64, minAck uint64) {
	t.Helper()
	waitForPeerAckReached(t, timeout, node, peerID, minAck)
}

func waitForSessionReconnect(t *testing.T, timeout time.Duration, node *clusterTestNode, peerID int64, oldSession *session) *session {
	t.Helper()

	var reconnected *session
	eventually(t, timeout, func() bool {
		current := node.CurrentSession(peerID)
		if current != nil && current != oldSession {
			reconnected = current
			return true
		}
		return false
	})
	return reconnected
}

func waitForRouteReachable(t *testing.T, timeout time.Duration, node *clusterTestNode, targetNodeID int64) {
	t.Helper()
	eventually(t, timeout, func() bool {
		return node.manager.bestRouteSession(targetNodeID) != nil
	})
}

func waitForRouteUnreachable(t *testing.T, timeout time.Duration, node *clusterTestNode, targetNodeID int64) {
	t.Helper()
	eventually(t, timeout, func() bool {
		return node.manager.bestRouteSession(targetNodeID) == nil
	})
}

func waitForClusterRoutes(t *testing.T, timeout time.Duration, node *clusterTestNode, targetNodeIDs ...int64) {
	t.Helper()
	eventually(t, timeout, func() bool {
		for _, targetNodeID := range targetNodeIDs {
			if node.manager.bestRouteSession(targetNodeID) == nil {
				return false
			}
		}
		return true
	})
}

func (n *clusterTestNode) Start(t *testing.T) {
	t.Helper()
	n.StartManager(t)
	n.StartHTTPServer(t)
}

func (n *clusterTestNode) StartManager(t *testing.T) {
	t.Helper()

	n.startMu.Lock()
	if n.managerStarted {
		n.startMu.Unlock()
		return
	}
	n.managerStarted = true
	n.startMu.Unlock()
	if err := n.manager.Start(context.Background()); err != nil {
		t.Fatalf("start cluster manager: %v", err)
	}
}

func (n *clusterTestNode) StartHTTPServer(t *testing.T) {
	t.Helper()

	n.startMu.Lock()
	if n.serverStarted {
		n.startMu.Unlock()
		return
	}
	n.serverStarted = true
	n.startMu.Unlock()

	go func() {
		err := n.server.Serve(n.ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
}

func (n *clusterTestNode) SetTimeSyncSamples(samples ...timeSyncSample) {
	if n.timeSyncScript == nil {
		n.timeSyncScript = newClusterTestTimeSyncScript(nil)
		n.manager.timeSyncer = n.timeSyncScript.Next
	}
	n.timeSyncScript.Set(samples...)
}

func (n *clusterTestNode) ActivePeer(peerID int64) bool {
	return n.manager.hasActivePeer(peerID)
}

func (n *clusterTestNode) LastAck(peerID int64) uint64 {
	n.manager.mu.Lock()
	defer n.manager.mu.Unlock()

	peer, ok := n.manager.peers[peerID]
	if !ok {
		return 0
	}
	return peer.lastAck
}

func (n *clusterTestNode) PeerClockOffset(peerID int64) int64 {
	n.manager.mu.Lock()
	defer n.manager.mu.Unlock()

	peer, ok := n.manager.peers[peerID]
	if !ok {
		return 0
	}
	return peer.clockOffsetMs
}

func (n *clusterTestNode) CurrentSession(peerID int64) *session {
	n.manager.mu.Lock()
	defer n.manager.mu.Unlock()

	peer, ok := n.manager.peers[peerID]
	if !ok {
		return nil
	}
	return peer.active
}

func (n *clusterTestNode) APIBaseURL() string {
	return n.baseURL
}

func (n *clusterTestNode) adminToken(t *testing.T) string {
	t.Helper()

	var loginResp struct {
		Token string `json:"token"`
	}
	mustJSON(t, doJSON(t, n.APIBaseURL(), http.MethodPost, "/auth/login", map[string]any{
		"node_id":  n.id,
		"user_id":  store.BootstrapAdminUserID,
		"password": clusterTestAdminPassword,
	}, http.StatusOK), &loginResp)
	if loginResp.Token == "" {
		t.Fatalf("expected bootstrap admin token for node %s", n.name)
	}
	return loginResp.Token
}

func (n *clusterTestNode) adminHeaders(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		"Authorization": "Bearer " + n.adminToken(t),
	}
}

func doJSONAsAdmin(t *testing.T, node *clusterTestNode, method, path string, body any, wantStatus int) []byte {
	t.Helper()

	data, err := doJSONRequestWithHeaders(node.APIBaseURL(), method, path, body, wantStatus, node.adminHeaders(t))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return data
}

func doJSONRequestAsAdmin(node *clusterTestNode, t *testing.T, method, path string, body any, wantStatus int) ([]byte, error) {
	t.Helper()
	return doJSONRequestWithHeaders(node.APIBaseURL(), method, path, body, wantStatus, node.adminHeaders(t))
}

func mustListen(t *testing.T) net.Listener {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln
}

func wsURL(ln net.Listener) string {
	return "ws://" + ln.Addr().String() + websocketPath
}

func dialClusterClientWebSocket(t *testing.T, serverURL string) *websocket.Conn {
	t.Helper()

	serverURL = "ws" + strings.TrimPrefix(serverURL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(serverURL+"/ws/client", nil)
	if err != nil {
		t.Fatalf("dial client websocket: %v", err)
	}
	return conn
}

func writeClusterClientEnvelope(t *testing.T, conn *websocket.Conn, envelope *internalproto.ClientEnvelope) {
	t.Helper()

	data, err := proto.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal client envelope: %v", err)
	}
	if err := conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
		t.Fatalf("write client websocket: %v", err)
	}
}

func readClusterServerEnvelope(t *testing.T, conn *websocket.Conn) *internalproto.ServerEnvelope {
	t.Helper()

	messageType, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read server websocket: %v", err)
	}
	if messageType != websocket.BinaryMessage {
		t.Fatalf("unexpected websocket message type: %d", messageType)
	}
	var envelope internalproto.ServerEnvelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		t.Fatalf("unmarshal server envelope: %v", err)
	}
	return &envelope
}

func doJSON(t *testing.T, baseURL, method, path string, body any, wantStatus int) []byte {
	t.Helper()

	data, err := doJSONRequest(baseURL, method, path, body, wantStatus)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return data
}

func doJSONRequest(baseURL, method, path string, body any, wantStatus int) ([]byte, error) {
	return doJSONRequestWithHeaders(baseURL, method, path, body, wantStatus, nil)
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

func newClusterHTTPTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()

	server := httptest.NewUnstartedServer(handler)
	server.Listener = mustListen(t)
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func mustJSON(t *testing.T, data []byte, dst any) {
	t.Helper()
	if err := json.Unmarshal(data, dst); err != nil {
		t.Fatalf("unmarshal json: %v body=%s", err, string(data))
	}
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

func newReplicationTestStoreWithWindow(t *testing.T, nodeID string, slot uint16, messageWindowSize int) *store.Store {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), nodeID+".db")
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

func mustHashPassword(t *testing.T, password string) string {
	t.Helper()

	hash, err := auth.HashPassword(password)
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	return hash
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

func readySnapshotTestSession(mgr *Manager, peerID int64, remoteMessageWindowSize int) *session {
	sess := &session{
		manager:                 mgr,
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
			clockState:   clockStateTrusted,
			sessions:     make(map[uint64]*session),
			routeAdverts: make(map[int64]routeAdvertisement),
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
