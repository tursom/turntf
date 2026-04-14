package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	websocketPath                    = "/internal/cluster/ws"
	writeWait                        = 10 * time.Second
	pingInterval                     = 15 * time.Second
	readTimeout                      = 45 * time.Second
	outboundQueueSize                = 128
	managerPublishQueue              = 256
	pullBatchSize                    = 128
	timeSyncSampleCount              = 7
	timeSyncInterval                 = 30 * time.Second
	timeSyncTimeout                  = 8 * time.Second
	queryLoggedInUsersTimeout        = 3 * time.Second
	catchupRetryInterval             = time.Second
	antiEntropyInterval              = 60 * time.Second
	routingUpdateInterval            = 5 * time.Second
	membershipUpdateInterval         = 5 * time.Second
	discoveryCandidateTTL            = 10 * time.Minute
	maxDynamicDiscoveredPeers        = 8
	routeRetryInterval               = 200 * time.Millisecond
	packetSeenTTL                    = 30 * time.Second
	routeRetryTTL                    = 3 * time.Second
	defaultPacketTTLHops             = 8
	defaultLoggedInUsersQueryMaxHops = 8
	routingSwitchThresholdMs         = 15
)

var marshalOptions = proto.MarshalOptions{Deterministic: true}

var errSessionClosed = errors.New("session closed")
var errClockProtectionRejected = errors.New("clock protection rejected")

type Manager struct {
	cfg   Config
	store *store.Store
	clock *clock.Clock

	mux       *http.ServeMux
	publishCh chan store.Event
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	startErr  error
	closeOnce sync.Once
	websocket *webSocketTransport
	dialers   map[string]Dialer

	zeroMQListener        Listener
	zeroMQListenerFactory func(bindURL string) Listener

	mu              sync.Mutex
	peers           map[int64]*peerState
	configuredPeers []*configuredPeer
	discoveredPeers map[string]*discoveredPeerState
	dynamicPeers    map[string]*configuredPeer
	selfKnownURLs   map[string]uint64

	lastTrustedClockSync     time.Time
	clockState               clockState
	clockReason              string
	clockStateTransitions    map[clockStateTransitionKey]uint64
	timeSyncer               func(*session) (timeSyncSample, error)
	transientHandler         func(store.TransientPacket) bool
	loggedInUsersProvider    func(context.Context) ([]app.LoggedInUserSummary, error)
	supportsRouting          bool
	supportsMembership       bool
	membershipGeneration     uint64
	membershipUpdatesSent    uint64
	membershipUpdatesRecv    uint64
	discoveryRejects         uint64
	discoveryPersistFailures uint64
	routingGeneration        uint64
	routingTable             map[int64]routeEntry
	seenPackets              map[string]time.Time
	retryQueue               map[string]queuedPacket
	nextLoggedInUsersQueryID uint64
	pendingLoggedInUsers     map[uint64]chan loggedInUsersQueryResult
}

type configuredPeer struct {
	URL     string
	nodeID  int64
	dynamic bool
	source  string
}

type discoveredPeerState struct {
	nodeID           int64
	url              string
	sourcePeerNodeID int64
	state            string
	firstSeenAt      time.Time
	lastSeenAt       time.Time
	lastConnectedAt  time.Time
	lastError        string
	generation       uint64
	dialing          bool
}

type peerState struct {
	active                   *session
	lastAck                  uint64
	trustedSession           *session
	clockState               clockState
	clockOffsetMs            int64
	clockUncertaintyMs       int64
	lastClockSync            time.Time
	lastCredibleClockSync    time.Time
	clockLastError           string
	clockSamples             []timeSyncSample
	clockFailures            uint64
	clockFailureStreak       int
	clockSkewViolationStreak int
	clockHealthyStreak       int
	sessions                 map[uint64]*session
	routeAdverts             map[int64]routeAdvertisement
	joinedLogged             bool

	snapshotDigestsSent     uint64
	snapshotDigestsReceived uint64
	snapshotChunksSent      uint64
	snapshotChunksReceived  uint64
	lastSnapshotDigestAt    time.Time
	lastSnapshotChunkAt     time.Time
}

type session struct {
	manager  *Manager
	conn     TransportConn
	outbound bool

	configuredPeer *configuredPeer
	peerID         int64
	connectionID   uint64

	send chan *internalproto.Envelope

	mu                      sync.Mutex
	closed                  bool
	remoteOriginProgress    map[int64]uint64
	pendingPulls            map[int64]pendingPullState
	replicationReady        bool
	bootstrapStarted        bool
	syncLoopStarted         bool
	remoteSnapshotVersion   string
	remoteMessageWindowSize int
	pendingSnapshotParts    map[string]struct{}
	nextTimeSyncID          uint64
	nextPullRequestID       uint64
	pendingTimeSync         map[uint64]chan timeSyncResult
	clockOffsetMs           int64
	supportsRouting         bool
	supportsMembership      bool
	smoothedRTTMs           int64
	jitterPenaltyMs         int64
	lastRTTUpdate           time.Time
}

type routeAdvertisement struct {
	destinationNodeID int64
	reachable         bool
	totalCostMs       int64
	residualCostMs    int64
	residualJitterMs  int64
}

type routeEntry struct {
	destinationNodeID    int64
	nextHopPeer          int64
	selectedConnectionID uint64
	totalCostMs          int64
	residualCostMs       int64
	residualJitterMs     int64
	learnedFromPeer      int64
	generation           uint64
	updatedAt            time.Time
}

type queuedPacket struct {
	packet      store.TransientPacket
	queuedAt    time.Time
	nextAttempt time.Time
	attempts    int
}

type pendingPullState struct {
	RequestID    uint64
	AfterEventID uint64
}

type timeSyncResult struct {
	response     *internalproto.TimeSyncResponse
	receivedAtMs int64
	err          error
}

type loggedInUsersQueryResult struct {
	response *internalproto.QueryLoggedInUsersResponse
	err      error
}

type timeSyncSample struct {
	offsetMs      int64
	rttMs         int64
	uncertaintyMs int64
	sampledAt     time.Time
	credible      bool
}

func NewManager(cfg Config, st *store.Store) (*Manager, error) {
	cfg = cfg.WithDefaults()
	cfg.MessageWindowSize = normalizedMessageWindowSize(cfg.MessageWindowSize)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	configuredPeers := make([]*configuredPeer, 0, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		configuredPeers = append(configuredPeers, &configuredPeer{URL: peer.URL, source: peerSourceStatic})
	}

	clockRef := clock.NewClock(cfg.NodeID)
	if st != nil && st.Clock() != nil {
		clockRef = st.Clock()
	}

	mgr := &Manager{
		cfg:                   cfg,
		store:                 st,
		clock:                 clockRef,
		websocket:             newWebSocketTransport(),
		dialers:               make(map[string]Dialer, 2),
		zeroMQListenerFactory: newZeroMQListener,
		mux:                   http.NewServeMux(),
		publishCh:             make(chan store.Event, managerPublishQueue),
		peers:                 make(map[int64]*peerState, len(cfg.Peers)),
		configuredPeers:       configuredPeers,
		discoveredPeers:       make(map[string]*discoveredPeerState),
		dynamicPeers:          make(map[string]*configuredPeer),
		selfKnownURLs:         make(map[string]uint64),
		supportsRouting:       true,
		supportsMembership:    !cfg.DiscoveryDisabled,
		routingTable:          make(map[int64]routeEntry),
		seenPackets:           make(map[string]time.Time),
		retryQueue:            make(map[string]queuedPacket),
		pendingLoggedInUsers:  make(map[uint64]chan loggedInUsersQueryResult),
		clockStateTransitions: make(map[clockStateTransitionKey]uint64),
	}
	mgr.dialers[transportWebSocket] = mgr.websocket
	mgr.dialers[transportZeroMQ] = newZeroMQDialer()
	if !cfg.DiscoveryDisabled {
		if err := mgr.loadDiscoveredPeers(context.Background()); err != nil {
			return nil, err
		}
	}
	mgr.mux.HandleFunc("GET "+cfg.AdvertisePath, mgr.handleWebSocket)
	return mgr, nil
}

func (m *Manager) Handler() http.Handler {
	return m.mux
}

func (m *Manager) AdvertisePath() string {
	return m.cfg.AdvertisePath
}

func (m *Manager) validateConfiguredTransports() error {
	if m == nil {
		return nil
	}
	needsZeroMQ := m.cfg.ZeroMQ.Enabled
	if !needsZeroMQ {
		for _, peer := range m.configuredPeers {
			if peer != nil && isZeroMQPeerURL(peer.URL) {
				needsZeroMQ = true
				break
			}
		}
	}
	if needsZeroMQ && !zeroMQEnabled() {
		return errZeroMQNotBuilt
	}
	if m.cfg.ZeroMQ.Enabled && m.zeroMQListenerFactory == nil {
		return errors.New("zeromq listener factory is not configured")
	}
	return nil
}

func (m *Manager) transportForPeerURL(peerURL string) (string, error) {
	switch {
	case isWebSocketPeerURL(peerURL):
		return transportWebSocket, nil
	case isZeroMQPeerURL(peerURL):
		return transportZeroMQ, nil
	default:
		return "", fmt.Errorf("unsupported peer transport for %q", peerURL)
	}
}

func (m *Manager) dialerForPeerURL(peerURL string) (Dialer, error) {
	transport, err := m.transportForPeerURL(peerURL)
	if err != nil {
		return nil, err
	}
	dialer := m.dialers[transport]
	if dialer == nil {
		return nil, fmt.Errorf("%s dialer is not configured", transport)
	}
	return dialer, nil
}

func (m *Manager) Start(parent context.Context) error {
	m.startOnce.Do(func() {
		m.ctx, m.cancel = context.WithCancel(parent)

		if err := m.validateConfiguredTransports(); err != nil {
			m.startErr = err
			m.cancel()
			return
		}
		if m.cfg.ZeroMQ.Enabled {
			listener := m.zeroMQListenerFactory(m.cfg.ZeroMQ.BindURL)
			if err := listener.Start(m.ctx, m.handleZeroMQConn); err != nil {
				m.startErr = err
				m.cancel()
				return
			}
			m.zeroMQListener = listener
			m.logInfo("zeromq_listener_started").
				Str("transport", transportZeroMQ).
				Str("bind_url", m.cfg.ZeroMQ.BindURL).
				Msg("started zeromq listener")
		}

		m.wg.Add(1)
		go m.publishLoop()
		m.wg.Add(1)
		go m.routingLoop()
		if !m.cfg.DiscoveryDisabled {
			m.wg.Add(1)
			go m.discoveryLoop()
		}

		for _, peer := range m.configuredPeers {
			peer := peer
			m.wg.Add(1)
			go m.dialLoop(peer)
		}
	})
	return m.startErr
}

func (m *Manager) handleZeroMQConn(conn TransportConn) {
	if conn == nil {
		return
	}
	if m.ctx == nil || m.ctx.Err() != nil {
		closeTransport(conn, "shutdown")
		return
	}
	m.logInfo("peer_inbound_accepted").
		Str("direction", "inbound").
		Str("transport", conn.Transport()).
		Str("remote_addr", conn.RemoteAddr()).
		Str("bind_url", m.cfg.ZeroMQ.BindURL).
		Msg("accepted inbound peer zeromq connection")

	sess := m.newSession(conn, false, nil)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runSession(sess)
	}()
}

func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
		if m.zeroMQListener != nil {
			_ = m.zeroMQListener.Close()
		}

		m.mu.Lock()
		sessions := make([]*session, 0, len(m.peers))
		for _, peer := range m.peers {
			if peer.active != nil {
				sessions = append(sessions, peer.active)
			}
		}
		m.mu.Unlock()

		for _, sess := range sessions {
			sess.close()
		}
		m.wg.Wait()
	})
	return nil
}

func (m *Manager) Publish(event store.Event) {
	if m == nil || m.ctx == nil {
		return
	}

	select {
	case m.publishCh <- event:
	case <-m.ctx.Done():
	default:
		go func() {
			select {
			case m.publishCh <- event:
			case <-m.ctx.Done():
			}
		}()
	}
}

func (m *Manager) SetTransientHandler(handler func(store.TransientPacket) bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transientHandler = handler
}

func (m *Manager) SetLoggedInUsersProvider(provider func(context.Context) ([]app.LoggedInUserSummary, error)) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loggedInUsersProvider = provider
}

func (m *Manager) setSupportsRouting(supports bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.supportsRouting = supports
}

func (m *Manager) routingSupported() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.supportsRouting
}

func (m *Manager) membershipSupported() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.supportsMembership
}

func (m *Manager) QueryLoggedInUsers(ctx context.Context, nodeID int64) ([]app.LoggedInUserSummary, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: cluster manager is not configured", app.ErrServiceUnavailable)
	}
	if nodeID <= 0 {
		return nil, fmt.Errorf("%w: target node id cannot be empty", store.ErrInvalidInput)
	}
	if nodeID == m.cfg.NodeID {
		return m.listLocalLoggedInUsers(ctx)
	}

	sess := m.bestRouteSession(nodeID)
	if sess == nil {
		return nil, fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
	}

	requestID, resultCh := m.beginLoggedInUsersQuery()
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_QueryLoggedInUsersRequest{
			QueryLoggedInUsersRequest: &internalproto.QueryLoggedInUsersRequest{
				RequestId:     requestID,
				TargetNodeId:  nodeID,
				OriginNodeId:  m.cfg.NodeID,
				RemainingHops: defaultLoggedInUsersQueryMaxHops,
			},
		},
	})

	timeoutCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, queryLoggedInUsersTimeout)
		defer cancel()
	}

	select {
	case <-timeoutCtx.Done():
		m.cancelLoggedInUsersQuery(requestID, timeoutCtx.Err())
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: timed out querying node %d logged-in users", app.ErrServiceUnavailable, nodeID)
		}
		return nil, timeoutCtx.Err()
	case result := <-resultCh:
		if result.err != nil {
			if errors.Is(result.err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("%w: timed out querying node %d logged-in users", app.ErrServiceUnavailable, nodeID)
			}
			if errors.Is(result.err, errSessionClosed) {
				return nil, fmt.Errorf("%w: peer session closed while querying node %d", app.ErrServiceUnavailable, nodeID)
			}
			return nil, result.err
		}
		if err := clusterQueryError(result.response); err != nil {
			return nil, err
		}
		return loggedInUsersFromCluster(result.response.GetItems()), nil
	}
}

func (m *Manager) RouteTransientPacket(_ context.Context, packet store.TransientPacket) error {
	if m == nil {
		return nil
	}
	if packet.TTLHops <= 0 {
		packet.TTLHops = defaultPacketTTLHops
	}
	m.routeOrQueueTransient(packet)
	return nil
}

func (m *Manager) publishLoop() {
	defer m.wg.Done()

	for {
		select {
		case <-m.ctx.Done():
			return
		case event := <-m.publishCh:
			m.broadcastEvent(event)
		}
	}
}

func (m *Manager) routingLoop() {
	defer m.wg.Done()

	updateTicker := time.NewTicker(routingUpdateInterval)
	retryTicker := time.NewTicker(routeRetryInterval)
	defer updateTicker.Stop()
	defer retryTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-updateTicker.C:
			m.broadcastRoutingUpdate()
		case <-retryTicker.C:
			m.retryTransientPackets()
		}
	}
}

func (m *Manager) broadcastEvent(event store.Event) {
	replicated := store.ToReplicatedEvent(event)
	envelope := &internalproto.Envelope{
		NodeId:    m.cfg.NodeID,
		Sequence:  uint64(event.Sequence),
		SentAtHlc: event.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events:       []*internalproto.ReplicatedEvent{replicated},
				OriginNodeId: event.OriginNodeID,
			},
		},
	}

	for _, sess := range m.activeSessions() {
		sess.enqueue(envelope)
	}
}

func (m *Manager) activeSessions() []*session {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessions := make([]*session, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer.active != nil {
			sessions = append(sessions, peer.active)
		}
	}
	return sessions
}

func (m *Manager) dialLoop(peer *configuredPeer) {
	defer m.wg.Done()

	backoff := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	attempt := 0

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		if peerID := m.configuredPeerNodeID(peer); peerID > 0 && m.cfg.NodeID > peerID && m.hasActivePeer(peerID) {
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		m.recordConfiguredPeerDialing(peer)
		m.logInfo("peer_dial_started").
			Str("direction", "outbound").
			Str("peer_url", peer.URL).
			Msg("starting outbound peer dial")
		dialer, err := m.dialerForPeerURL(peer.URL)
		if err != nil {
			m.recordConfiguredPeerDialFailure(peer, err)
			wait := backoff[minInt(attempt, len(backoff)-1)]
			m.logWarn("peer_dial_failed", err).
				Str("direction", "outbound").
				Str("peer_url", peer.URL).
				Dur("retry_in", wait).
				Msg("outbound peer dial failed")
			attempt++
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}
		conn, err := dialer.Dial(m.ctx, peer.URL)
		if err != nil {
			m.recordConfiguredPeerDialFailure(peer, err)
			wait := backoff[minInt(attempt, len(backoff)-1)]
			m.logWarn("peer_dial_failed", err).
				Str("direction", "outbound").
				Str("peer_url", peer.URL).
				Dur("retry_in", wait).
				Msg("outbound peer dial failed")
			attempt++
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}

		attempt = 0
		sess := m.newSession(conn, true, peer)
		m.logSessionEvent("peer_dial_succeeded", sess).
			Msg("outbound peer dial succeeded")
		m.runSession(sess)
		m.recordConfiguredPeerSessionClosed(peer, sess)

		select {
		case <-m.ctx.Done():
			return
		default:
		}
	}
}

func (m *Manager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if m.ctx == nil || m.ctx.Err() != nil {
		http.Error(w, "cluster manager not started", http.StatusServiceUnavailable)
		return
	}

	conn, err := m.websocket.Upgrade(w, r)
	if err != nil {
		return
	}
	if m.ctx.Err() != nil {
		closeTransport(conn, "shutdown")
		return
	}
	m.logInfo("peer_inbound_accepted").
		Str("direction", "inbound").
		Str("transport", conn.Transport()).
		Str("remote_addr", r.RemoteAddr).
		Str("path", r.URL.Path).
		Msg("accepted inbound peer websocket")

	sess := m.newSession(conn, false, nil)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runSession(sess)
	}()
}

func (m *Manager) newSession(conn TransportConn, outbound bool, configuredPeer *configuredPeer) *session {
	m.mu.Lock()
	m.routingGeneration++
	connectionID := m.routingGeneration
	m.mu.Unlock()
	return &session{
		manager:              m,
		conn:                 conn,
		outbound:             outbound,
		configuredPeer:       configuredPeer,
		connectionID:         connectionID,
		send:                 make(chan *internalproto.Envelope, outboundQueueSize),
		remoteOriginProgress: make(map[int64]uint64),
		pendingPulls:         make(map[int64]pendingPullState),
		pendingTimeSync:      make(map[uint64]chan timeSyncResult),
		supportsRouting:      true,
		supportsMembership:   !m.cfg.DiscoveryDisabled,
	}
}

func (m *Manager) runSession(sess *session) {
	m.logSessionEvent("peer_session_started", sess).
		Msg("peer session started")
	defer sess.close()
	defer m.deactivateSession(sess)

	hello, err := m.buildHelloEnvelope(sess)
	if err != nil {
		log.Warn().Err(err).Str("component", "cluster").Str("event", "build_hello_failed").Msg("build hello failed")
		return
	}

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		m.writeLoop(sess)
	}()

	sess.enqueue(hello)
	m.readLoop(sess)
	sess.close()
	<-writeDone
}

func (m *Manager) buildHelloEnvelope(sessions ...*session) (*internalproto.Envelope, error) {
	var sess *session
	if len(sessions) > 0 {
		sess = sessions[0]
	}
	originProgress, err := m.store.ListOriginProgress(context.Background())
	if err != nil {
		return nil, err
	}

	progress := make([]*internalproto.OriginProgress, 0, len(originProgress))
	for _, item := range originProgress {
		progress = append(progress, &internalproto.OriginProgress{
			OriginNodeId: item.OriginNodeID,
			LastEventId:  uint64(item.LastEventID),
		})
	}

	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_Hello{
			Hello: &internalproto.Hello{
				NodeId:             m.cfg.NodeID,
				AdvertiseAddr:      m.cfg.AdvertisePath,
				ProtocolVersion:    internalproto.ProtocolVersion,
				OriginProgress:     progress,
				SnapshotVersion:    internalproto.SnapshotVersion,
				MessageWindowSize:  uint32(m.cfg.MessageWindowSize),
				SupportsRouting:    m.routingSupported(),
				ConnectionId:       connectionIDForHello(sess),
				SupportsMembership: m.membershipSupported(),
			},
		},
	}, nil
}

func (m *Manager) writeLoop(sess *session) {
	for {
		select {
		case <-m.ctx.Done():
			closeTransport(sess.conn, "shutdown")
			return
		case envelope, ok := <-sess.send:
			if !ok {
				closeTransport(sess.conn, "session closed")
				return
			}

			data, err := m.marshalSignedEnvelope(envelope)
			if err != nil {
				log.Warn().Err(err).Str("component", "cluster").Int64("peer", sess.peerID).Str("event", "marshal_envelope_failed").Msg("marshal envelope failed")
				return
			}

			if err := sess.conn.Send(m.ctx, data); err != nil {
				return
			}
		}
	}
}

func (m *Manager) readLoop(sess *session) {
	for {
		data, err := sess.conn.Receive(m.ctx)
		if err != nil {
			return
		}

		var envelope internalproto.Envelope
		if err := proto.Unmarshal(data, &envelope); err != nil {
			return
		}
		if err := m.verifyEnvelope(&envelope); err != nil {
			return
		}

		if sess.peerID == 0 && envelope.GetHello() == nil {
			return
		}

		switch envelope.Body.(type) {
		case *internalproto.Envelope_Hello:
			if err := m.handleHello(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_TimeSyncRequest:
			if err := m.handleTimeSyncRequest(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_TimeSyncResponse:
			if err := m.handleTimeSyncResponse(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_QueryLoggedInUsersRequest:
			if err := m.handleQueryLoggedInUsersRequest(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_QueryLoggedInUsersResponse:
			if err := m.handleQueryLoggedInUsersResponse(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_MembershipUpdate:
			if err := m.handleMembershipUpdate(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_Ack:
			if err := m.handleAck(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_EventBatch:
			if err := m.handleEventBatch(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		case *internalproto.Envelope_PullEvents:
			if err := m.handlePullEvents(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_SnapshotDigest:
			if err := m.handleSnapshotDigest(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		case *internalproto.Envelope_SnapshotChunk:
			if err := m.handleSnapshotChunk(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		case *internalproto.Envelope_RoutingUpdate:
			if err := m.handleRoutingUpdate(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_TransientPacket:
			if err := m.handleTransientPacket(sess, &envelope); err != nil {
				if errors.Is(err, errClockProtectionRejected) {
					continue
				}
				return
			}
		default:
			return
		}
	}
}

func (m *Manager) handleHello(sess *session, envelope *internalproto.Envelope) error {
	if sess.peerID != 0 {
		return errors.New("duplicate hello")
	}

	hello := envelope.GetHello()
	if hello == nil {
		return errors.New("hello body cannot be empty")
	}

	envelopeNodeID := envelope.NodeId
	peerID := hello.NodeId
	if peerID <= 0 || peerID == m.cfg.NodeID {
		return fmt.Errorf("invalid peer id %d", peerID)
	}
	if envelopeNodeID <= 0 {
		return errors.New("hello envelope node id cannot be empty")
	}
	if envelopeNodeID != peerID {
		return fmt.Errorf("hello envelope node id mismatch: got %d want %d", envelopeNodeID, peerID)
	}
	if hello.ProtocolVersion != internalproto.ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %q", hello.ProtocolVersion)
	}
	if hello.SnapshotVersion != internalproto.SnapshotVersion {
		return fmt.Errorf("unsupported snapshot version %q", hello.SnapshotVersion)
	}
	if hello.MessageWindowSize == 0 {
		return errors.New("hello message window size must be positive")
	}
	if strings.TrimSpace(hello.AdvertiseAddr) == "" {
		return errors.New("hello advertise path cannot be empty")
	}
	if !strings.HasPrefix(strings.TrimSpace(hello.AdvertiseAddr), "/") {
		return errors.New("hello advertise path must start with /")
	}
	if err := m.bindPeerIdentity(sess, peerID); err != nil {
		return err
	}
	if int(hello.MessageWindowSize) != m.cfg.MessageWindowSize {
		m.logSessionWarn("message_window_mismatch", sess, nil).
			Int("local_message_window_size", m.cfg.MessageWindowSize).
			Uint32("remote_message_window_size", hello.MessageWindowSize).
			Msg("continuing with per-node windows")
	}

	sess.remoteSnapshotVersion = hello.SnapshotVersion
	sess.remoteMessageWindowSize = int(hello.MessageWindowSize)
	sess.supportsRouting = hello.SupportsRouting
	sess.supportsMembership = hello.SupportsMembership
	if hello.ConnectionId != 0 {
		sess.connectionID = hello.ConnectionId
	}
	sess.smoothedRTTMs = int64(hello.SmoothedRttMs)
	sess.jitterPenaltyMs = int64(hello.JitterPenaltyMs)
	sess.noteRemoteOriginProgress(hello.OriginProgress)
	m.logSessionEvent("peer_hello_accepted", sess).
		Str("protocol_version", hello.ProtocolVersion).
		Str("snapshot_version", hello.SnapshotVersion).
		Uint32("message_window_size", hello.MessageWindowSize).
		Bool("supports_routing", hello.SupportsRouting).
		Bool("supports_membership", hello.SupportsMembership).
		Msg("peer hello accepted")

	if !sess.beginBootstrap() {
		return errors.New("duplicate bootstrap rejected")
	}
	go m.bootstrapSession(sess)
	return nil
}

func (m *Manager) bootstrapSession(sess *session) {
	if err := m.performTimeSync(sess); err != nil {
		m.logSessionWarn("time_sync_failed", sess, err).
			Msg("time sync failed")
		sess.close()
		return
	}
	state, reason := m.peerClockState(sess.peerID)
	logger := m.logSessionEvent("time_sync_succeeded", sess)
	if state != string(clockStateTrusted) {
		logger = m.logSessionWarn("time_sync_observing", sess, nil)
	}
	logger.
		Str("clock_state", state).
		Str("clock_reason", reason).
		Int64("offset_ms", sess.clockOffset()).
		Int64("rtt_ms", sess.smoothedRTTMs).
		Int64("max_clock_skew_ms", m.cfg.MaxClockSkewMs).
		Msg("time sync completed")
	if !m.activateSession(sess) {
		m.logSessionWarn("duplicate_session_rejected", sess, nil).
			Msg("duplicate session rejected")
		sess.close()
		return
	}
	m.recordSessionDiscoveryConnected(sess)
	m.sendMembershipUpdate(sess)

	sess.markReplicationReady()
	sess.startSyncLoop(func() {
		m.sessionSyncLoop(sess)
	})

	if m.store != nil {
		requested, err := m.requestCatchupIfNeeded(sess)
		if err != nil {
			m.logSessionWarn("request_catchup_failed", sess, err).
				Msg("request catchup failed")
			sess.close()
			return
		}
		if !requested {
			m.sendSnapshotDigest(sess)
		}
	}
}

func (m *Manager) handleTimeSyncRequest(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	req := envelope.GetTimeSyncRequest()
	if req == nil {
		return errors.New("time sync request body cannot be empty")
	}
	if req.RequestId == 0 {
		return errors.New("time sync request id cannot be empty")
	}

	receivedAtMs := m.clock.PhysicalTimeMs()
	sentAtMs := m.clock.PhysicalTimeMs()
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_TimeSyncResponse{
			TimeSyncResponse: &internalproto.TimeSyncResponse{
				RequestId:           req.RequestId,
				ClientSendTimeMs:    req.ClientSendTimeMs,
				ServerReceiveTimeMs: receivedAtMs,
				ServerSendTimeMs:    sentAtMs,
			},
		},
	})
	return nil
}

func (m *Manager) handleTimeSyncResponse(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	resp := envelope.GetTimeSyncResponse()
	if resp == nil {
		return errors.New("time sync response body cannot be empty")
	}
	if resp.RequestId == 0 {
		return errors.New("time sync response id cannot be empty")
	}

	if !sess.resolveTimeSync(resp.RequestId, timeSyncResult{
		response:     resp,
		receivedAtMs: m.clock.PhysicalTimeMs(),
	}) {
		return nil
	}
	return nil
}

func (m *Manager) handleQueryLoggedInUsersRequest(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	req := envelope.GetQueryLoggedInUsersRequest()
	if req == nil {
		return errors.New("query logged-in users request body cannot be empty")
	}
	if req.RequestId == 0 {
		return errors.New("query logged-in users request id cannot be empty")
	}
	if req.OriginNodeId <= 0 {
		return errors.New("query logged-in users origin node id cannot be empty")
	}
	if req.TargetNodeId <= 0 {
		return errors.New("query logged-in users target node id cannot be empty")
	}

	if req.TargetNodeId == m.cfg.NodeID {
		response := &internalproto.QueryLoggedInUsersResponse{
			RequestId:     req.RequestId,
			TargetNodeId:  req.TargetNodeId,
			OriginNodeId:  req.OriginNodeId,
			RemainingHops: req.RemainingHops,
		}
		users, err := m.listLocalLoggedInUsers(context.Background())
		if err != nil {
			response.ErrorCode = "service_unavailable"
			response.ErrorMessage = err.Error()
		} else {
			response.Items = clusterLoggedInUsers(users)
		}
		return m.forwardQueryLoggedInUsersResponse(response)
	}

	if req.RemainingHops <= 0 {
		return m.forwardQueryLoggedInUsersResponse(&internalproto.QueryLoggedInUsersResponse{
			RequestId:     req.RequestId,
			TargetNodeId:  req.TargetNodeId,
			OriginNodeId:  req.OriginNodeId,
			ErrorCode:     "service_unavailable",
			ErrorMessage:  fmt.Sprintf("route hop limit exceeded while querying node %d", req.TargetNodeId),
			RemainingHops: 0,
		})
	}

	next := proto.Clone(req).(*internalproto.QueryLoggedInUsersRequest)
	next.RemainingHops--
	if err := m.forwardQueryLoggedInUsersRequest(next); err != nil {
		return m.forwardQueryLoggedInUsersResponse(&internalproto.QueryLoggedInUsersResponse{
			RequestId:     req.RequestId,
			TargetNodeId:  req.TargetNodeId,
			OriginNodeId:  req.OriginNodeId,
			ErrorCode:     "service_unavailable",
			ErrorMessage:  err.Error(),
			RemainingHops: next.RemainingHops,
		})
	}
	return nil
}

func (m *Manager) handleQueryLoggedInUsersResponse(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	resp := envelope.GetQueryLoggedInUsersResponse()
	if resp == nil {
		return errors.New("query logged-in users response body cannot be empty")
	}
	if resp.RequestId == 0 {
		return errors.New("query logged-in users response id cannot be empty")
	}
	if resp.OriginNodeId <= 0 {
		return errors.New("query logged-in users response origin node id cannot be empty")
	}
	if resp.TargetNodeId <= 0 {
		return errors.New("query logged-in users response target node id cannot be empty")
	}
	if resp.OriginNodeId == m.cfg.NodeID {
		if !m.resolveLoggedInUsersQuery(resp.RequestId, loggedInUsersQueryResult{response: resp}) {
			m.logDebug("query_logged_in_users_response_ignored").
				Uint64("request_id", resp.RequestId).
				Int64("origin_node_id", resp.OriginNodeId).
				Int64("target_node_id", resp.TargetNodeId).
				Int32("remaining_hops", resp.RemainingHops).
				Msg("ignoring late logged-in users response without pending origin query")
		}
		return nil
	}
	if resp.RemainingHops <= 0 {
		m.logWarn("query_logged_in_users_response_dropped", nil).
			Uint64("request_id", resp.RequestId).
			Int64("origin_node_id", resp.OriginNodeId).
			Int64("target_node_id", resp.TargetNodeId).
			Int32("remaining_hops", resp.RemainingHops).
			Msg("dropping logged-in users response because hop limit was reached")
		return nil
	}

	next := proto.Clone(resp).(*internalproto.QueryLoggedInUsersResponse)
	next.RemainingHops--
	if err := m.forwardQueryLoggedInUsersResponse(next); err != nil {
		m.logWarn("query_logged_in_users_response_forward_failed", err).
			Uint64("request_id", resp.RequestId).
			Int64("origin_node_id", resp.OriginNodeId).
			Int64("target_node_id", resp.TargetNodeId).
			Int32("remaining_hops", next.RemainingHops).
			Msg("failed to forward logged-in users response")
	}
	return nil
}

func (m *Manager) forwardQueryLoggedInUsersRequest(req *internalproto.QueryLoggedInUsersRequest) error {
	if req == nil {
		return errors.New("query logged-in users request cannot be empty")
	}
	sess := m.bestRouteSession(req.TargetNodeId)
	if sess == nil {
		return fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, req.TargetNodeId)
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_QueryLoggedInUsersRequest{
			QueryLoggedInUsersRequest: req,
		},
	})
	return nil
}

func (m *Manager) forwardQueryLoggedInUsersResponse(resp *internalproto.QueryLoggedInUsersResponse) error {
	if resp == nil {
		return errors.New("query logged-in users response cannot be empty")
	}
	sess := m.bestRouteSession(resp.OriginNodeId)
	if sess == nil {
		return fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, resp.OriginNodeId)
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_QueryLoggedInUsersResponse{
			QueryLoggedInUsersResponse: resp,
		},
	})
	return nil
}

func (m *Manager) handleAck(sess *session, envelope *internalproto.Envelope) error {
	ack := envelope.GetAck()
	if ack == nil {
		return errors.New("ack body cannot be empty")
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	if ack.NodeId <= 0 {
		return errors.New("ack node id cannot be empty")
	}
	if ack.NodeId != sess.peerID {
		return fmt.Errorf("ack node id mismatch: got %d want %d", ack.NodeId, sess.peerID)
	}
	if ack.OriginNodeId <= 0 {
		return errors.New("ack origin node id cannot be empty")
	}
	if m.store != nil {
		if err := m.store.RecordPeerAck(context.Background(), sess.peerID, ack.OriginNodeId, int64(ack.AckedEventId)); err != nil {
			return err
		}
	}

	m.mu.Lock()
	if peer, ok := m.peers[sess.peerID]; ok && ack.AckedEventId > peer.lastAck {
		peer.lastAck = ack.AckedEventId
	}
	m.mu.Unlock()
	m.logSessionDebug("peer_ack_recorded", sess).
		Int64("origin_node_id", ack.OriginNodeId).
		Uint64("acked_event_id", ack.AckedEventId).
		Msg("peer ack recorded")
	return nil
}

func (m *Manager) handleEventBatch(sess *session, envelope *internalproto.Envelope) error {
	if !sess.isReplicationReady() {
		return errors.New("event batch received before replication was ready")
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	batch := envelope.GetEventBatch()
	if batch == nil {
		return errors.New("event batch body cannot be empty")
	}

	events := batch.GetEvents()
	originNodeID := batch.GetOriginNodeId()
	if originNodeID <= 0 && len(events) > 0 && events[0] != nil {
		originNodeID = events[0].GetOriginNodeId()
	}
	if len(events) == 0 {
		if batch.GetPullRequestId() == 0 {
			return errors.New("event batch cannot be empty")
		}
		if originNodeID <= 0 {
			return errors.New("empty pull response origin node id cannot be empty")
		}
		sess.noteRemoteOriginEvent(originNodeID, sess.remoteOriginEventID(originNodeID))
		sess.completePendingPull(originNodeID, batch.GetPullRequestId())
		if m.store != nil {
			requested, err := m.requestCatchupIfNeeded(sess)
			if err != nil {
				return err
			}
			if !requested {
				m.sendSnapshotDigest(sess)
			}
		}
		m.logSessionDebug("event_batch_pull_completed", sess).
			Int64("origin_node_id", originNodeID).
			Uint64("pull_request_id", batch.GetPullRequestId()).
			Msg("empty pull response completed")
		return nil
	}

	if envelope.Sequence == 0 {
		return errors.New("event batch sequence cannot be empty")
	}
	if strings.TrimSpace(envelope.SentAtHlc) == "" {
		return errors.New("event batch sent_at_hlc cannot be empty")
	}
	m.mu.Lock()
	if err := m.allowEventApplyLocked(sess.peerID); err != nil {
		m.mu.Unlock()
		m.logSessionWarn("event_batch_rejected_by_clock", sess, nil).
			Str("reason", err.Error()).
			Msg("event batch rejected by clock protection")
		return err
	}
	m.mu.Unlock()
	if err := m.validateBatchHLC(envelope.SentAtHlc, events); err != nil {
		return err
	}

	lastEventID, err := validateOriginEventBatch(originNodeID, events)
	if err != nil {
		return err
	}
	sess.noteRemoteOriginEvent(originNodeID, uint64(lastEventID))

	for _, event := range events {
		if err := m.store.ApplyReplicatedEvent(context.Background(), event); err != nil {
			return err
		}
	}

	shouldAdvanceCursor := batch.GetPullRequestId() > 0 && sess.completePendingPull(originNodeID, batch.GetPullRequestId())
	if !shouldAdvanceCursor && !sess.hasPendingPull(originNodeID) {
		shouldAdvanceCursor = true
	}

	ackedEventID := uint64(0)
	if m.store != nil {
		if shouldAdvanceCursor {
			if err := m.store.RecordOriginApplied(context.Background(), originNodeID, int64(lastEventID)); err != nil {
				return err
			}
		}
		cursor, err := m.store.GetOriginCursor(context.Background(), originNodeID)
		if err != nil {
			return err
		}
		ackedEventID = uint64(cursor.AppliedEventID)
	}

	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_Ack{
			Ack: &internalproto.Ack{
				NodeId:       m.cfg.NodeID,
				OriginNodeId: originNodeID,
				AckedEventId: ackedEventID,
			},
		},
	})
	m.logSessionEvent("event_batch_applied", sess).
		Int64("origin_node_id", originNodeID).
		Int("event_count", len(events)).
		Int64("last_event_id", lastEventID).
		Bool("from_pull", batch.GetPullRequestId() > 0).
		Uint64("pull_request_id", batch.GetPullRequestId()).
		Uint64("acked_event_id", ackedEventID).
		Msg("event batch applied")
	if m.store != nil {
		requested, err := m.requestCatchupIfNeeded(sess)
		if err != nil {
			return err
		}
		if !requested {
			m.sendSnapshotDigest(sess)
		}
	}
	return nil
}

func (m *Manager) handlePullEvents(sess *session, envelope *internalproto.Envelope) error {
	if !sess.isReplicationReady() {
		return errors.New("pull events received before replication was ready")
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	pull := envelope.GetPullEvents()
	if pull == nil {
		return errors.New("pull events body cannot be empty")
	}
	if pull.OriginNodeId <= 0 {
		return errors.New("pull events origin node id cannot be empty")
	}
	if pull.RequestId == 0 {
		return errors.New("pull events request id cannot be empty")
	}

	limit := int(pull.GetLimit())
	if limit <= 0 || limit > pullBatchSize {
		limit = pullBatchSize
	}

	events, err := m.store.ListEventsByOrigin(context.Background(), pull.OriginNodeId, int64(pull.GetAfterEventId()), limit)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		m.logSessionDebug("pull_events_served", sess).
			Int64("origin_node_id", pull.OriginNodeId).
			Uint64("request_id", pull.RequestId).
			Int("event_count", 0).
			Uint64("after_event_id", pull.GetAfterEventId()).
			Msg("served empty pull events response")
		sess.enqueue(&internalproto.Envelope{
			NodeId: m.cfg.NodeID,
			Body: &internalproto.Envelope_EventBatch{
				EventBatch: &internalproto.EventBatch{
					PullRequestId: pull.RequestId,
					OriginNodeId:  pull.OriginNodeId,
				},
			},
		})
		return nil
	}

	replicated := make([]*internalproto.ReplicatedEvent, 0, len(events))
	for _, event := range events {
		replicated = append(replicated, store.ToReplicatedEvent(event))
	}

	last := events[len(events)-1]
	m.logSessionEvent("pull_events_served", sess).
		Int64("origin_node_id", pull.OriginNodeId).
		Uint64("request_id", pull.RequestId).
		Int("event_count", len(events)).
		Uint64("after_event_id", pull.GetAfterEventId()).
		Int64("last_event_id", last.EventID).
		Msg("served pull events response")
	sess.enqueue(&internalproto.Envelope{
		NodeId:    m.cfg.NodeID,
		Sequence:  uint64(last.Sequence),
		SentAtHlc: last.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events:        replicated,
				PullRequestId: pull.RequestId,
				OriginNodeId:  pull.OriginNodeId,
			},
		},
	})
	return nil
}

func (m *Manager) requestCatchupIfNeeded(sess *session) (bool, error) {
	if m.store == nil || sess.peerID == 0 {
		return false, nil
	}

	remoteProgress := sess.remoteOriginProgressSnapshot()
	originNodeIDs := make([]int64, 0, len(remoteProgress))
	for originNodeID := range remoteProgress {
		originNodeIDs = append(originNodeIDs, originNodeID)
	}
	sort.Slice(originNodeIDs, func(i, j int) bool {
		return originNodeIDs[i] < originNodeIDs[j]
	})

	requested := false
	for _, originNodeID := range originNodeIDs {
		remoteLastEventID := remoteProgress[originNodeID]
		cursor, err := m.store.GetOriginCursor(context.Background(), originNodeID)
		if err != nil {
			return false, err
		}
		appliedEventID := uint64(cursor.AppliedEventID)
		if appliedEventID >= remoteLastEventID {
			continue
		}
		requestID, ok := sess.beginPendingPull(originNodeID, appliedEventID)
		if !ok {
			requested = true
			continue
		}

		envelope, err := m.buildPullEventsEnvelope(originNodeID, appliedEventID, requestID)
		if err != nil {
			sess.cancelPendingPull(originNodeID, requestID)
			return false, err
		}
		m.logSessionEvent("catchup_requested", sess).
			Int64("origin_node_id", originNodeID).
			Uint64("after_event_id", appliedEventID).
			Uint64("remote_last_event_id", remoteLastEventID).
			Uint64("request_id", requestID).
			Msg("requested catchup from peer")
		sess.enqueue(envelope)
		requested = true
	}
	return requested, nil
}

func (m *Manager) buildPullEventsEnvelope(originNodeID int64, afterEventID, requestID uint64) (*internalproto.Envelope, error) {
	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_PullEvents{
			PullEvents: &internalproto.PullEvents{
				OriginNodeId: originNodeID,
				AfterEventId: afterEventID,
				Limit:        pullBatchSize,
				RequestId:    requestID,
			},
		},
	}, nil
}

func (m *Manager) AllowWrite(context.Context) error {
	if m == nil || (len(m.configuredPeers) == 0 && len(m.peers) == 0) {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.hasWritableClockSyncLocked() {
		return nil
	}
	return app.ErrClockNotSynchronized
}

func (m *Manager) hasWritableClockSyncLocked() bool {
	state, _ := m.nodeClockStateLocked()
	return state == clockStateTrusted || state == clockStateObserving
}

func (m *Manager) performTimeSync(sess *session) error {
	var (
		best timeSyncSample
		err  error
	)
	if m.timeSyncer != nil {
		best, err = m.timeSyncer(sess)
	} else {
		best, err = m.collectTimeSyncSample(sess)
	}
	if err != nil {
		state, reason := m.recordTimeSyncFailure(sess, err)
		if state == clockStateRejected {
			return fmt.Errorf("peer %d clock rejected after time sync failure: %s", sess.peerID, reason)
		}
		return nil
	}

	sess.setClockOffset(best.offsetMs)
	sess.observeRTT(best.rttMs)
	state, reason := m.recordTimeSyncSample(sess, best)
	if state == clockStateRejected {
		return fmt.Errorf("peer %d clock rejected after time sync sample: %s", sess.peerID, reason)
	}
	return nil
}

func (m *Manager) collectTimeSyncSample(sess *session) (timeSyncSample, error) {
	var (
		best      timeSyncSample
		found     bool
		lastErr   error
		minRTT    int64
		maxRTT    int64
		haveRange bool
	)

	for range timeSyncSampleCount {
		sample, err := m.timeSyncRoundTrip(sess)
		if err != nil {
			lastErr = err
			continue
		}
		if !haveRange {
			minRTT = sample.rttMs
			maxRTT = sample.rttMs
			haveRange = true
		} else {
			if sample.rttMs < minRTT {
				minRTT = sample.rttMs
			}
			if sample.rttMs > maxRTT {
				maxRTT = sample.rttMs
			}
		}
		if !found || sample.rttMs < best.rttMs {
			best = sample
			found = true
		}
	}
	if found {
		jitterMs := maxRTT - minRTT
		best.uncertaintyMs = maxInt64(best.rttMs/2, jitterMs/2) + 50
		best.credible = best.rttMs <= m.cfg.ClockCredibleRttMs
		if best.sampledAt.IsZero() {
			best.sampledAt = time.Now().UTC()
		}
		return best, nil
	}
	if lastErr == nil {
		lastErr = errors.New("time sync failed without successful samples")
	}
	return timeSyncSample{}, lastErr
}

func (m *Manager) timeSyncRoundTrip(sess *session) (timeSyncSample, error) {
	requestID, resultCh := sess.beginTimeSync()
	clientSendTimeMs := m.clock.PhysicalTimeMs()
	ctx := m.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_TimeSyncRequest{
			TimeSyncRequest: &internalproto.TimeSyncRequest{
				RequestId:        requestID,
				ClientSendTimeMs: clientSendTimeMs,
			},
		},
	})

	timer := time.NewTimer(m.clockSyncTimeout())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		sess.cancelTimeSync(requestID, context.Canceled)
		return timeSyncSample{}, ctx.Err()
	case result := <-resultCh:
		if result.err != nil {
			return timeSyncSample{}, result.err
		}
		if result.response == nil {
			return timeSyncSample{}, errors.New("time sync response was empty")
		}

		serverReceiveMs := result.response.ServerReceiveTimeMs
		serverSendMs := result.response.ServerSendTimeMs
		clientReceiveMs := result.receivedAtMs
		offsetMs := ((serverReceiveMs - clientSendTimeMs) + (serverSendMs - clientReceiveMs)) / 2
		rttMs := clientReceiveMs - clientSendTimeMs - (serverSendMs - serverReceiveMs)
		if rttMs < 0 {
			rttMs = 0
		}
		return timeSyncSample{
			offsetMs:  offsetMs,
			rttMs:     rttMs,
			sampledAt: time.Now().UTC(),
		}, nil
	case <-timer.C:
		sess.cancelTimeSync(requestID, context.DeadlineExceeded)
		return timeSyncSample{}, fmt.Errorf("time sync with peer %d timed out", sess.peerID)
	}
}

func (m *Manager) sessionSyncLoop(sess *session) {
	if m.ctx == nil {
		return
	}
	timeSyncTicker := time.NewTicker(timeSyncInterval)
	defer timeSyncTicker.Stop()
	catchupTicker := time.NewTicker(catchupRetryInterval)
	defer catchupTicker.Stop()
	antiEntropyTicker := time.NewTicker(antiEntropyInterval)
	defer antiEntropyTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-timeSyncTicker.C:
			if sess.isClosed() {
				return
			}
			if err := m.performTimeSync(sess); err != nil {
				m.logSessionWarn("periodic_time_sync_failed", sess, err).
					Msg("periodic time sync failed")
				sess.close()
				return
			}
			state, reason := m.peerClockState(sess.peerID)
			eventName := "periodic_time_sync_succeeded"
			logger := m.logSessionDebug(eventName, sess)
			if state != string(clockStateTrusted) {
				eventName = "periodic_time_sync_observing"
				logger = m.logSessionWarn(eventName, sess, nil)
			}
			logger.
				Str("clock_state", state).
				Str("clock_reason", reason).
				Int64("offset_ms", sess.clockOffset()).
				Int64("rtt_ms", sess.smoothedRTTMs).
				Msg("periodic time sync completed")
		case <-catchupTicker.C:
			if sess.isClosed() {
				return
			}
			if _, err := m.requestCatchupIfNeeded(sess); err != nil {
				m.logSessionWarn("periodic_catchup_failed", sess, err).
					Msg("periodic catchup failed")
				sess.close()
				return
			}
		case <-antiEntropyTicker.C:
			if sess.isClosed() {
				return
			}
			m.sendSnapshotDigest(sess)
		}
	}
}

func (m *Manager) markPeerClockSynced(sess *session, offsetMs int64) {
	_, _ = m.recordTimeSyncSample(sess, timeSyncSample{
		offsetMs:      offsetMs,
		rttMs:         maxInt64(sess.smoothedRTTMs, 1),
		uncertaintyMs: 50,
		sampledAt:     time.Now().UTC(),
		credible:      true,
	})
}

func (m *Manager) clearPeerClockSync(sess *session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return
	}
	cleared := false
	if peer.trustedSession == sess {
		peer.trustedSession = nil
		peer.clockState = clockStateObserving
		cleared = true
	}
	m.recomputeRoutesLocked()
	m.recomputeClockOffsetLocked()
	m.refreshNodeClockStateLocked()
	if cleared {
		m.logSessionEvent("peer_clock_untrusted", sess).
			Msg("peer clock is no longer trusted")
	}
}

func (m *Manager) recomputeClockOffsetLocked() {
	offsets := make([]int64, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer.trustedSession != nil {
			offsets = append(offsets, peer.clockOffsetMs)
		}
	}
	if len(offsets) == 0 {
		m.clock.SetOffsetMs(0)
		return
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	m.clock.SetOffsetMs(offsets[len(offsets)/2])
}

func (m *Manager) validateBatchHLC(sentAtRaw string, events []*internalproto.ReplicatedEvent) error {
	if m.cfg.MaxClockSkewMs == 0 {
		return nil
	}
	maxAllowedWallTime := m.clock.WallTimeMs() + m.cfg.MaxClockSkewMs
	sentAt, err := clock.ParseTimestamp(strings.TrimSpace(sentAtRaw))
	if err != nil {
		return fmt.Errorf("parse event batch sent_at_hlc: %w", err)
	}
	if sentAt.WallTimeMs > maxAllowedWallTime {
		return fmt.Errorf("event batch sent_at_hlc %s exceeds local wall time %d by more than %dms", sentAt, m.clock.WallTimeMs(), m.cfg.MaxClockSkewMs)
	}
	maxEventHLC := clock.Timestamp{}
	for _, event := range events {
		if event == nil {
			continue
		}
		hlc, err := clock.ParseTimestamp(strings.TrimSpace(event.Hlc))
		if err != nil {
			return fmt.Errorf("parse replicated event hlc: %w", err)
		}
		if hlc.WallTimeMs > maxAllowedWallTime {
			return fmt.Errorf("replicated event hlc %s exceeds local wall time %d by more than %dms", hlc, m.clock.WallTimeMs(), m.cfg.MaxClockSkewMs)
		}
		if maxEventHLC == (clock.Timestamp{}) || hlc.Compare(maxEventHLC) > 0 {
			maxEventHLC = hlc
		}
	}
	if maxEventHLC != (clock.Timestamp{}) && sentAt.Compare(maxEventHLC) < 0 {
		return fmt.Errorf("event batch sent_at_hlc %s is earlier than max event hlc %s", sentAt, maxEventHLC)
	}
	return nil
}

func (m *Manager) peerClockState(peerID int64) (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, reason := m.peerClockStateLocked(peerID)
	return string(state), reason
}

func (m *Manager) listLocalLoggedInUsers(ctx context.Context) ([]app.LoggedInUserSummary, error) {
	m.mu.Lock()
	provider := m.loggedInUsersProvider
	m.mu.Unlock()
	if provider == nil {
		return nil, fmt.Errorf("%w: local logged-in users provider is not configured", app.ErrServiceUnavailable)
	}
	return provider(ctx)
}

func clusterLoggedInUsers(users []app.LoggedInUserSummary) []*internalproto.ClusterLoggedInUser {
	items := make([]*internalproto.ClusterLoggedInUser, 0, len(users))
	for _, user := range users {
		items = append(items, &internalproto.ClusterLoggedInUser{
			NodeId:   user.NodeID,
			UserId:   user.UserID,
			Username: user.Username,
		})
	}
	return items
}

func loggedInUsersFromCluster(users []*internalproto.ClusterLoggedInUser) []app.LoggedInUserSummary {
	items := make([]app.LoggedInUserSummary, 0, len(users))
	for _, user := range users {
		if user == nil {
			continue
		}
		items = append(items, app.LoggedInUserSummary{
			NodeID:   user.NodeId,
			UserID:   user.UserId,
			Username: user.Username,
		})
	}
	return items
}

func clusterQueryError(resp *internalproto.QueryLoggedInUsersResponse) error {
	if resp == nil || strings.TrimSpace(resp.ErrorCode) == "" {
		return nil
	}
	message := strings.TrimSpace(resp.ErrorMessage)
	if message == "" {
		message = "cluster query failed"
	}
	switch resp.ErrorCode {
	case "invalid_request":
		return fmt.Errorf("%w: %s", store.ErrInvalidInput, message)
	case "not_found":
		return fmt.Errorf("%w: %s", store.ErrNotFound, message)
	case "forbidden":
		return fmt.Errorf("%w: %s", store.ErrForbidden, message)
	default:
		return fmt.Errorf("%w: %s", app.ErrServiceUnavailable, message)
	}
}

func normalizedMessageWindowSize(size int) int {
	if size <= 0 {
		return store.DefaultMessageWindowSize
	}
	return size
}

func (m *Manager) marshalSignedEnvelope(envelope *internalproto.Envelope) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("envelope cannot be nil")
	}
	clone, ok := proto.Clone(envelope).(*internalproto.Envelope)
	if !ok {
		return nil, errors.New("clone envelope")
	}
	signature, err := m.envelopeHMAC(clone)
	if err != nil {
		return nil, err
	}
	clone.Hmac = signature
	return marshalOptions.Marshal(clone)
}

func (m *Manager) verifyEnvelope(envelope *internalproto.Envelope) error {
	if envelope == nil {
		return errors.New("envelope cannot be nil")
	}
	if len(envelope.Hmac) == 0 {
		return errors.New("envelope hmac cannot be empty")
	}
	expected, err := m.envelopeHMAC(envelope)
	if err != nil {
		return err
	}
	if !hmac.Equal(envelope.Hmac, expected) {
		return errors.New("envelope hmac mismatch")
	}
	return nil
}

func (m *Manager) envelopeHMAC(envelope *internalproto.Envelope) ([]byte, error) {
	clone, ok := proto.Clone(envelope).(*internalproto.Envelope)
	if !ok {
		return nil, errors.New("clone envelope")
	}
	clone.Hmac = nil
	payload, err := marshalOptions.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope for hmac: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(m.cfg.ClusterSecret))
	if _, err := mac.Write(payload); err != nil {
		return nil, fmt.Errorf("write envelope hmac: %w", err)
	}
	return mac.Sum(nil), nil
}

func validatePeerEnvelope(sess *session, envelope *internalproto.Envelope) error {
	envelopeNodeID := envelope.NodeId
	if envelopeNodeID <= 0 {
		return errors.New("envelope node id cannot be empty")
	}
	if sess.peerID == 0 {
		return errors.New("session has not completed hello")
	}
	if envelopeNodeID != sess.peerID {
		return fmt.Errorf("envelope node id mismatch: got %d want %d", envelopeNodeID, sess.peerID)
	}
	return nil
}

func (m *Manager) activateSession(sess *session) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return false
	}

	if peer.active == nil {
		peer.active = sess
		if peer.sessions == nil {
			peer.sessions = make(map[uint64]*session)
		}
		peer.sessions[sess.connectionID] = sess
		eventName := "peer_joined"
		message := "peer joined"
		if peer.joinedLogged {
			eventName = "peer_reconnected"
			message = "peer reconnected"
		} else {
			peer.joinedLogged = true
		}
		m.logSessionEvent(eventName, sess).
			Msg(message)
		return true
	}
	if peer.active == sess {
		if peer.sessions == nil {
			peer.sessions = make(map[uint64]*session)
		}
		peer.sessions[sess.connectionID] = sess
		return true
	}

	preferOutbound := m.cfg.NodeID < sess.peerID
	shouldKeepNew := sess.outbound == preferOutbound
	if !shouldKeepNew {
		return false
	}

	old := peer.active
	peer.active = sess
	if peer.sessions == nil {
		peer.sessions = make(map[uint64]*session)
	}
	peer.sessions[sess.connectionID] = sess
	go old.close()
	return true
}

func sessionDirection(sess *session) string {
	if sess != nil && sess.outbound {
		return "outbound"
	}
	return "inbound"
}

func (m *Manager) deactivateSession(sess *session) {
	if sess.peerID == 0 {
		return
	}

	wasActive := false
	wasTrusted := false
	m.mu.Lock()
	peer, ok := m.peers[sess.peerID]
	if ok && peer.active == sess {
		peer.active = nil
		wasActive = true
	}
	if ok && peer.sessions != nil {
		delete(peer.sessions, sess.connectionID)
	}
	if ok && peer.trustedSession == sess {
		if peer.active != nil && peer.active != sess {
			peer.trustedSession = peer.active
			peer.clockOffsetMs = peer.active.clockOffset()
			peer.clockState = clockStateTrusted
		} else {
			peer.trustedSession = nil
			peer.clockOffsetMs = 0
			peer.clockState = clockStateObserving
			wasTrusted = true
		}
	}
	if ok && len(peer.sessions) == 0 {
		peer.routeAdverts = nil
	}
	m.recomputeRoutesLocked()
	m.recomputeClockOffsetLocked()
	m.refreshNodeClockStateLocked()
	m.mu.Unlock()
	m.logSessionEvent("peer_session_closed", sess).
		Bool("was_active", wasActive).
		Bool("was_trusted", wasTrusted).
		Msg("peer session closed")
}

func (s *session) enqueue(envelope *internalproto.Envelope) {
	defer func() {
		_ = recover()
	}()

	if s.manager == nil || s.manager.ctx == nil {
		select {
		case s.send <- envelope:
		default:
		}
		return
	}

	select {
	case s.send <- envelope:
	case <-s.manager.ctx.Done():
	}
}

func (s *session) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	pending := make([]chan timeSyncResult, 0, len(s.pendingTimeSync))
	for requestID, ch := range s.pendingTimeSync {
		delete(s.pendingTimeSync, requestID)
		pending = append(pending, ch)
	}
	close(s.send)
	s.mu.Unlock()

	for _, ch := range pending {
		select {
		case ch <- timeSyncResult{err: errSessionClosed}:
		default:
		}
		close(ch)
	}

	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (s *session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *session) noteRemoteOriginProgress(progress []*internalproto.OriginProgress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remoteOriginProgress == nil {
		s.remoteOriginProgress = make(map[int64]uint64)
	}
	for _, item := range progress {
		if item == nil || item.OriginNodeId <= 0 {
			continue
		}
		if item.LastEventId > s.remoteOriginProgress[item.OriginNodeId] {
			s.remoteOriginProgress[item.OriginNodeId] = item.LastEventId
		}
	}
}

func (s *session) noteRemoteOriginEvent(originNodeID int64, eventID uint64) {
	if originNodeID <= 0 || eventID == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remoteOriginProgress == nil {
		s.remoteOriginProgress = make(map[int64]uint64)
	}
	if eventID > s.remoteOriginProgress[originNodeID] {
		s.remoteOriginProgress[originNodeID] = eventID
	}
}

func (s *session) remoteOriginEventID(originNodeID int64) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteOriginProgress[originNodeID]
}

func (s *session) remoteOriginProgressSnapshot() map[int64]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	progress := make(map[int64]uint64, len(s.remoteOriginProgress))
	for originNodeID, eventID := range s.remoteOriginProgress {
		progress[originNodeID] = eventID
	}
	return progress
}

func (s *session) beginPendingPull(originNodeID int64, afterEventID uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingPulls == nil {
		s.pendingPulls = make(map[int64]pendingPullState)
	}
	if _, ok := s.pendingPulls[originNodeID]; ok {
		return 0, false
	}
	s.nextPullRequestID++
	requestID := s.nextPullRequestID
	s.pendingPulls[originNodeID] = pendingPullState{
		RequestID:    requestID,
		AfterEventID: afterEventID,
	}
	return requestID, true
}

func (s *session) hasPendingPull(originNodeID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pendingPulls[originNodeID]
	return ok
}

func (s *session) hasPendingPulls() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pendingPulls) > 0
}

func (s *session) cancelPendingPull(originNodeID int64, requestID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.pendingPulls[originNodeID]
	if ok && state.RequestID == requestID {
		delete(s.pendingPulls, originNodeID)
	}
}

func (s *session) completePendingPull(originNodeID int64, requestID uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.pendingPulls[originNodeID]
	if !ok || state.RequestID != requestID {
		return false
	}
	delete(s.pendingPulls, originNodeID)
	return true
}

func (s *session) beginBootstrap() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bootstrapStarted {
		return false
	}
	s.bootstrapStarted = true
	return true
}

func (s *session) markReplicationReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicationReady = true
}

func (s *session) isReplicationReady() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.replicationReady
}

func (s *session) startSyncLoop(run func()) {
	s.mu.Lock()
	if s.syncLoopStarted {
		s.mu.Unlock()
		return
	}
	s.syncLoopStarted = true
	s.mu.Unlock()

	go run()
}

func (s *session) beginTimeSync() (uint64, chan timeSyncResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingTimeSync == nil {
		s.pendingTimeSync = make(map[uint64]chan timeSyncResult)
	}
	s.nextTimeSyncID++
	requestID := s.nextTimeSyncID
	ch := make(chan timeSyncResult, 1)
	s.pendingTimeSync[requestID] = ch
	return requestID, ch
}

func (s *session) cancelTimeSync(requestID uint64, err error) {
	s.mu.Lock()
	ch, ok := s.pendingTimeSync[requestID]
	if ok {
		delete(s.pendingTimeSync, requestID)
	}
	s.mu.Unlock()

	if !ok {
		return
	}
	select {
	case ch <- timeSyncResult{err: err}:
	default:
	}
	close(ch)
}

func (s *session) resolveTimeSync(requestID uint64, result timeSyncResult) bool {
	s.mu.Lock()
	ch, ok := s.pendingTimeSync[requestID]
	if ok {
		delete(s.pendingTimeSync, requestID)
	}
	s.mu.Unlock()

	if !ok {
		return false
	}
	ch <- result
	close(ch)
	return true
}

func (m *Manager) beginLoggedInUsersQuery() (uint64, chan loggedInUsersQueryResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextLoggedInUsersQueryID++
	requestID := m.nextLoggedInUsersQueryID
	ch := make(chan loggedInUsersQueryResult, 1)
	m.pendingLoggedInUsers[requestID] = ch
	return requestID, ch
}

func (m *Manager) cancelLoggedInUsersQuery(requestID uint64, err error) {
	m.mu.Lock()
	ch, ok := m.pendingLoggedInUsers[requestID]
	if ok {
		delete(m.pendingLoggedInUsers, requestID)
	}
	m.mu.Unlock()

	if !ok {
		return
	}
	select {
	case ch <- loggedInUsersQueryResult{err: err}:
	default:
	}
	close(ch)
}

func (m *Manager) resolveLoggedInUsersQuery(requestID uint64, result loggedInUsersQueryResult) bool {
	m.mu.Lock()
	ch, ok := m.pendingLoggedInUsers[requestID]
	if ok {
		delete(m.pendingLoggedInUsers, requestID)
	}
	m.mu.Unlock()

	if !ok {
		return false
	}
	ch <- result
	close(ch)
	return true
}

func (s *session) setClockOffset(offsetMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clockOffsetMs = offsetMs
}

func (s *session) clockOffset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clockOffsetMs
}

func (s *session) observeRTT(rttMs int64) {
	if rttMs < 0 {
		rttMs = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.smoothedRTTMs == 0 {
		s.smoothedRTTMs = rttMs
		s.jitterPenaltyMs = 0
		s.lastRTTUpdate = time.Now().UTC()
		return
	}
	diff := absInt64(rttMs - s.smoothedRTTMs)
	s.jitterPenaltyMs = (3*s.jitterPenaltyMs + diff) / 4
	s.smoothedRTTMs = (7*s.smoothedRTTMs + rttMs) / 8
	s.lastRTTUpdate = time.Now().UTC()
}

func (s *session) beginSnapshotRequest(partition string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pendingSnapshotParts == nil {
		s.pendingSnapshotParts = make(map[string]struct{})
	}
	if _, ok := s.pendingSnapshotParts[partition]; ok {
		return false
	}
	s.pendingSnapshotParts[partition] = struct{}{}
	return true
}

func (s *session) completeSnapshotRequest(partition string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pendingSnapshotParts, partition)
}

func validateOriginEventBatch(originNodeID int64, events []*internalproto.ReplicatedEvent) (int64, error) {
	if originNodeID <= 0 {
		return 0, errors.New("event batch origin node id cannot be empty")
	}
	if len(events) == 0 {
		return 0, errors.New("event batch cannot be empty")
	}

	var lastEventID int64
	for i, event := range events {
		if event == nil {
			return 0, errors.New("event batch cannot contain nil events")
		}
		if event.OriginNodeId != originNodeID {
			return 0, fmt.Errorf("event batch origin mismatch: got %d want %d", event.OriginNodeId, originNodeID)
		}
		if event.EventId == 0 {
			return 0, errors.New("event batch event id cannot be empty")
		}
		if i > 0 && event.EventId <= lastEventID {
			return 0, fmt.Errorf("event batch event ids must be strictly increasing: %d then %d", lastEventID, event.EventId)
		}
		lastEventID = event.EventId
	}
	return lastEventID, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func connectionIDForHello(sess *session) uint64 {
	if sess == nil {
		return 0
	}
	return sess.connectionID
}

func (m *Manager) hasActivePeer(peerID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[peerID]
	return ok && peer.active != nil
}

func (m *Manager) configuredPeerNodeID(peer *configuredPeer) int64 {
	if peer == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return peer.nodeID
}

func (m *Manager) bindPeerIdentity(sess *session, peerID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sess.peerID != 0 {
		if sess.peerID != peerID {
			return fmt.Errorf("session peer id mismatch: got %d want %d", peerID, sess.peerID)
		}
		return nil
	}

	if sess.configuredPeer != nil {
		if sess.configuredPeer.nodeID != 0 && sess.configuredPeer.nodeID != peerID {
			return fmt.Errorf("peer mismatch: expected %d got %d", sess.configuredPeer.nodeID, peerID)
		}
		for _, configured := range m.configuredPeers {
			if configured != sess.configuredPeer && configured.nodeID == peerID {
				return fmt.Errorf("peer %d already bound to configured url %s", peerID, configured.URL)
			}
		}
		sess.configuredPeer.nodeID = peerID
	}

	sess.peerID = peerID
	if _, ok := m.peers[peerID]; !ok {
		m.peers[peerID] = &peerState{
			clockState:   clockStateProbing,
			sessions:     make(map[uint64]*session),
			routeAdverts: make(map[int64]routeAdvertisement),
		}
	}
	m.peers[peerID].sessions[sess.connectionID] = sess
	m.refreshNodeClockStateLocked()
	return nil
}
