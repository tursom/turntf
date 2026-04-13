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

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	websocketPath            = "/internal/cluster/ws"
	writeWait                = 10 * time.Second
	pingInterval             = 15 * time.Second
	readTimeout              = 45 * time.Second
	outboundQueueSize        = 128
	managerPublishQueue      = 256
	pullBatchSize            = 128
	timeSyncSampleCount      = 5
	timeSyncInterval         = 30 * time.Second
	timeSyncTimeout          = 3 * time.Second
	catchupRetryInterval     = time.Second
	antiEntropyInterval      = 60 * time.Second
	writeGateGrace           = 90 * time.Second
	routingUpdateInterval    = 5 * time.Second
	routeRetryInterval       = 200 * time.Millisecond
	packetSeenTTL            = 30 * time.Second
	routeRetryTTL            = 3 * time.Second
	defaultPacketTTLHops     = 8
	routingSwitchThresholdMs = 15
)

var marshalOptions = proto.MarshalOptions{Deterministic: true}

var errSessionClosed = errors.New("session closed")

type Manager struct {
	cfg      Config
	store    *store.Store
	clock    *clock.Clock
	upgrader websocket.Upgrader

	mux       *http.ServeMux
	publishCh chan store.Event
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	closeOnce sync.Once
	dialer    *websocket.Dialer

	mu              sync.Mutex
	peers           map[int64]*peerState
	configuredPeers []*configuredPeer

	lastSuccessfulClockSync time.Time
	timeSyncer              func(*session) (timeSyncSample, error)
	transientHandler        func(store.TransientPacket) bool
	routingGeneration       uint64
	routingTable            map[int64]routeEntry
	seenPackets             map[string]time.Time
	retryQueue              map[string]queuedPacket
}

type configuredPeer struct {
	URL    string
	nodeID int64
}

type peerState struct {
	active         *session
	lastAck        uint64
	trustedSession *session
	clockOffsetMs  int64
	lastClockSync  time.Time
	sessions       map[uint64]*session
	routeAdverts   map[int64]routeAdvertisement
	joinedLogged   bool

	snapshotDigestsSent     uint64
	snapshotDigestsReceived uint64
	snapshotChunksSent      uint64
	snapshotChunksReceived  uint64
	lastSnapshotDigestAt    time.Time
	lastSnapshotChunkAt     time.Time
}

type session struct {
	manager  *Manager
	conn     *websocket.Conn
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

type timeSyncSample struct {
	offsetMs int64
	rttMs    int64
}

func NewManager(cfg Config, st *store.Store) (*Manager, error) {
	cfg.MessageWindowSize = normalizedMessageWindowSize(cfg.MessageWindowSize)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	configuredPeers := make([]*configuredPeer, 0, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		configuredPeers = append(configuredPeers, &configuredPeer{URL: peer.URL})
	}

	clockRef := clock.NewClock(cfg.NodeID)
	if st != nil && st.Clock() != nil {
		clockRef = st.Clock()
	}

	mgr := &Manager{
		cfg:   cfg,
		store: st,
		clock: clockRef,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		mux:             http.NewServeMux(),
		publishCh:       make(chan store.Event, managerPublishQueue),
		dialer:          websocket.DefaultDialer,
		peers:           make(map[int64]*peerState, len(cfg.Peers)),
		configuredPeers: configuredPeers,
		routingTable:    make(map[int64]routeEntry),
		seenPackets:     make(map[string]time.Time),
		retryQueue:      make(map[string]queuedPacket),
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

func (m *Manager) Start(parent context.Context) {
	m.startOnce.Do(func() {
		m.ctx, m.cancel = context.WithCancel(parent)

		m.wg.Add(1)
		go m.publishLoop()
		m.wg.Add(1)
		go m.routingLoop()

		for _, peer := range m.configuredPeers {
			peer := peer
			m.wg.Add(1)
			go m.dialLoop(peer)
		}
	})
}

func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
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

		m.logInfo("peer_dial_started").
			Str("direction", "outbound").
			Str("peer_url", peer.URL).
			Msg("starting outbound peer dial")
		conn, _, err := m.dialer.DialContext(m.ctx, peer.URL, nil)
		if err != nil {
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

		select {
		case <-m.ctx.Done():
			return
		default:
		}
	}
}

func (m *Manager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if m.ctx == nil {
		http.Error(w, "cluster manager not started", http.StatusServiceUnavailable)
		return
	}

	conn, err := m.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	m.logInfo("peer_inbound_accepted").
		Str("direction", "inbound").
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

func (m *Manager) newSession(conn *websocket.Conn, outbound bool, configuredPeer *configuredPeer) *session {
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
	}
}

func (m *Manager) runSession(sess *session) {
	m.logSessionEvent("peer_session_started", sess).
		Msg("peer session started")
	defer sess.close()
	defer m.deactivateSession(sess)

	sess.conn.SetReadLimit(8 << 20)
	_ = sess.conn.SetReadDeadline(time.Now().Add(readTimeout))
	sess.conn.SetPongHandler(func(string) error {
		return sess.conn.SetReadDeadline(time.Now().Add(readTimeout))
	})

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
				NodeId:            m.cfg.NodeID,
				AdvertiseAddr:     m.cfg.AdvertisePath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				OriginProgress:    progress,
				SnapshotVersion:   internalproto.SnapshotVersion,
				MessageWindowSize: uint32(m.cfg.MessageWindowSize),
				SupportsRouting:   true,
				ConnectionId:      connectionIDForHello(sess),
			},
		},
	}, nil
}

func (m *Manager) writeLoop(sess *session) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			_ = sess.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"), time.Now().Add(writeWait))
			return
		case envelope, ok := <-sess.send:
			if !ok {
				_ = sess.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "session closed"), time.Now().Add(writeWait))
				return
			}

			data, err := m.marshalSignedEnvelope(envelope)
			if err != nil {
				log.Warn().Err(err).Str("component", "cluster").Int64("peer", sess.peerID).Str("event", "marshal_envelope_failed").Msg("marshal envelope failed")
				return
			}

			_ = sess.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := sess.conn.WriteMessage(websocket.BinaryMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			_ = sess.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := sess.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (m *Manager) readLoop(sess *session) {
	for {
		_, data, err := sess.conn.ReadMessage()
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
		case *internalproto.Envelope_Ack:
			if err := m.handleAck(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_EventBatch:
			if err := m.handleEventBatch(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_PullEvents:
			if err := m.handlePullEvents(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_SnapshotDigest:
			if err := m.handleSnapshotDigest(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_SnapshotChunk:
			if err := m.handleSnapshotChunk(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_RoutingUpdate:
			if err := m.handleRoutingUpdate(sess, &envelope); err != nil {
				return
			}
		case *internalproto.Envelope_TransientPacket:
			if err := m.handleTransientPacket(sess, &envelope); err != nil {
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
	m.logSessionEvent("time_sync_succeeded", sess).
		Int64("offset_ms", sess.clockOffset()).
		Int64("rtt_ms", sess.smoothedRTTMs).
		Int64("max_clock_skew_ms", m.cfg.MaxClockSkewMs).
		Msg("time sync succeeded")
	if !m.activateSession(sess) {
		m.logSessionWarn("duplicate_session_rejected", sess, nil).
			Msg("duplicate session rejected")
		sess.close()
		return
	}

	sess.markReplicationReady()
	m.markPeerClockSynced(sess, sess.clockOffset())
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
	if err := m.validateBatchHLC(events); err != nil {
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
	if len(m.configuredPeers) == 0 && len(m.peers) == 0 {
		return true
	}
	for _, peer := range m.peers {
		if peer.trustedSession != nil {
			return true
		}
	}
	if !m.lastSuccessfulClockSync.IsZero() && time.Since(m.lastSuccessfulClockSync) <= writeGateGrace {
		return true
	}
	return false
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
		return err
	}

	sess.setClockOffset(best.offsetMs)
	sess.observeRTT(best.rttMs)
	if m.cfg.MaxClockSkewMs > 0 && absInt64(best.offsetMs) > m.cfg.MaxClockSkewMs {
		return fmt.Errorf("clock offset %dms exceeds max skew %dms", best.offsetMs, m.cfg.MaxClockSkewMs)
	}

	warnThreshold := m.cfg.MaxClockSkewMs
	if warnThreshold == 0 {
		warnThreshold = DefaultMaxClockSkewMs
	}
	if warnThreshold > 0 && absInt64(best.offsetMs) > warnThreshold {
		m.logSessionWarn("clock_offset_warn", sess, nil).
			Int64("offset_ms", best.offsetMs).
			Int64("threshold_ms", warnThreshold).
			Msg("clock offset exceeds warning threshold")
	}
	return nil
}

func (m *Manager) collectTimeSyncSample(sess *session) (timeSyncSample, error) {
	var (
		best    timeSyncSample
		found   bool
		lastErr error
	)

	for range timeSyncSampleCount {
		sample, err := m.timeSyncRoundTrip(sess)
		if err != nil {
			lastErr = err
			continue
		}
		if !found || sample.rttMs < best.rttMs {
			best = sample
			found = true
		}
	}
	if found {
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

	timer := time.NewTimer(timeSyncTimeout)
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
		return timeSyncSample{offsetMs: offsetMs, rttMs: rttMs}, nil
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
			m.logSessionDebug("periodic_time_sync_succeeded", sess).
				Int64("offset_ms", sess.clockOffset()).
				Int64("rtt_ms", sess.smoothedRTTMs).
				Msg("periodic time sync succeeded")
			m.markPeerClockSynced(sess, sess.clockOffset())
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
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return
	}
	if peer.active != sess {
		return
	}

	wasTrusted := peer.trustedSession == sess
	peer.trustedSession = sess
	peer.clockOffsetMs = offsetMs
	peer.lastClockSync = time.Now()
	m.lastSuccessfulClockSync = peer.lastClockSync
	if sess.smoothedRTTMs <= 0 {
		sess.smoothedRTTMs = 1
	}
	if peer.sessions != nil {
		peer.sessions[sess.connectionID] = sess
	}
	m.recomputeRoutesLocked()
	m.recomputeClockOffsetLocked()
	if !wasTrusted {
		m.logSessionEvent("peer_clock_trusted", sess).
			Int64("offset_ms", offsetMs).
			Msg("peer clock is trusted")
	}
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
		peer.clockOffsetMs = 0
		peer.lastClockSync = time.Time{}
		cleared = true
	}
	m.recomputeRoutesLocked()
	m.recomputeClockOffsetLocked()
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
		return
	}
	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})
	m.clock.SetOffsetMs(offsets[len(offsets)/2])
}

func (m *Manager) validateBatchHLC(events []*internalproto.ReplicatedEvent) error {
	if m.cfg.MaxClockSkewMs == 0 {
		return nil
	}

	maxAllowedWallTime := m.clock.WallTimeMs() + m.cfg.MaxClockSkewMs
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
	}
	return nil
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
		peer.trustedSession = nil
		peer.clockOffsetMs = 0
		peer.lastClockSync = time.Time{}
		wasTrusted = true
	}
	if ok && len(peer.sessions) == 0 {
		peer.routeAdverts = nil
	}
	m.recomputeRoutesLocked()
	m.recomputeClockOffsetLocked()
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
			sessions:     make(map[uint64]*session),
			routeAdverts: make(map[int64]routeAdvertisement),
		}
	}
	m.peers[peerID].sessions[sess.connectionID] = sess
	return nil
}
