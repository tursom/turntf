package cluster

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	"notifier/internal/app"
	"notifier/internal/clock"
	internalproto "notifier/internal/proto"
	"notifier/internal/store"
)

const (
	websocketPath        = "/internal/cluster/ws"
	writeWait            = 10 * time.Second
	pingInterval         = 15 * time.Second
	readTimeout          = 45 * time.Second
	outboundQueueSize    = 128
	managerPublishQueue  = 256
	pullBatchSize        = 128
	timeSyncSampleCount  = 5
	timeSyncInterval     = 30 * time.Second
	timeSyncTimeout      = 3 * time.Second
	catchupRetryInterval = time.Second
	antiEntropyInterval  = 60 * time.Second
	writeGateGrace       = 90 * time.Second
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

	mu    sync.Mutex
	peers map[string]*peerState

	lastSuccessfulClockSync time.Time
	timeSyncer              func(*session) (timeSyncSample, error)
}

type peerState struct {
	cfg            Peer
	active         *session
	lastAck        uint64
	trustedSession *session
	clockOffsetMs  int64
	lastClockSync  time.Time

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

	configuredPeerID string
	peerID           string

	send chan *internalproto.Envelope

	mu                      sync.Mutex
	closed                  bool
	remoteLastSequence      uint64
	catchupInFlight         bool
	pendingPullAfter        uint64
	replicationReady        bool
	bootstrapStarted        bool
	syncLoopStarted         bool
	remoteSnapshotVersion   string
	remoteMessageWindowSize int
	pendingSnapshotParts    map[string]struct{}
	nextTimeSyncID          uint64
	pendingTimeSync         map[uint64]chan timeSyncResult
	clockOffsetMs           int64
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

	peers := make(map[string]*peerState, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		peers[peer.NodeID] = &peerState{cfg: peer}
	}

	clockRef := clock.NewClock(cfg.NodeSlot)
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
		mux:       http.NewServeMux(),
		publishCh: make(chan store.Event, managerPublishQueue),
		dialer:    websocket.DefaultDialer,
		peers:     peers,
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

		for _, peer := range m.cfg.Peers {
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

func (m *Manager) broadcastEvent(event store.Event) {
	replicated := store.ToReplicatedEvent(event)
	envelope := &internalproto.Envelope{
		NodeId:    m.cfg.NodeID,
		Sequence:  uint64(event.Sequence),
		SentAtHlc: event.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events: []*internalproto.ReplicatedEvent{replicated},
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

func (m *Manager) dialLoop(peer Peer) {
	defer m.wg.Done()

	backoff := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	attempt := 0

	for {
		select {
		case <-m.ctx.Done():
			return
		default:
		}

		if m.cfg.NodeID > peer.NodeID && m.hasActivePeer(peer.NodeID) {
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(2 * time.Second):
				continue
			}
		}

		conn, _, err := m.dialer.DialContext(m.ctx, peer.URL, nil)
		if err != nil {
			wait := backoff[minInt(attempt, len(backoff)-1)]
			attempt++
			select {
			case <-m.ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}

		attempt = 0
		sess := m.newSession(conn, true, peer.NodeID)
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

	sess := m.newSession(conn, false, "")
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runSession(sess)
	}()
}

func (m *Manager) newSession(conn *websocket.Conn, outbound bool, configuredPeerID string) *session {
	return &session{
		manager:          m,
		conn:             conn,
		outbound:         outbound,
		configuredPeerID: configuredPeerID,
		send:             make(chan *internalproto.Envelope, outboundQueueSize),
		pendingTimeSync:  make(map[uint64]chan timeSyncResult),
	}
}

func (m *Manager) runSession(sess *session) {
	defer sess.close()
	defer m.deactivateSession(sess)

	sess.conn.SetReadLimit(8 << 20)
	_ = sess.conn.SetReadDeadline(time.Now().Add(readTimeout))
	sess.conn.SetPongHandler(func(string) error {
		return sess.conn.SetReadDeadline(time.Now().Add(readTimeout))
	})

	hello, err := m.buildHelloEnvelope()
	if err != nil {
		log.Printf("level=warn component=cluster event=build_hello_failed err=%q", err)
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

func (m *Manager) buildHelloEnvelope() (*internalproto.Envelope, error) {
	lastSequence, err := m.store.LastEventSequence(context.Background())
	if err != nil {
		return nil, err
	}

	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_Hello{
			Hello: &internalproto.Hello{
				NodeId:            m.cfg.NodeID,
				AdvertiseAddr:     m.cfg.AdvertisePath,
				ProtocolVersion:   internalproto.ProtocolVersion,
				LastSequence:      uint64(lastSequence),
				SnapshotVersion:   internalproto.SnapshotVersion,
				MessageWindowSize: uint32(m.cfg.MessageWindowSize),
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
				log.Printf("level=warn component=cluster peer=%s event=marshal_envelope_failed err=%q", sess.peerID, err)
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

		if sess.peerID == "" && envelope.GetHello() == nil {
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
		default:
			return
		}
	}
}

func (m *Manager) handleHello(sess *session, envelope *internalproto.Envelope) error {
	if sess.peerID != "" {
		return errors.New("duplicate hello")
	}

	hello := envelope.GetHello()
	if hello == nil {
		return errors.New("hello body cannot be empty")
	}

	envelopeNodeID := strings.TrimSpace(envelope.NodeId)
	peerID := strings.TrimSpace(hello.NodeId)
	if peerID == "" || peerID == m.cfg.NodeID {
		return fmt.Errorf("invalid peer id %q", peerID)
	}
	if envelopeNodeID == "" {
		return errors.New("hello envelope node id cannot be empty")
	}
	if envelopeNodeID != peerID {
		return fmt.Errorf("hello envelope node id mismatch: got %s want %s", envelopeNodeID, peerID)
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
	if sess.outbound && sess.configuredPeerID != "" && sess.configuredPeerID != peerID {
		return fmt.Errorf("peer mismatch: expected %s got %s", sess.configuredPeerID, peerID)
	}
	if int(hello.MessageWindowSize) != m.cfg.MessageWindowSize {
		log.Printf(
			"level=warn component=cluster peer=%s event=message_window_mismatch local=%d remote=%d msg=%q",
			peerID,
			m.cfg.MessageWindowSize,
			hello.MessageWindowSize,
			"continuing with per-node windows",
		)
	}

	sess.peerID = peerID
	sess.remoteSnapshotVersion = hello.SnapshotVersion
	sess.remoteMessageWindowSize = int(hello.MessageWindowSize)
	sess.noteRemoteLastSequence(hello.LastSequence)
	if !m.isConfiguredPeer(peerID) {
		return fmt.Errorf("peer %s is not configured", peerID)
	}

	if !sess.beginBootstrap() {
		return errors.New("duplicate bootstrap rejected")
	}
	go m.bootstrapSession(sess)
	return nil
}

func (m *Manager) bootstrapSession(sess *session) {
	if err := m.performTimeSync(sess); err != nil {
		log.Printf("level=warn component=cluster peer=%s event=time_sync_failed err=%q", sess.peerID, err)
		sess.close()
		return
	}
	if !m.activateSession(sess) {
		log.Printf("level=warn component=cluster peer=%s event=duplicate_session_rejected", sess.peerID)
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
			log.Printf("level=warn component=cluster peer=%s event=request_catchup_failed err=%q", sess.peerID, err)
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
	if strings.TrimSpace(ack.NodeId) == "" {
		return errors.New("ack node id cannot be empty")
	}
	if ack.NodeId != sess.peerID {
		return fmt.Errorf("ack node id mismatch: got %s want %s", ack.NodeId, sess.peerID)
	}
	if m.store != nil {
		if err := m.store.RecordPeerAck(context.Background(), sess.peerID, int64(ack.AckedSequence)); err != nil {
			return err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return nil
	}
	if ack.GetAckedSequence() > peer.lastAck {
		peer.lastAck = ack.GetAckedSequence()
	}
	return nil
}

func (m *Manager) handleEventBatch(sess *session, envelope *internalproto.Envelope) error {
	if !sess.isReplicationReady() {
		return errors.New("event batch received before replication was ready")
	}
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}
	if envelope.Sequence == 0 {
		return errors.New("event batch sequence cannot be empty")
	}
	if strings.TrimSpace(envelope.SentAtHlc) == "" {
		return errors.New("event batch sent_at_hlc cannot be empty")
	}

	batch := envelope.GetEventBatch()
	if batch == nil {
		return errors.New("event batch body cannot be empty")
	}
	if len(batch.GetEvents()) == 0 {
		return errors.New("event batch cannot be empty")
	}

	batchStart, err := batchStartSequence(envelope.Sequence, len(batch.GetEvents()))
	if err != nil {
		return err
	}

	if err := m.validateBatchHLC(batch.GetEvents()); err != nil {
		return err
	}
	sess.noteRemoteLastSequence(envelope.Sequence)
	wasCatchupResponse := sess.matchesPendingPull(batchStart)

	for _, event := range batch.GetEvents() {
		if err := m.store.ApplyReplicatedEvent(context.Background(), event); err != nil {
			return err
		}
	}
	ackedSequence := uint64(0)
	if m.store != nil {
		cursor, err := m.store.GetPeerCursor(context.Background(), sess.peerID)
		if err != nil {
			return err
		}
		ackedSequence = uint64(cursor.AppliedSequence)
		if nextApplied := advanceContiguousSequence(uint64(cursor.AppliedSequence), batchStart, envelope.Sequence); nextApplied > uint64(cursor.AppliedSequence) {
			if err := m.store.RecordPeerApplied(context.Background(), sess.peerID, int64(nextApplied)); err != nil {
				return err
			}
			ackedSequence = nextApplied
		}
	}
	if wasCatchupResponse {
		sess.completePendingPull(batchStart)
	}

	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_Ack{
			Ack: &internalproto.Ack{
				NodeId:        m.cfg.NodeID,
				AckedSequence: ackedSequence,
			},
		},
	})
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

	limit := int(pull.GetLimit())
	if limit <= 0 || limit > pullBatchSize {
		limit = pullBatchSize
	}

	events, err := m.store.ListEvents(context.Background(), int64(pull.GetAfterSequence()), limit)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	replicated := make([]*internalproto.ReplicatedEvent, 0, len(events))
	for _, event := range events {
		replicated = append(replicated, store.ToReplicatedEvent(event))
	}

	last := events[len(events)-1]
	sess.enqueue(&internalproto.Envelope{
		NodeId:    m.cfg.NodeID,
		Sequence:  uint64(last.Sequence),
		SentAtHlc: last.HLC.String(),
		Body: &internalproto.Envelope_EventBatch{
			EventBatch: &internalproto.EventBatch{
				Events: replicated,
			},
		},
	})
	return nil
}

func (m *Manager) requestCatchupIfNeeded(sess *session) (bool, error) {
	if m.store == nil || sess.peerID == "" {
		return false, nil
	}

	cursor, err := m.store.GetPeerCursor(context.Background(), sess.peerID)
	if err != nil {
		return false, err
	}

	remoteLastSequence := sess.remoteSequence()
	appliedSequence := uint64(cursor.AppliedSequence)
	if appliedSequence >= remoteLastSequence {
		return false, nil
	}
	if !sess.beginPendingPull(appliedSequence) {
		return true, nil
	}

	envelope, err := m.buildPullEventsEnvelope(appliedSequence)
	if err != nil {
		sess.cancelPendingPull(appliedSequence)
		return false, err
	}
	sess.enqueue(envelope)
	return true, nil
}

func (m *Manager) buildPullEventsEnvelope(afterSequence uint64) (*internalproto.Envelope, error) {
	return &internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_PullEvents{
			PullEvents: &internalproto.PullEvents{
				AfterSequence: afterSequence,
				Limit:         pullBatchSize,
			},
		},
	}, nil
}

func (m *Manager) AllowWrite(context.Context) error {
	if m == nil || len(m.peers) == 0 {
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
	if len(m.peers) == 0 {
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
	if m.cfg.MaxClockSkewMs > 0 && absInt64(best.offsetMs) > m.cfg.MaxClockSkewMs {
		return fmt.Errorf("clock offset %dms exceeds max skew %dms", best.offsetMs, m.cfg.MaxClockSkewMs)
	}

	warnThreshold := m.cfg.MaxClockSkewMs
	if warnThreshold == 0 {
		warnThreshold = DefaultMaxClockSkewMs
	}
	if warnThreshold > 0 && absInt64(best.offsetMs) > warnThreshold {
		log.Printf("level=warn component=cluster peer=%s event=clock_offset_warn offset_ms=%d threshold_ms=%d", sess.peerID, best.offsetMs, warnThreshold)
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
		return timeSyncSample{}, fmt.Errorf("time sync with peer %s timed out", sess.peerID)
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
				log.Printf("level=warn component=cluster peer=%s event=periodic_time_sync_failed err=%q", sess.peerID, err)
				sess.close()
				return
			}
			m.markPeerClockSynced(sess, sess.clockOffset())
		case <-catchupTicker.C:
			if sess.isClosed() {
				return
			}
			if _, err := m.requestCatchupIfNeeded(sess); err != nil {
				log.Printf("level=warn component=cluster peer=%s event=periodic_catchup_failed err=%q", sess.peerID, err)
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

	peer.trustedSession = sess
	peer.clockOffsetMs = offsetMs
	peer.lastClockSync = time.Now()
	m.lastSuccessfulClockSync = peer.lastClockSync
	m.recomputeClockOffsetLocked()
}

func (m *Manager) clearPeerClockSync(sess *session) {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return
	}
	if peer.trustedSession == sess {
		peer.trustedSession = nil
		peer.clockOffsetMs = 0
		peer.lastClockSync = time.Time{}
	}
	m.recomputeClockOffsetLocked()
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
	envelopeNodeID := strings.TrimSpace(envelope.NodeId)
	if envelopeNodeID == "" {
		return errors.New("envelope node id cannot be empty")
	}
	if sess.peerID == "" {
		return errors.New("session has not completed hello")
	}
	if envelopeNodeID != sess.peerID {
		return fmt.Errorf("envelope node id mismatch: got %s want %s", envelopeNodeID, sess.peerID)
	}
	return nil
}

func (m *Manager) isConfiguredPeer(peerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.peers[peerID]
	return ok
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
		return true
	}
	if peer.active == sess {
		return true
	}

	preferOutbound := m.cfg.NodeID < sess.peerID
	shouldKeepNew := sess.outbound == preferOutbound
	if !shouldKeepNew {
		return false
	}

	old := peer.active
	peer.active = sess
	go old.close()
	return true
}

func (m *Manager) deactivateSession(sess *session) {
	if sess.peerID == "" {
		return
	}

	m.mu.Lock()
	peer, ok := m.peers[sess.peerID]
	if ok && peer.active == sess {
		peer.active = nil
	}
	if ok && peer.trustedSession == sess {
		peer.trustedSession = nil
		peer.clockOffsetMs = 0
		peer.lastClockSync = time.Time{}
	}
	m.recomputeClockOffsetLocked()
	m.mu.Unlock()
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

func (s *session) noteRemoteLastSequence(sequence uint64) {
	s.mu.Lock()
	if sequence > s.remoteLastSequence {
		s.remoteLastSequence = sequence
	}
	s.mu.Unlock()
}

func (s *session) remoteSequence() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.remoteLastSequence
}

func (s *session) beginPendingPull(afterSequence uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.catchupInFlight {
		return false
	}
	s.catchupInFlight = true
	s.pendingPullAfter = afterSequence
	return true
}

func (s *session) hasPendingPull() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.catchupInFlight
}

func (s *session) cancelPendingPull(afterSequence uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.catchupInFlight && s.pendingPullAfter == afterSequence {
		s.catchupInFlight = false
	}
}

func (s *session) matchesPendingPull(batchStart uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.catchupInFlight && batchStart == s.pendingPullAfter+1
}

func (s *session) completePendingPull(batchStart uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.catchupInFlight && batchStart == s.pendingPullAfter+1 {
		s.catchupInFlight = false
	}
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

func batchStartSequence(endSequence uint64, batchLen int) (uint64, error) {
	if batchLen <= 0 {
		return 0, errors.New("event batch cannot be empty")
	}
	if uint64(batchLen) > endSequence {
		return 0, fmt.Errorf("event batch sequence %d smaller than batch length %d", endSequence, batchLen)
	}
	return endSequence - uint64(batchLen) + 1, nil
}

func advanceContiguousSequence(current, batchStart, batchEnd uint64) uint64 {
	if current >= batchEnd {
		return current
	}
	if current+1 < batchStart {
		return current
	}
	return batchEnd
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

func (m *Manager) hasActivePeer(peerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[peerID]
	return ok && peer.active != nil
}
