package cluster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	internalproto "notifier/internal/proto"
	"notifier/internal/store"
)

const (
	websocketPath       = "/internal/cluster/ws"
	writeWait           = 10 * time.Second
	pingInterval        = 15 * time.Second
	readTimeout         = 45 * time.Second
	outboundQueueSize   = 128
	managerPublishQueue = 256
	pullBatchSize       = 128
)

type Manager struct {
	cfg      Config
	store    *store.Store
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
}

type peerState struct {
	cfg     Peer
	active  *session
	lastAck uint64
}

type session struct {
	manager  *Manager
	conn     *websocket.Conn
	outbound bool

	configuredPeerID string
	peerID           string

	send chan *internalproto.Envelope

	mu                 sync.Mutex
	closed             bool
	remoteLastSequence uint64
	catchupInFlight    bool
	pendingPullAfter   uint64
}

func NewManager(cfg Config, st *store.Store) (*Manager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	peers := make(map[string]*peerState, len(cfg.Peers))
	for _, peer := range cfg.Peers {
		peers[peer.NodeID] = &peerState{cfg: peer}
	}

	mgr := &Manager{
		cfg:   cfg,
		store: st,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		mux:       http.NewServeMux(),
		publishCh: make(chan store.Event, managerPublishQueue),
		dialer:    websocket.DefaultDialer,
		peers:     peers,
	}
	mgr.mux.HandleFunc("GET "+websocketPath, mgr.handleWebSocket)
	return mgr, nil
}

func (m *Manager) Handler() http.Handler {
	return m.mux
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
		log.Printf("warn: build hello envelope: %v", err)
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
				NodeId:          m.cfg.NodeID,
				AdvertiseAddr:   m.cfg.AdvertiseAddr,
				ProtocolVersion: internalproto.ProtocolVersion,
				LastSequence:    uint64(lastSequence),
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

			data, err := proto.Marshal(envelope)
			if err != nil {
				log.Printf("warn: marshal envelope: %v", err)
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

		if sess.peerID == "" && envelope.GetHello() == nil {
			return
		}

		switch envelope.Body.(type) {
		case *internalproto.Envelope_Hello:
			if err := m.handleHello(sess, &envelope); err != nil {
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
	if strings.TrimSpace(hello.AdvertiseAddr) == "" {
		return errors.New("hello advertise addr cannot be empty")
	}
	if sess.outbound && sess.configuredPeerID != "" && sess.configuredPeerID != peerID {
		return fmt.Errorf("peer mismatch: expected %s got %s", sess.configuredPeerID, peerID)
	}

	sess.peerID = peerID
	sess.noteRemoteLastSequence(hello.LastSequence)
	if !m.isConfiguredPeer(peerID) {
		return fmt.Errorf("peer %s is not configured", peerID)
	}

	if !m.activateSession(sess) {
		return errors.New("duplicate session rejected")
	}
	if m.store != nil {
		if err := m.requestCatchupIfNeeded(sess); err != nil {
			return err
		}
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
		if err := m.requestCatchupIfNeeded(sess); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) handlePullEvents(sess *session, envelope *internalproto.Envelope) error {
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

func (m *Manager) requestCatchupIfNeeded(sess *session) error {
	if m.store == nil || sess.peerID == "" {
		return nil
	}

	cursor, err := m.store.GetPeerCursor(context.Background(), sess.peerID)
	if err != nil {
		return err
	}

	remoteLastSequence := sess.remoteSequence()
	appliedSequence := uint64(cursor.AppliedSequence)
	if appliedSequence >= remoteLastSequence {
		return nil
	}
	if !sess.beginPendingPull(appliedSequence) {
		return nil
	}

	envelope, err := m.buildPullEventsEnvelope(appliedSequence)
	if err != nil {
		sess.cancelPendingPull(appliedSequence)
		return err
	}
	sess.enqueue(envelope)
	return nil
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
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return
	}
	if peer.active == sess {
		peer.active = nil
	}
}

func (s *session) enqueue(envelope *internalproto.Envelope) {
	defer func() {
		_ = recover()
	}()

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
	close(s.send)
	s.mu.Unlock()

	if s.conn != nil {
		_ = s.conn.Close()
	}
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

func (m *Manager) hasActivePeer(peerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[peerID]
	return ok && peer.active != nil
}
