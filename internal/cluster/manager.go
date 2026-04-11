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

	mu     sync.Mutex
	closed bool
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
	payload, err := proto.Marshal(&internalproto.EventBatch{
		Events: []*internalproto.ReplicatedEvent{replicated},
	})
	if err != nil {
		log.Printf("warn: marshal event batch: %v", err)
		return
	}

	envelope := &internalproto.Envelope{
		NodeId:      m.cfg.NodeID,
		Sequence:    uint64(event.Sequence),
		SentAtHlc:   event.HLC.String(),
		MessageType: string(internalproto.MessageTypeEventBatch),
		Payload:     payload,
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

	helloPayload, err := proto.Marshal(&internalproto.Hello{
		NodeId:          m.cfg.NodeID,
		AdvertiseAddr:   m.cfg.AdvertiseAddr,
		ProtocolVersion: internalproto.ProtocolVersion,
		LastSequence:    uint64(lastSequence),
	})
	if err != nil {
		return nil, err
	}

	return &internalproto.Envelope{
		NodeId:      m.cfg.NodeID,
		MessageType: string(internalproto.MessageTypeHello),
		Payload:     helloPayload,
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

		messageType := internalproto.MessageType(envelope.MessageType)
		if sess.peerID == "" && messageType != internalproto.MessageTypeHello {
			return
		}

		switch messageType {
		case internalproto.MessageTypeHello:
			if err := m.handleHello(sess, &envelope); err != nil {
				return
			}
		case internalproto.MessageTypeAck:
			if err := m.handleAck(sess, &envelope); err != nil {
				return
			}
		case internalproto.MessageTypeEventBatch:
			if err := m.handleEventBatch(sess, &envelope); err != nil {
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

	var hello internalproto.Hello
	if err := proto.Unmarshal(envelope.Payload, &hello); err != nil {
		return err
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
	if !m.isConfiguredPeer(peerID) {
		return fmt.Errorf("peer %s is not configured", peerID)
	}

	if !m.activateSession(sess) {
		return errors.New("duplicate session rejected")
	}
	return nil
}

func (m *Manager) handleAck(sess *session, envelope *internalproto.Envelope) error {
	var ack internalproto.Ack
	if err := proto.Unmarshal(envelope.Payload, &ack); err != nil {
		return err
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

	m.mu.Lock()
	defer m.mu.Unlock()

	peer, ok := m.peers[sess.peerID]
	if !ok {
		return nil
	}
	if ack.AckedSequence > peer.lastAck {
		peer.lastAck = ack.AckedSequence
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

	var batch internalproto.EventBatch
	if err := proto.Unmarshal(envelope.Payload, &batch); err != nil {
		return err
	}
	if len(batch.Events) == 0 {
		return errors.New("event batch cannot be empty")
	}

	for _, event := range batch.Events {
		if err := m.store.ApplyReplicatedEvent(context.Background(), event); err != nil {
			return err
		}
	}

	ackPayload, err := proto.Marshal(&internalproto.Ack{
		NodeId:        m.cfg.NodeID,
		AckedSequence: envelope.Sequence,
	})
	if err != nil {
		return err
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId:      m.cfg.NodeID,
		MessageType: string(internalproto.MessageTypeAck),
		Payload:     ackPayload,
	})
	return nil
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
