package cluster

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	WebSocketPath                      = websocketPath
	websocketPath                      = "/internal/cluster/ws"
	writeWait                          = 10 * time.Second
	pingInterval                       = 15 * time.Second
	readTimeout                        = 45 * time.Second
	outboundQueueSize                  = 128
	managerPublishQueue                = 256
	pullBatchSize                      = 128
	maxBatchEvents                     = 32
	maxBatchBytes                      = 64 << 10
	maxBatchDelay                      = 2 * time.Millisecond
	timeSyncSampleCount                = 7
	timeSyncInterval                   = 30 * time.Second
	timeSyncTimeout                    = 8 * time.Second
	queryLoggedInUsersTimeout          = 3 * time.Second
	catchupRetryInterval               = time.Second
	antiEntropyInterval                = 60 * time.Second
	snapshotDigestMinInterval          = 250 * time.Millisecond
	snapshotDigestSweepInterval        = 25 * time.Millisecond
	snapshotDigestImmediateAfterRepair = true
	membershipUpdateInterval           = 5 * time.Second
	discoveryCandidateTTL              = 10 * time.Minute
	maxDynamicDiscoveredPeers          = 8
	routeRetryInterval                 = 200 * time.Millisecond
	routeRetryTTL                      = 3 * time.Second
	defaultPacketTTLHops               = 8
	defaultLoggedInUsersQueryMaxHops   = 8
	disconnectSuspicionSweepInterval   = time.Second
)

var marshalOptions = proto.MarshalOptions{Deterministic: true}
var managerRuntimeEpochCounter atomic.Uint64

var errSessionClosed = errors.New("session closed")
var errClockProtectionRejected = errors.New("clock protection rejected")

type Manager struct {
	cfg   Config
	store *store.Store
	clock *clock.Clock

	mux                *http.ServeMux
	publishCh          chan store.Event
	replicationBatches *replicationBatcher
	ctx                context.Context
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	startOnce          sync.Once
	startErr           error
	closeOnce          sync.Once
	websocket          *webSocketTransport
	dialers            map[string]Dialer
	libp2p             *libP2PTransport

	zeroMQListenerRunning bool

	meshRuntime *MeshRuntimeBinding

	mu              sync.Mutex
	peers           map[int64]*peerState
	configuredPeers []*configuredPeer
	discoveredPeers map[string]*discoveredPeerState
	dynamicPeers    map[string]*configuredPeer
	selfKnownURLs   map[string]uint64

	lastTrustedClockSync       time.Time
	clockState                 clockState
	clockReason                string
	clockStateTransitions      map[clockStateTransitionKey]uint64
	timeSyncer                 func(*session) (timeSyncSample, error)
	transientHandler           func(store.TransientPacket) bool
	loggedInUsersProvider      func(context.Context) ([]app.LoggedInUserSummary, error)
	supportsMembership         bool
	membershipGeneration       uint64
	membershipUpdatesSent      uint64
	membershipUpdatesRecv      uint64
	discoveryRejects           uint64
	discoveryPersistFailures   uint64
	retryQueue                 map[string]queuedPacket
	nextConnectionID           uint64
	nextResolveSessionsQueryID uint64
	pendingResolveSessions     map[uint64]chan resolveUserSessionsQueryResult
	loggedInUsersByNode        map[int64][]app.LoggedInUserSummary
	localOnlineSessions        map[store.UserKey]map[string]store.OnlineSession
	onlinePresenceByUser       map[store.UserKey]map[int64]store.OnlineNodePresence
	localRuntimeEpoch          uint64
	remoteRuntimeEpochs        map[int64]uint64
	directAdjacencyCounts      map[int64]int
	onlinePresenceGeneration   uint64
	onlinePresenceOrigins      map[int64]uint64
	onlinePresenceEpochs       map[int64]uint64
	disconnectSuspicions       map[disconnectSuspicionKey]disconnectSuspicionState
	seenConnectivityRumors     map[connectivityRumorKey]time.Time
	nextMeshPacketID           uint64
	meshForwardedPackets       map[string]uint64
	meshForwardedBytes         map[string]uint64
	meshRoutingNoPath          map[string]uint64
	meshRoutingDecisionCost    map[string]int64
	meshBridgeForwards         map[string]uint64
}

type disconnectSuspicionKey struct {
	targetNodeID int64
	runtimeEpoch uint64
}

type disconnectSuspicionState struct {
	deadline   time.Time
	observedAt time.Time
	reason     string
	reporters  map[int64]uint64
}

type connectivityRumorKey struct {
	targetNodeID         int64
	targetRuntimeEpoch   uint64
	reporterNodeID       int64
	reporterRuntimeEpoch uint64
	observedAtMs         int64
}

type configuredPeer struct {
	URL                        string
	zeroMQCurveServerPublicKey string
	libP2PPeerID               string
	nodeID                     int64
	dynamic                    bool
	source                     string
}

type discoveredPeerState struct {
	nodeID                     int64
	url                        string
	zeroMQCurveServerPublicKey string
	sourcePeerNodeID           int64
	state                      string
	firstSeenAt                time.Time
	lastSeenAt                 time.Time
	lastConnectedAt            time.Time
	lastError                  string
	generation                 uint64
	dialing                    bool
}

type peerState struct {
	active                   *session
	lastAck                  uint64
	trustedSession           *session
	libP2PPeerID             string
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
	joinedLogged             bool

	snapshotDigestsSent        uint64
	snapshotDigestsReceived    uint64
	snapshotChunksSent         uint64
	snapshotChunksReceived     uint64
	lastSnapshotDigestAt       time.Time
	lastSnapshotDigestQueuedAt time.Time
	lastSnapshotChunkAt        time.Time
	snapshotDigestDirty        bool
	snapshotDigestImmediate    bool
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
	libP2PPeerID            string
	pendingSnapshotParts    map[string]struct{}
	nextTimeSyncID          uint64
	nextPullRequestID       uint64
	pendingTimeSync         map[uint64]chan timeSyncResult
	clockOffsetMs           int64
	supportsMembership      bool
	smoothedRTTMs           int64
	jitterPenaltyMs         int64
	lastRTTUpdate           time.Time
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

type resolveUserSessionsQueryResult struct {
	response *internalproto.QueryResolveUserSessionsResponse
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
		configuredPeers = append(configuredPeers, &configuredPeer{
			URL:                        peer.URL,
			zeroMQCurveServerPublicKey: peer.ZeroMQCurveServerPublicKey,
			libP2PPeerID:               libP2PPeerIDFromAddr(peer.URL),
			source:                     peerSourceStatic,
		})
	}

	clockRef := clock.NewClock(cfg.NodeID)
	if st != nil && st.Clock() != nil {
		clockRef = st.Clock()
	}

	mgr := &Manager{
		cfg:                     cfg,
		store:                   st,
		clock:                   clockRef,
		websocket:               newWebSocketTransport(),
		dialers:                 make(map[string]Dialer, 2),
		mux:                     http.NewServeMux(),
		publishCh:               make(chan store.Event, managerPublishQueue),
		replicationBatches:      newReplicationBatcher(),
		peers:                   make(map[int64]*peerState, len(cfg.Peers)),
		configuredPeers:         configuredPeers,
		discoveredPeers:         make(map[string]*discoveredPeerState),
		dynamicPeers:            make(map[string]*configuredPeer),
		selfKnownURLs:           make(map[string]uint64),
		supportsMembership:      !cfg.DiscoveryDisabled,
		retryQueue:              make(map[string]queuedPacket),
		pendingResolveSessions:  make(map[uint64]chan resolveUserSessionsQueryResult),
		loggedInUsersByNode:     make(map[int64][]app.LoggedInUserSummary),
		localOnlineSessions:     make(map[store.UserKey]map[string]store.OnlineSession),
		onlinePresenceByUser:    make(map[store.UserKey]map[int64]store.OnlineNodePresence),
		localRuntimeEpoch:       nextManagerRuntimeEpoch(time.Now().UTC()),
		remoteRuntimeEpochs:     make(map[int64]uint64),
		directAdjacencyCounts:   make(map[int64]int),
		onlinePresenceOrigins:   make(map[int64]uint64),
		onlinePresenceEpochs:    make(map[int64]uint64),
		disconnectSuspicions:    make(map[disconnectSuspicionKey]disconnectSuspicionState),
		seenConnectivityRumors:  make(map[connectivityRumorKey]time.Time),
		clockStateTransitions:   make(map[clockStateTransitionKey]uint64),
		meshForwardedPackets:    make(map[string]uint64),
		meshForwardedBytes:      make(map[string]uint64),
		meshRoutingNoPath:       make(map[string]uint64),
		meshRoutingDecisionCost: make(map[string]int64),
		meshBridgeForwards:      make(map[string]uint64),
	}
	mgr.dialers[transportWebSocket] = mgr.websocket
	mgr.dialers[transportZeroMQ] = newZeroMQDialerWithConfig(cfg.ZeroMQ, mgr.zeroMQCurveServerKeyForPeer)
	if cfg.LibP2P.Enabled {
		mgr.libp2p = newLibP2PTransport(cfg.LibP2P, cfg.ClusterSecret, mgr)
		mgr.dialers[transportLibP2P] = mgr.libp2p
	}
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
	m.loggedInUsersProvider = provider
	ctx := m.ctx
	m.mu.Unlock()
	if ctx != nil && ctx.Err() == nil {
		m.broadcastOnlinePresence()
	}
}

func (m *Manager) membershipSupported() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.supportsMembership
}

func (m *Manager) RouteTransientPacket(ctx context.Context, packet store.TransientPacket) error {
	if m == nil {
		return nil
	}
	if packet.TTLHops <= 0 {
		packet.TTLHops = defaultPacketTTLHops
	}
	m.routeOrQueueTransientPacket(ctx, packet)
	return nil
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

func nextManagerRuntimeEpoch(now time.Time) uint64 {
	base := uint64(now.UnixNano())
	if base == 0 {
		base = 1
	}
	for {
		prev := managerRuntimeEpochCounter.Load()
		next := base
		if next <= prev {
			next = prev + 1
		}
		if managerRuntimeEpochCounter.CompareAndSwap(prev, next) {
			return next
		}
	}
}

func (m *Manager) hasWritableClockSyncLocked() bool {
	state, _ := m.nodeClockStateLocked()
	return state == clockStateTrusted || state == clockStateObserving
}

func normalizedMessageWindowSize(size int) int {
	if size <= 0 {
		return store.DefaultMessageWindowSize
	}
	return size
}
