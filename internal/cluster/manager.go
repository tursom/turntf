package cluster

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

const (
	WebSocketPath                    = websocketPath
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

	zeroMQListenerRunning bool

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
	URL                        string
	zeroMQCurveServerPublicKey string
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
		configuredPeers = append(configuredPeers, &configuredPeer{
			URL:                        peer.URL,
			zeroMQCurveServerPublicKey: peer.ZeroMQCurveServerPublicKey,
			source:                     peerSourceStatic,
		})
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
	mgr.dialers[transportZeroMQ] = newZeroMQDialerWithConfig(cfg.ZeroMQ, mgr.zeroMQCurveServerKeyForPeer)
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

func normalizedMessageWindowSize(size int) int {
	if size <= 0 {
		return store.DefaultMessageWindowSize
	}
	return size
}
