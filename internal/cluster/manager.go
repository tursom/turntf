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

// 集群操作的常量和超时设置。
const (
	// WebSocketPath 是对外暴露的集群WebSocket路径。
	WebSocketPath = websocketPath
	websocketPath = "/internal/cluster/ws"

	// writeWait 是WebSocket写入的超时时间。
	writeWait = 10 * time.Second
	// pingInterval 是WebSocket心跳ping的发送间隔。
	pingInterval = 15 * time.Second
	// readTimeout 是WebSocket读取的超时时间。
	readTimeout = 45 * time.Second
	// outboundQueueSize 是每个会话的出站消息队列容量。
	outboundQueueSize = 128
	// managerPublishQueue 是Manager级别的事件发布队列容量。
	managerPublishQueue = 256
	// pullBatchSize 是每次PullEvents请求拉取的最大事件数。
	pullBatchSize = 128
	// maxBatchEvents 是复制批次中的最大事件数。
	maxBatchEvents = 32
	// maxBatchBytes 是复制批次的最大字节数（64KB）。
	maxBatchBytes = 64 << 10
	// maxBatchDelay 是复制批次的最大滞留时间。
	maxBatchDelay = 2 * time.Millisecond
	// timeSyncSampleCount 是每次时间同步收集的样本数。
	timeSyncSampleCount = 7
	// timeSyncInterval 是定期时间同步的执行间隔。
	timeSyncInterval = 30 * time.Second
	// timeSyncTimeout 是单次时间同步请求的超时时间。
	timeSyncTimeout = 8 * time.Second
	// queryLoggedInUsersTimeout 是查询已登录用户的超时时间。
	queryLoggedInUsersTimeout = 3 * time.Second
	// catchupRetryInterval 是数据追赶重试的间隔。
	catchupRetryInterval = time.Second
	// antiEntropyInterval 是反熵（快照摘要）的发送间隔。
	antiEntropyInterval = 60 * time.Second
	// snapshotDigestMinInterval 是两次快照摘要之间的最小间隔。
	snapshotDigestMinInterval = 250 * time.Millisecond
	// snapshotDigestSweepInterval 是快照摘要轮询间隔。
	snapshotDigestSweepInterval = 25 * time.Millisecond
	// snapshotDigestImmediateAfterRepair 控制在修复后是否立即发送摘要。
	snapshotDigestImmediateAfterRepair = true
	// membershipUpdateInterval 是成员资格更新广播的间隔。
	membershipUpdateInterval = 5 * time.Second
	// discoveryCandidateTTL 是发现的候选节点保留时间。
	discoveryCandidateTTL = 10 * time.Minute
	// maxDynamicDiscoveredPeers 是动态发现的最大对等节点数。
	maxDynamicDiscoveredPeers = 8
	// routeRetryInterval 是瞬态数据包重试间隔。
	routeRetryInterval = 200 * time.Millisecond
	// routeRetryTTL 是瞬态数据包的最大存活时间。
	routeRetryTTL = 3 * time.Second
	// defaultPacketTTLHops 是数据包的默认TTL跳数。
	defaultPacketTTLHops = 8
	// defaultLoggedInUsersQueryMaxHops 是查询已登录用户的最大跳数。
	defaultLoggedInUsersQueryMaxHops = 8
	// disconnectSuspicionSweepInterval 是断开连接怀疑的清理间隔。
	disconnectSuspicionSweepInterval = time.Second
)

// marshalOptions 使用确定性序列化，确保相同消息产生相同的二进制表示，
// 这对于HMAC签名验证至关重要。
var marshalOptions = proto.MarshalOptions{Deterministic: true}

// managerRuntimeEpochCounter 是Manager运行时纪元的全局计数器。
var managerRuntimeEpochCounter atomic.Uint64

// 常见错误定义。
var errSessionClosed = errors.New("session closed")
var errClockProtectionRejected = errors.New("clock protection rejected")

// Manager 是集群模块的核心编排器，管理所有对等节点连接、状态复制、
// 时钟同步和网状路由。每个节点只有一个Manager实例。
//
// Manager协调以下子系统：
//   - 传输层：通过TransportConn/Dialer/Listener接口抽象WebSocket、ZeroMQ和libp2p
//   - 会话管理：每个对等节点可以有一个活跃会话，包含消息收发、事件复制和时间同步状态
//   - 时钟保护：基于分布式时钟状态机的写入门控
//   - 对等发现：通过成员资格更新协议自动发现新节点
//   - 事件复制：将存储事件广播到所有已连接对等节点
//   - 网状路由：通过覆盖网络路由报文，支持多种流量类别
type Manager struct {
	cfg   Config
	store *store.Store
	clock *clock.Clock

	// mux 是HTTP路由复用器，用于注册WebSocket升级端点。
	mux *http.ServeMux
	// publishCh 接收本地产生的事件，等待广播到对等节点。
	publishCh chan store.Event
	// replicationBatches 按(peerID, originNodeID)分组缓存待发送事件。
	replicationBatches *replicationBatcher
	// ctx 是Manager的生命周期上下文。
	ctx context.Context
	// cancel 取消Manager的生命周期上下文。
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	startOnce sync.Once
	startErr  error
	closeOnce sync.Once

	// websocket 是WebSocket传输实现。
	websocket *webSocketTransport
	// dialers 按传输类型存储拨号器。
	dialers map[string]Dialer
	// libp2p 是libp2p传输实现（可选）。
	libp2p *libP2PTransport

	// zeroMQListenerRunning 标记ZeroMQ监听器是否正在运行。
	zeroMQListenerRunning bool

	// meshRuntime 是网格运行时绑定，提供覆盖网络路由。
	meshRuntime *MeshRuntimeBinding

	mu              sync.Mutex
	// peers 按节点ID索引的已连接对等节点状态。
	peers map[int64]*peerState
	// configuredPeers 是配置文件中静态指定的对等节点列表。
	configuredPeers []*configuredPeer
	// discoveredPeers 是通过发现协议动态发现的节点（按URL索引）。
	discoveredPeers map[string]*discoveredPeerState
	// dynamicPeers 是已提升为"已配置"状态的动态发现节点。
	dynamicPeers map[string]*configuredPeer
	// selfKnownURLs 是本节点在外部网络中的已知URL及其generation。
	selfKnownURLs map[string]uint64

	// 时钟保护相关字段。

	// lastTrustedClockSync 是最近一次可信时钟同步的时间。
	lastTrustedClockSync time.Time
	// clockState 是当前节点的全局时钟状态。
	clockState clockState
	// clockReason 是当前时钟状态的原因描述。
	clockReason string
	// clockStateTransitions 统计各状态转移的发生次数。
	clockStateTransitions map[clockStateTransitionKey]uint64
	// timeSyncer 是可选的自定义时间同步函数，替代默认实现。
	timeSyncer func(*session) (timeSyncSample, error)
	// transientHandler 处理投递到本节点的瞬态数据包。
	transientHandler func(store.TransientPacket) bool
	// loggedInUsersProvider 提供本节点的已登录用户列表。
	loggedInUsersProvider func(context.Context) ([]app.LoggedInUserSummary, error)
	// supportsMembership 表示本节点是否支持成员资格协议。
	supportsMembership bool
	// membershipGeneration 是成员资格更新的代数。
	membershipGeneration uint64
	// membershipUpdatesSent 是已发送的成员资格更新数量。
	membershipUpdatesSent uint64
	// membershipUpdatesRecv 是已接收的成员资格更新数量。
	membershipUpdatesRecv uint64
	// discoveryRejects 是发现结果被拒绝的次数。
	discoveryRejects  uint64
	discoveryPersistFailures uint64

	// retryQueue 是等待重试的瞬态数据包队列。
	retryQueue map[string]queuedPacket
	// nextConnectionID 是连接ID的自增计数器。
	nextConnectionID uint64
	// nextResolveSessionsQueryID 是解析会话查询ID的自增计数器。
	nextResolveSessionsQueryID uint64
	// pendingResolveSessions 是等待响应的解析会话查询。
	pendingResolveSessions map[uint64]chan resolveUserSessionsQueryResult
	// loggedInUsersByNode 按节点ID缓存各节点的已登录用户。
	loggedInUsersByNode map[int64][]app.LoggedInUserSummary
	// localOnlineSessions 是本节点的在线会话记录。
	localOnlineSessions map[store.UserKey]map[string]store.OnlineSession
	// onlinePresenceByUser 按用户键聚合的在线节点存在信息。
	onlinePresenceByUser map[store.UserKey]map[int64]store.OnlineNodePresence
	// localRuntimeEpoch 是本节点的运行时纪元。
	localRuntimeEpoch uint64
	// remoteRuntimeEpochs 记录远程节点的运行时纪元。
	remoteRuntimeEpochs map[int64]uint64
	// directAdjacencyCounts 记录到各对等节点的直接邻接计数。
	directAdjacencyCounts map[int64]int
	// onlinePresenceGeneration 是在线存在信息的代数。
	onlinePresenceGeneration uint64
	// onlinePresenceOrigins 记录在线存在信息的来源节点。
	onlinePresenceOrigins map[int64]uint64
	// onlinePresenceEpochs 记录在线存在信息的纪元。
	onlinePresenceEpochs map[int64]uint64
	// disconnectSuspicions 是待处理的断开连接怀疑。
	disconnectSuspicions map[disconnectSuspicionKey]disconnectSuspicionState
	// seenConnectivityRumors 是已见过的连接性传闻（用于去重）。
	seenConnectivityRumors map[connectivityRumorKey]time.Time

	// nextMeshPacketID 是网格数据包的ID自增计数器。
	nextMeshPacketID uint64
	// meshForwardedPackets 统计转发的网格数据包数。
	meshForwardedPackets map[string]uint64
	// meshForwardedBytes 统计转发的网格数据字节数。
	meshForwardedBytes map[string]uint64
	// meshRoutingNoPath 统计无路径的网格路由尝试。
	meshRoutingNoPath map[string]uint64
	// meshRoutingDecisionCost 累积路由决策成本。
	meshRoutingDecisionCost map[string]int64
	// meshBridgeForwards 统计网桥转发次数。
	meshBridgeForwards map[string]uint64
}

// disconnectSuspicionKey 唯一标识一个断开连接怀疑。
type disconnectSuspicionKey struct {
	targetNodeID int64
	runtimeEpoch uint64
}

// disconnectSuspicionState 记录断开连接怀疑的详细信息。
type disconnectSuspicionState struct {
	deadline   time.Time
	observedAt time.Time
	reason     string
	reporters  map[int64]uint64
}

// connectivityRumorKey 唯一标识一个连接性传闻。
type connectivityRumorKey struct {
	targetNodeID         int64
	targetRuntimeEpoch   uint64
	reporterNodeID       int64
	reporterRuntimeEpoch uint64
	observedAtMs         int64
}

// configuredPeer 表示一个已配置的对等节点（来自配置文件或动态发现）。
type configuredPeer struct {
	URL                        string
	zeroMQCurveServerPublicKey string
	libP2PPeerID               string
	nodeID                     int64
	dynamic                    bool
	source                     string
}

// discoveredPeerState 跟踪一个动态发现的节点状态。
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

// peerState 跟踪一个已连接对等节点的运行时状态。
type peerState struct {
	// active 是当前活跃的会话。
	active *session
	// lastAck 是最后确认的事件ID。
	lastAck uint64
	// trustedSession 是当前被信任的会话（用于时钟保护）。
	trustedSession *session
	libP2PPeerID   string
	// clockState 是对等节点的当前时钟状态。
	clockState clockState
	// clockOffsetMs 是计算出与该对等节点的时钟偏移（毫秒）。
	clockOffsetMs int64
	// clockUncertaintyMs 是时钟偏移的不确定性（毫秒）。
	clockUncertaintyMs int64
	// lastClockSync 是最近一次时钟同步的时间。
	lastClockSync time.Time
	// lastCredibleClockSync 是最近一次可信时钟同步的时间。
	lastCredibleClockSync time.Time
	// clockLastError 是最近一次时钟相关的错误。
	clockLastError string
	// clockSamples 是保留的时钟同步样本。
	clockSamples []timeSyncSample
	// clockFailures 是时钟同步失败的累计次数。
	clockFailures uint64
	// clockFailureStreak 是连续的时钟同步失败次数。
	clockFailureStreak int
	// clockSkewViolationStreak 是连续检测到时钟偏差的次数。
	clockSkewViolationStreak int
	// clockHealthyStreak 是连续健康样本的次数。
	clockHealthyStreak int
	// sessions 是该对等节点的所有已知会话（按连接ID索引）。
	sessions map[uint64]*session
	// joinedLogged 标记首次连接是否已经记录日志。
	joinedLogged bool

	// 快照同步统计。
	snapshotDigestsSent        uint64
	snapshotDigestsReceived    uint64
	snapshotChunksSent         uint64
	snapshotChunksReceived     uint64
	lastSnapshotDigestAt       time.Time
	lastSnapshotDigestQueuedAt time.Time
	lastSnapshotChunkAt        time.Time
	// snapshotDigestDirty 标记需要生成新的快照摘要。
	snapshotDigestDirty bool
	// snapshotDigestImmediate 标记是否需要立即发送快照摘要。
	snapshotDigestImmediate bool
}

// session 表示与单个对等节点的传输层连接及其协议状态。
// 每个会话封装了消息收发、事件复制、时间同步和快照同步的状态。
type session struct {
	manager  *Manager
	conn     TransportConn
	outbound bool

	configuredPeer *configuredPeer
	peerID         int64
	connectionID   uint64

	// send 是出站信封的发送通道。
	send chan *internalproto.Envelope

	mu sync.Mutex
	// closed 标记会话是否已关闭。
	closed bool
	// remoteOriginProgress 记录对端报告的各个原始节点的事件进度。
	remoteOriginProgress map[int64]uint64
	// pendingPulls 是等待响应的PullEvents请求。
	pendingPulls map[int64]pendingPullState
	// replicationReady 标记会话是否已准备好接收复制事件。
	replicationReady bool
	// bootstrapStarted 标记引导流程是否已开始。
	bootstrapStarted bool
	// syncLoopStarted 标记同步循环是否已启动。
	syncLoopStarted bool
	// remoteSnapshotVersion 是对端支持的快照版本。
	remoteSnapshotVersion string
	// remoteMessageWindowSize 是对端的消息窗口大小。
	remoteMessageWindowSize int
	libP2PPeerID            string
	// pendingSnapshotParts 是等待响应的快照分区请求。
	pendingSnapshotParts map[string]struct{}
	nextTimeSyncID       uint64
	nextPullRequestID    uint64
	// pendingTimeSync 是等待响应的时间同步请求。
	pendingTimeSync map[uint64]chan timeSyncResult
	// clockOffsetMs 是此会话计算出的时钟偏移。
	clockOffsetMs int64
	// supportsMembership 标记对端是否支持成员资格协议。
	supportsMembership bool
	// smoothedRTTMs 是指数平滑后的RTT值。
	smoothedRTTMs int64
	// jitterPenaltyMs 是RTT抖动惩罚值。
	jitterPenaltyMs int64
	lastRTTUpdate   time.Time
}

// queuedPacket 是等待重试的瞬态数据包。
type queuedPacket struct {
	packet      store.TransientPacket
	queuedAt    time.Time
	nextAttempt time.Time
	attempts    int
}

// pendingPullState 记录一个等待中的PullEvents请求。
type pendingPullState struct {
	RequestID    uint64
	AfterEventID uint64
}

// timeSyncResult 封装一个时间同步请求的结果。
type timeSyncResult struct {
	response     *internalproto.TimeSyncResponse
	receivedAtMs int64
	err          error
}

// resolveUserSessionsQueryResult 封装一个用户会话解析查询的结果。
type resolveUserSessionsQueryResult struct {
	response *internalproto.QueryResolveUserSessionsResponse
	err      error
}

// timeSyncSample 是单次时间同步往返的测量结果。
// 使用NTP风格的4时间戳协议：客户端发送 → 服务器接收 → 服务器发送 → 客户端接收。
type timeSyncSample struct {
	// offsetMs 是计算出的时钟偏移（毫秒），公式为 ((T2-T1)+(T3-T4))/2。
	offsetMs int64
	// rttMs 是往返时间（毫秒）。
	rttMs int64
	// uncertaintyMs 是时钟偏移的不确定性。
	uncertaintyMs int64
	// sampledAt 是样本采集的时间。
	sampledAt time.Time
	// credible 标记此样本是否可信（RTT在可接受范围内）。
	credible bool
}

// NewManager 根据配置和存储创建新的Manager实例。
// 初始化传输层、对等节点映射、内部通道和HTTP路由。
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

// Handler 返回集群的HTTP处理器，用于注册到外部HTTP服务器。
func (m *Manager) Handler() http.Handler {
	return m.mux
}

// AdvertisePath 返回集群对外通告的WebSocket路径。
func (m *Manager) AdvertisePath() string {
	return m.cfg.AdvertisePath
}

// SetTransientHandler 设置瞬态数据包的本地投递处理器。
func (m *Manager) SetTransientHandler(handler func(store.TransientPacket) bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transientHandler = handler
}

// SetLoggedInUsersProvider 设置已登录用户的数据提供者。
// 设置后立即广播在线状态。
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

// membershipSupported 返回本节点是否支持成员资格发现协议。
func (m *Manager) membershipSupported() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.supportsMembership
}

// RouteTransientPacket 通过集群路由一个瞬态数据包。
// 如果未设置TTL跳数，则使用默认值。
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

// AllowWrite 检查当前节点的时钟状态是否允许写入操作。
// 当节点处于unwritable或unsynced状态时返回错误。
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

// nextManagerRuntimeEpoch 使用CAS循环生成唯一且单调的运行时纪元。
// 初始值基于当前纳秒时间戳，保证跨重启唯一。
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

// hasWritableClockSyncLocked 检查节点时钟是否处于可写状态（trusted或observing）。
func (m *Manager) hasWritableClockSyncLocked() bool {
	state, _ := m.nodeClockStateLocked()
	return state == clockStateTrusted || state == clockStateObserving
}

// normalizedMessageWindowSize 规范化消息窗口大小，未设置时返回默认值。
func normalizedMessageWindowSize(size int) int {
	if size <= 0 {
		return store.DefaultMessageWindowSize
	}
	return size
}
