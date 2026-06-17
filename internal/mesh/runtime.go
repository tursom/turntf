// Package mesh 实现 Mesh 覆盖网络运行时。
//
// Mesh 覆盖网络是一个去中心化的对等节点（Peer-to-Peer）通信层，基于
// TCP/WebSocket/libp2p 等多种传输协议，在物理网络之上构建一个逻辑上
// 全连通的虚拟网络。其核心设计围绕以下概念：
//
//   - 邻接关系（Adjacency）：与远程对等节点建立的经握手验证的直连连接，
//     是消息交换的基本单元。
//   - 消息信封（Envelope）：所有协议内通信均封装在 Envelope 中，由
//     传输适配器负责编解码和签名验证。
//   - 生成号（Generation）：每次本地拓扑变化时递增的单调计数器，用于
//     拓扑更新广播的版本控制与去重。
//   - 转发引擎（Engine）：根据全局拓扑快照和转发策略，决定数据包在
//     覆盖网络中的下一跳路径。
//   - 链路测量：每对邻接关系维护 EWMA RTT 和 Jitter 值，用于路由决策
//     和链路质量评估。
//
// 运行时（Runtime）是 mesh 包的核心入口，管理传输适配器的生命周期、
// 邻接关系的建立与拆除、拓扑信息的广播与同步，以及数据包的路由转发。
package mesh

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

var (
	// ErrRuntimeClosed 表示运行时已关闭，无法继续执行操作。
	ErrRuntimeClosed = errors.New("mesh: runtime closed")
	// ErrHelloRejected 表示远程节点的 Hello 握手消息被拒绝，
	// 原因可能包括协议版本不匹配、节点 ID 冲突或缺乏共同传输能力。
	ErrHelloRejected = errors.New("mesh: hello rejected")
	// ErrAdapterAlreadyBound 表示同一传输类型已有适配器注册。
	ErrAdapterAlreadyBound = errors.New("mesh: adapter transport already registered")
)

// envelopeMarshalOptions 是 Protobuf 序列化配置，启用确定性编码
// （相同 protobuf 消息每次编码结果一致），便于测试和签名验证。
var envelopeMarshalOptions = proto.MarshalOptions{Deterministic: true}

// EnvelopeCodec 接口定义了 ClusterEnvelope 消息的编解码器。
//
// 默认实现使用 Protobuf 序列化；测试可注入伪造编解码器以验证
// 网络行为而不依赖实际 Protobuf 编码。
type EnvelopeCodec interface {
	Encode(envelope *ClusterEnvelope) ([]byte, error)
	Decode(data []byte) (*ClusterEnvelope, error)
}

// EnvelopeSigner 包装出站 Envelope 的编码后载荷，允许在传输层之外
// 追加签名数据。默认的签名器为无操作（no-op）。
type EnvelopeSigner interface {
	Sign(envelope *ClusterEnvelope, encoded []byte) ([]byte, error)
}

// EnvelopeVerifier 在运行时处理入站 Envelope 之前对其进行验证。
// 如果返回错误，运行时将关闭对应的连接。可用于实现传输无关的
// 消息认证（如 HMAC 或 Ed25519 签名验证）。
type EnvelopeVerifier interface {
	Verify(envelope *ClusterEnvelope, raw []byte) error
}

// GenerationPersistence 让运行时能持久化记住最后发布的生成号，
// 以便重启后从之前的状态继续，避免拓扑版本回退。
type GenerationPersistence interface {
	Load() (uint64, error)
	Store(generation uint64) error
}

// LocalEnvelopeHandler 是处理发往本节点的 Envelope 的回调类型。
// 参数 packet 包含完整的转发元数据（源节点、目标节点、TTL 等），
// envelope 是解码后的业务消息。仅当目标节点为本地节点时被调用。
type LocalEnvelopeHandler func(ctx context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error

// LocalForwardedPacketHandler 是处理发往本节点的原始转发数据包的回调类型，
// 用于 TrafficClass 为 TransientInteractive（无需二次解码）的场景。
type LocalForwardedPacketHandler func(ctx context.Context, packet *ForwardedPacket) error

// TimeSyncObservation 记录一次完整的时间同步往返测量结果。
// 包含四个关键时间戳，可用于计算单向延迟和时钟偏移。
type TimeSyncObservation struct {
	// RemoteNodeID 远程对等节点的 ID
	RemoteNodeID int64
	// Transport 传输类型（TCP、WebSocket 等）
	Transport TransportKind
	// RemoteHint 远程节点的连接提示信息（如 NAT 类型、中继地址）
	RemoteHint string
	// RequestID 时间同步请求的标识符，用于匹配请求和响应
	RequestID uint64
	// ClientSendTimeMs 客户端发送请求时的时间戳（毫秒）
	ClientSendTimeMs int64
	// ServerReceiveTimeMs 服务端收到请求时的时间戳（毫秒）
	ServerReceiveTimeMs int64
	// ServerSendTimeMs 服务端发送响应时的时间戳（毫秒）
	ServerSendTimeMs int64
	// ClientReceiveTimeMs 客户端收到响应时的时间戳（毫秒）
	ClientReceiveTimeMs int64
	// RTTMs 往返时间（毫秒）
	RTTMs int64
	// JitterMs 抖动值（毫秒），即 RTT 偏差的 EWMA 估计
	JitterMs int64
}

// TimeSyncObserver 是时间同步观测结果的通知回调类型。
type TimeSyncObserver func(observation TimeSyncObservation)

// AdjacencyObservation 记录一次邻接关系状态变化的观测事件。
// 当邻接关系建立或断开时，运行时调用 AdjacencyObserver 通知外部。
type AdjacencyObservation struct {
	// RemoteNodeID 远程对等节点的 ID
	RemoteNodeID int64
	// Transport 该邻接关系使用的传输类型
	Transport TransportKind
	// RemoteHint 远程节点的连接提示信息
	RemoteHint string
	// Inbound 是否为入站连接（true=对方主动连接本节点）
	Inbound bool
	// Established 当前是否已建立（true=连接正常，false=已断开）
	Established bool
	// Hello 远程节点的 NodeHello 消息副本
	Hello *NodeHello
}

// AdjacencyObserver 是邻接状态变化通知的回调类型。
type AdjacencyObserver func(observation AdjacencyObservation)

// DialSeed 描述运行时在启动后应主动拨号连接的传输端点。
// 用于预先配置需要保持长连接的对等节点。
type DialSeed struct {
	// Transport 目标传输类型
	Transport TransportKind
	// Endpoint 目标端点地址（如 "192.168.1.1:8080" 或 "ws://host/path"）
	Endpoint string
}

// protoCodec 是基于 Protobuf 的默认 EnvelopeCodec 实现。
type protoCodec struct{}

func (protoCodec) Encode(envelope *ClusterEnvelope) ([]byte, error) {
	return envelopeMarshalOptions.Marshal(envelope)
}

func (protoCodec) Decode(data []byte) (*ClusterEnvelope, error) {
	envelope := &ClusterEnvelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

// noopSigner 是默认的无操作签名器，不对载荷做任何修改。
type noopSigner struct{}

func (noopSigner) Sign(_ *ClusterEnvelope, encoded []byte) ([]byte, error) {
	return encoded, nil
}

// noopVerifier 是默认的无操作验证器，接受所有入站消息。
type noopVerifier struct{}

func (noopVerifier) Verify(_ *ClusterEnvelope, _ []byte) error { return nil }

// ownedEnvelopeSender 是 TransportConn 的可选接口。如果连接实现了此接口，
// sendEnvelopeCtx 将直接调用 SendOwned（避免不必要的内存拷贝），
// 否则回退到标准 Send 方法。
type ownedEnvelopeSender interface {
	SendOwned(ctx context.Context, envelope []byte) error
}

// RuntimeOptions 配置 Runtime 的运行参数。只有 LocalNodeID 和 Adapters
// 为必填项；其余字段有合理的默认值。
type RuntimeOptions struct {
	// LocalNodeID 本节点在 Mesh 网络中的唯一标识符（必须为正整数）
	LocalNodeID int64
	// LocalRuntimeEpoch 本地运行时纪元，用于区分同一节点的不同运行实例。
	// 如果为零，运行时自动使用当前时间的纳秒值。
	LocalRuntimeEpoch uint64
	// Adapters 传输适配器列表，每个 TransportKind 只能有一个适配器。
	// 运行时通过适配器监听入站连接和发起出站连接。
	Adapters []TransportAdapter
	// LocalPolicy 本地转发策略，控制数据包在不同目的地和流量类型下的
	// 转发行为（如最大跳数、备选路径数等）。
	LocalPolicy *ForwardingPolicy
	// TopologyStore 拓扑数据的持久化存储接口。
	// 如果为 nil，使用内存存储（MemoryTopologyStore）。
	TopologyStore TopologyStore
	// DialSeeds 启动后应主动拨号连接的对等节点列表。
	DialSeeds []DialSeed
	// Codec Envelope 编解码器，默认使用 Protobuf。
	Codec EnvelopeCodec
	// Signer 出站 Envelope 签名器，默认无操作。
	Signer EnvelopeSigner
	// Verifier 入站 Envelope 验证器，默认接收所有消息。
	Verifier EnvelopeVerifier
	// GenerationPersistence 生成号持久化接口，用于跨重启保持生成号连续性。
	GenerationPersistence GenerationPersistence
	// TrafficClassifier 流量分类器，根据 Envelope 内容决定其流量类别。
	// 默认使用 DefaultTrafficClassifier。
	TrafficClassifier TrafficClassifier
	// EnvelopeHandler 处理发往本节点的业务消息的回调。
	EnvelopeHandler LocalEnvelopeHandler
	// QueryHandler 处理发往本节点的控制查询消息的回调。
	QueryHandler LocalEnvelopeHandler
	// ForwardedPacketHandler TransientInteractive 类型数据包的处理回调。
	ForwardedPacketHandler LocalForwardedPacketHandler
	// ForwardingObserver 转发事件的观察者回调（如丢包、路径变化等）。
	ForwardingObserver ForwardingObserver
	// TimeSyncObserver 时间同步测量结果的通知回调。
	TimeSyncObserver TimeSyncObserver
	// AdjacencyObserver 邻接关系状态变化的通知回调。
	AdjacencyObserver AdjacencyObserver
	// HelloTimeout 握手超时时间，默认 5 秒。
	HelloTimeout time.Duration
	// DialRetryInterval 拨号重试间隔，默认 1 秒。
	DialRetryInterval time.Duration
	// PingInterval 心跳/链路测量间隔，默认 2 秒。
	PingInterval time.Duration
	// TopologyPublishPeriod 拓扑公告定期发布周期间隔，默认 30 秒。
	TopologyPublishPeriod time.Duration
	// Now 时间获取函数，用于测试时注入固定时间。
	Now func() time.Time
}

// Runtime 是 Mesh 覆盖网络运行时的核心结构体。它管理传输适配器的生命周期、
// 邻接关系的建立与拆除、生成号的递增与广播、拓扑信息的存储与传播，
// 以及数据包的路由转发。
//
// 并发安全性：Runtime 内部的字段访问通过 sync.Mutex 互斥保护，
// 原子操作（atomic.Uint64）用于无锁递增计数器。
type Runtime struct {
	// ---- 静态配置 ----
	localNodeID       int64             // 本节点在 Mesh 网络中的唯一标识符
	localRuntimeEpoch uint64            // 本地运行时纪元，唯一标识本次运行实例
	policy            *ForwardingPolicy // 本地转发策略

	// ---- 组件依赖 ----
	store       TopologyStore         // 拓扑数据存储
	codec       EnvelopeCodec         // Envelope 编解码器
	signer      EnvelopeSigner        // 出站 Envelope 签名器
	verifier    EnvelopeVerifier      // 入站 Envelope 验证器
	persistence GenerationPersistence // 生成号持久化
	classifier  TrafficClassifier     // 流量分类器
	engine      *Engine               // 转发引擎，处理数据包路由
	planner     RoutePlanner          // 路线规划器，计算最优下一跳

	// ---- 外部回调 ----
	envelopeHandler        LocalEnvelopeHandler        // 业务消息处理回调
	queryHandler           LocalEnvelopeHandler        // 控制查询处理回调
	forwardedPacketHandler LocalForwardedPacketHandler // 透明数据包处理回调
	forwardingObserver     ForwardingObserver          // 转发事件观察者
	timeSyncObserver       TimeSyncObserver            // 时间同步结果观察者
	adjacencyObserver      AdjacencyObserver           // 邻接变化观察者

	// ---- 定时器/时间参数 ----
	helloTimeout          time.Duration    // 握手超时时间
	dialRetryInterval     time.Duration    // 拨号失败后的重试间隔
	pingInterval          time.Duration    // 心跳探测间隔
	topologyPublishPeriod time.Duration    // 拓扑公告定期发布周期
	now                   func() time.Time // 时间获取函数（可注入）

	// ---- 传输适配器 ----
	adapters     []TransportAdapter                 // 已注册的传输适配器列表
	adapterByKnd map[TransportKind]TransportAdapter // 按 TransportKind 索引的适配器映射

	// ---- 运行时状态 ----
	mu      sync.Mutex         // 保护所有可变状态的互斥锁
	started bool               // 是否已启动（保证 Start 最多被调用一次）
	closed  bool               // 是否已关闭
	ctx     context.Context    // 运行时的根 Context，在 Start 时创建，Close 时取消
	cancel  context.CancelFunc // 用于取消根 Context 的函数
	wg      sync.WaitGroup     // 等待所有后台 goroutine 退出的同步原语

	// ---- 邻接关系索引 ----
	adjByConn  map[TransportConn]*Adjacency                  // 按连接对象索引的邻接表
	adjByKey   map[adjacencyKey]map[TransportConn]*Adjacency // 按（节点ID+传输+提示）索引的邻接表
	adjByRoute map[routeAdjacencyKey][]*Adjacency            // 按（节点ID+传输）索引的路由候选列表

	// ---- 生成号与拓扑 ----
	generation        uint64                         // 本地当前生成号（每次拓扑变更递增）
	knownGeneration   map[int64]uint64               // 已知的远程节点生成号（用于去重和版本判断）
	seenFlood         map[floodKey]struct{}          // 已处理的洪水更新记录（防止循环转发）
	lastUpdate        map[int64]*TopologyUpdate      // 每个远程节点最后接收到的拓扑更新（用于对新邻接节点重放）
	pendingTombstones []*LinkAdvertisement           // 待发布的墓碑记录（断开链接通告）
	dialSeeds         map[dialSeedKey]*dialSeedEntry // 主动拨号种子（按传输+端点索引）

	// ---- 计数器 ----
	pingID   atomic.Uint64 // 心跳探测请求 ID 生成器（原子递增）
	packetID atomic.Uint64 // 转发数据包 ID 生成器（原子递增）
}

// adjacencyKey 是邻接关系在三元组（节点ID、传输类型、连接提示）下的唯一键。
// 同一节点可能通过不同传输类型或不同端点建立多条连接。
type adjacencyKey struct {
	nodeID    int64
	transport TransportKind
	hint      string
}

// routeAdjacencyKey 是路由索引使用的键，仅包含（节点ID、传输类型）。
// 与 adjacencyKey 的区别在于不含 hint，因此可以聚合同一节点同一传输下
// 的所有邻接关系作为路由候选。
type routeAdjacencyKey struct {
	nodeID    int64
	transport TransportKind
}

// floodKey 用于记录已处理的拓扑更新洪水广播，防止拓扑更新在网络中
// 无限循环转发。每个唯一的（来源节点ID + 生成号）对只处理一次。
type floodKey struct {
	origin     int64
	generation uint64
}

// dialSeedKey 是主动拨号种子的唯一键，由传输类型和端点地址组成。
type dialSeedKey struct {
	transport TransportKind
	endpoint  string
}

// dialSeedEntry 封装一个拨号种子及其生命周期控制状态。
type dialSeedEntry struct {
	seed   DialSeed           // 拨号种子信息
	cancel context.CancelFunc // 取消函数，用于停止该种子的重试循环
}

// Adjacency 表示与远程节点之间的一条经 NodeHello 握手验证的邻接关系。
// 调用者可以读取公开字段；互斥锁保护链路测量状态。
//
// 同一远程节点可能通过不同传输类型或不同连接路径与本节点建立多个
// Adjacency 实例，运行时会在路由决策中选择质量最优的一个。
type Adjacency struct {
	// RemoteNodeID 远程对等节点的 ID
	RemoteNodeID int64
	// Transport 该邻接关系使用的传输类型
	Transport TransportKind
	// RemoteHint 远程节点的连接提示（如 NAT 类型、中继路径）
	RemoteHint string
	// Hello 建立连接时远程节点发送的 NodeHello 消息副本
	Hello *NodeHello
	// Conn 底层传输连接对象
	Conn TransportConn
	// Inbound 是否为入站连接（true=对方主动连过来的，false=本节点主动拨号的）
	Inbound bool

	mu            sync.Mutex           // 保护链路测量状态的互斥锁
	rttEWMA       float64              // RTT 的指数加权移动平均值（毫秒）
	jitterEWMA    float64              // 抖动的指数加权移动平均值（毫秒）
	samples       int                  // 已采集的测量样本数
	established   bool                 // 连接是否已成功建立
	inflightPings map[uint64]time.Time // 正在途中的 Ping 请求（ID -> 发送时间）
}

// NewRuntime 根据配置选项构造一个 Runtime 实例。
// 它不启动任何 goroutine；调用者需显式调用 Start 方法启动。
//
// 参数验证包括：
//   - LocalNodeID 必须为正整数
//   - 至少需要注册一个传输适配器
//   - 同一 TransportKind 不能有多个适配器
//
// 未指定的可选字段会使用合理的默认值：
//   - Codec: Protobuf 编解码器
//   - Signer: 无操作
//   - Verifier: 无操作
//   - TopologyStore: 内存存储
//   - HelloTimeout: 5 秒
//   - PingInterval: 2 秒
//   - TopologyPublishPeriod: 30 秒
//   - 初始生成号: max(持久化值, 当前时间戳毫秒)
func NewRuntime(opts RuntimeOptions) (*Runtime, error) {
	if opts.LocalNodeID <= 0 {
		return nil, fmt.Errorf("mesh: local node id must be positive")
	}
	if len(opts.Adapters) == 0 {
		return nil, fmt.Errorf("mesh: at least one transport adapter is required")
	}
	adapterByKnd := make(map[TransportKind]TransportAdapter, len(opts.Adapters))
	for _, adapter := range opts.Adapters {
		if adapter == nil {
			continue
		}
		kind := adapter.Kind()
		if kind == TransportUnspecified {
			return nil, fmt.Errorf("mesh: adapter with unspecified transport kind")
		}
		if _, exists := adapterByKnd[kind]; exists {
			return nil, fmt.Errorf("%w: %v", ErrAdapterAlreadyBound, kind)
		}
		adapterByKnd[kind] = adapter
	}
	if len(adapterByKnd) == 0 {
		return nil, fmt.Errorf("mesh: no usable transport adapters")
	}
	codec := opts.Codec
	if codec == nil {
		codec = protoCodec{}
	}
	signer := opts.Signer
	if signer == nil {
		signer = noopSigner{}
	}
	verifier := opts.Verifier
	if verifier == nil {
		verifier = noopVerifier{}
	}
	classifier := opts.TrafficClassifier
	if classifier == nil {
		classifier = DefaultTrafficClassifier{}
	}
	store := opts.TopologyStore
	if store == nil {
		store = NewMemoryTopologyStore()
	}
	policy := NormalizeForwardingPolicy(ClonePolicy(opts.LocalPolicy))
	if policy == nil {
		policy = NormalizeForwardingPolicy(DefaultForwardingPolicy(1))
	}
	helloTimeout := opts.HelloTimeout
	if helloTimeout <= 0 {
		helloTimeout = 5 * time.Second
	}
	dialRetryInterval := opts.DialRetryInterval
	if dialRetryInterval <= 0 {
		dialRetryInterval = time.Second
	}
	pingInterval := opts.PingInterval
	if pingInterval <= 0 {
		pingInterval = 2 * time.Second
	}
	publishPeriod := opts.TopologyPublishPeriod
	if publishPeriod <= 0 {
		publishPeriod = 30 * time.Second
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	// 从持久化存储加载初始生成号，但确保不低于当前时间戳毫秒，
	// 以防止时钟回退导致生成号重复。
	var initialGeneration uint64
	if opts.GenerationPersistence != nil {
		if persisted, err := opts.GenerationPersistence.Load(); err == nil {
			initialGeneration = persisted
		}
	}
	if ms := uint64(now().UnixMilli()); ms > initialGeneration {
		initialGeneration = ms
	}

	// 初始化拨号种子列表，过滤掉传输适配器不支持或无效的种子。
	dialSeeds := make(map[dialSeedKey]*dialSeedEntry, len(opts.DialSeeds))
	for _, seed := range opts.DialSeeds {
		normalized, ok := normalizeDialSeed(seed)
		if !ok {
			continue
		}
		if adapterByKnd[normalized.Transport] == nil {
			continue
		}
		key := keyForDialSeed(normalized)
		dialSeeds[key] = &dialSeedEntry{seed: normalized}
	}

	runtime := &Runtime{
		localNodeID:            opts.LocalNodeID,
		localRuntimeEpoch:      opts.LocalRuntimeEpoch,
		policy:                 policy,
		store:                  store,
		codec:                  codec,
		signer:                 signer,
		verifier:               verifier,
		persistence:            opts.GenerationPersistence,
		classifier:             classifier,
		envelopeHandler:        opts.EnvelopeHandler,
		queryHandler:           opts.QueryHandler,
		forwardedPacketHandler: opts.ForwardedPacketHandler,
		forwardingObserver:     opts.ForwardingObserver,
		timeSyncObserver:       opts.TimeSyncObserver,
		adjacencyObserver:      opts.AdjacencyObserver,
		helloTimeout:           helloTimeout,
		dialRetryInterval:      dialRetryInterval,
		pingInterval:           pingInterval,
		topologyPublishPeriod:  publishPeriod,
		now:                    now,
		adapters:               opts.Adapters,
		adapterByKnd:           adapterByKnd,
		adjByConn:              make(map[TransportConn]*Adjacency),
		adjByKey:               make(map[adjacencyKey]map[TransportConn]*Adjacency),
		adjByRoute:             make(map[routeAdjacencyKey][]*Adjacency),
		knownGeneration:        make(map[int64]uint64),
		seenFlood:              make(map[floodKey]struct{}),
		lastUpdate:             make(map[int64]*TopologyUpdate),
		dialSeeds:              dialSeeds,
		generation:             initialGeneration,
	}
	if runtime.localRuntimeEpoch == 0 {
		runtime.localRuntimeEpoch = uint64(now().UnixNano())
		if runtime.localRuntimeEpoch == 0 {
			runtime.localRuntimeEpoch = 1
		}
	}
	runtime.planner = NewPlanner(opts.LocalNodeID)
	runtime.engine = NewEngine(opts.LocalNodeID, store.Snapshot, runtime.planner, runtime, runtime.handleLocalForwardedPacket, opts.ForwardingObserver)
	return runtime, nil
}

// LocalNodeID 返回运行时配置的本节点 ID。
func (r *Runtime) LocalNodeID() int64 { return r.localNodeID }

// LocalPolicy 返回本地转发策略的一份克隆副本，防止调用者修改原始策略。
func (r *Runtime) LocalPolicy() *ForwardingPolicy {
	return ClonePolicy(r.policy)
}

// CurrentGeneration 返回当前的本地生成号。
// 线程安全：通过互斥锁保护访问。
func (r *Runtime) CurrentGeneration() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generation
}

// AddDialSeed 调度一个主动拨号连接。
//
//   - 在 Start 之前调用：将种子记录在 dialSeeds 中，Start 时会自动启动拨号循环。
//   - 在 Start 之后调用：立即启动一个尽力而为的拨号循环。
//
// 如果种子对应的适配器不存在或种子无效，静默忽略。
func (r *Runtime) AddDialSeed(seed DialSeed) error {
	seed, ok := normalizeDialSeed(seed)
	if !ok {
		return nil
	}
	key := keyForDialSeed(seed)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	if r.adapterByKnd[seed.Transport] == nil {
		r.mu.Unlock()
		return fmt.Errorf("mesh: no adapter for transport %v", seed.Transport)
	}
	if entry := r.dialSeeds[key]; entry != nil {
		if !r.started || entry.cancel != nil {
			r.mu.Unlock()
			return nil
		}
		ctx := r.ctx
		if ctx == nil {
			r.mu.Unlock()
			return nil
		}
		runCtx, cancel := context.WithCancel(ctx)
		entry.cancel = cancel
		r.wg.Add(1)
		r.mu.Unlock()
		go r.dialSeedLoop(runCtx, seed)
		return nil
	}
	entry := &dialSeedEntry{seed: seed}
	r.dialSeeds[key] = entry
	if !r.started {
		r.mu.Unlock()
		return nil
	}
	ctx := r.ctx
	if ctx == nil {
		r.mu.Unlock()
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	entry.cancel = cancel
	r.wg.Add(1)
	r.mu.Unlock()
	go r.dialSeedLoop(runCtx, seed)
	return nil
}

// RemoveDialSeed 停止对指定种子的主动拨号。
// 已建立的邻接关系不会被关闭，仅停止创建新的出站连接的重试循环。
func (r *Runtime) RemoveDialSeed(seed DialSeed) error {
	seed, ok := normalizeDialSeed(seed)
	if !ok {
		return nil
	}
	key := keyForDialSeed(seed)
	r.mu.Lock()
	entry := r.dialSeeds[key]
	if entry == nil {
		r.mu.Unlock()
		return nil
	}
	delete(r.dialSeeds, key)
	cancel := entry.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// normalizeDialSeed 清理并验证拨号种子：去除端点地址的前后空白，
// 检查端点是否为空以及传输类型是否有效。
func normalizeDialSeed(seed DialSeed) (DialSeed, bool) {
	seed.Endpoint = strings.TrimSpace(seed.Endpoint)
	if seed.Endpoint == "" || seed.Transport == TransportUnspecified {
		return DialSeed{}, false
	}
	return seed, true
}

func keyForDialSeed(seed DialSeed) dialSeedKey {
	return dialSeedKey{transport: seed.Transport, endpoint: seed.Endpoint}
}

// RouteEnvelope 将一个内部 Mesh Envelope 包装为 ForwardedPacket 并通过
// 转发引擎进行路由。这是发送端生成消息的标准入口。
//
// 参数 targetNodeID 为目标节点在 Mesh 网络中的 ID。
// 返回 ErrRuntimeClosed 如果运行时已关闭，或 ErrNoRoute 如果无法到达目标。
func (r *Runtime) RouteEnvelope(ctx context.Context, targetNodeID int64, envelope *ClusterEnvelope) error {
	if r == nil {
		return ErrRuntimeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if targetNodeID <= 0 {
		return fmt.Errorf("mesh: target node id must be positive")
	}
	if envelope == nil {
		return fmt.Errorf("mesh: envelope cannot be nil")
	}
	trafficClass := r.classifier.Classify(envelope)
	if trafficClass == TrafficClassUnspecified {
		return fmt.Errorf("mesh: envelope traffic class is unspecified")
	}
	payload, err := r.codec.Encode(envelope)
	if err != nil {
		return err
	}
	return r.ForwardPacket(ctx, &ForwardedPacket{
		PacketId:     r.packetID.Add(1),
		SourceNodeId: r.localNodeID,
		TargetNodeId: targetNodeID,
		TrafficClass: trafficClass,
		TtlHops:      DefaultTTLHops,
		Payload:      payload,
	})
}

// ForwardPacket 将一个预构造的数据包送入转发引擎。
//
// 与 RouteEnvelope 的区别在于 ForwardPacket 接受已经包装好的
// ForwardedPacket（可手动设置 SourceNodeId、PacketId 等字段），
// 用于需要精细控制包元数据的场景。
func (r *Runtime) ForwardPacket(ctx context.Context, packet *ForwardedPacket) error {
	if r == nil || r.engine == nil {
		return ErrRuntimeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if packet == nil {
		return fmt.Errorf("mesh: forwarded packet cannot be nil")
	}
	next := cloneForwardedPacket(packet)
	if next.SourceNodeId == 0 {
		next.SourceNodeId = r.localNodeID
	}
	if next.PacketId == 0 {
		next.PacketId = r.packetID.Add(1)
	}
	return r.engine.Forward(ctx, next)
}

// DescribeRoute 根据当前拓扑快照计算到达指定目的地和流量类型的最佳路由。
// 返回路由决策和是否可达。可用于调试和外部监控。
func (r *Runtime) DescribeRoute(destinationNodeID int64, trafficClass TrafficClass) (RouteDecision, bool) {
	if r == nil || r.planner == nil {
		return RouteDecision{}, false
	}
	return r.planner.Compute(r.store.Snapshot(), destinationNodeID, trafficClass, TransportUnspecified)
}

// SendPacket 实现 PacketSender 接口，供 Engine 调用以发送数据包到下一跳。
//
// 它选择一个质量最优的邻接关系（按 RTT+Jitter 评分），将数据包封装为
// ClusterEnvelope 后通过该连接发送。如果找不到符合条件的邻接关系，
// 返回 ErrNoRoute。此方法在转发路径上，对每个中间节点都会被调用。
func (r *Runtime) SendPacket(ctx context.Context, nextHopNodeID int64, transport TransportKind, packet *ForwardedPacket) error {
	if r == nil {
		return ErrRuntimeClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	adj := r.bestAdjacency(nextHopNodeID, transport)
	if adj == nil {
		return ErrNoRoute
	}
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: packet}}
	return r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout)
}

// bestAdjacency 从路由索引中选出到达指定下一跳节点的最优邻接关系。
//
// 选择策略：在已建立的（established）邻接候选中，选择 RTT_EWMA + Jitter_EWMA
// 评分数值最小的一个。如果同一节点通过同一传输类型有多个连接，
// 返回综合质量最好的那个。
func (r *Runtime) bestAdjacency(nextHopNodeID int64, transport TransportKind) *Adjacency {
	if nextHopNodeID <= 0 || transport == TransportUnspecified {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	candidates := r.adjByRoute[routeAdjacencyKey{nodeID: nextHopNodeID, transport: transport}]
	var best *Adjacency
	var bestScore int64
	for _, adj := range candidates {
		if adj == nil {
			continue
		}
		adj.mu.Lock()
		established := adj.established
		score := int64(adj.rttEWMA + adj.jitterEWMA)
		adj.mu.Unlock()
		if !established {
			continue
		}
		if best == nil || score < bestScore {
			best = adj
			bestScore = score
		}
	}
	return best
}

// handleLocalForwardedPacket 是转发引擎传递到本节点数据包的处理入口。
// 根据流量类型决定处理方式：
//   - TransientInteractive：直接转发给 ForwardedPacketHandler（不解码 payload）。
//   - 其他类型：先解码 Envelope，然后优先调用 EnvelopeHandler；若为控制查询
//     且 EnvelopeHandler 未注册，则调用 QueryHandler。
func (r *Runtime) handleLocalForwardedPacket(ctx context.Context, packet *ForwardedPacket) error {
	if packet == nil {
		return nil
	}
	if packet.TrafficClass != TrafficTransientInteractive {
		envelope, err := r.codec.Decode(packet.Payload)
		if err != nil {
			return err
		}
		if r.envelopeHandler != nil {
			return r.envelopeHandler(ctx, packet, envelope)
		}
		if packet.TrafficClass == TrafficControlQuery && r.queryHandler != nil {
			return r.queryHandler(ctx, packet, envelope)
		}
		return nil
	}
	if r.forwardedPacketHandler != nil {
		return r.forwardedPacketHandler(ctx, packet)
	}
	return nil
}

// LocalCapabilities 收集当前所有已注册适配器公布的能力描述信息。
// 每次调用都会克隆一份能力副本，以确保调用者不会影响运行时内部状态。
func (r *Runtime) LocalCapabilities() []*TransportCapability {
	caps := make([]*TransportCapability, 0, len(r.adapters))
	for _, adapter := range r.adapters {
		if adapter == nil {
			continue
		}
		capability := adapter.LocalCapabilities()
		if capability == nil {
			continue
		}
		caps = append(caps, CloneCapability(capability))
	}
	return caps
}

// Start 启动运行时：初始化适配器、启动接受循环和拨号循环、启动心跳检测
// 和拓扑发布协程。该方法保证最多被成功调用一次。
//
// 启动顺序：
//  1. 启动所有传输适配器（adapter.Start）。
//  2. 发布初始拓扑公告，使本节点信息在邻接关系建立前即可见。
//  3. 为每个适配器启动入站连接接受循环（acceptLoop）。
//  4. 为每个拨号种子启动出站连接循环（dialSeedLoop）。
//  5. 启动周期性拓扑发布循环（topologyPublishLoop）。
func (r *Runtime) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	if r.started {
		r.mu.Unlock()
		return fmt.Errorf("mesh: runtime already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.ctx = runCtx
	r.cancel = cancel
	r.started = true
	adapters := append([]TransportAdapter(nil), r.adapters...)
	seedStarts := make([]struct {
		ctx  context.Context
		seed DialSeed
	}, 0, len(r.dialSeeds))
	for _, entry := range r.dialSeeds {
		if entry == nil || entry.cancel != nil {
			continue
		}
		seedCtx, seedCancel := context.WithCancel(runCtx)
		entry.cancel = seedCancel
		seedStarts = append(seedStarts, struct {
			ctx  context.Context
			seed DialSeed
		}{ctx: seedCtx, seed: entry.seed})
		r.wg.Add(1)
	}
	r.mu.Unlock()

	for _, adapter := range adapters {
		if adapter == nil {
			continue
		}
		if err := adapter.Start(runCtx); err != nil {
			cancel()
			return fmt.Errorf("mesh: adapter %v start: %w", adapter.Kind(), err)
		}
	}

	// 在邻接关系建立前发布初始拓扑，使本地节点状态、能力和策略
	// 在快照中立即可见。
	r.publishLocalTopology(runCtx)

	for _, adapter := range adapters {
		if adapter == nil {
			continue
		}
		r.wg.Add(1)
		go r.acceptLoop(runCtx, adapter)
	}
	for _, start := range seedStarts {
		start := start
		go r.dialSeedLoop(start.ctx, start.seed)
	}
	r.wg.Add(1)
	go r.topologyPublishLoop(runCtx)
	return nil
}

// Close 关闭运行时并等待所有后台 goroutine（接受循环、拨号循环、
// 心跳检测、拓扑发布）退出。
//
// 关闭过程：
//  1. 设置 closed 标志，阻止新操作。
//  2. 取消根 Context，通知所有 goroutine 退出。
//  3. 关闭所有已建立的传输连接。
//  4. 等待 WaitGroup 计数器归零。
func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	cancel := r.cancel
	conns := make([]TransportConn, 0, len(r.adjByConn))
	for conn := range r.adjByConn {
		conns = append(conns, conn)
	}
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	r.wg.Wait()
	return nil
}

// Adjacencies 返回当前所有已建立邻接关系的快照列表。
// 快照是只读副本，修改不会影响运行时内部状态。
func (r *Runtime) Adjacencies() []AdjacencySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]AdjacencySnapshot, 0, len(r.adjByConn))
	for _, adj := range r.adjByConn {
		out = append(out, adj.snapshot())
	}
	return out
}

// AdjacencySnapshot 是 Adjacency 状态的一份只读副本，
// 用于外部监控和调试，避免调用者直接访问运行时内部数据结构。
type AdjacencySnapshot struct {
	RemoteNodeID int64         // 远程节点 ID
	Transport    TransportKind // 传输类型
	RemoteHint   string        // 远程连接提示
	Inbound      bool          // 是否为入站连接
	RTTMs        int64         // 当前 RTT 估计值（毫秒）
	JitterMs     int64         // 当前抖动估计值（毫秒）
	Samples      int           // 已采集的测量样本数
	Established  bool          // 是否已建立
}

// snapshot 创建 Adjacency 状态的一份快照副本（带锁保护）。
func (a *Adjacency) snapshot() AdjacencySnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	return AdjacencySnapshot{
		RemoteNodeID: a.RemoteNodeID,
		Transport:    a.Transport,
		RemoteHint:   a.RemoteHint,
		Inbound:      a.Inbound,
		RTTMs:        int64(a.rttEWMA),
		JitterMs:     int64(a.jitterEWMA),
		Samples:      a.samples,
		Established:  a.established,
	}
}

// acceptLoop 从适配器的 Accept 通道接收入站连接。对每个接受的新连接，
// 启动一个独立的 handleConn goroutine 处理握手和后续通信。
func (r *Runtime) acceptLoop(ctx context.Context, adapter TransportAdapter) {
	defer r.wg.Done()
	accept := adapter.Accept()
	if accept == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case conn, ok := <-accept:
			if !ok {
				return
			}
			if conn == nil {
				continue
			}
			r.wg.Add(1)
			go r.handleConn(ctx, adapter.Kind(), conn, true)
		}
	}
}

// dialSeedLoop 周期性地尝试对指定种子进行出站拨号，直到上下文取消
// 或拨号成功。连接建立后由 runConn 管理生命周期；连接断开后继续重试。
// 重试间隔由 dialRetryInterval 配置。
func (r *Runtime) dialSeedLoop(ctx context.Context, seed DialSeed) {
	defer r.wg.Done()
	if strings.TrimSpace(seed.Endpoint) == "" {
		return
	}
	for {
		adapter := r.adapterForKind(seed.Transport)
		if adapter == nil {
			return
		}
		conn, err := adapter.Dial(ctx, seed.Endpoint)
		if err == nil && conn != nil {
			r.runConn(ctx, adapter.Kind(), conn, false)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.dialRetryInterval):
		}
	}
}

// adapterForKind 按传输类型查找已注册的适配器。
// 线程安全：通过互斥锁保护访问。
func (r *Runtime) adapterForKind(kind TransportKind) TransportAdapter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.adapterByKnd[kind]
}

// handleConn 是 acceptLoop 为每个入站连接启动的包装函数。
// 它调用 runConn 执行完整的握手和读取循环，完成后递减 WaitGroup 计数器。
func (r *Runtime) handleConn(ctx context.Context, kind TransportKind, conn TransportConn, inbound bool) {
	defer r.wg.Done()
	r.runConn(ctx, kind, conn, inbound)
}

// runConn 管理单条传输连接的生命周期，包含完整的握手流程：
//
//  1. 发送本地的 NodeHello 消息。
//  2. 读取并验证远程节点的 NodeHello 消息。
//  3. 注册邻接关系（registerAdjacency）。
//  4. 将远程节点信息写入拓扑存储。
//  5. 触发邻接观察者回调。
//  6. 递增生成号并广播拓扑更新。
//  7. 向新邻接节点重放缓存的拓扑信息。
//  8. 启动链路测量循环（linkMeasurementLoop）。
//  9. 进入读取循环（readLoop），处理该连接上的所有入站消息。
//
// 当读取循环退出（连接断开或上下文取消）时，执行清理：
//   - 调用 onAdjacencyLost 处理邻接丢失。
//   - 连接由 deferred conn.Close() 关闭。
func (r *Runtime) runConn(ctx context.Context, kind TransportKind, conn TransportConn, inbound bool) {
	defer func() {
		_ = conn.Close()
	}()

	if err := r.sendHello(ctx, conn, kind); err != nil {
		return
	}
	envelope, hello, raw, err := r.readHello(ctx, conn, kind)
	if err != nil {
		return
	}
	if err := r.verifier.Verify(envelope, raw); err != nil {
		return
	}
	if err := r.validateRemoteHello(hello, kind); err != nil {
		return
	}
	adj := r.registerAdjacency(conn, kind, hello, inbound)
	if adj == nil {
		return
	}
	defer r.onAdjacencyLost(adj)

	r.store.ApplyHello(adj.RemoteNodeID, adj.Hello)
	r.emitAdjacencyObservation(adj, true)
	r.bumpGenerationAndPublish(ctx)
	r.replayCachedTopology(ctx, adj)

	// 为此邻接关系启动链路测量 goroutine。
	r.wg.Add(1)
	go r.linkMeasurementLoop(ctx, adj)

	r.readLoop(ctx, adj)
}

// sendHello 构造本地的 NodeHello 消息并通过指定连接发送。
// 使用 helloTimeout 控制发送超时。
func (r *Runtime) sendHello(ctx context.Context, conn TransportConn, kind TransportKind) error {
	hello := r.localHello(kind)
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_NodeHello{NodeHello: hello}}
	if err := r.sendEnvelopeCtx(ctx, conn, envelope, r.helloTimeout); err != nil {
		return err
	}
	return nil
}

// sendEnvelopeCtx 是发送 Envelope 的核心方法。它负责：
//  1. 使用 Codec 将 Envelope 编码为二进制。
//  2. 使用 Signer 对编码后的载荷进行签名。
//  3. 通过 TransportConn 发送结果数据。
//
// 如果连接实现了 ownedEnvelopeSender 接口，会调用 SendOwned
// 以避免不必要的内存拷贝。
func (r *Runtime) sendEnvelopeCtx(ctx context.Context, conn TransportConn, envelope *ClusterEnvelope, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	encoded, err := r.codec.Encode(envelope)
	if err != nil {
		return err
	}
	signed, err := r.signer.Sign(envelope, encoded)
	if err != nil {
		return err
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if sender, ok := conn.(ownedEnvelopeSender); ok {
		return sender.SendOwned(sendCtx, signed)
	}
	return conn.Send(sendCtx, signed)
}

// readHello 从连接中读取一条消息，解码为 ClusterEnvelope，
// 并验证其是否为有效的 NodeHello 类型。
// 读取操作受 helloTimeout 超时限制。
func (r *Runtime) readHello(ctx context.Context, conn TransportConn, kind TransportKind) (*ClusterEnvelope, *NodeHello, []byte, error) {
	recvCtx, cancel := context.WithTimeout(ctx, r.helloTimeout)
	defer cancel()
	raw, err := conn.Receive(recvCtx)
	if err != nil {
		return nil, nil, nil, err
	}
	envelope, err := r.codec.Decode(raw)
	if err != nil {
		return nil, nil, nil, err
	}
	body, ok := envelope.Body.(*ClusterEnvelope_NodeHello)
	if !ok || body.NodeHello == nil {
		return nil, nil, nil, ErrHelloRejected
	}
	_ = kind
	return envelope, body.NodeHello, raw, nil
}

// localHello 构造本节点的 NodeHello 握手消息。
//
// 消息中包含：
//   - 本节点 ID 和协议版本。
//   - 传输能力列表：优先放置当前连接的传输类型对应的能力，再放置其他。
//   - 克隆的转发策略副本。
//   - 本地运行时纪元，用于检测节点重启。
func (r *Runtime) localHello(kind TransportKind) *NodeHello {
	caps := r.LocalCapabilities()
	ordered := make([]*TransportCapability, 0, len(caps))
	if self := r.adapterForKind(kind); self != nil {
		if capability := self.LocalCapabilities(); capability != nil {
			ordered = append(ordered, CloneCapability(capability))
		}
	}
	for _, capability := range caps {
		if capability == nil || capability.Transport == kind {
			continue
		}
		ordered = append(ordered, capability)
	}
	return &NodeHello{
		NodeId:           r.localNodeID,
		ProtocolVersion:  ProtocolVersion,
		Transports:       ordered,
		ForwardingPolicy: ClonePolicy(r.policy),
		RuntimeEpoch:     r.localRuntimeEpoch,
	}
}

// validateRemoteHello 验证远程节点的 Hello 握手消息是否有效。
// 验证条件：
//  1. Hello 消息不为空。
//  2. 远程节点 ID 为正整数且不等于本节点 ID（禁止自连接）。
//  3. 协议版本匹配。
//  4. 远程节点公布了当前连接所使用的传输能力。
//  5. 远程节点的转发策略可以成功归一化。
func (r *Runtime) validateRemoteHello(hello *NodeHello, kind TransportKind) error {
	if hello == nil {
		return ErrHelloRejected
	}
	if hello.NodeId <= 0 || hello.NodeId == r.localNodeID {
		return ErrHelloRejected
	}
	if hello.ProtocolVersion != ProtocolVersion {
		return ErrHelloRejected
	}
	if !helloAdvertisesTransport(hello, kind) {
		return ErrHelloRejected
	}
	if NormalizeForwardingPolicy(ClonePolicy(hello.ForwardingPolicy)) == nil {
		return ErrHelloRejected
	}
	return nil
}

// helloAdvertisesTransport 检查远程节点的 Hello 消息中是否包含
// 指定传输类型的能力声明。
func helloAdvertisesTransport(hello *NodeHello, kind TransportKind) bool {
	if kind == TransportUnspecified {
		return false
	}
	for _, capability := range hello.Transports {
		if capability != nil && capability.Transport == kind {
			return true
		}
	}
	return false
}

// registerAdjacency 在运行时中注册一条新的邻接关系。
//
// 它会将邻接关系添加到三个索引中：
//   - adjByConn: 按连接对象索引，用于快速查找和关闭时的清理。
//   - adjByKey: 按（节点ID+传输+hint）索引，用于同一对等节点的多连接管理。
//   - adjByRoute: 按（节点ID+传输）索引，用于路由选择。
//
// 如果运行时已关闭，返回 nil 且不注册。
func (r *Runtime) registerAdjacency(conn TransportConn, kind TransportKind, hello *NodeHello, inbound bool) *Adjacency {
	hint := ""
	if conn != nil {
		hint = strings.TrimSpace(conn.RemoteNodeHint())
	}
	adj := &Adjacency{
		RemoteNodeID:  hello.NodeId,
		Transport:     kind,
		RemoteHint:    hint,
		Hello:         hello,
		Conn:          conn,
		Inbound:       inbound,
		established:   true,
		inflightPings: make(map[uint64]time.Time),
	}
	key := adjacencyKey{nodeID: hello.NodeId, transport: kind, hint: hint}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.adjByConn[conn] = adj
	conns := r.adjByKey[key]
	if conns == nil {
		conns = make(map[TransportConn]*Adjacency)
		r.adjByKey[key] = conns
	}
	conns[conn] = adj
	routeKey := routeAdjacencyKey{nodeID: hello.NodeId, transport: kind}
	r.adjByRoute[routeKey] = append(r.adjByRoute[routeKey], adj)
	return adj
}

// onAdjacencyLost 处理邻接关系丢失的清理工作。
//
// 清理步骤：
//  1. 从所有邻接索引中移除该连接。
//  2. 为该链路生成一条墓碑记录（tombstone），标记为 established=false，
//     在下一次拓扑发布中通告相邻节点该连接已断开。
//  3. 标记邻接关系为未建立（established=false）。
//  4. 触发邻接观察者回调。
//  5. 连续两次 bumpGenerationAndPublish：
//     第一次发布携带墓碑记录，第二次发布彻底移除该链路。
func (r *Runtime) onAdjacencyLost(adj *Adjacency) {
	r.mu.Lock()
	if a, ok := r.adjByConn[adj.Conn]; ok && a == adj {
		delete(r.adjByConn, adj.Conn)
		key := adjacencyKey{nodeID: adj.RemoteNodeID, transport: adj.Transport, hint: adj.RemoteHint}
		if conns := r.adjByKey[key]; conns != nil {
			delete(conns, adj.Conn)
			if len(conns) == 0 {
				delete(r.adjByKey, key)
			}
		}
		r.removeAdjacencyFromRouteIndexLocked(adj)
	}
	// 为断开的链接生成墓碑记录，使下一次拓扑公告在链路完全消失前
	// 先通告一次断连事件。
	tombstone := &LinkAdvertisement{
		FromNodeId:  r.localNodeID,
		ToNodeId:    adj.RemoteNodeID,
		Transport:   adj.Transport,
		PathClass:   classifyPathClass(adj),
		Established: false,
	}
	r.pendingTombstones = append(r.pendingTombstones, tombstone)
	ctx := r.ctx
	r.mu.Unlock()
	adj.mu.Lock()
	adj.established = false
	adj.mu.Unlock()
	if ctx == nil {
		return
	}
	r.emitAdjacencyObservation(adj, false)
	// 第一次发布携带墓碑记录；第二次发布彻底移除该链路。
	r.bumpGenerationAndPublish(ctx)
	r.bumpGenerationAndPublish(ctx)
}

// emitAdjacencyObservation 通过 AdjacencyObserver 回调触发邻接状态
// 变化通知（建立或断开）。如果观察者未注册则静默跳过。
func (r *Runtime) emitAdjacencyObservation(adj *Adjacency, established bool) {
	if r == nil || r.adjacencyObserver == nil || adj == nil {
		return
	}
	r.adjacencyObserver(AdjacencyObservation{
		RemoteNodeID: adj.RemoteNodeID,
		Transport:    adj.Transport,
		RemoteHint:   adj.RemoteHint,
		Inbound:      adj.Inbound,
		Established:  established,
		Hello:        cloneNodeHello(adj.Hello),
	})
}

// cloneNodeHello 深度克隆一个 NodeHello 消息。
// 使用 protobuf 克隆确保所有嵌套字段都被正确复制。
func cloneNodeHello(hello *NodeHello) *NodeHello {
	if hello == nil {
		return nil
	}
	cloned, ok := proto.Clone(hello).(*NodeHello)
	if !ok {
		return nil
	}
	return cloned
}

// readLoop 从连接中持续读取入站消息，解码并验证后分发到
// dispatchEnvelope 处理。当连接断开或上下文取消时退出。
func (r *Runtime) readLoop(ctx context.Context, adj *Adjacency) {
	for {
		data, err := adj.Conn.Receive(ctx)
		if err != nil {
			return
		}
		envelope, err := r.codec.Decode(data)
		if err != nil {
			return
		}
		if err := r.verifier.Verify(envelope, data); err != nil {
			return
		}
		r.dispatchEnvelope(ctx, adj, envelope)
	}
}

// removeAdjacencyFromRouteIndexLocked 从 adjByRoute 路由索引中移除指定
// 的邻接关系。如果移除后某（节点ID+传输）键下没有候选连接了，删除该键。
// 调用方需持有 r.mu 锁。
func (r *Runtime) removeAdjacencyFromRouteIndexLocked(adj *Adjacency) {
	if r == nil || adj == nil {
		return
	}
	key := routeAdjacencyKey{nodeID: adj.RemoteNodeID, transport: adj.Transport}
	candidates := r.adjByRoute[key]
	if len(candidates) == 0 {
		return
	}
	filtered := candidates[:0]
	for _, candidate := range candidates {
		if candidate != nil && candidate != adj {
			filtered = append(filtered, candidate)
		}
	}
	if len(filtered) == 0 {
		delete(r.adjByRoute, key)
		return
	}
	r.adjByRoute[key] = filtered
}

// dispatchEnvelope 根据 Envelope 的 Body 类型将入站消息分派到
// 相应的处理函数：
//   - TimeSyncRequest: 时间同步请求处理
//   - TimeSyncResponse: 时间同步响应处理（更新链路 RTT/Jitter 测量值）
//   - TopologyUpdate: 拓扑更新处理（洪水广播扩散）
//   - ForwardedPacket: 转发数据包处理（送入转发引擎 HandleInbound）
func (r *Runtime) dispatchEnvelope(ctx context.Context, adj *Adjacency, envelope *ClusterEnvelope) {
	switch body := envelope.Body.(type) {
	case *ClusterEnvelope_TimeSyncRequest:
		if body.TimeSyncRequest == nil {
			return
		}
		r.handleTimeSyncRequest(ctx, adj, body.TimeSyncRequest)
	case *ClusterEnvelope_TimeSyncResponse:
		if body.TimeSyncResponse == nil {
			return
		}
		r.handleTimeSyncResponse(adj, body.TimeSyncResponse)
	case *ClusterEnvelope_TopologyUpdate:
		if body.TopologyUpdate == nil {
			return
		}
		r.handleTopologyUpdate(ctx, adj, body.TopologyUpdate)
	case *ClusterEnvelope_ForwardedPacket:
		if body.ForwardedPacket == nil || r.engine == nil {
			return
		}
		body.ForwardedPacket.IngressTransport = adj.Transport
		_ = r.engine.HandleInbound(ctx, body.ForwardedPacket)
	default:
		// 其他 Envelope 类型由后续阶段的处理逻辑覆盖。
	}
}

// ---------------- 生成号管理 + 拓扑洪水广播 ----------------

// bumpGenerationAndPublish 递增本地生成号并发布拓扑更新。
//
// 生成号递增策略：
//  1. 当前值加 1。
//  2. 如果当前时间戳（毫秒）大于新值，则使用时间戳。
//     这确保了即使不同节点的生成号起始值不同，单调性仍然保持。
//
// 如果配置了 GenerationPersistence，会将新生成号持久化。
func (r *Runtime) bumpGenerationAndPublish(ctx context.Context) {
	r.mu.Lock()
	r.generation++
	if ms := uint64(r.now().UnixMilli()); ms > r.generation {
		r.generation = ms
	}
	gen := r.generation
	persistence := r.persistence
	r.mu.Unlock()
	if persistence != nil {
		_ = persistence.Store(gen)
	}
	r.publishLocalTopology(ctx)
}

// topologyPublishLoop 按 topologyPublishPeriod 周期定期发布本地拓扑更新。
// 这确保了即使没有拓扑变化事件，对等节点也能定期收到本节点仍在线的信号。
func (r *Runtime) topologyPublishLoop(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.topologyPublishPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.publishLocalTopology(ctx)
		}
	}
}

// publishLocalTopology 构建并广播本地的拓扑更新消息。
//
// 流程：
//  1. 构建本地拓扑更新（包括本节点链路、能力、策略和待处理的墓碑记录）。
//  2. 先将更新应用到本地拓扑存储，使快照立即反映最新状态。
//  3. 记录该生成号到 knownGeneration 和 seenFlood，避免后续对本节点自己的
//     更新进行循环处理。
//  4. 向所有当前已建立的邻接连接发送该拓扑更新 Envelope。
func (r *Runtime) publishLocalTopology(ctx context.Context) {
	update := NormalizeTopologyUpdate(r.buildLocalTopologyUpdate())
	if update == nil {
		return
	}
	// 先本地应用，使 Snapshot 能即时反映最新的生成号。
	r.store.ApplyTopologyUpdate(update)

	r.mu.Lock()
	if old := r.knownGeneration[update.OriginNodeId]; old != 0 && old != update.Generation {
		delete(r.seenFlood, floodKey{origin: update.OriginNodeId, generation: old})
	}
	r.knownGeneration[update.OriginNodeId] = update.Generation
	r.seenFlood[floodKey{origin: update.OriginNodeId, generation: update.Generation}] = struct{}{}
	r.lastUpdate[update.OriginNodeId] = update
	targets := make([]TransportConn, 0, len(r.adjByConn))
	for conn := range r.adjByConn {
		targets = append(targets, conn)
	}
	r.mu.Unlock()

	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_TopologyUpdate{TopologyUpdate: update}}
	for _, conn := range targets {
		_ = r.sendEnvelopeCtx(ctx, conn, envelope, r.helloTimeout)
	}
}

// buildLocalTopologyUpdate 构建本地拓扑更新消息。
//
// 消息中包含：
//   - 本节点 ID 和当前生成号。
//   - 所有当前已建立连接对应的 LinkAdvertisement。
//   - 所有待处理的墓碑记录（标记为 established=false），确保对等节点
//     至少收到一次断链通告。
//   - 克隆的转发策略和传输能力列表。
//
// 收集完成后清空 pendingTombstones。
func (r *Runtime) buildLocalTopologyUpdate() *TopologyUpdate {
	r.mu.Lock()
	gen := r.generation
	caps := r.LocalCapabilities()
	links := make([]*LinkAdvertisement, 0, len(r.adjByConn)+len(r.pendingTombstones))
	for _, adj := range r.adjByConn {
		links = append(links, r.buildLinkAdvertisementLocked(adj, true))
	}
	// 包含缓存的墓碑记录，确保远程节点至少在 established=false 状态下
	// 看到一次链路断开，然后才会完全从拓扑中移除。
	for _, tomb := range r.pendingTombstones {
		links = append(links, &LinkAdvertisement{
			FromNodeId:  tomb.FromNodeId,
			ToNodeId:    tomb.ToNodeId,
			Transport:   tomb.Transport,
			PathClass:   tomb.PathClass,
			Established: false,
		})
	}
	r.pendingTombstones = nil
	r.mu.Unlock()
	return &TopologyUpdate{
		OriginNodeId:     r.localNodeID,
		Generation:       gen,
		Links:            links,
		ForwardingPolicy: ClonePolicy(r.policy),
		Transports:       caps,
	}
}

// buildLinkAdvertisementLocked 为指定的邻接关系构建链路公告。
// 使用当前的 RTT 和 Jitter 值作为链路成本指标。
func (r *Runtime) buildLinkAdvertisementLocked(adj *Adjacency, established bool) *LinkAdvertisement {
	adj.mu.Lock()
	cost := int64(adj.rttEWMA)
	jitter := int64(adj.jitterEWMA)
	adj.mu.Unlock()
	return &LinkAdvertisement{
		FromNodeId:  r.localNodeID,
		ToNodeId:    adj.RemoteNodeID,
		Transport:   adj.Transport,
		PathClass:   classifyPathClass(adj),
		CostMs:      uint32(clampNonNegative(cost)),
		JitterMs:    uint32(clampNonNegative(jitter)),
		Established: established,
	}
}

// clampNonNegative 将 int64 值裁剪到 uint32 非负范围 [0, MaxUint32]。
// 用于将链路成本指标安全地转换为 protobuf 中的 uint32 字段。
func clampNonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	if v > int64(^uint32(0)) {
		return int64(^uint32(0))
	}
	return v
}

// classifyPathClass 根据传输类型和远程提示判断链路的路径分类。
//   - WebSocket 和 ZeroMQ 始终归类为 PathClassDirect。
//   - libp2p 若远程提示表明是中继路径则归为 PathClassNativeRelay，
//     否则归为 PathClassDirect。
func classifyPathClass(adj *Adjacency) PathClass {
	switch adj.Transport {
	case TransportWebSocket, TransportZeroMQ:
		return PathClassDirect
	case TransportLibP2P:
		if remoteHintSuggestsRelay(adj.RemoteHint) {
			return PathClassNativeRelay
		}
		return PathClassDirect
	default:
		return PathClassUnspecified
	}
}

// remoteHintSuggestsRelay 根据远程提示信息判断是否通过中继连接。
// 如果提示中包含 "/p2p-circuit" 或 "relay" 字样，则认为使用了中继。
func remoteHintSuggestsRelay(hint string) bool {
	if hint == "" {
		return false
	}
	lower := strings.ToLower(hint)
	return strings.Contains(lower, "/p2p-circuit") || strings.Contains(lower, "relay")
}

// handleTopologyUpdate 处理从远程节点接收到的拓扑更新消息。
//
// 处理逻辑（洪水广播协议）：
//  1. 通过 NormalizeTopologyUpdate 标准化更新（去除无效链接、排序等）。
//  2. 如果更新来源是本节点自己，忽略（已本地处理过）。
//  3. 使用 floodKey（来源节点 ID + 生成号）去重：已处理过的更新忽略。
//  4. 仅接受比已知生成号更新的版本，防止旧版本覆盖新版本。
//  5. 记录处理过的更新，防止循环。
//  6. 将更新应用到本地拓扑存储。
//  7. 将更新广播给除入站连接之外的所有其他邻接节点（洪水扩散）。
func (r *Runtime) handleTopologyUpdate(ctx context.Context, ingress *Adjacency, update *TopologyUpdate) {
	update = NormalizeTopologyUpdate(update)
	if update == nil {
		return
	}
	if update.OriginNodeId == r.localNodeID {
		return
	}
	key := floodKey{origin: update.OriginNodeId, generation: update.Generation}
	r.mu.Lock()
	if _, seen := r.seenFlood[key]; seen {
		r.mu.Unlock()
		return
	}
	known := r.knownGeneration[update.OriginNodeId]
	// 仅接受比已接受的生成号严格更新的版本，防止陈旧更新进入，
	// 即使调用者注入非内存存储也是如此。
	if update.Generation < known {
		r.mu.Unlock()
		return
	}
	if update.Generation == known {
		r.mu.Unlock()
		return
	}
	if known != 0 {
		delete(r.seenFlood, floodKey{origin: update.OriginNodeId, generation: known})
	}
	r.knownGeneration[update.OriginNodeId] = update.Generation
	r.seenFlood[key] = struct{}{}
	r.lastUpdate[update.OriginNodeId] = update
	targets := make([]TransportConn, 0, len(r.adjByConn))
	for conn, adj := range r.adjByConn {
		if adj == ingress {
			continue
		}
		targets = append(targets, conn)
	}
	r.mu.Unlock()
	r.store.ApplyTopologyUpdate(update)

	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_TopologyUpdate{TopologyUpdate: update}}
	for _, conn := range targets {
		_ = r.sendEnvelopeCtx(ctx, conn, envelope, r.helloTimeout)
	}
}

// replayCachedTopology 向新建立的邻接连接发送所有已知的拓扑更新缓存，
// 但不包括该远程节点自己的更新。
//
// 这确保新连接的对等节点能快速获取完整的全局拓扑视图，而无需等待
// 各节点的定期发布，加速了拓扑收敛。
func (r *Runtime) replayCachedTopology(ctx context.Context, adj *Adjacency) {
	r.mu.Lock()
	updates := make([]*TopologyUpdate, 0, len(r.lastUpdate))
	for origin, update := range r.lastUpdate {
		if origin == adj.RemoteNodeID || update == nil {
			continue
		}
		updates = append(updates, update)
	}
	r.mu.Unlock()
	for _, update := range updates {
		envelope := &ClusterEnvelope{Body: &ClusterEnvelope_TopologyUpdate{TopologyUpdate: update}}
		_ = r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout)
	}
}

// ---------------- 链路质量测量 ----------------

// linkMeasurementLoop 周期性地向邻接节点发送 Ping 请求以测量链路质量。
//
// 测量指标：
//   - RTT（往返时间）：使用 EWMA（指数加权移动平均）平滑，系数 alpha=0.2。
//   - Jitter（抖动）：RTT 偏差的 EWMA 估计。
//
// 当邻接关系不再活跃（adjacencyActive 返回 false）时退出循环。
func (r *Runtime) linkMeasurementLoop(ctx context.Context, adj *Adjacency) {
	defer r.wg.Done()
	if r.adjacencyActive(adj) {
		r.sendPing(ctx, adj)
	}
	ticker := time.NewTicker(r.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if !r.adjacencyActive(adj) {
			return
		}
		r.sendPing(ctx, adj)
	}
}

// adjacencyActive 检查指定的邻接关系是否仍在运行时中活跃。
// 通过在 adjByConn 中查找连接对象来判断。
func (r *Runtime) adjacencyActive(adj *Adjacency) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.adjByConn[adj.Conn]
	return ok
}

// sendPing 向邻接节点发送一个时间同步请求（TimeSyncRequest），
// 用于测量链路 RTT 和 Jitter。发送前记录 Ping ID 和发送时间戳到
// inlightPings 映射中，用于后续匹配响应和计算延迟。
func (r *Runtime) sendPing(ctx context.Context, adj *Adjacency) {
	id := r.pingID.Add(1)
	now := r.now().UnixMilli()
	adj.mu.Lock()
	adj.inflightPings[id] = time.Unix(0, now*int64(time.Millisecond))
	adj.mu.Unlock()
	envelope := &ClusterEnvelope{
		Body: &ClusterEnvelope_TimeSyncRequest{TimeSyncRequest: &TimeSyncRequest{
			RequestId:        id,
			ClientSendTimeMs: now,
		}},
	}
	if err := r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout); err != nil {
		adj.mu.Lock()
		delete(adj.inflightPings, id)
		adj.mu.Unlock()
	}
}

// handleTimeSyncRequest 处理远程节点发来的时间同步请求。
//
// 处理逻辑：直接在响应中填入 ServerReceiveTimeMs 和 ServerSendTimeMs
// （本实现中两者相同，因为处理是即时完成的），然后将请求中的
// ClientSendTimeMs 原样返回，使发起方能计算完整 RTT。
func (r *Runtime) handleTimeSyncRequest(ctx context.Context, adj *Adjacency, req *TimeSyncRequest) {
	nowMs := r.now().UnixMilli()
	resp := &ClusterEnvelope{
		Body: &ClusterEnvelope_TimeSyncResponse{TimeSyncResponse: &TimeSyncResponse{
			RequestId:           req.RequestId,
			ClientSendTimeMs:    req.ClientSendTimeMs,
			ServerReceiveTimeMs: nowMs,
			ServerSendTimeMs:    nowMs,
		}},
	}
	_ = r.sendEnvelopeCtx(ctx, adj.Conn, resp, r.helloTimeout)
}

// handleTimeSyncResponse 处理远程节点返回的时间同步响应，更新链路的
// RTT 和 Jitter 的 EWMA 估计值。
//
// EWMA 更新公式（alpha = 0.2）：
//   - RTT_EWMA = (1-alpha) * RTT_EWMA + alpha * RTT
//   - Jitter_EWMA = (1-alpha) * Jitter_EWMA + alpha * |RTT - RTT_EWMA_prev|
//
// 当链路成本发生有意义的变更时（linkCostChangedMeaningfully 返回 true），
// 触发 bumpGenerationAndPublish 以广播更新后的链路质量。
func (r *Runtime) handleTimeSyncResponse(adj *Adjacency, resp *TimeSyncResponse) {
	if resp == nil || resp.RequestId == 0 {
		return
	}
	receivedAt := r.now()
	clientReceiveMs := receivedAt.UnixMilli()
	adj.mu.Lock()
	start, ok := adj.inflightPings[resp.RequestId]
	if ok {
		delete(adj.inflightPings, resp.RequestId)
	}
	adj.mu.Unlock()
	if !ok {
		return
	}
	rtt := receivedAt.Sub(start).Milliseconds()
	if rtt < 0 {
		rtt = 0
	}
	adj.mu.Lock()
	prevCost := int64(adj.rttEWMA)
	const alpha = 0.2
	if adj.samples == 0 {
		adj.rttEWMA = float64(rtt)
		adj.jitterEWMA = 0
	} else {
		diff := float64(rtt) - adj.rttEWMA
		if diff < 0 {
			diff = -diff
		}
		adj.jitterEWMA = (1-alpha)*adj.jitterEWMA + alpha*diff
		adj.rttEWMA = (1-alpha)*adj.rttEWMA + alpha*float64(rtt)
	}
	adj.samples++
	newCost := int64(adj.rttEWMA)
	jitter := int64(adj.jitterEWMA)
	adj.mu.Unlock()

	if r.timeSyncObserver != nil {
		r.timeSyncObserver(TimeSyncObservation{
			RemoteNodeID:        adj.RemoteNodeID,
			Transport:           adj.Transport,
			RemoteHint:          adj.RemoteHint,
			RequestID:           resp.RequestId,
			ClientSendTimeMs:    resp.ClientSendTimeMs,
			ServerReceiveTimeMs: resp.ServerReceiveTimeMs,
			ServerSendTimeMs:    resp.ServerSendTimeMs,
			ClientReceiveTimeMs: clientReceiveMs,
			RTTMs:               rtt,
			JitterMs:            jitter,
		})
	}

	if linkCostChangedMeaningfully(prevCost, newCost) {
		r.bumpGenerationAndPublish(r.runtimeContext())
	}
}

// runtimeContext 返回运行时的根 Context。
// 如果运行时已关闭或未启动（ctx 为 nil），回退到 context.Background()。
func (r *Runtime) runtimeContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx == nil {
		return context.Background()
	}
	return r.ctx
}

// linkCostChangedMeaningfully 判断新的链路成本是否与之前发布的成本
// 有足够差异，值得产生新的拓扑生成号。
//
// 变更判定规则：
//  1. 如果前后相等，不触发。
//  2. 如果差值 >= 10ms（绝对阈值），触发。
//  3. 如果之前发布值为 0，新值 >= 2ms 时触发。
//  4. 否则，变化比例 >= 25% 时触发。
//
// 此函数防止 RTT 采样的小抖动导致拓扑广播的频繁波动。
func linkCostChangedMeaningfully(prev, next int64) bool {
	if prev == next {
		return false
	}
	diff := next - prev
	if diff < 0 {
		diff = -diff
	}
	if diff >= 10 {
		return true
	}
	if prev == 0 {
		return next >= 2
	}
	return diff*100/prev >= 25
}
