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
	ErrRuntimeClosed       = errors.New("mesh: runtime closed")
	ErrHelloRejected       = errors.New("mesh: hello rejected")
	ErrAdapterAlreadyBound = errors.New("mesh: adapter transport already registered")
)

// EnvelopeCodec encodes and decodes ClusterEnvelope messages on the wire.
// The default implementation uses protobuf; tests can inject a fake codec.
type EnvelopeCodec interface {
	Encode(envelope *ClusterEnvelope) ([]byte, error)
	Decode(data []byte) (*ClusterEnvelope, error)
}

// EnvelopeSigner wraps an outbound envelope payload (post-encode) so a
// transport-independent signature can be added later. The default signer
// is a no-op.
type EnvelopeSigner interface {
	Sign(envelope *ClusterEnvelope, encoded []byte) ([]byte, error)
}

// EnvelopeVerifier inspects an inbound envelope before the runtime acts on
// it. Returning an error causes the connection to be closed.
type EnvelopeVerifier interface {
	Verify(envelope *ClusterEnvelope, raw []byte) error
}

// GenerationPersistence lets the runtime durably remember the last
// generation it published.
type GenerationPersistence interface {
	Load() (uint64, error)
	Store(generation uint64) error
}

type LocalEnvelopeHandler func(ctx context.Context, packet *ForwardedPacket, envelope *ClusterEnvelope) error

type LocalForwardedPacketHandler func(ctx context.Context, packet *ForwardedPacket) error

type TimeSyncObservation struct {
	RemoteNodeID        int64
	Transport           TransportKind
	RemoteHint          string
	RequestID           uint64
	ClientSendTimeMs    int64
	ServerReceiveTimeMs int64
	ServerSendTimeMs    int64
	ClientReceiveTimeMs int64
	RTTMs               int64
	JitterMs            int64
}

type TimeSyncObserver func(observation TimeSyncObservation)

type AdjacencyObservation struct {
	RemoteNodeID int64
	Transport    TransportKind
	RemoteHint   string
	Inbound      bool
	Established  bool
	Hello        *NodeHello
}

type AdjacencyObserver func(observation AdjacencyObservation)

// DialSeed describes a transport endpoint the runtime should proactively
// dial after startup.
type DialSeed struct {
	Transport TransportKind
	Endpoint  string
}

type protoCodec struct{}

func (protoCodec) Encode(envelope *ClusterEnvelope) ([]byte, error) {
	return proto.Marshal(envelope)
}

func (protoCodec) Decode(data []byte) (*ClusterEnvelope, error) {
	envelope := &ClusterEnvelope{}
	if err := proto.Unmarshal(data, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

type noopSigner struct{}

func (noopSigner) Sign(_ *ClusterEnvelope, encoded []byte) ([]byte, error) {
	return encoded, nil
}

type noopVerifier struct{}

func (noopVerifier) Verify(_ *ClusterEnvelope, _ []byte) error { return nil }

// RuntimeOptions configures a Runtime. Only LocalNodeID and Adapters are
// required; other fields fall back to sensible defaults.
type RuntimeOptions struct {
	LocalNodeID            int64
	Adapters               []TransportAdapter
	LocalPolicy            *ForwardingPolicy
	TopologyStore          TopologyStore
	DialSeeds              []DialSeed
	Codec                  EnvelopeCodec
	Signer                 EnvelopeSigner
	Verifier               EnvelopeVerifier
	GenerationPersistence  GenerationPersistence
	TrafficClassifier      TrafficClassifier
	EnvelopeHandler        LocalEnvelopeHandler
	QueryHandler           LocalEnvelopeHandler
	ForwardedPacketHandler LocalForwardedPacketHandler
	ForwardingObserver     ForwardingObserver
	TimeSyncObserver       TimeSyncObserver
	AdjacencyObserver      AdjacencyObserver
	HelloTimeout           time.Duration
	DialRetryInterval      time.Duration
	PingInterval           time.Duration
	TopologyPublishPeriod  time.Duration
	Now                    func() time.Time
}

// Runtime owns transport adapters and the set of established adjacencies.
type Runtime struct {
	localNodeID            int64
	policy                 *ForwardingPolicy
	store                  TopologyStore
	codec                  EnvelopeCodec
	signer                 EnvelopeSigner
	verifier               EnvelopeVerifier
	persistence            GenerationPersistence
	classifier             TrafficClassifier
	engine                 *Engine
	planner                RoutePlanner
	envelopeHandler        LocalEnvelopeHandler
	queryHandler           LocalEnvelopeHandler
	forwardedPacketHandler LocalForwardedPacketHandler
	forwardingObserver     ForwardingObserver
	timeSyncObserver       TimeSyncObserver
	adjacencyObserver      AdjacencyObserver

	helloTimeout          time.Duration
	dialRetryInterval     time.Duration
	pingInterval          time.Duration
	topologyPublishPeriod time.Duration
	now                   func() time.Time

	adapters     []TransportAdapter
	adapterByKnd map[TransportKind]TransportAdapter

	mu        sync.Mutex
	started   bool
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	adjByConn map[TransportConn]*Adjacency
	adjByKey  map[adjacencyKey]map[TransportConn]*Adjacency

	generation        uint64
	knownGeneration   map[int64]uint64
	seenFlood         map[floodKey]struct{}
	lastUpdate        map[int64]*TopologyUpdate
	pendingTombstones []*LinkAdvertisement
	dialSeeds         map[dialSeedKey]*dialSeedEntry

	pingID   atomic.Uint64
	packetID atomic.Uint64
}

type adjacencyKey struct {
	nodeID    int64
	transport TransportKind
	hint      string
}

type floodKey struct {
	origin     int64
	generation uint64
}

type dialSeedKey struct {
	transport TransportKind
	endpoint  string
}

type dialSeedEntry struct {
	seed   DialSeed
	cancel context.CancelFunc
}

// Adjacency represents a single accepted or dialed connection to a remote
// node after a successful NodeHello exchange. Callers may read fields; the
// mutex guards link measurement state.
type Adjacency struct {
	RemoteNodeID int64
	Transport    TransportKind
	RemoteHint   string
	Hello        *NodeHello
	Conn         TransportConn
	Inbound      bool

	mu            sync.Mutex
	rttEWMA       float64
	jitterEWMA    float64
	samples       int
	established   bool
	inflightPings map[uint64]time.Time
}

// NewRuntime constructs a Runtime from options. It does not start any
// goroutines; call Start.
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

	var initialGeneration uint64
	if opts.GenerationPersistence != nil {
		if persisted, err := opts.GenerationPersistence.Load(); err == nil {
			initialGeneration = persisted
		}
	}
	if ms := uint64(now().UnixMilli()); ms > initialGeneration {
		initialGeneration = ms
	}

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
		knownGeneration:        make(map[int64]uint64),
		seenFlood:              make(map[floodKey]struct{}),
		lastUpdate:             make(map[int64]*TopologyUpdate),
		dialSeeds:              dialSeeds,
		generation:             initialGeneration,
	}
	runtime.planner = NewPlanner(opts.LocalNodeID)
	runtime.engine = NewEngine(opts.LocalNodeID, store.Snapshot, runtime.planner, runtime, runtime.handleLocalForwardedPacket, opts.ForwardingObserver)
	return runtime, nil
}

// LocalNodeID returns the id the runtime was configured with.
func (r *Runtime) LocalNodeID() int64 { return r.localNodeID }

// LocalPolicy returns a clone of the normalized local forwarding policy.
func (r *Runtime) LocalPolicy() *ForwardingPolicy {
	return ClonePolicy(r.policy)
}

// CurrentGeneration returns the current local generation value.
func (r *Runtime) CurrentGeneration() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.generation
}

// AddDialSeed schedules a proactive dial. Before Start it records the seed
// for startup; after Start it launches a best-effort dial immediately.
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

// RemoveDialSeed stops future proactive dials for a previously-added seed.
// Existing established adjacencies are not closed; this only stops the retry
// loop that would create new outbound connections.
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

// RouteEnvelope wraps an inner mesh envelope in a ForwardedPacket and
// routes it through the forwarding engine.
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

// ForwardPacket routes a pre-built packet through the forwarding engine.
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

// DescribeRoute computes the current forwarding decision for the given
// destination and traffic class using the latest topology snapshot.
func (r *Runtime) DescribeRoute(destinationNodeID int64, trafficClass TrafficClass) (RouteDecision, bool) {
	if r == nil || r.planner == nil {
		return RouteDecision{}, false
	}
	return r.planner.Compute(r.store.Snapshot(), destinationNodeID, trafficClass, TransportUnspecified)
}

// SendPacket implements PacketSender for Engine.
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
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_ForwardedPacket{ForwardedPacket: cloneForwardedPacket(packet)}}
	return r.sendEnvelopeCtx(ctx, adj.Conn, envelope, r.helloTimeout)
}

func (r *Runtime) bestAdjacency(nextHopNodeID int64, transport TransportKind) *Adjacency {
	if nextHopNodeID <= 0 || transport == TransportUnspecified {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var best *Adjacency
	var bestScore int64
	for _, adj := range r.adjByConn {
		if adj == nil || adj.RemoteNodeID != nextHopNodeID || adj.Transport != transport {
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
			return r.envelopeHandler(ctx, cloneForwardedPacket(packet), proto.Clone(envelope).(*ClusterEnvelope))
		}
		if packet.TrafficClass == TrafficControlQuery && r.queryHandler != nil {
			return r.queryHandler(ctx, cloneForwardedPacket(packet), proto.Clone(envelope).(*ClusterEnvelope))
		}
		return nil
	}
	if r.forwardedPacketHandler != nil {
		return r.forwardedPacketHandler(ctx, cloneForwardedPacket(packet))
	}
	return nil
}

// LocalCapabilities collects the capability descriptors advertised by the
// registered adapters at this moment.
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

// Start launches adapters, accept loops, dial seeds, the ping loop and the
// topology publisher. It is safe to call at most once.
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

	// Publish the initial topology with the startup generation before any
	// adjacency forms so that local node state, capabilities and policy are
	// visible in snapshots immediately.
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

// Close tears the runtime down and blocks until accept/dial goroutines
// finish.
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

// Adjacencies returns a snapshot of currently-established adjacencies.
func (r *Runtime) Adjacencies() []AdjacencySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]AdjacencySnapshot, 0, len(r.adjByConn))
	for _, adj := range r.adjByConn {
		out = append(out, adj.snapshot())
	}
	return out
}

// AdjacencySnapshot is a copy of Adjacency state suitable for inspection.
type AdjacencySnapshot struct {
	RemoteNodeID int64
	Transport    TransportKind
	RemoteHint   string
	Inbound      bool
	RTTMs        int64
	JitterMs     int64
	Samples      int
	Established  bool
}

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

func (r *Runtime) adapterForKind(kind TransportKind) TransportAdapter {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.adapterByKnd[kind]
}

func (r *Runtime) handleConn(ctx context.Context, kind TransportKind, conn TransportConn, inbound bool) {
	defer r.wg.Done()
	r.runConn(ctx, kind, conn, inbound)
}

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

	// Start measurement loop for this adjacency.
	r.wg.Add(1)
	go r.linkMeasurementLoop(ctx, adj)

	r.readLoop(ctx, adj)
}

func (r *Runtime) sendHello(ctx context.Context, conn TransportConn, kind TransportKind) error {
	hello := r.localHello(kind)
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_NodeHello{NodeHello: hello}}
	if err := r.sendEnvelopeCtx(ctx, conn, envelope, r.helloTimeout); err != nil {
		return err
	}
	return nil
}

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
	return conn.Send(sendCtx, signed)
}

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
	}
}

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
	return adj
}

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
	}
	// Queue an explicit tombstone for this link so the next publication
	// announces the loss before the link disappears entirely.
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
	// First publish carries the tombstone; a follow-up publish drops the
	// link entirely.
	r.bumpGenerationAndPublish(ctx)
	r.bumpGenerationAndPublish(ctx)
}

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
		packet := cloneForwardedPacket(body.ForwardedPacket)
		packet.IngressTransport = adj.Transport
		_ = r.engine.HandleInbound(ctx, packet)
	default:
		// Other envelope types are handled by later phases.
	}
}

// ---------------- generation + flooding ----------------

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

func (r *Runtime) publishLocalTopology(ctx context.Context) {
	update := NormalizeTopologyUpdate(r.buildLocalTopologyUpdate())
	if update == nil {
		return
	}
	// Apply locally so Snapshot reflects our own generation promptly.
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

func (r *Runtime) buildLocalTopologyUpdate() *TopologyUpdate {
	r.mu.Lock()
	gen := r.generation
	caps := r.LocalCapabilities()
	links := make([]*LinkAdvertisement, 0, len(r.adjByConn)+len(r.pendingTombstones))
	for _, adj := range r.adjByConn {
		links = append(links, r.buildLinkAdvertisementLocked(adj, true))
	}
	// Include any queued tombstones so remote peers see the link drop at
	// least once with established=false before it is removed entirely.
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

func clampNonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	if v > int64(^uint32(0)) {
		return int64(^uint32(0))
	}
	return v
}

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

func remoteHintSuggestsRelay(hint string) bool {
	if hint == "" {
		return false
	}
	lower := strings.ToLower(hint)
	return strings.Contains(lower, "/p2p-circuit") || strings.Contains(lower, "relay")
}

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
	// Only accept strictly newer generations than what this runtime has
	// already accepted for the origin. This keeps stale updates out even
	// when callers inject a non-memory store.
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

// ---------------- link measurement ----------------

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

func (r *Runtime) adjacencyActive(adj *Adjacency) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.adjByConn[adj.Conn]
	return ok
}

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

func (r *Runtime) runtimeContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ctx == nil {
		return context.Background()
	}
	return r.ctx
}

// linkCostChangedMeaningfully returns true when the new cost differs from
// the previous advertised cost by enough to warrant a new topology
// generation. Small jitter in RTT samples should not flood the network.
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
