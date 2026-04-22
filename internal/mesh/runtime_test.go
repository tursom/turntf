package mesh

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

type fakeConn struct {
	kind       TransportKind
	hint       string
	inbox      chan []byte
	peer       *fakeConn
	mu         sync.Mutex
	closed     bool
	closeCh    chan struct{}
	closeOnce  sync.Once
	sendHook   func([]byte) error
	receiveErr error
}

func newFakeConnPair(kind TransportKind, aHint, bHint string) (*fakeConn, *fakeConn) {
	a := &fakeConn{kind: kind, hint: bHint, inbox: make(chan []byte, 16), closeCh: make(chan struct{})}
	b := &fakeConn{kind: kind, hint: aHint, inbox: make(chan []byte, 16), closeCh: make(chan struct{})}
	a.peer = b
	b.peer = a
	return a, b
}

func (c *fakeConn) Send(ctx context.Context, data []byte) error {
	c.mu.Lock()
	closed := c.closed
	hook := c.sendHook
	peer := c.peer
	c.mu.Unlock()
	if closed {
		return io.ErrClosedPipe
	}
	if hook != nil {
		if err := hook(data); err != nil {
			return err
		}
	}
	if peer == nil {
		return io.ErrClosedPipe
	}
	cp := append([]byte(nil), data...)
	select {
	case peer.inbox <- cp:
		return nil
	case <-peer.closeCh:
		return io.ErrClosedPipe
	case <-c.closeCh:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *fakeConn) Receive(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	err := c.receiveErr
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case data, ok := <-c.inbox:
		if !ok {
			return nil, io.EOF
		}
		return data, nil
	case <-c.closeCh:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeConn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.closeCh)
		if peer := c.peer; peer != nil {
			peer.closeOnce.Do(func() {
				peer.mu.Lock()
				peer.closed = true
				peer.mu.Unlock()
				close(peer.closeCh)
			})
		}
	})
	return nil
}

func (c *fakeConn) RemoteNodeHint() string { return c.hint }

func (c *fakeConn) Transport() TransportKind { return c.kind }

type fakeAdapter struct {
	kind   TransportKind
	accept chan TransportConn
	caps   *TransportCapability
	dials  chan dialRequest
}

type dialRequest struct {
	endpoint string
	result   chan dialResult
}

type dialResult struct {
	conn TransportConn
	err  error
}

func newFakeAdapter(kind TransportKind) *fakeAdapter {
	return &fakeAdapter{
		kind:   kind,
		accept: make(chan TransportConn, 16),
		caps: &TransportCapability{
			Transport:       kind,
			InboundEnabled:  true,
			OutboundEnabled: true,
		},
		dials: make(chan dialRequest, 16),
	}
}

func (a *fakeAdapter) Start(ctx context.Context) error { return nil }

func (a *fakeAdapter) Dial(ctx context.Context, endpoint string) (TransportConn, error) {
	req := dialRequest{endpoint: endpoint, result: make(chan dialResult, 1)}
	select {
	case a.dials <- req:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case res := <-req.result:
		return res.conn, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *fakeAdapter) Accept() <-chan TransportConn { return a.accept }

func (a *fakeAdapter) Kind() TransportKind { return a.kind }

func (a *fakeAdapter) LocalCapabilities() *TransportCapability {
	return CloneCapability(a.caps)
}

type acceptingTopologyStore struct {
	mu      sync.Mutex
	applied []*TopologyUpdate
}

type memoryGenerationPersistence struct {
	mu         sync.Mutex
	generation uint64
}

func (s *acceptingTopologyStore) ApplyHello(int64, *NodeHello) {}

func (s *acceptingTopologyStore) ApplyTopologyUpdate(update *TopologyUpdate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applied = append(s.applied, update)
}

func (s *acceptingTopologyStore) Snapshot() TopologySnapshot {
	return TopologySnapshot{}
}

func (s *acceptingTopologyStore) appliedGenerations() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]uint64, 0, len(s.applied))
	for _, update := range s.applied {
		out = append(out, update.Generation)
	}
	return out
}

func (p *memoryGenerationPersistence) Load() (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation, nil
}

func (p *memoryGenerationPersistence) Store(generation uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.generation = generation
	return nil
}

func newTestRuntime(t *testing.T, localID int64, adapter TransportAdapter, opts ...func(*RuntimeOptions)) *Runtime {
	t.Helper()
	options := RuntimeOptions{
		LocalNodeID:   localID,
		Adapters:      []TransportAdapter{adapter},
		LocalPolicy:   DefaultForwardingPolicy(1),
		TopologyStore: NewMemoryTopologyStore(),
		HelloTimeout:  500 * time.Millisecond,
	}
	for _, fn := range opts {
		fn(&options)
	}
	runtime, err := NewRuntime(options)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	return runtime
}

func TestRuntimeRejectsStaleTopologyUpdateWithInjectedStore(t *testing.T) {
	t.Parallel()

	store := &acceptingTopologyStore{}
	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P), func(opts *RuntimeOptions) {
		opts.TopologyStore = store
	})

	runtime.handleTopologyUpdate(context.Background(), nil, &TopologyUpdate{
		OriginNodeId:     2,
		Generation:       10,
		ForwardingPolicy: DefaultForwardingPolicy(1),
	})
	runtime.handleTopologyUpdate(context.Background(), nil, &TopologyUpdate{
		OriginNodeId:     2,
		Generation:       9,
		ForwardingPolicy: DefaultForwardingPolicy(1),
	})

	generations := store.appliedGenerations()
	if len(generations) != 1 || generations[0] != 10 {
		t.Fatalf("expected only generation 10 to be applied, got %v", generations)
	}
}

func TestRuntimePersistsGenerationAcrossRestart(t *testing.T) {
	t.Parallel()

	persistence := &memoryGenerationPersistence{}
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)

	runtimeA := newTestRuntime(t, 1, adapterA, func(opts *RuntimeOptions) {
		opts.GenerationPersistence = persistence
	})
	runtimeB := newTestRuntime(t, 2, adapterB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("start runtimeA: %v", err)
	}
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("start runtimeB: %v", err)
	}

	connA, connB := newFakeConnPair(TransportLibP2P, "A", "B")
	adapterA.accept <- connA
	adapterB.accept <- connB
	waitForAdjacency(t, runtimeA, 1, time.Second)

	if err := runtimeA.Close(); err != nil {
		t.Fatalf("close runtimeA: %v", err)
	}
	persisted, err := persistence.Load()
	if err != nil {
		t.Fatalf("load persisted generation: %v", err)
	}
	if persisted == 0 {
		t.Fatal("expected persisted generation after runtime close")
	}

	restarted := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P), func(opts *RuntimeOptions) {
		opts.GenerationPersistence = persistence
	})
	if restarted.CurrentGeneration() < persisted {
		t.Fatalf("expected restarted runtime generation >= persisted, got=%d persisted=%d", restarted.CurrentGeneration(), persisted)
	}
}

func TestRuntimeRejectsConflictingSameGenerationUpdate(t *testing.T) {
	t.Parallel()

	runtime := newTestRuntime(t, 1, newFakeAdapter(TransportLibP2P))
	update := &TopologyUpdate{
		OriginNodeId:     2,
		Generation:       10,
		ForwardingPolicy: DefaultForwardingPolicy(1),
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, InboundEnabled: true, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 3, Transport: TransportLibP2P, PathClass: PathClassDirect, CostMs: 5, Established: true},
		},
	}
	runtime.handleTopologyUpdate(context.Background(), nil, update)
	runtime.handleTopologyUpdate(context.Background(), nil, &TopologyUpdate{
		OriginNodeId:     2,
		Generation:       10,
		ForwardingPolicy: DefaultForwardingPolicy(9),
		Transports: []*TransportCapability{
			{Transport: TransportZeroMQ, InboundEnabled: true, OutboundEnabled: true},
		},
		Links: []*LinkAdvertisement{
			{FromNodeId: 2, ToNodeId: 4, Transport: TransportZeroMQ, PathClass: PathClassDirect, CostMs: 1, Established: true},
		},
	})

	snapshot := runtime.store.Snapshot()
	node, ok := snapshot.Node(2)
	if !ok {
		t.Fatal("expected origin node in snapshot")
	}
	if node.TransportCaps[TransportLibP2P] == nil || node.TransportCaps[TransportZeroMQ] != nil {
		t.Fatalf("expected conflicting same-generation update to be ignored, got %+v", node.TransportCaps)
	}
	if got := DispositionForTraffic(node.ForwardingPolicy, TrafficReplicationStream); got != DispositionAllow {
		t.Fatalf("expected original same-generation policy to be preserved, got=%v", got)
	}
}

func waitForAdjacency(t *testing.T, runtime *Runtime, expected int, timeout time.Duration) []AdjacencySnapshot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		adjs := runtime.Adjacencies()
		if len(adjs) >= expected {
			return adjs
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d adjacencies, got %d", expected, len(runtime.Adjacencies()))
	return nil
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !fn() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

func TestRuntimeHelloEstablishesAdjacency(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("runtimeA start: %v", err)
	}
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("runtimeB start: %v", err)
	}
	defer runtimeA.Close()
	defer runtimeB.Close()

	connA, connB := newFakeConnPair(TransportLibP2P, "A", "B")
	adapterA.accept <- connA
	adapterB.accept <- connB

	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForAdjacency(t, runtimeB, 1, time.Second)

	adjs := runtimeA.Adjacencies()
	if adjs[0].RemoteNodeID != 2 || adjs[0].Transport != TransportLibP2P {
		t.Fatalf("unexpected adjacency: %+v", adjs[0])
	}

	// Closing the connection should remove the adjacency.
	_ = connA.Close()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(runtimeA.Adjacencies()) == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if adjs := runtimeA.Adjacencies(); len(adjs) != 0 {
		t.Fatalf("expected adjacency removed, got %d", len(adjs))
	}
}

func TestRuntimeRejectsSelfNodeHello(t *testing.T) {
	t.Parallel()
	adapter := newFakeAdapter(TransportLibP2P)
	runtime := newTestRuntime(t, 1, adapter)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer runtime.Close()

	connA, connB := newFakeConnPair(TransportLibP2P, "A", "B")
	adapter.accept <- connA

	// Peer sends a hello claiming the same id.
	hello := &NodeHello{
		NodeId:          1,
		ProtocolVersion: ProtocolVersion,
		Transports: []*TransportCapability{
			{Transport: TransportLibP2P, InboundEnabled: true, OutboundEnabled: true},
		},
		ForwardingPolicy: DefaultForwardingPolicy(1),
	}
	envelope := &ClusterEnvelope{Body: &ClusterEnvelope_NodeHello{NodeHello: hello}}
	data, err := (protoCodec{}).Encode(envelope)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// Drain runtime's outbound hello so our Send below doesn't block.
	sendCtx, sendCancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer sendCancel()
	if _, err := connB.Receive(sendCtx); err != nil {
		t.Fatalf("receive runtime hello: %v", err)
	}
	if err := connB.Send(sendCtx, data); err != nil {
		t.Fatalf("send peer hello: %v", err)
	}

	// Runtime should reject and close the connection.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		connB.mu.Lock()
		closed := connB.closed
		connB.mu.Unlock()
		if closed {
			break
		}
		if _, err := connB.Receive(sendCtx); err != nil {
			break
		}
	}
	if adjs := runtime.Adjacencies(); len(adjs) != 0 {
		t.Fatalf("expected no adjacency, got %d", len(adjs))
	}
}

type rejectingVerifier struct {
	reject bool
}

func (v *rejectingVerifier) Verify(envelope *ClusterEnvelope, _ []byte) error {
	if !v.reject {
		return nil
	}
	if _, ok := envelope.Body.(*ClusterEnvelope_NodeHello); ok {
		return errors.New("verifier: rejected")
	}
	return nil
}

func TestRuntimeVerifierRejectsHello(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportLibP2P)
	adapterB := newFakeAdapter(TransportLibP2P)
	verifier := &rejectingVerifier{reject: true}
	runtimeA := newTestRuntime(t, 1, adapterA, func(opts *RuntimeOptions) {
		opts.Verifier = verifier
	})
	runtimeB := newTestRuntime(t, 2, adapterB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("runtimeA start: %v", err)
	}
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("runtimeB start: %v", err)
	}
	defer runtimeA.Close()
	defer runtimeB.Close()

	connA, connB := newFakeConnPair(TransportLibP2P, "A", "B")
	adapterA.accept <- connA
	adapterB.accept <- connB

	time.Sleep(200 * time.Millisecond)
	if adjs := runtimeA.Adjacencies(); len(adjs) != 0 {
		t.Fatalf("runtimeA should reject, got %d adjacencies", len(adjs))
	}
}

func TestRuntimeDialSeedsBuildAdjacency(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportZeroMQ)
	adapterB := newFakeAdapter(TransportZeroMQ)

	runtimeB := newTestRuntime(t, 2, adapterB)
	runtimeA := newTestRuntime(t, 1, adapterA, func(opts *RuntimeOptions) {
		opts.DialSeeds = []DialSeed{{Transport: TransportZeroMQ, Endpoint: "tcp://b:1"}}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("runtimeB start: %v", err)
	}
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("runtimeA start: %v", err)
	}
	defer runtimeA.Close()
	defer runtimeB.Close()

	// Handle the dial from A: create a paired conn and deliver one end to
	// runtimeA (dial result) and the other to runtimeB (via accept).
	select {
	case req := <-adapterA.dials:
		if req.endpoint != "tcp://b:1" {
			t.Fatalf("unexpected endpoint: %s", req.endpoint)
		}
		connA, connB := newFakeConnPair(TransportZeroMQ, "A", "B")
		req.result <- dialResult{conn: connA}
		adapterB.accept <- connB
	case <-time.After(time.Second):
		t.Fatalf("expected dial request")
	}

	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForAdjacency(t, runtimeB, 1, time.Second)
}

func TestRuntimeDialSeedRetriesUntilPeerAvailable(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportZeroMQ)
	adapterB := newFakeAdapter(TransportZeroMQ)

	runtimeA := newTestRuntime(t, 1, adapterA, func(opts *RuntimeOptions) {
		opts.DialSeeds = []DialSeed{{Transport: TransportZeroMQ, Endpoint: "tcp://b:late"}}
		opts.DialRetryInterval = 20 * time.Millisecond
	})
	runtimeB := newTestRuntime(t, 2, adapterB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("runtimeA start: %v", err)
	}
	defer runtimeA.Close()

	select {
	case req := <-adapterA.dials:
		if req.endpoint != "tcp://b:late" {
			t.Fatalf("unexpected endpoint: %s", req.endpoint)
		}
		req.result <- dialResult{err: errors.New("peer unavailable")}
	case <-time.After(time.Second):
		t.Fatalf("expected initial dial request")
	}

	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("runtimeB start: %v", err)
	}
	defer runtimeB.Close()

	select {
	case req := <-adapterA.dials:
		if req.endpoint != "tcp://b:late" {
			t.Fatalf("unexpected retry endpoint: %s", req.endpoint)
		}
		connA, connB := newFakeConnPair(TransportZeroMQ, "A", "B")
		req.result <- dialResult{conn: connA}
		adapterB.accept <- connB
	case <-time.After(time.Second):
		t.Fatalf("expected retry dial request")
	}

	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForAdjacency(t, runtimeB, 1, time.Second)
}

func TestRuntimeDialSeedReconnectsAfterAdjacencyLoss(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportZeroMQ)
	adapterB := newFakeAdapter(TransportZeroMQ)

	runtimeA := newTestRuntime(t, 1, adapterA, func(opts *RuntimeOptions) {
		opts.DialSeeds = []DialSeed{{Transport: TransportZeroMQ, Endpoint: "tcp://b:reconnect"}}
		opts.DialRetryInterval = 20 * time.Millisecond
	})
	runtimeB := newTestRuntime(t, 2, adapterB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("runtimeB start: %v", err)
	}
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("runtimeA start: %v", err)
	}
	defer runtimeA.Close()
	defer runtimeB.Close()

	var firstConnA *fakeConn
	select {
	case req := <-adapterA.dials:
		connA, connB := newFakeConnPair(TransportZeroMQ, "A", "B")
		firstConnA = connA
		req.result <- dialResult{conn: connA}
		adapterB.accept <- connB
	case <-time.After(time.Second):
		t.Fatalf("expected initial dial request")
	}
	waitForAdjacency(t, runtimeA, 1, time.Second)

	_ = firstConnA.Close()
	waitFor(t, time.Second, func() bool { return len(runtimeA.Adjacencies()) == 0 })

	select {
	case req := <-adapterA.dials:
		if req.endpoint != "tcp://b:reconnect" {
			t.Fatalf("unexpected reconnect endpoint: %s", req.endpoint)
		}
		connA, connB := newFakeConnPair(TransportZeroMQ, "A2", "B2")
		req.result <- dialResult{conn: connA}
		adapterB.accept <- connB
	case <-time.After(time.Second):
		t.Fatalf("expected reconnect dial request")
	}
	waitForAdjacency(t, runtimeA, 1, time.Second)
}

func TestRuntimeAddDialSeedAfterStartBuildsAdjacency(t *testing.T) {
	t.Parallel()
	adapterA := newFakeAdapter(TransportZeroMQ)
	adapterB := newFakeAdapter(TransportZeroMQ)

	runtimeA := newTestRuntime(t, 1, adapterA)
	runtimeB := newTestRuntime(t, 2, adapterB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtimeB.Start(ctx); err != nil {
		t.Fatalf("runtimeB start: %v", err)
	}
	if err := runtimeA.Start(ctx); err != nil {
		t.Fatalf("runtimeA start: %v", err)
	}
	defer runtimeA.Close()
	defer runtimeB.Close()

	if err := runtimeA.AddDialSeed(DialSeed{Transport: TransportZeroMQ, Endpoint: "tcp://b:2"}); err != nil {
		t.Fatalf("add dial seed: %v", err)
	}

	select {
	case req := <-adapterA.dials:
		if req.endpoint != "tcp://b:2" {
			t.Fatalf("unexpected endpoint: %s", req.endpoint)
		}
		connA, connB := newFakeConnPair(TransportZeroMQ, "A", "B")
		req.result <- dialResult{conn: connA}
		adapterB.accept <- connB
	case <-time.After(time.Second):
		t.Fatalf("expected dynamic dial request")
	}

	waitForAdjacency(t, runtimeA, 1, time.Second)
	waitForAdjacency(t, runtimeB, 1, time.Second)
}

func TestRuntimeAddDialSeedDeduplicatesAndRemoveStopsRetryLoop(t *testing.T) {
	t.Parallel()
	adapter := newFakeAdapter(TransportZeroMQ)
	runtime := newTestRuntime(t, 1, adapter, func(opts *RuntimeOptions) {
		opts.DialRetryInterval = 25 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("runtime start: %v", err)
	}
	defer runtime.Close()

	seed := DialSeed{Transport: TransportZeroMQ, Endpoint: "tcp://b:dedupe"}
	if err := runtime.AddDialSeed(seed); err != nil {
		t.Fatalf("add dial seed: %v", err)
	}
	if err := runtime.AddDialSeed(seed); err != nil {
		t.Fatalf("add duplicate dial seed: %v", err)
	}

	var req dialRequest
	select {
	case req = <-adapter.dials:
		if req.endpoint != seed.Endpoint {
			t.Fatalf("unexpected dial endpoint: got=%q want=%q", req.endpoint, seed.Endpoint)
		}
	case <-time.After(time.Second):
		t.Fatalf("expected dial request")
	}
	select {
	case extra := <-adapter.dials:
		t.Fatalf("expected one dial loop for duplicated seed, got extra request for %q", extra.endpoint)
	case <-time.After(100 * time.Millisecond):
	}

	if err := runtime.RemoveDialSeed(seed); err != nil {
		t.Fatalf("remove dial seed: %v", err)
	}
	select {
	case req.result <- dialResult{err: context.Canceled}:
	default:
	}
	select {
	case extra := <-adapter.dials:
		t.Fatalf("expected removed dial seed to stop retrying, got %q", extra.endpoint)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRuntimeSeenFloodKeepsOnlyLatestGenerationPerOrigin(t *testing.T) {
	t.Parallel()
	adapter := newFakeAdapter(TransportLibP2P)
	runtime := newTestRuntime(t, 1, adapter)

	ctx := context.Background()
	runtime.publishLocalTopology(ctx)
	firstGeneration := runtime.CurrentGeneration()
	runtime.bumpGenerationAndPublish(ctx)
	secondGeneration := runtime.CurrentGeneration()
	runtime.bumpGenerationAndPublish(ctx)

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.seenFlood) != 1 {
		t.Fatalf("expected one seen flood entry for local origin, got %d", len(runtime.seenFlood))
	}
	if _, ok := runtime.seenFlood[floodKey{origin: 1, generation: firstGeneration}]; ok {
		t.Fatalf("old local generation %d should have been evicted", firstGeneration)
	}
	if _, ok := runtime.seenFlood[floodKey{origin: 1, generation: secondGeneration}]; ok {
		t.Fatalf("previous local generation %d should have been evicted", secondGeneration)
	}
}
