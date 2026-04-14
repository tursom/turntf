package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

type clusterTestEnvelopeMatcher func(*internalproto.Envelope) bool

type clusterTestFlushOrder int

const (
	clusterTestFlushFIFO clusterTestFlushOrder = iota
	clusterTestFlushLIFO
)

type clusterTestScenario struct {
	t     *testing.T
	nodes map[string]*clusterTestNode
	links map[clusterTestLinkKey]*clusterTestLink

	mu       sync.Mutex
	states   map[clusterTestDirectionKey]*clusterTestDirectionState
	observed []clusterTestObservedEnvelope
}

type clusterTestLinkKey struct {
	source string
	target string
	alias  string
}

type clusterTestDirectionKey struct {
	from  string
	to    string
	alias string
}

type clusterTestDirectionState struct {
	dropNext      []clusterTestEnvelopeMatcher
	duplicateNext []clusterTestEnvelopeMatcher
	holdNext      []clusterTestEnvelopeMatcher
	delayNext     []clusterTestDelayedMatcher
	held          []clusterTestHeldFrame
	blocked       bool
	disabled      bool
}

type clusterTestDelayedMatcher struct {
	matcher clusterTestEnvelopeMatcher
	delay   time.Duration
}

type clusterTestHeldFrame struct {
	messageType int
	data        []byte
	envelope    *internalproto.Envelope
}

type clusterTestFrameAction struct {
	frames  []clusterTestHeldFrame
	delayed []clusterTestDelayedFrame
}

type clusterTestDelayedFrame struct {
	frame clusterTestHeldFrame
	delay time.Duration
}

type clusterTestObservedEnvelope struct {
	From      string
	To        string
	LinkAlias string
	Observed  time.Time
	Envelope  *internalproto.Envelope
}

type clusterTestHelloMatcher func(*internalproto.Hello) bool

type clusterTestNodeAssertion func(context.Context, *clusterTestNode) error

type clusterTestLink struct {
	t        *testing.T
	scenario *clusterTestScenario
	source   string
	target   string
	alias    string
	targetWS string
	server   *http.Server
	ln       net.Listener

	upgrader websocket.Upgrader

	mu      sync.Mutex
	pipes   map[clusterTestDirectionKey]*clusterTestPipeWriter
	closers map[*websocket.Conn]struct{}
}

type clusterTestPipeWriter struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func newClusterTestScenario(t *testing.T, specs ...clusterTestNodeSpec) *clusterTestScenario {
	t.Helper()
	if len(specs) == 0 {
		t.Fatalf("cluster test scenario topology must not be empty")
	}

	nodeListeners := make(map[string]net.Listener, len(specs))
	specByName := make(map[string]clusterTestNodeSpec, len(specs))
	for _, spec := range specs {
		if spec.Name == "" {
			t.Fatalf("cluster test scenario node name must not be empty")
		}
		if _, exists := specByName[spec.Name]; exists {
			t.Fatalf("duplicate cluster test scenario node name: %s", spec.Name)
		}
		specByName[spec.Name] = spec
		nodeListeners[spec.Name] = mustListen(t)
	}

	scenario := &clusterTestScenario{
		t:      t,
		nodes:  make(map[string]*clusterTestNode, len(specs)),
		links:  make(map[clusterTestLinkKey]*clusterTestLink),
		states: make(map[clusterTestDirectionKey]*clusterTestDirectionState),
	}

	for _, spec := range specs {
		for _, peerLink := range clusterTestPeerLinks(t, spec) {
			targetListener, ok := nodeListeners[peerLink.Target]
			if !ok {
				t.Fatalf("unknown cluster test peer %q for node %q", peerLink.Target, spec.Name)
			}
			key := clusterTestLinkKey{source: spec.Name, target: peerLink.Target, alias: peerLink.Alias}
			if _, exists := scenario.links[key]; exists {
				t.Fatalf("duplicate cluster test link %s -> %s alias %q", spec.Name, peerLink.Target, peerLink.Alias)
			}
			link := newClusterTestLink(t, scenario, spec.Name, peerLink.Target, peerLink.Alias, wsURL(targetListener))
			scenario.links[key] = link
			if peerLink.InitiallyBlocked {
				scenario.setLinkBlocked(spec.Name, peerLink.Target, peerLink.Alias, true)
				scenario.setLinkBlocked(peerLink.Target, spec.Name, peerLink.Alias, true)
			}
		}
	}

	for _, spec := range specs {
		peerLinks := clusterTestPeerLinks(t, spec)
		peers := make([]Peer, 0, len(peerLinks))
		for _, peerLink := range peerLinks {
			link := scenario.links[clusterTestLinkKey{source: spec.Name, target: peerLink.Target, alias: peerLink.Alias}]
			peers = append(peers, Peer{URL: link.URL()})
		}

		cfg := clusterTestNodeConfig{
			Name:              spec.Name,
			Slot:              spec.Slot,
			Peers:             peers,
			MessageWindowSize: spec.MessageWindowSize,
			ClockSkewMs:       spec.ClockSkewMs,
			MaxClockSkewMs:    spec.MaxClockSkewMs,
			TimeSyncSamples:   spec.TimeSyncSamples,
			DisableRouting:    spec.DisableRouting,
			DiscoveryEnabled:  spec.DiscoveryEnabled,
		}
		if cfg.MessageWindowSize == 0 {
			cfg.MessageWindowSize = store.DefaultMessageWindowSize
		}
		if cfg.MaxClockSkewMs == 0 {
			cfg.MaxClockSkewMs = DefaultMaxClockSkewMs
		}
		scenario.nodes[spec.Name] = newClusterTestNodeFixtureWithListener(t, cfg, nodeListeners[spec.Name])
	}

	return scenario
}

func clusterTestPeerLinks(t *testing.T, spec clusterTestNodeSpec) []clusterTestPeerLinkSpec {
	t.Helper()

	links := make([]clusterTestPeerLinkSpec, 0, len(spec.PeerNames)+len(spec.PeerLinks))
	for _, peerName := range spec.PeerNames {
		links = append(links, clusterTestPeerLinkSpec{
			Alias:  peerName,
			Target: peerName,
		})
	}
	for _, peerLink := range spec.PeerLinks {
		if peerLink.Target == "" {
			t.Fatalf("cluster test peer link target must not be empty for node %q", spec.Name)
		}
		if peerLink.Alias == "" {
			peerLink.Alias = peerLink.Target
		}
		links = append(links, peerLink)
	}
	return links
}

func newClusterTestLink(t *testing.T, scenario *clusterTestScenario, source, target, alias, targetWS string) *clusterTestLink {
	t.Helper()

	ln := mustListen(t)
	link := &clusterTestLink{
		t:        t,
		scenario: scenario,
		source:   source,
		target:   target,
		alias:    alias,
		targetWS: targetWS,
		server:   &http.Server{},
		ln:       ln,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(*http.Request) bool { return true },
		},
		pipes:   make(map[clusterTestDirectionKey]*clusterTestPipeWriter),
		closers: make(map[*websocket.Conn]struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc(websocketPath, link.handleWebSocket)
	link.server.Handler = mux
	go func() {
		err := link.server.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()
	t.Cleanup(func() {
		link.closeActive()
		_ = link.server.Close()
		_ = link.ln.Close()
	})
	return link
}

func (s *clusterTestScenario) Node(name string) *clusterTestNode {
	s.t.Helper()
	node, ok := s.nodes[name]
	if !ok {
		s.t.Fatalf("unknown cluster test scenario node %q", name)
	}
	return node
}

func (s *clusterTestScenario) Start(names ...string) {
	s.t.Helper()
	if len(names) == 0 {
		names = make([]string, 0, len(s.nodes))
		for name := range s.nodes {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	for _, name := range names {
		s.Node(name).Start(s.t)
	}
}

func (s *clusterTestScenario) StartManagers(names ...string) {
	s.t.Helper()
	for _, name := range s.nodeNames(names...) {
		s.Node(name).StartManager(s.t)
	}
}

func (s *clusterTestScenario) StartHTTPServers(names ...string) {
	s.t.Helper()
	for _, name := range s.nodeNames(names...) {
		s.Node(name).StartHTTPServer(s.t)
	}
}

func (s *clusterTestScenario) nodeNames(names ...string) []string {
	s.t.Helper()
	if len(names) > 0 {
		return names
	}
	names = make([]string, 0, len(s.nodes))
	for name := range s.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *clusterTestScenario) Partition(a, b string) {
	s.t.Helper()
	for _, link := range s.linksBetween(a, b) {
		s.setLinkBlocked(a, b, link.alias, true)
		s.setLinkBlocked(b, a, link.alias, true)
		link.closeActive()
	}
}

func (s *clusterTestScenario) Heal(a, b string) {
	s.t.Helper()
	for _, link := range s.linksBetween(a, b) {
		s.setLinkBlocked(a, b, link.alias, false)
		s.setLinkBlocked(b, a, link.alias, false)
	}
}

func (s *clusterTestScenario) PartitionLink(a, b, alias string) {
	s.t.Helper()
	for _, link := range s.mustLinksBetween(a, b, alias) {
		s.setLinkBlocked(a, b, link.alias, true)
		s.setLinkBlocked(b, a, link.alias, true)
		link.closeActive()
	}
}

func (s *clusterTestScenario) HealLink(a, b, alias string) {
	s.t.Helper()
	for _, link := range s.mustLinksBetween(a, b, alias) {
		s.setLinkBlocked(a, b, link.alias, false)
		s.setLinkBlocked(b, a, link.alias, false)
	}
}

func (s *clusterTestScenario) DisableLink(a, b, alias string) {
	s.t.Helper()
	for _, link := range s.mustLinksBetween(a, b, alias) {
		s.setLinkDisabled(a, b, link.alias, true)
		s.setLinkDisabled(b, a, link.alias, true)
		link.closeActive()
	}
}

func (s *clusterTestScenario) EnableLink(a, b, alias string) {
	s.t.Helper()
	for _, link := range s.mustLinksBetween(a, b, alias) {
		s.setLinkDisabled(a, b, link.alias, false)
		s.setLinkDisabled(b, a, link.alias, false)
	}
}

func (s *clusterTestScenario) DropNext(from, to string, matcher clusterTestEnvelopeMatcher) {
	s.t.Helper()
	if matcher == nil {
		matcher = clusterTestAnyEnvelope()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, alias := range s.directionAliasesLocked(from, to) {
		state := s.directionStateLocked(from, to, alias)
		state.dropNext = append(state.dropNext, matcher)
	}
}

func (s *clusterTestScenario) DuplicateNext(from, to string, matcher clusterTestEnvelopeMatcher) {
	s.t.Helper()
	if matcher == nil {
		matcher = clusterTestAnyEnvelope()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, alias := range s.directionAliasesLocked(from, to) {
		state := s.directionStateLocked(from, to, alias)
		state.duplicateNext = append(state.duplicateNext, matcher)
	}
}

func (s *clusterTestScenario) Hold(from, to string, matcher clusterTestEnvelopeMatcher) {
	s.t.Helper()
	if matcher == nil {
		matcher = clusterTestAnyEnvelope()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, alias := range s.directionAliasesLocked(from, to) {
		state := s.directionStateLocked(from, to, alias)
		state.holdNext = append(state.holdNext, matcher)
	}
}

func (s *clusterTestScenario) DelayNext(from, to string, delay time.Duration, matcher clusterTestEnvelopeMatcher) {
	s.t.Helper()
	if matcher == nil {
		matcher = clusterTestAnyEnvelope()
	}
	if delay <= 0 {
		delay = time.Millisecond
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, alias := range s.directionAliasesLocked(from, to) {
		state := s.directionStateLocked(from, to, alias)
		state.delayNext = append(state.delayNext, clusterTestDelayedMatcher{
			matcher: matcher,
			delay:   delay,
		})
	}
}

func (s *clusterTestScenario) FlushHeld(from, to string, order clusterTestFlushOrder) {
	s.t.Helper()

	s.mu.Lock()
	type heldByDirection struct {
		key    clusterTestDirectionKey
		frames []clusterTestHeldFrame
	}
	heldGroups := make([]heldByDirection, 0)
	for _, alias := range s.directionAliasesLocked(from, to) {
		state := s.directionStateLocked(from, to, alias)
		if len(state.held) == 0 {
			continue
		}
		held := append([]clusterTestHeldFrame(nil), state.held...)
		state.held = nil
		heldGroups = append(heldGroups, heldByDirection{
			key:    clusterTestDirectionKey{from: from, to: to, alias: alias},
			frames: held,
		})
	}
	s.mu.Unlock()

	for _, group := range heldGroups {
		held := group.frames
		if order == clusterTestFlushLIFO {
			for i, j := 0, len(held)-1; i < j; i, j = i+1, j-1 {
				held[i], held[j] = held[j], held[i]
			}
		}
		for _, frame := range held {
			if !s.writeFrame(group.key, frame) {
				s.t.Fatalf("no active cluster test pipe for held frame %s -> %s alias %q", from, to, group.key.alias)
			}
		}
	}
}

func (s *clusterTestScenario) Jitter(a, b string, cycles int) {
	s.t.Helper()
	if cycles <= 0 {
		return
	}
	for range cycles {
		s.Partition(a, b)
		time.Sleep(50 * time.Millisecond)
		s.Heal(a, b)
		time.Sleep(75 * time.Millisecond)
	}
}

func (s *clusterTestScenario) TriggerSnapshotDigest(from, to string) {
	s.t.Helper()
	fromNode := s.Node(from)
	toNode := s.Node(to)
	sess := fromNode.CurrentSession(toNode.id)
	if sess == nil {
		s.t.Fatalf("expected active session from %s to %s", from, to)
	}
	fromNode.manager.sendSnapshotDigest(sess)
}

func (s *clusterTestScenario) SetTimeSyncSamples(nodeName string, samples ...timeSyncSample) {
	s.t.Helper()
	s.Node(nodeName).SetTimeSyncSamples(samples...)
}

func (s *clusterTestScenario) TriggerTimeSync(from, to string) {
	s.t.Helper()
	fromNode := s.Node(from)
	toNode := s.Node(to)
	sess := fromNode.CurrentSession(toNode.id)
	if sess == nil {
		s.t.Fatalf("expected active session from %s to %s", from, to)
	}
	if err := fromNode.manager.performTimeSync(sess); err != nil {
		s.t.Fatalf("time sync from %s to %s failed: %v", from, to, err)
	}
	fromNode.manager.markPeerClockSynced(sess, sess.clockOffset())
}

func (s *clusterTestScenario) HeldCount(from, to string) int {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, alias := range s.directionAliasesLocked(from, to) {
		count += len(s.directionStateLocked(from, to, alias).held)
	}
	return count
}

func (s *clusterTestScenario) WaitForHeld(from, to string, minCount int, timeout time.Duration) {
	s.t.Helper()
	eventually(s.t, timeout, func() bool {
		return s.HeldCount(from, to) >= minCount
	})
}

func (s *clusterTestScenario) WaitForPeersActive(timeout time.Duration, peers ...clusterPeerExpectation) {
	s.t.Helper()
	waitForPeersActive(s.t, timeout, peers...)
}

func (s *clusterTestScenario) WaitForPeersInactive(timeout time.Duration, peers ...clusterPeerExpectation) {
	s.t.Helper()
	eventually(s.t, timeout, func() bool {
		for _, peer := range peers {
			if peer.node.ActivePeer(peer.peerID) {
				return false
			}
		}
		return true
	})
}

func (s *clusterTestScenario) WaitForUsersConverged(timeout time.Duration, key store.UserKey, expectedUsername, expectedProfile string, nodeNames ...string) {
	s.t.Helper()
	nodes := s.nodesByName(nodeNames...)
	waitForUsersConverged(s.t, timeout, key, expectedUsername, expectedProfile, nodes...)
}

func (s *clusterTestScenario) WaitForUserVisible(timeout time.Duration, nodeName string, key store.UserKey) {
	s.t.Helper()
	waitForReplicatedUser(s.t, timeout, s.Node(nodeName), key)
}

func (s *clusterTestScenario) WaitForMessagesEqual(timeout time.Duration, nodeName string, key store.UserKey, limit int, expectedBodies ...string) {
	s.t.Helper()
	waitForMessagesEqual(s.t, timeout, s.Node(nodeName), key, limit, expectedBodies...)
}

func (s *clusterTestScenario) WaitForEventCount(timeout time.Duration, nodeName string, afterID int64, limit, expectedCount int) {
	s.t.Helper()
	waitForEventCount(s.t, timeout, s.Node(nodeName), afterID, limit, expectedCount)
}

func (s *clusterTestScenario) WaitForOriginCursor(timeout time.Duration, nodeName string, originNodeID int64, minAppliedEventID int64) {
	s.t.Helper()
	node := s.Node(nodeName)
	eventually(s.t, timeout, func() bool {
		cursor, err := node.store.GetOriginCursor(context.Background(), originNodeID)
		return err == nil && cursor.AppliedEventID >= minAppliedEventID
	})
}

func (s *clusterTestScenario) WaitForPeerAck(timeout time.Duration, nodeName string, peerID int64, minAck uint64) {
	s.t.Helper()
	waitForPeerAckReached(s.t, timeout, s.Node(nodeName), peerID, minAck)
}

func (s *clusterTestScenario) WaitForPeerClockOffset(timeout time.Duration, nodeName string, peerID int64, expectedOffsetMs int64) {
	s.t.Helper()
	node := s.Node(nodeName)
	eventually(s.t, timeout, func() bool {
		return node.PeerClockOffset(peerID) == expectedOffsetMs
	})
}

func (s *clusterTestScenario) WaitForRouteReachable(timeout time.Duration, nodeName string, targetNodeID int64) {
	s.t.Helper()
	waitForRouteReachable(s.t, timeout, s.Node(nodeName), targetNodeID)
}

func (s *clusterTestScenario) WaitForRouteUnreachable(timeout time.Duration, nodeName string, targetNodeID int64) {
	s.t.Helper()
	waitForRouteUnreachable(s.t, timeout, s.Node(nodeName), targetNodeID)
}

func (s *clusterTestScenario) WaitForSnapshotRepairCompleted(timeout time.Duration, nodeName string, key store.UserKey, expectedMessages ...string) {
	s.t.Helper()
	node := s.Node(nodeName)
	eventually(s.t, timeout, func() bool {
		if _, err := node.store.GetUser(context.Background(), key); err != nil {
			return false
		}
		if len(expectedMessages) == 0 {
			return true
		}
		messages, err := node.store.ListMessagesByUser(context.Background(), key, len(expectedMessages)+5)
		if err != nil || len(messages) != len(expectedMessages) {
			return false
		}
		for i, expected := range expectedMessages {
			if string(messages[i].Body) != expected {
				return false
			}
		}
		return true
	})
}

func (s *clusterTestScenario) WaitForEnvelope(from, to string, matcher clusterTestEnvelopeMatcher, timeout time.Duration) clusterTestObservedEnvelope {
	s.t.Helper()
	return s.WaitForEnvelopeOnLink(from, to, "", matcher, timeout)
}

func (s *clusterTestScenario) WaitForEnvelopeOnLink(from, to, alias string, matcher clusterTestEnvelopeMatcher, timeout time.Duration) clusterTestObservedEnvelope {
	s.t.Helper()
	if matcher == nil {
		matcher = clusterTestAnyEnvelope()
	}

	var matched clusterTestObservedEnvelope
	eventually(s.t, timeout, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, observed := range s.observed {
			if observed.From != from || observed.To != to {
				continue
			}
			if alias != "" && observed.LinkAlias != alias {
				continue
			}
			if matcher(observed.Envelope) {
				matched = observed
				return true
			}
		}
		return false
	})
	return matched
}

func (s *clusterTestScenario) WaitForHello(from, to string, matcher clusterTestHelloMatcher, timeout time.Duration) clusterTestObservedEnvelope {
	s.t.Helper()
	return s.WaitForHelloOnLink(from, to, "", matcher, timeout)
}

func (s *clusterTestScenario) WaitForHelloOnLink(from, to, alias string, matcher clusterTestHelloMatcher, timeout time.Duration) clusterTestObservedEnvelope {
	s.t.Helper()
	return s.WaitForEnvelopeOnLink(from, to, alias, func(envelope *internalproto.Envelope) bool {
		hello := envelope.GetHello()
		if hello == nil {
			return false
		}
		return matcher == nil || matcher(hello)
	}, timeout)
}

func (s *clusterTestScenario) WaitForHandshakeAccepted(from, to string, timeout time.Duration) {
	s.t.Helper()
	fromNode := s.Node(from)
	toNode := s.Node(to)
	s.WaitForHello(from, to, clusterTestHelloNodeID(fromNode.id), timeout)
	waitForPeersActive(s.t, timeout, expectClusterPeer(toNode, fromNode.id))
}

func (s *clusterTestScenario) WaitForAllNodesSatisfy(timeout time.Duration, assertion clusterTestNodeAssertion, nodeNames ...string) {
	s.t.Helper()
	if assertion == nil {
		s.t.Fatalf("cluster test node assertion must not be nil")
	}
	if timeout < 10*time.Second {
		timeout = 10 * time.Second
	}

	nodes := s.nodesByName(nodeNames...)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		allSatisfied := true
		for _, node := range nodes {
			if err := assertion(ctx, node); err != nil {
				allSatisfied = false
				lastErr = fmt.Errorf("%s: %w", node.name, err)
				break
			}
		}
		if allSatisfied {
			return
		}

		select {
		case <-ctx.Done():
			if lastErr != nil {
				s.t.Fatalf("cluster test assertion not satisfied within %s: %v", timeout, lastErr)
			}
			s.t.Fatalf("cluster test assertion not satisfied within %s", timeout)
		case <-ticker.C:
		}
	}
}

func (s *clusterTestScenario) nodesByName(names ...string) []*clusterTestNode {
	s.t.Helper()
	if len(names) == 0 {
		names = make([]string, 0, len(s.nodes))
		for name := range s.nodes {
			names = append(names, name)
		}
		sort.Strings(names)
	}
	nodes := make([]*clusterTestNode, 0, len(names))
	for _, name := range names {
		nodes = append(nodes, s.Node(name))
	}
	return nodes
}

func (s *clusterTestScenario) setLinkBlocked(from, to, alias string, blocked bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.directionStateLocked(from, to, alias).blocked = blocked
}

func (s *clusterTestScenario) setLinkDisabled(from, to, alias string, disabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.directionStateLocked(from, to, alias).disabled = disabled
}

func (s *clusterTestScenario) directionStateLocked(from, to, alias string) *clusterTestDirectionState {
	key := clusterTestDirectionKey{from: from, to: to, alias: alias}
	state := s.states[key]
	if state == nil {
		state = &clusterTestDirectionState{}
		s.states[key] = state
	}
	return state
}

func (s *clusterTestScenario) directionAliasesLocked(from, to string) []string {
	seen := make(map[string]struct{})
	for _, link := range s.linksBetweenLocked(from, to) {
		seen[link.alias] = struct{}{}
	}
	if len(seen) == 0 {
		return []string{""}
	}
	aliases := make([]string, 0, len(seen))
	for alias := range seen {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	return aliases
}

func (s *clusterTestScenario) linksBetween(a, b string) []*clusterTestLink {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.linksBetweenLocked(a, b)
}

func (s *clusterTestScenario) mustLinksBetween(a, b, alias string) []*clusterTestLink {
	s.t.Helper()
	links := make([]*clusterTestLink, 0)
	for _, link := range s.linksBetween(a, b) {
		if link.alias == alias {
			links = append(links, link)
		}
	}
	if len(links) == 0 {
		s.t.Fatalf("unknown cluster test link %s <-> %s alias %q", a, b, alias)
	}
	return links
}

func (s *clusterTestScenario) linksBetweenLocked(a, b string) []*clusterTestLink {
	links := make([]*clusterTestLink, 0, 2)
	for key, link := range s.links {
		if (key.source == a && key.target == b) || (key.source == b && key.target == a) {
			links = append(links, link)
		}
	}
	sort.Slice(links, func(i, j int) bool {
		if links[i].source != links[j].source {
			return links[i].source < links[j].source
		}
		if links[i].target != links[j].target {
			return links[i].target < links[j].target
		}
		return links[i].alias < links[j].alias
	})
	return links
}

func (s *clusterTestScenario) applyFrame(key clusterTestDirectionKey, frame clusterTestHeldFrame) clusterTestFrameAction {
	if frame.envelope == nil {
		return clusterTestFrameAction{frames: []clusterTestHeldFrame{frame}}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.directionStateLocked(key.from, key.to, key.alias)
	if state.blocked || state.disabled {
		return clusterTestFrameAction{}
	}

	if consumeMatcher(&state.dropNext, frame.envelope) {
		return clusterTestFrameAction{}
	}

	if consumeMatcher(&state.holdNext, frame.envelope) {
		state.held = append(state.held, frame)
		return clusterTestFrameAction{}
	}

	if delay, ok := consumeDelayedMatcher(&state.delayNext, frame.envelope); ok {
		return clusterTestFrameAction{delayed: []clusterTestDelayedFrame{{frame: frame, delay: delay}}}
	}

	if consumeMatcher(&state.duplicateNext, frame.envelope) {
		return clusterTestFrameAction{frames: []clusterTestHeldFrame{frame, frame}}
	}
	return clusterTestFrameAction{frames: []clusterTestHeldFrame{frame}}
}

func (s *clusterTestScenario) writeFrame(key clusterTestDirectionKey, frame clusterTestHeldFrame) bool {
	for _, link := range s.linksBetween(key.from, key.to) {
		if key.alias != "" && link.alias != key.alias {
			continue
		}
		if link.writeFrame(key, frame) {
			return true
		}
	}
	return false
}

func (s *clusterTestScenario) recordEnvelope(key clusterTestDirectionKey, envelope *internalproto.Envelope) {
	clone, ok := proto.Clone(envelope).(*internalproto.Envelope)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.observed = append(s.observed, clusterTestObservedEnvelope{
		From:      key.from,
		To:        key.to,
		LinkAlias: key.alias,
		Observed:  time.Now().UTC(),
		Envelope:  clone,
	})
}

func consumeMatcher(matchers *[]clusterTestEnvelopeMatcher, envelope *internalproto.Envelope) bool {
	for i, matcher := range *matchers {
		if matcher != nil && matcher(envelope) {
			*matchers = append((*matchers)[:i], (*matchers)[i+1:]...)
			return true
		}
	}
	return false
}

func consumeDelayedMatcher(matchers *[]clusterTestDelayedMatcher, envelope *internalproto.Envelope) (time.Duration, bool) {
	for i, item := range *matchers {
		if item.matcher != nil && item.matcher(envelope) {
			*matchers = append((*matchers)[:i], (*matchers)[i+1:]...)
			return item.delay, true
		}
	}
	return 0, false
}

func (l *clusterTestLink) URL() string {
	return "ws://" + l.ln.Addr().String() + websocketPath
}

func (l *clusterTestLink) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if l.scenario.directionUnavailable(l.source, l.target, l.alias) || l.scenario.directionUnavailable(l.target, l.source, l.alias) {
		http.Error(w, "cluster test link partitioned", http.StatusServiceUnavailable)
		return
	}

	inbound, err := l.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	target, _, err := websocket.DefaultDialer.Dial(l.targetWS, nil)
	if err != nil {
		_ = inbound.Close()
		return
	}

	l.track(inbound)
	l.track(target)
	defer l.untrack(inbound)
	defer l.untrack(target)
	defer inbound.Close()
	defer target.Close()

	clientToTarget := clusterTestDirectionKey{from: l.source, to: l.target, alias: l.alias}
	targetToClient := clusterTestDirectionKey{from: l.target, to: l.source, alias: l.alias}
	l.setPipe(clientToTarget, target)
	l.setPipe(targetToClient, inbound)
	defer l.clearPipe(clientToTarget, target)
	defer l.clearPipe(targetToClient, inbound)

	done := make(chan struct{}, 2)
	go l.copyLoop(inbound, target, clientToTarget, done)
	go l.copyLoop(target, inbound, targetToClient, done)
	<-done
}

func (s *clusterTestScenario) directionUnavailable(from, to, alias string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.directionStateLocked(from, to, alias)
	return state.blocked || state.disabled
}

func (l *clusterTestLink) copyLoop(src, dst *websocket.Conn, key clusterTestDirectionKey, done chan<- struct{}) {
	defer func() {
		_ = src.Close()
		_ = dst.Close()
		done <- struct{}{}
	}()

	for {
		messageType, data, err := src.ReadMessage()
		if err != nil {
			return
		}

		frame := clusterTestHeldFrame{
			messageType: messageType,
			data:        append([]byte(nil), data...),
		}
		if messageType == websocket.BinaryMessage {
			var envelope internalproto.Envelope
			if err := proto.Unmarshal(data, &envelope); err == nil {
				frame.envelope = &envelope
				l.scenario.recordEnvelope(key, &envelope)
			}
		}

		action := l.scenario.applyFrame(key, frame)
		for _, outgoing := range action.frames {
			if !l.writeFrame(key, outgoing) {
				return
			}
		}
		for _, outgoing := range action.delayed {
			outgoing := outgoing
			go func() {
				time.Sleep(outgoing.delay)
				_ = l.writeFrame(key, outgoing.frame)
			}()
		}
	}
}

func (l *clusterTestLink) setPipe(key clusterTestDirectionKey, conn *websocket.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pipes[key] = &clusterTestPipeWriter{conn: conn}
}

func (l *clusterTestLink) clearPipe(key clusterTestDirectionKey, conn *websocket.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if writer := l.pipes[key]; writer != nil && writer.conn == conn {
		delete(l.pipes, key)
	}
}

func (l *clusterTestLink) writeFrame(key clusterTestDirectionKey, frame clusterTestHeldFrame) bool {
	l.mu.Lock()
	writer := l.pipes[key]
	l.mu.Unlock()
	if writer == nil {
		return false
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.conn.WriteMessage(frame.messageType, frame.data) == nil
}

func (l *clusterTestLink) track(conn *websocket.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closers[conn] = struct{}{}
}

func (l *clusterTestLink) untrack(conn *websocket.Conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.closers, conn)
}

func (l *clusterTestLink) closeActive() {
	l.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(l.closers))
	for conn := range l.closers {
		conns = append(conns, conn)
	}
	l.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func clusterTestAnyEnvelope() clusterTestEnvelopeMatcher {
	return func(*internalproto.Envelope) bool {
		return true
	}
}

func clusterTestEventBatch() clusterTestEnvelopeMatcher {
	return func(envelope *internalproto.Envelope) bool {
		return envelope.GetEventBatch() != nil
	}
}

func clusterTestBroadcastEventBatch() clusterTestEnvelopeMatcher {
	return func(envelope *internalproto.Envelope) bool {
		batch := envelope.GetEventBatch()
		return batch != nil && batch.GetPullRequestId() == 0 && len(batch.GetEvents()) > 0
	}
}

func clusterTestPullEventBatch() clusterTestEnvelopeMatcher {
	return func(envelope *internalproto.Envelope) bool {
		batch := envelope.GetEventBatch()
		return batch != nil && batch.GetPullRequestId() > 0
	}
}

func clusterTestPullEvents() clusterTestEnvelopeMatcher {
	return func(envelope *internalproto.Envelope) bool {
		return envelope.GetPullEvents() != nil
	}
}

func clusterTestSnapshotDigest() clusterTestEnvelopeMatcher {
	return func(envelope *internalproto.Envelope) bool {
		return envelope.GetSnapshotDigest() != nil
	}
}

func clusterTestSnapshotChunkResponse() clusterTestEnvelopeMatcher {
	return func(envelope *internalproto.Envelope) bool {
		chunk := envelope.GetSnapshotChunk()
		return chunk != nil && !chunk.GetRequest()
	}
}

func clusterTestAck() clusterTestEnvelopeMatcher {
	return func(envelope *internalproto.Envelope) bool {
		return envelope.GetAck() != nil
	}
}

func clusterTestHelloAll(matchers ...clusterTestHelloMatcher) clusterTestHelloMatcher {
	return func(hello *internalproto.Hello) bool {
		for _, matcher := range matchers {
			if matcher != nil && !matcher(hello) {
				return false
			}
		}
		return true
	}
}

func clusterTestHelloNodeID(nodeID int64) clusterTestHelloMatcher {
	return func(hello *internalproto.Hello) bool {
		return hello.GetNodeId() == nodeID
	}
}

func clusterTestHelloProtocolVersion(version string) clusterTestHelloMatcher {
	return func(hello *internalproto.Hello) bool {
		return hello.GetProtocolVersion() == version
	}
}

func clusterTestHelloSnapshotVersion(version string) clusterTestHelloMatcher {
	return func(hello *internalproto.Hello) bool {
		return hello.GetSnapshotVersion() == version
	}
}

func clusterTestHelloSupportsRouting(supports bool) clusterTestHelloMatcher {
	return func(hello *internalproto.Hello) bool {
		return hello.GetSupportsRouting() == supports
	}
}

func clusterTestHelloConnectionIDNonZero() clusterTestHelloMatcher {
	return func(hello *internalproto.Hello) bool {
		return hello.GetConnectionId() != 0
	}
}
