package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/tursom/turntf/internal/clock"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func TestManagerTimeSyncRequestReturnsServerTimestamps(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	var clockRead atomic.Int32
	mgr.clock = clock.NewClockWithSource(mgr.cfg.NodeID, func() int64 {
		if clockRead.Add(1) == 1 {
			return 2_000
		}
		return 2_003
	})
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, 1)}

	err := mgr.handleTimeSyncRequest(sess, &internalproto.Envelope{
		NodeId: testNodeID(2),
		Body: &internalproto.Envelope_TimeSyncRequest{TimeSyncRequest: &internalproto.TimeSyncRequest{
			RequestId:        41,
			ClientSendTimeMs: 1_000,
		}},
	})
	if err != nil {
		t.Fatalf("handle time sync request: %v", err)
	}

	responseEnvelope := <-sess.send
	response := responseEnvelope.GetTimeSyncResponse()
	if responseEnvelope.GetNodeId() != mgr.cfg.NodeID || response == nil || response.GetRequestId() != 41 ||
		response.GetClientSendTimeMs() != 1_000 || response.GetServerReceiveTimeMs() != 2_000 ||
		response.GetServerSendTimeMs() != 2_003 {
		t.Fatalf("unexpected time sync response: %+v", responseEnvelope)
	}
}

func TestManagerTimeSyncRoundTripUsesFourTimestamps(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	var clockRead atomic.Int32
	mgr.clock = clock.NewClockWithSource(mgr.cfg.NodeID, func() int64 {
		if clockRead.Add(1) == 1 {
			return 1_000
		}
		return 1_040
	})
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, 1)}

	type roundTripResult struct {
		sample timeSyncSample
		err    error
	}
	resultCh := make(chan roundTripResult, 1)
	go func() {
		sample, err := mgr.timeSyncRoundTrip(sess)
		resultCh <- roundTripResult{sample: sample, err: err}
	}()

	requestEnvelope := <-sess.send
	request := requestEnvelope.GetTimeSyncRequest()
	if request == nil || request.GetRequestId() == 0 || request.GetClientSendTimeMs() != 1_000 {
		t.Fatalf("unexpected time sync request: %+v", requestEnvelope)
	}
	if err := mgr.handleTimeSyncResponse(sess, &internalproto.Envelope{
		NodeId: testNodeID(2),
		Body: &internalproto.Envelope_TimeSyncResponse{TimeSyncResponse: &internalproto.TimeSyncResponse{
			RequestId:           request.GetRequestId(),
			ClientSendTimeMs:    request.GetClientSendTimeMs(),
			ServerReceiveTimeMs: 1_010,
			ServerSendTimeMs:    1_015,
		}},
	}); err != nil {
		t.Fatalf("handle time sync response: %v", err)
	}

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("time sync round trip: %v", result.err)
	}
	if result.sample.offsetMs != -7 || result.sample.rttMs != 35 {
		t.Fatalf("unexpected NTP sample: got offset=%d rtt=%d want offset=-7 rtt=35", result.sample.offsetMs, result.sample.rttMs)
	}
}

func TestManagerCollectTimeSyncSampleChoosesBestRTTAndAccountsForJitter(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	mgr.ctx = context.Background()
	var clockRead atomic.Int32
	mgr.clock = clock.NewClockWithSource(mgr.cfg.NodeID, func() int64 {
		read := clockRead.Add(1) - 1
		round := int64(read / 2)
		if read%2 == 0 {
			return 1_000 + round*100
		}
		return 1_040 + round*100
	})
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, timeSyncSampleCount)}
	responseErr := make(chan error, 1)
	go func() {
		for sample := 0; sample < timeSyncSampleCount; sample++ {
			requestEnvelope := <-sess.send
			request := requestEnvelope.GetTimeSyncRequest()
			serverDelay := int64(5 + sample)
			if err := mgr.handleTimeSyncResponse(sess, &internalproto.Envelope{
				NodeId: testNodeID(2),
				Body: &internalproto.Envelope_TimeSyncResponse{TimeSyncResponse: &internalproto.TimeSyncResponse{
					RequestId:           request.GetRequestId(),
					ClientSendTimeMs:    request.GetClientSendTimeMs(),
					ServerReceiveTimeMs: request.GetClientSendTimeMs() + 10,
					ServerSendTimeMs:    request.GetClientSendTimeMs() + 10 + serverDelay,
				}},
			}); err != nil {
				responseErr <- err
				return
			}
		}
		responseErr <- nil
	}()

	sample, err := mgr.collectTimeSyncSample(sess)
	if err != nil {
		t.Fatalf("collect time sync sample: %v", err)
	}
	if err := <-responseErr; err != nil {
		t.Fatalf("respond to time sync request: %v", err)
	}
	if sample.offsetMs != -4 || sample.rttMs != 29 || sample.uncertaintyMs != 64 || !sample.credible || sample.sampledAt.IsZero() {
		t.Fatalf("unexpected aggregated time sync sample: %+v", sample)
	}
}

func TestManagerTimeSyncRoundTripStopsWhenManagerContextIsCanceled(t *testing.T) {
	t.Parallel()

	mgr := newHandshakeTestManager(t)
	mgr.ctx, mgr.cancel = context.WithCancel(context.Background())
	mgr.cancel()
	sess := &session{manager: mgr, peerID: testNodeID(2), send: make(chan *internalproto.Envelope, 1)}

	if _, err := mgr.timeSyncRoundTrip(sess); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled time sync, got %v", err)
	}
	sess.mu.Lock()
	pending := len(sess.pendingTimeSync)
	sess.mu.Unlock()
	if pending != 0 {
		t.Fatalf("expected canceled request to be removed, got %d pending requests", pending)
	}
}

func TestManagerRejectedClockRequiresConsecutiveHealthySamplesToRecover(t *testing.T) {
	t.Parallel()

	mgr := newReplicationTestManager(t, newReplicationTestStore(t, "clock-recovery", 2))
	sess := readySnapshotTestSession(mgr, testNodeID(1), store.DefaultMessageWindowSize)
	mgr.cfg.ClockRejectAfterFailures = 2
	mgr.cfg.ClockRecoverAfterHealthySamples = 3
	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{}, errors.New("time source unavailable")
	}

	if err := mgr.performTimeSync(sess); err != nil {
		t.Fatalf("first sync failure should enter observation: %v", err)
	}
	if err := mgr.performTimeSync(sess); err == nil {
		t.Fatal("second consecutive sync failure should reject the peer")
	}
	assertPeerClockStatus(t, mgr, sess.peerID, string(clockStateRejected), false)

	mgr.timeSyncer = func(*session) (timeSyncSample, error) {
		return timeSyncSample{offsetMs: 4, rttMs: 2, credible: true}, nil
	}
	for sample := 1; sample < mgr.cfg.ClockRecoverAfterHealthySamples; sample++ {
		if err := mgr.performTimeSync(sess); err != nil {
			t.Fatalf("healthy recovery sample %d: %v", sample, err)
		}
		assertPeerClockStatus(t, mgr, sess.peerID, string(clockStateObserving), false)
	}
	if err := mgr.performTimeSync(sess); err != nil {
		t.Fatalf("final healthy recovery sample: %v", err)
	}
	assertPeerClockStatus(t, mgr, sess.peerID, string(clockStateTrusted), true)
}

func assertPeerClockStatus(t *testing.T, mgr *Manager, peerID int64, wantState string, wantTrusted bool) {
	t.Helper()
	status, err := mgr.Status(context.Background())
	if err != nil {
		t.Fatalf("manager status: %v", err)
	}
	for _, peer := range status.Peers {
		if peer.NodeID != peerID {
			continue
		}
		if peer.ClockState != wantState || peer.TrustedForOffset != wantTrusted {
			t.Fatalf("unexpected peer clock status: got state=%s trusted=%t want state=%s trusted=%t", peer.ClockState, peer.TrustedForOffset, wantState, wantTrusted)
		}
		return
	}
	t.Fatalf("peer %d not found in status: %+v", peerID, status.Peers)
}
