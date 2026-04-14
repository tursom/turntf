package cluster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/app"
	internalproto "github.com/tursom/turntf/internal/proto"
	"github.com/tursom/turntf/internal/store"
)

func (m *Manager) QueryLoggedInUsers(ctx context.Context, nodeID int64) ([]app.LoggedInUserSummary, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: cluster manager is not configured", app.ErrServiceUnavailable)
	}
	if nodeID <= 0 {
		return nil, fmt.Errorf("%w: target node id cannot be empty", store.ErrInvalidInput)
	}
	if nodeID == m.cfg.NodeID {
		return m.listLocalLoggedInUsers(ctx)
	}

	sess := m.bestRouteSession(nodeID)
	if sess == nil {
		return nil, fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, nodeID)
	}

	requestID, resultCh := m.beginLoggedInUsersQuery()
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_QueryLoggedInUsersRequest{
			QueryLoggedInUsersRequest: &internalproto.QueryLoggedInUsersRequest{
				RequestId:     requestID,
				TargetNodeId:  nodeID,
				OriginNodeId:  m.cfg.NodeID,
				RemainingHops: defaultLoggedInUsersQueryMaxHops,
			},
		},
	})

	timeoutCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, queryLoggedInUsersTimeout)
		defer cancel()
	}

	select {
	case <-timeoutCtx.Done():
		m.cancelLoggedInUsersQuery(requestID, timeoutCtx.Err())
		if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: timed out querying node %d logged-in users", app.ErrServiceUnavailable, nodeID)
		}
		return nil, timeoutCtx.Err()
	case result := <-resultCh:
		if result.err != nil {
			if errors.Is(result.err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("%w: timed out querying node %d logged-in users", app.ErrServiceUnavailable, nodeID)
			}
			if errors.Is(result.err, errSessionClosed) {
				return nil, fmt.Errorf("%w: peer session closed while querying node %d", app.ErrServiceUnavailable, nodeID)
			}
			return nil, result.err
		}
		if err := clusterQueryError(result.response); err != nil {
			return nil, err
		}
		return loggedInUsersFromCluster(result.response.GetItems()), nil
	}
}

func (m *Manager) handleQueryLoggedInUsersRequest(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	req := envelope.GetQueryLoggedInUsersRequest()
	if req == nil {
		return errors.New("query logged-in users request body cannot be empty")
	}
	if req.RequestId == 0 {
		return errors.New("query logged-in users request id cannot be empty")
	}
	if req.OriginNodeId <= 0 {
		return errors.New("query logged-in users origin node id cannot be empty")
	}
	if req.TargetNodeId <= 0 {
		return errors.New("query logged-in users target node id cannot be empty")
	}

	if req.TargetNodeId == m.cfg.NodeID {
		response := &internalproto.QueryLoggedInUsersResponse{
			RequestId:     req.RequestId,
			TargetNodeId:  req.TargetNodeId,
			OriginNodeId:  req.OriginNodeId,
			RemainingHops: req.RemainingHops,
		}
		users, err := m.listLocalLoggedInUsers(context.Background())
		if err != nil {
			response.ErrorCode = "service_unavailable"
			response.ErrorMessage = err.Error()
		} else {
			response.Items = clusterLoggedInUsers(users)
		}
		return m.forwardQueryLoggedInUsersResponse(response)
	}

	if req.RemainingHops <= 0 {
		return m.forwardQueryLoggedInUsersResponse(&internalproto.QueryLoggedInUsersResponse{
			RequestId:     req.RequestId,
			TargetNodeId:  req.TargetNodeId,
			OriginNodeId:  req.OriginNodeId,
			ErrorCode:     "service_unavailable",
			ErrorMessage:  fmt.Sprintf("route hop limit exceeded while querying node %d", req.TargetNodeId),
			RemainingHops: 0,
		})
	}

	next := proto.Clone(req).(*internalproto.QueryLoggedInUsersRequest)
	next.RemainingHops--
	if err := m.forwardQueryLoggedInUsersRequest(next); err != nil {
		return m.forwardQueryLoggedInUsersResponse(&internalproto.QueryLoggedInUsersResponse{
			RequestId:     req.RequestId,
			TargetNodeId:  req.TargetNodeId,
			OriginNodeId:  req.OriginNodeId,
			ErrorCode:     "service_unavailable",
			ErrorMessage:  err.Error(),
			RemainingHops: next.RemainingHops,
		})
	}
	return nil
}

func (m *Manager) handleQueryLoggedInUsersResponse(sess *session, envelope *internalproto.Envelope) error {
	if err := validatePeerEnvelope(sess, envelope); err != nil {
		return err
	}

	resp := envelope.GetQueryLoggedInUsersResponse()
	if resp == nil {
		return errors.New("query logged-in users response body cannot be empty")
	}
	if resp.RequestId == 0 {
		return errors.New("query logged-in users response id cannot be empty")
	}
	if resp.OriginNodeId <= 0 {
		return errors.New("query logged-in users response origin node id cannot be empty")
	}
	if resp.TargetNodeId <= 0 {
		return errors.New("query logged-in users response target node id cannot be empty")
	}
	if resp.OriginNodeId == m.cfg.NodeID {
		if !m.resolveLoggedInUsersQuery(resp.RequestId, loggedInUsersQueryResult{response: resp}) {
			m.logDebug("query_logged_in_users_response_ignored").
				Uint64("request_id", resp.RequestId).
				Int64("origin_node_id", resp.OriginNodeId).
				Int64("target_node_id", resp.TargetNodeId).
				Int32("remaining_hops", resp.RemainingHops).
				Msg("ignoring late logged-in users response without pending origin query")
		}
		return nil
	}
	if resp.RemainingHops <= 0 {
		m.logWarn("query_logged_in_users_response_dropped", nil).
			Uint64("request_id", resp.RequestId).
			Int64("origin_node_id", resp.OriginNodeId).
			Int64("target_node_id", resp.TargetNodeId).
			Int32("remaining_hops", resp.RemainingHops).
			Msg("dropping logged-in users response because hop limit was reached")
		return nil
	}

	next := proto.Clone(resp).(*internalproto.QueryLoggedInUsersResponse)
	next.RemainingHops--
	if err := m.forwardQueryLoggedInUsersResponse(next); err != nil {
		m.logWarn("query_logged_in_users_response_forward_failed", err).
			Uint64("request_id", resp.RequestId).
			Int64("origin_node_id", resp.OriginNodeId).
			Int64("target_node_id", resp.TargetNodeId).
			Int32("remaining_hops", next.RemainingHops).
			Msg("failed to forward logged-in users response")
	}
	return nil
}

func (m *Manager) forwardQueryLoggedInUsersRequest(req *internalproto.QueryLoggedInUsersRequest) error {
	if req == nil {
		return errors.New("query logged-in users request cannot be empty")
	}
	sess := m.bestRouteSession(req.TargetNodeId)
	if sess == nil {
		return fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, req.TargetNodeId)
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_QueryLoggedInUsersRequest{
			QueryLoggedInUsersRequest: req,
		},
	})
	return nil
}

func (m *Manager) forwardQueryLoggedInUsersResponse(resp *internalproto.QueryLoggedInUsersResponse) error {
	if resp == nil {
		return errors.New("query logged-in users response cannot be empty")
	}
	sess := m.bestRouteSession(resp.OriginNodeId)
	if sess == nil {
		return fmt.Errorf("%w: node %d is not reachable", app.ErrServiceUnavailable, resp.OriginNodeId)
	}
	sess.enqueue(&internalproto.Envelope{
		NodeId: m.cfg.NodeID,
		Body: &internalproto.Envelope_QueryLoggedInUsersResponse{
			QueryLoggedInUsersResponse: resp,
		},
	})
	return nil
}

func (m *Manager) listLocalLoggedInUsers(ctx context.Context) ([]app.LoggedInUserSummary, error) {
	m.mu.Lock()
	provider := m.loggedInUsersProvider
	m.mu.Unlock()
	if provider == nil {
		return nil, fmt.Errorf("%w: local logged-in users provider is not configured", app.ErrServiceUnavailable)
	}
	return provider(ctx)
}

func clusterLoggedInUsers(users []app.LoggedInUserSummary) []*internalproto.ClusterLoggedInUser {
	items := make([]*internalproto.ClusterLoggedInUser, 0, len(users))
	for _, user := range users {
		items = append(items, &internalproto.ClusterLoggedInUser{
			NodeId:   user.NodeID,
			UserId:   user.UserID,
			Username: user.Username,
		})
	}
	return items
}

func loggedInUsersFromCluster(users []*internalproto.ClusterLoggedInUser) []app.LoggedInUserSummary {
	items := make([]app.LoggedInUserSummary, 0, len(users))
	for _, user := range users {
		if user == nil {
			continue
		}
		items = append(items, app.LoggedInUserSummary{
			NodeID:   user.NodeId,
			UserID:   user.UserId,
			Username: user.Username,
		})
	}
	return items
}

func clusterQueryError(resp *internalproto.QueryLoggedInUsersResponse) error {
	if resp == nil || strings.TrimSpace(resp.ErrorCode) == "" {
		return nil
	}
	message := strings.TrimSpace(resp.ErrorMessage)
	if message == "" {
		message = "cluster query failed"
	}
	switch resp.ErrorCode {
	case "invalid_request":
		return fmt.Errorf("%w: %s", store.ErrInvalidInput, message)
	case "not_found":
		return fmt.Errorf("%w: %s", store.ErrNotFound, message)
	case "forbidden":
		return fmt.Errorf("%w: %s", store.ErrForbidden, message)
	default:
		return fmt.Errorf("%w: %s", app.ErrServiceUnavailable, message)
	}
}

func (m *Manager) beginLoggedInUsersQuery() (uint64, chan loggedInUsersQueryResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextLoggedInUsersQueryID++
	requestID := m.nextLoggedInUsersQueryID
	ch := make(chan loggedInUsersQueryResult, 1)
	m.pendingLoggedInUsers[requestID] = ch
	return requestID, ch
}

func (m *Manager) cancelLoggedInUsersQuery(requestID uint64, err error) {
	m.mu.Lock()
	ch, ok := m.pendingLoggedInUsers[requestID]
	if ok {
		delete(m.pendingLoggedInUsers, requestID)
	}
	m.mu.Unlock()

	if !ok {
		return
	}
	select {
	case ch <- loggedInUsersQueryResult{err: err}:
	default:
	}
	close(ch)
}

func (m *Manager) resolveLoggedInUsersQuery(requestID uint64, result loggedInUsersQueryResult) bool {
	m.mu.Lock()
	ch, ok := m.pendingLoggedInUsers[requestID]
	if ok {
		delete(m.pendingLoggedInUsers, requestID)
	}
	m.mu.Unlock()

	if !ok {
		return false
	}
	ch <- result
	close(ch)
	return true
}
