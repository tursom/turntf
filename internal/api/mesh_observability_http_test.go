package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/tursom/turntf/internal/app"
	"github.com/tursom/turntf/internal/store"
)

func TestMeshObservabilityHTTPStatusAndMetrics(t *testing.T) {
	t.Parallel()

	testAPI := newAuthenticatedTestAPIWithSink(t, fakeClusterStatusSink{
		status: app.ClusterStatus{
			NodeID: testNodeID(1),
			Mesh: app.ClusterMeshStatus{
				Enabled:            true,
				ForwardingEnabled:  true,
				BridgeEnabled:      false,
				NodeFeeWeight:      7,
				TopologyGeneration: 42,
				TransportCapabilities: []app.ClusterMeshTransportCapability{{
					Transport:           "websocket",
					InboundEnabled:      true,
					OutboundEnabled:     true,
					AdvertisedEndpoints: []string{"ws://127.0.0.1:9081/internal/cluster/ws"},
				}},
				TrafficRules: []app.ClusterMeshTrafficRule{{
					TrafficClass: "replication_stream",
					Disposition:  "deny",
				}},
				Routes: []app.ClusterMeshRoute{{
					DestinationNodeID:  testNodeID(2),
					TrafficClass:       "control_query",
					Reachable:          true,
					NextHopNodeID:      testNodeID(2),
					OutboundTransport:  "websocket",
					PathClass:          "direct",
					EstimatedCost:      13,
					TopologyGeneration: 42,
				}},
				Metrics: app.ClusterMeshMetrics{
					ForwardedPackets: []app.ClusterMeshMetricSample{{
						TrafficClass: "control_query",
						PathClass:    "direct",
						Value:        3,
					}},
					ForwardedBytes: []app.ClusterMeshMetricSample{{
						TrafficClass: "control_query",
						PathClass:    "direct",
						Value:        2048,
					}},
					RoutingNoPath: []app.ClusterMeshMetricSample{{
						TrafficClass: "snapshot_bulk",
						PathClass:    "no_path",
						Value:        2,
					}},
					DecisionCost: []app.ClusterMeshCostSample{{
						TrafficClass: "control_query",
						Value:        13,
					}},
					BridgeForwards: []app.ClusterMeshMetricSample{{
						TrafficClass: "control_critical",
						PathClass:    "cross_transport_bridge",
						Value:        1,
					}},
				},
			},
		},
	})

	adminKey := store.UserKey{NodeID: testNodeID(1), UserID: store.BootstrapAdminUserID}
	adminToken := loginToken(t, testAPI.handler, adminKey, "root-password")

	var status operationsStatus
	mustJSON(t, doJSONWithHeaders(t, testAPI.handler, http.MethodGet, "/ops/status", nil, map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK), &status)
	if !status.Mesh.Enabled || !status.Mesh.ForwardingEnabled || status.Mesh.BridgeEnabled || status.Mesh.NodeFeeWeight != 7 || status.Mesh.TopologyGeneration != 42 {
		t.Fatalf("unexpected mesh status: %+v", status.Mesh)
	}
	if len(status.Mesh.TransportCapabilities) != 1 || status.Mesh.TransportCapabilities[0].Transport != "websocket" {
		t.Fatalf("unexpected mesh transport capabilities: %+v", status.Mesh.TransportCapabilities)
	}
	if len(status.Mesh.TrafficRules) != 1 || status.Mesh.TrafficRules[0].TrafficClass != "replication_stream" || status.Mesh.TrafficRules[0].Disposition != "deny" {
		t.Fatalf("unexpected mesh traffic rules: %+v", status.Mesh.TrafficRules)
	}
	if len(status.Mesh.Routes) != 1 || !status.Mesh.Routes[0].Reachable || status.Mesh.Routes[0].EstimatedCost != 13 {
		t.Fatalf("unexpected mesh routes: %+v", status.Mesh.Routes)
	}
	if len(status.Mesh.Metrics.ForwardedPackets) != 1 || status.Mesh.Metrics.ForwardedPackets[0].Value != 3 {
		t.Fatalf("unexpected mesh metrics: %+v", status.Mesh.Metrics)
	}

	metrics := doPlain(t, testAPI.handler, http.MethodGet, "/metrics", map[string]string{
		"Authorization": "Bearer " + adminToken,
	}, http.StatusOK)
	for _, want := range []string{
		`forwarded_packets_total{node_id="4096",path_class="direct",traffic_class="control_query"} 3`,
		`forwarded_bytes_total{node_id="4096",path_class="direct",traffic_class="control_query"} 2048`,
		`routing_decision_cost{node_id="4096",traffic_class="control_query"} 13`,
		`routing_no_path_total{node_id="4096",traffic_class="snapshot_bulk"} 2`,
		`topology_generation{node_id="4096"} 42`,
		`node_fee_weight{node_id="4096"} 7`,
		`bridge_forward_total{node_id="4096",traffic_class="control_critical"} 1`,
	} {
		if !strings.Contains(metrics, want) {
			t.Fatalf("metrics missing %q in:\n%s", want, metrics)
		}
	}
}
