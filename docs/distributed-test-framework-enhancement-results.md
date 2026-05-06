# 分布式测试框架增强执行结果

本文档记录 [分布式测试框架增强计划](/root/dev/sys/turntf/turntf/docs/distributed-test-framework-enhancement.md) 在当前仓库中的实际落地结果。它只描述当前代码树里已经存在的测试基础设施、覆盖边界和验证结果，不再沿用已经不存在的历史测试文件名。

当前结论是：计划中的三层测试思路已经基本落地，但实际代码形态和早期设想不同。当前仓库并没有以 `cluster_test_fixture_test.go`、`cluster_regression_test.go`、`cluster_fault_*`、`cluster_extension_points_test.go` 这组文件名承载结果；相关能力现在主要分布在 `internal/store` 的收敛测试、`internal/cluster` 的 helper + manager/mesh 集成测试，以及 `internal/mesh` 的 runtime/forwarding/scale 测试中。

## 当前代码落点

### 1. 统一 helper 与节点启动骨架

当前统一测试 helper 主要落在以下文件：

- [manager_test_helpers_test.go](/root/dev/sys/turntf/turntf/internal/cluster/manager_test_helpers_test.go)：提供日志采集、临时 store、`waitFor`、HTTP 测试服务、JSON 请求辅助、握手与快照断言等公共 helper。
- [linear_websocket_test_helpers_test.go](/root/dev/sys/turntf/turntf/internal/cluster/linear_websocket_test_helpers_test.go)：提供线性多节点 WebSocket/mesh 启动骨架，例如 `startLinearWebSocketManagers`、`startLinearWebSocketManagersWithStoreFactory`、`startLinearMeshManagers` 和 `waitForMeshRoute`。

这部分已经承担了原计划里“统一节点夹具”和“高频等待/断言收敛”的职责，但仍然是 `internal/cluster` 包内测试基础设施，不是对外公共测试库。

### 2. 协议状态机与复制回归

当前协议层和复制语义的核心回归测试主要集中在：

- [manager_test.go](/root/dev/sys/turntf/turntf/internal/cluster/manager_test.go)：覆盖握手方向裁决、`Ack` 语义、补拉、retention 截断后的快照修复、消息窗口不一致时跳过消息快照、快照分区顺序、未来时间戳拒绝、首次可信校时前写闸门、`Status` 输出等。
- [replication_batcher_test.go](/root/dev/sys/turntf/turntf/internal/cluster/replication_batcher_test.go)：覆盖按 origin 分批、批量 apply 后 `Ack` 推进，以及事件批与快照分区对 snapshot digest dirty 标记的影响。
- [ephemeral_state_test.go](/root/dev/sys/turntf/turntf/internal/cluster/ephemeral_state_test.go) 与 [mesh_membership_clock_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_membership_clock_test.go)：补充覆盖 runtime epoch、断链怀疑、membership 广播和 mesh 校时边界。

这部分已经形成固定回归集，但仍然以包内 helper 和真实 `Manager` 组合为主，没有额外拆成独立测试模块。

### 3. 真实多节点 / mesh 集成测试

当前多节点端到端验证主要落在以下文件：

- [mesh_manager_integration_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_manager_integration_test.go)：覆盖 `Manager.Start()` 自动挂接 mesh runtime、discovered peer 作为 dial seed、configured peer `node_id` 回填、动态 slot 重平衡等。
- [mesh_replication_integration_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_replication_integration_test.go)：覆盖多跳复制、跨 mesh 多跳快照修复，以及高费用 transit 节点拒绝复制流量和快照流量。
- [mesh_data_plane_integration_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_data_plane_integration_test.go)：覆盖 `QueryLoggedInUsers`、presence mirror、`ResolveUserSessions`、瞬时包多跳路由、`target_session`、TTL 递减、重试队列清理、snapshot mirror 查询与转发指标。
- [mesh_large_cluster_integration_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_large_cluster_integration_test.go)：覆盖 7 节点线性 WebSocket 拓扑，以及 5 节点 WebSocket/LibP2P 交替桥接拓扑下的查询、瞬时包和 forwarding metrics。

这部分已经承担了原计划里的“固定回归集”和“大拓扑真实节点验证”职责。

### 4. 多传输桥接与适配器测试

当前多传输测试已经直接落地：

- [mesh_mixed_transport_integration_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_mixed_transport_integration_test.go)：验证 WebSocket <-> LibP2P 桥接下，查询和瞬时包可以跨桥转发，而复制流和快照流按当前策略返回 `mesh.ErrNoRoute`。
- [mesh_mixed_transport_zeromq_integration_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_mixed_transport_zeromq_integration_test.go)：在 `zeromq` build tag 下验证 WebSocket <-> ZeroMQ 桥接的查询和瞬时包路径。
- [mesh_transport_adapter_libp2p_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_transport_adapter_libp2p_test.go)、[mesh_transport_adapter_zeromq_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_transport_adapter_zeromq_test.go)、[transport_websocket_test.go](/root/dev/sys/turntf/turntf/internal/cluster/transport_websocket_test.go)、[transport_libp2p_test.go](/root/dev/sys/turntf/turntf/internal/cluster/transport_libp2p_test.go)：覆盖 hello 交换、capability 克隆、relay hint、直连 peer ID 和底层传输读写语义。

这部分也说明：当前实现明确支持“控制流量和瞬时流量可跨桥”，但并不把复制流量和快照流量跨桥作为成功路径。

### 5. `internal/mesh` runtime 层测试

原计划里一部分“故障注入 / 拓扑收敛 / forwarding 策略”能力，现在主要由 `internal/mesh` 自身测试承担，而不是单独的 `clusterTestScenario` 驱动器：

- [runtime_integration_test.go](/root/dev/sys/turntf/turntf/internal/mesh/runtime_integration_test.go)：覆盖三节点收敛、query/replication 包跨 transit 路由、mixed transport、bridge 指标和 invalid transit policy。
- [runtime_flooding_test.go](/root/dev/sys/turntf/turntf/internal/mesh/runtime_flooding_test.go) 与 [runtime_scale_test.go](/root/dev/sys/turntf/turntf/internal/mesh/runtime_scale_test.go)：覆盖拓扑 flooding、线性拓扑收敛、大规模节点传播和 next-hop 故障切换。
- [forwarding_test.go](/root/dev/sys/turntf/turntf/internal/mesh/forwarding_test.go)、[planner_test.go](/root/dev/sys/turntf/turntf/internal/mesh/planner_test.go)、[topology_test.go](/root/dev/sys/turntf/turntf/internal/mesh/topology_test.go)：覆盖转发引擎去重、TTL/环路防护、路径规划、bridge 成本策略和拓扑 store 一致性。

因此，原文里“统一场景驱动器已完成”的结论已经不适用于当前仓库。当前确实已有较完整的故障与收敛测试，但实现形态是“cluster 集成测试 + mesh runtime 测试”组合，而不是单一场景驱动器。

## 已覆盖边界

当前仓库已经明确覆盖以下能力：

- `store` 层的字段级 `LWW`、墓碑、消息窗口、登录名和用户元数据等本地收敛规则，见 [internal/store](/root/dev/sys/turntf/turntf/internal/store) 下测试文件。
- `cluster manager` 层的握手、`Ack`、补拉、retention 截断后的快照修复、时钟写闸门和未来时间戳拒绝。
- mesh 多跳下的复制、快照修复、已登录用户查询、presence mirror、`ResolveUserSessions` 和瞬时包路由。
- `target_session`、TTL 递减、retry queue 清理、snapshot mirror 命中与 forwarding metrics。
- WebSocket <-> LibP2P 与 WebSocket <-> ZeroMQ 桥接下的控制查询和瞬时包路径。
- 线性大拓扑与 mixed transport 大拓扑下的路由可达性、bridge 指标和 no-route 策略。
- mesh runtime 层的 flooding、reroute、forwarding policy、bridge 路径选择和 topology store 一致性。

## 当前边界

- 当前没有按计划文档原样落地一个统一的 `clusterTestScenario` 故障注入驱动器；故障、拓扑和时序覆盖分散在 `internal/cluster` 与 `internal/mesh` 测试中。
- 当前测试框架仍是仓库内部测试基础设施，不是对外复用的公共测试 SDK。
- `zeromq` 相关端到端场景需要 `zeromq` build tag；默认 `go test ./...` 不会覆盖 [mesh_mixed_transport_zeromq_integration_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_mixed_transport_zeromq_integration_test.go) 和 [mesh_transport_adapter_zeromq_test.go](/root/dev/sys/turntf/turntf/internal/cluster/mesh_transport_adapter_zeromq_test.go)。
- 当前策略下，跨桥复制和跨桥快照被视为不支持路径，并由测试显式断言 `mesh.ErrNoRoute`，而不是当作失败待修问题。

## 验证结果

当前 CI 基线以 [verify.yml](/root/dev/sys/turntf/turntf/.github/workflows/verify.yml) 为准，执行的是：

```bash
go test ./... -count=1
./scripts/smoke.sh
```

我在 2026-05-06 本地核对当前仓库时得到的结果是：

- `./scripts/smoke.sh` 通过。
- `go test ./... -count=1` 未全部通过，失败点是 [runtime_scale_test.go](/root/dev/sys/turntf/turntf/internal/mesh/runtime_scale_test.go) 中的 `TestRuntimeLinearFloodingConvergesAt50Nodes` 超时。
- 同一次 `go test ./... -count=1` 输出里，[internal/cluster](/root/dev/sys/turntf/turntf/internal/cluster) 与 [internal/store](/root/dev/sys/turntf/turntf/internal/store) 包本身已经通过，因此本文件涉及的 cluster/store 测试证据仍然存在，但“全仓测试基线已全部通过”的旧结论不再成立。

## 与其他文档的关系

- [分布式测试框架增强计划](/root/dev/sys/turntf/turntf/docs/distributed-test-framework-enhancement.md) 负责描述目标架构、分阶段设计与验收标准。
- [分布式系统未来演进路线图](/root/dev/sys/turntf/turntf/docs/distributed-system-roadmap.md) 负责说明测试增强在整体演进中的位置与依赖关系。
- [复制语义专题文档](/root/dev/sys/turntf/turntf/docs/replication-semantics.md) 负责定义 `Ack`、补拉、快照和消息窗口等测试断言应对齐的语义边界。

后续如果继续增强测试基础设施，应优先更新本文档里的“实际落点、当前覆盖和验证结果”，而不是恢复对已经不存在文件名和旧验证结论的引用。
