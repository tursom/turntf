# 分布式测试框架增强执行结果

本文档记录 [分布式测试框架增强计划](/root/dev/sys/turntf/docs/distributed-test-framework-enhancement.md) 在当前仓库中的实际落地结果。它不重复计划文档中的设计目标，而是回答“哪些阶段已经完成、代码证据在哪里、当前边界还剩什么”。

当前结论是：计划中的三层测试体系已经落地，统一节点夹具、固定回归集、故障注入场景驱动器和后续能力扩展点都已在仓库内形成可复用测试基础。后续能力实现可以沿用“先补夹具和断言，再补实现，再纳入回归”的节奏，而不需要继续临时拼装专用测试路径。

## 分阶段落地结果

### Phase 1：统一节点夹具与基础断言能力

已完成内容：

- `internal/cluster/cluster_test_fixture_test.go` 已提供统一节点测试句柄，封装单节点的构造、启动、关闭、日志采集、HTTP/API 访问、manager 状态读取和校时脚本注入。
- 当前夹具已经把原来散落在多节点测试中的监听器创建、节点拓扑拼装、等待逻辑和基础语义断言抽离成复用 helper。
- 节点配置已支持 `MessageWindowSize`、`ClockSkewMs`、`MaxClockSkewMs`、`TimeSyncSamples` 和 `DisableRouting` 等测试参数，便于后续协议能力在同一夹具中复用。

代表性证据：

- [cluster_test_fixture_test.go](/root/dev/sys/turntf/internal/cluster/cluster_test_fixture_test.go) 中的 `clusterTestNode`、`clusterTestNodeConfig`、`newClusterTestNodeFixture`、`newClusterTestNodes`。
- 同文件中的等待与语义断言 helper，例如 `waitForPeersActive`、`waitForMessagesEqual`、`waitForPeerAckReached`、`waitForRouteReachable`。

当前边界：

- 这一层仍是 `internal/cluster` 包内测试基础设施，不是对外公共测试库。
- 夹具负责统一节点生命周期和高频断言，不替代协议层的细粒度 handler 单测。

### Phase 2：固定回归集与协议层分层

已完成内容：

- `internal/cluster/cluster_regression_test.go` 已把核心分布式语义整理为固定回归集，不再依赖零散的一次性大用例。
- 回归集已覆盖双节点复制与 `Ack`、晚加入节点追平、跨多批次补拉、快照修复、相同与不同消息窗口下的收敛、三节点并发收敛、多跳路由与登录用户查询、时钟安全。
- 协议层的细粒度错误路径和状态机行为仍保留在 `internal/cluster/manager_test.go`，并通过 `internal/cluster/cluster_protocol_harness_test.go` 补充可控的 handler 入口验证，避免所有协议断言都依赖真实网络场景。

代表性证据：

- [cluster_regression_test.go](/root/dev/sys/turntf/internal/cluster/cluster_regression_test.go) 中的 `TestRegressionTwoNodeReplicationAckAndReconnect`、`TestRegressionLateJoinerCatchup`、`TestRegressionSnapshotRepair`、`TestRegressionMessageWindowConvergence`、`TestRegressionThreeNodeConcurrentConvergence`、`TestRegressionMultiHopRoutingAndLoggedInUsers`、`TestRegressionClockSafety`。
- [manager_test.go](/root/dev/sys/turntf/internal/cluster/manager_test.go) 中的 `TestHandleEventBatchAckMeansAppliedToLocalState`、`TestHandleEventBatchDuplicateDeliveryIsIdempotentAndStillAcks`、`TestHandleAckPersistsPeerCursor`、未来时间戳拒绝与写闸门相关测试。
- [cluster_protocol_harness_test.go](/root/dev/sys/turntf/internal/cluster/cluster_protocol_harness_test.go) 中的 `TestProtocolHarnessDirectHandlerQueue`。

当前边界：

- 协议层部分细粒度错误路径仍主要保留在 `manager_test.go`，尚未全部迁移到更高层抽象。
- 当前分层策略已经清晰，但协议层和多节点层仍共享同一包内测试上下文，而不是完全拆分为独立测试模块。

### Phase 3：场景驱动器与故障注入能力

已完成内容：

- `internal/cluster/cluster_fault_test_fixture_test.go` 已实现统一场景驱动器 `clusterTestScenario`，按“消息收发 + 连接生命周期 + 故障注入点”组织多节点仿真测试。
- 当前驱动器已经支持拓扑搭建、节点选择启动、链路开关、网络分区、重复投递、乱序重放、延迟、丢弃、held frame 刷新、jitter、快照触发和校时脚本控制。
- 高层等待与断言接口已能直接表达复制语义，而不是在每个测试里重新拼接轮询逻辑。

代表性证据：

- [cluster_fault_test_fixture_test.go](/root/dev/sys/turntf/internal/cluster/cluster_fault_test_fixture_test.go) 中的 `newClusterTestScenario`、`Partition`、`Heal`、`DropNext`、`DuplicateNext`、`Hold`、`DelayNext`、`FlushHeld`、`TriggerSnapshotDigest`、`SetTimeSyncSamples`、`WaitForSnapshotRepairCompleted`。
- [cluster_fault_simulation_test.go](/root/dev/sys/turntf/internal/cluster/cluster_fault_simulation_test.go) 中的：
  - `TestSimulationNetworkPartitionRecoveryConverges`
  - `TestSimulationOutOfOrderDuplicateReplayConverges`
  - `TestSimulationBroadcastDuringPullCatchupConverges`
  - `TestSimulationSnapshotRepairDuringEventStreamConverges`
  - `TestSimulationLinkJitterLateJoinerConverges`
  - `TestSimulationDroppedBroadcastRepairsThroughSnapshot`
  - `TestSimulationDelayedBroadcastConverges`
  - `TestSimulationScriptedTimeSyncCanAdvancePeerOffset`

当前边界：

- 场景驱动器虽然已经按传输无关的动作抽象测试步骤，但底层实现当前仍依托现有 WebSocket 测试链路。
- 该驱动器服务于仓库内部回归测试，不承担外部编排系统或独立仿真平台职责。

### Phase 4：后续能力扩展点

已完成内容：

- `internal/cluster/cluster_extension_points_test.go` 已验证后续能力接入所需的扩展点，包括握手观测、membership 链路别名、语义断言扩展、晚加入/移除/恢复、重复发现链路切换和 routing 能力开关。
- 这些测试证明当前框架不只覆盖现有复制语义，还能承载后续黑名单、自动发现、mTLS 或 routing 能力增强所需的接入形态。

代表性证据：

- [cluster_extension_points_test.go](/root/dev/sys/turntf/internal/cluster/cluster_extension_points_test.go) 中的 `TestExtensionPointHandshakeObservation`、`TestExtensionPointMembershipLinkAliases`、`TestExtensionPointCustomSemanticAssertion`、`TestExtensionPointMembershipLateJoinRemoveAndRecover`、`TestExtensionPointDiscoveryDuplicateLinkJitterSwitchesAliases`、`TestExtensionPointRoutingCanBeDisabledForRelay`。

当前边界：

- 当前扩展点测试验证的是“能接入什么形态”，并不等于后续特性语义本身已经定义完成。
- 黑名单、mTLS、自动发现等未来能力仍需要各自的语义文档、实现和回归集补充。

## 已覆盖能力清单

当前框架已经稳定覆盖以下能力：

- 单节点测试句柄：统一构造、启动、关闭、日志采集、HTTP/API 访问与 manager 观测。
- 拓扑搭建：双节点、三节点、链式、带别名链路和部分节点延迟启动。
- 链路控制：`Partition`、`Heal`、`DisableLink`、`EnableLink`、`Jitter`。
- 报文扰动：`DropNext`、`DuplicateNext`、`Hold`、`DelayNext`、`FlushHeld`。
- 快照与校时控制：`TriggerSnapshotDigest`、`TriggerTimeSync`、`SetTimeSyncSamples`。
- 语义断言：peer 活跃/失活、用户可见、消息窗口一致、`Ack` 推进、路由可达、快照修复完成、时钟偏移可观测。
- 固定回归集：复制、补拉、快照修复、消息窗口、并发收敛、路由传播、时钟安全。
- 扩展点验证：握手观测、membership alias、动态链路切换、自定义语义断言和 routing 开关。

## 验证结果

以下命令在 2026-04-14 对当前仓库状态执行并通过：

```bash
go test ./internal/store -count=1
go test ./internal/cluster -count=1
```

对应结论：

- `store` 层收敛规则测试当前可通过，说明字段级 `LWW`、墓碑、消息窗口和幂等吸收等基础规则处于可回归状态。
- `cluster` 层夹具、固定回归集、故障注入场景和扩展点测试当前可通过，说明增强计划落地后的测试体系已经形成稳定基线。

## 当前边界与未扩展部分

- 当前测试框架仍是仓库内部测试基础设施，不是对外复用的公共测试 SDK。
- 协议层部分细粒度错误路径和 handler 级状态机断言仍主要保留在 [manager_test.go](/root/dev/sys/turntf/internal/cluster/manager_test.go)，而不是全部上移到场景驱动器。
- 多节点仿真虽然已经通过通用动作封装 WebSocket 链路控制，但底层仍然依赖当前 HTTP + WebSocket + Protobuf 组合。
- 结果文档只总结当前仓库状态，不追溯具体提交历史，也不把现有实现外推为未来特性的语义承诺。

## 与其他文档的关系

- [分布式测试框架增强计划](/root/dev/sys/turntf/docs/distributed-test-framework-enhancement.md) 负责描述目标架构、分阶段设计与验收标准。
- [分布式系统未来演进路线图](/root/dev/sys/turntf/docs/distributed-system-roadmap.md) 负责说明测试增强在整体演进中的位置与依赖关系。
- [复制语义专题文档](/root/dev/sys/turntf/docs/replication-semantics.md) 负责定义测试断言应对齐的当前语义边界。

后续若测试框架继续扩展，应优先更新本结果文档中的“已落地能力、验证基线与当前边界”，并在计划文档中保留设计目标和未来扩展方向。
