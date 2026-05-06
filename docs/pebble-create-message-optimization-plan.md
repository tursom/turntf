# Pebble CreateMessage 优化计划

本文档聚焦 `store` 层 `CreateMessage` 在 `Pebble` 模式下的当前热点。和早期版本相比，这条写路径已经经历了多轮优化，文档里的很多旧前提已经不再成立；因此这里的目标不再是解释“为什么 `Pebble` 明显慢于 `SQLite`”，而是记录当前真实边界：哪些优化已经落地，哪些仍值得继续推进。

相关基线见 [performance-baseline.md](./performance-baseline.md)。

## 1. 当前结论

### 1.1 `Pebble` 已不能简单视为“明显慢于 SQLite”

当前 `BenchmarkStoreCreateMessage` 已拆成以下矩阵：

- `sqlite`
- `pebble/balanced/no_sync`
- `pebble/throughput/no_sync`
- `pebble/balanced/force_sync`
- `pebble/throughput/force_sync`

同时还新增了：

- `BenchmarkStoreCreateMessageSteadyState`
- `BenchmarkStoreCreateMessageParallel`

我在 2026-05-06 本地执行了一次快速核对：

```bash
go test ./internal/store -run '^$' -bench '^BenchmarkStoreCreateMessage$' -benchtime=1x -count=1
```

这次快速结果里：

- `tmp` 和 `disk` 场景下，`pebble/*/no_sync` 都已经明显快于 `sqlite`。
- 当前最贵的子场景是 `pebble/*/force_sync`，尤其是 `disk` 模式下单次同步提交成本很高。

因此，当前问题已经不是“`Pebble` 天生比 `SQLite` 慢”，而是：

- `force_sync` 的 durability 成本仍然很高。
- `CreateMessage` 仍包含一部分 SQLite 元数据读取和多索引写放大。
- 大量订阅者 / 超窗 trim / 快照修复等边界路径仍会放大成本。

### 1.2 `performance-baseline.md` 里的旧表格是历史样本

[performance-baseline.md](./performance-baseline.md) 中 `CreateMessage` 的旧表格仍保留了早期聚合样本，那里还没有拆出 `balanced|throughput` 与 `no_sync|force_sync` 四类子场景，也不包含 steady-state / parallel benchmark。当前优化决策应以现有 benchmark 矩阵和代码实现为准，而不能继续沿用“`Pebble` 整体显著慢于 `SQLite`”这一旧结论。

## 2. 当前 Pebble 写路径

### 2.1 仍然是混合引擎，但热路径已经不再走 SQLite 事务

`CreateMessage()` 入口仍会先做参数校验，然后在 `Pebble` 后端里执行：

- `recipient` / `sender` 读取
- 黑名单检查

对应实现见 [`store_backend.go`](../internal/store/store_backend.go) 的 `pebbleStoreBackend.CreateMessage()`。

这些读取仍然依赖 `UserRepository`、`BlacklistRepository`、`SubscriptionRepository` 等仓储；按当前实现边界，它们背后仍主要是 SQLite 数据，而不是 Pebble。

但是，完成这些前置检查之后，本地消息写入已经不再进入 SQLite 事务。当前路径会直接进入 [`pebble_local_message_writer.go`](../internal/store/pebble_local_message_writer.go) 的本地批处理循环，由 Pebble 批量完成：

- 本地消息序号预留
- 本地事件日志写入
- 消息投影写入
- 用户消息状态更新
- 必要时的同步 trim

### 2.2 本地消息已经有专门的 Pebble fast path

当前 `Pebble` 本地消息不会再走“通用 `Append()` + 通用投影”的老路径，而是通过 `submitLocalMessage()` 把请求送进本地批处理循环：

- 同一个 batch 最多聚合 `128` 个请求。
- batch 内会按 `PebbleMessageSyncMode` 分段，保证 `no_sync` / `force_sync` 不混写。
- 每段在持有 `eventLog.mu` 和用户分片锁的情况下，直接拼装一份 Pebble batch。
- 本地消息事件直接写 `event/seq`、`event/origin`、`meta/event_sequence`，不再做复制路径需要的幂等查重。

对应实现见：

- [`pebble_local_message_writer.go`](../internal/store/pebble_local_message_writer.go)
- [`pebble_projection.go`](../internal/store/pebble_projection.go)

复制事件仍保留单独的幂等路径：`AppendReplicated()` 会先检查 `originKey` 是否已存在。这条边界仍然存在，但已经只属于复制路径，而不是本地 `CreateMessage` 主路径。

### 2.3 消息序号、cursor、pending projection 已经迁到 Pebble

旧版文档把 `message_sequence_counters`、`peer_ack_cursors`、`origin_cursors`、`pending_projections` 都视为 SQLite 热路径，这已经不准确。

当前现状：

- 消息序号由 [`pebble_message_sequence.go`](../internal/store/pebble_message_sequence.go) 管理。
- `peer ack cursor` / `origin cursor` 由 [`pebble_metadata.go`](../internal/store/pebble_metadata.go) 管理。
- `pending projection` 也由 [`pebble_metadata.go`](../internal/store/pebble_metadata.go) 管理。

这些 Pebble 元数据仍保留了对旧 SQLite 数据的兼容 seed 逻辑，例如：

- 首次读取消息序号时，会尝试从旧的 SQLite 计数器和旧投影里取最大值作为起点。
- 但一旦进入 Pebble 路径，后续就不再继续更新 SQLite 里的同名计数表。

对应验证见 [`store_pebble_test.go`](../internal/store/store_pebble_test.go) 中的：

- `TestPebbleMessageSequenceSeedsFromLegacySQLiteCounter`
- `TestPebblePeerAndOriginCursorsBypassSQLite`
- `TestPebblePendingProjectionsBypassSQLite`

## 3. 已落地的优化阶段

### 3.1 旧工作点 1：消息 body 单副本化

状态：已落地，但实现形态比旧计划更细。

当前实现不是“所有索引都统一存引用”，而是：

- `message/id/...` 始终保存完整消息主记录。
- `message/producer/...` 当前走引用值。
- `message/user/...` 与部分 inbox 热索引默认走引用值。
- `throughput` profile 下，小 value 会允许直接内联在热点索引里；大 value 仍保留引用。

对应实现见 [`pebble_projection.go`](../internal/store/pebble_projection.go) 的：

- `pebbleMessageIndexValue()`
- `messageIndexValueForProfile()`
- `messageFromIndexValue()`

对应验证见 [`store_pebble_test.go`](../internal/store/store_pebble_test.go) 的：

- `TestPebbleThroughputProfileInlinesSmallHotIndexes`
- `TestPebbleThroughputProfileKeepsLargeHotIndexesReferenced`

这意味着旧版“3 份完整 protobuf value”已经不再是当前主路径的现状描述。

### 3.2 旧工作点 2：Pebble 本地 append 快路径

状态：已落地，并进一步演化为本地批处理写入器。

当前不只是 `Append()` / `AppendReplicated()` 分流了，本地消息甚至已经不再逐条调用通用 event log append，而是：

- 在 `pebble_local_message_writer.go` 里直接生成 `EventID` / `Sequence`
- 直接把 event log 和消息投影塞进同一个 batch
- 再按 `force_sync` 或 `no_sync` 一次提交

因此，旧版“本地消息也会先查 `originKey` 去重”的描述已经失效。

### 3.3 旧工作点 3：trim 阈值化 / 延迟化

状态：已落地，并且已经有后台 trim worker。

当前消息写入后不会无条件同步 trim，而是使用：

- `windowSize + 32` 作为普通 trim 调度阈值
- `windowSize + 128` 作为硬阈值；超过后会尝试同步 trim
- 后台 trim worker 会异步消费 dirty user 集合

对应实现见：

- [`pebble_message_state.go`](../internal/store/pebble_message_state.go)
- [`pebble_projection.go`](../internal/store/pebble_projection.go)

对应验证见 [`store_pebble_test.go`](../internal/store/store_pebble_test.go) 的：

- `TestPebbleDeferredTrimKeepsVisibleWindowBounded`
- `TestPebbleBackgroundTrimEventuallyUpdatesMessageUserState`
- `TestPebbleSnapshotApplyRepairsMessageUserState`

因此，旧版“每次写完都会立即全量读该用户消息并同步 trim”的说法也已经不再准确；它只在超过硬阈值或快照修复等边界场景下才会同步发生。

### 3.4 旧工作点 4：成功路径跳过无意义的 pending projection delete

状态：已落地。

当前 `Pebble` 成功路径不会再在 `CreateMessage()` 末尾额外做一次 SQLite `clearPendingProjection()`。`pending projection` 的记录与清理都已经切到 Pebble 元数据仓储，并只在真正的 deferred / replay 路径上使用。

### 3.5 旧工作点 5：消息序号与热点元数据去 SQLite 化

状态：部分落地。

已迁出 SQLite 的部分：

- `message sequence`
- `peer ack cursor`
- `origin cursor`
- `pending projection`

仍保留在 SQLite 的部分：

- 用户 / 登录名
- 订阅关系
- 黑名单
- 附件
- `user_metadata`

也就是说，当前 `Pebble` 仍然是混合引擎，但“消息序号和复制游标仍完全依赖 SQLite”的旧表述已经过期。

## 4. 当前仍然值得优化的点

### 4.1 `force_sync` 仍然是最重的子场景

当前本地消息 batch 在 `force_sync` 模式下会直接执行 `batch.Commit(pebble.Sync)`，对应实现见 [`pebble_local_message_writer.go`](../internal/store/pebble_local_message_writer.go) 的 `commitLocalMessageBatch()`。

这意味着：

- `force_sync` 的最终成本主要受磁盘同步影响。
- 当前本地消息 fast path 不会复用 [`pebble_write_coordinator.go`](../internal/store/pebble_write_coordinator.go) 的 relaxed group commit。
- 如果未来要继续优化 `force_sync`，需要单独评估它的语义边界，而不是简单套用 `no_sync` 的思路。

这是当前最值得继续单独盯住的热点。

### 4.2 SQLite 元数据读取仍然在主路径前半段

虽然消息写入本身已经走 Pebble，但下面这些读仍然存在：

- `recipient` / `sender` 查询
- 黑名单检查
- channel 消息时的订阅者查询

其中 channel / broadcast / inbox 扇出还会进一步触发订阅和收件箱索引写入。对于高扇出场景，当前成本不再主要来自“消息 body 重复存 3 份”，而是来自：

- 元数据读取
- inbox fan-out
- 多 key 写入

### 4.3 消息投影仍然是多索引写路径

当前一条直接消息通常至少会写：

- `message/id`
- `message/user`
- `message/producer`
- `message/session`
- 登录用户自己的 `inbox`

如果接收方是 channel，还会为每个订阅者追加：

- `inbox`
- `inbox_source`

所以，哪怕 body 单副本化已经完成，当前写路径仍然不是“单 key 写入”。如果后续还要继续压榨吞吐，高扇出 inbox 路径比“消息主记录本身”更值得重点 profile。

### 4.4 硬 trim / snapshot repair 仍会触发全量扫描

一旦进入：

- 超过硬阈值的同步 trim
- `ApplyMessageSnapshotRows()` 后的强制 trim

当前实现仍会：

- 枚举该用户当前已存消息
- 删除超窗消息的主记录与索引
- 刷新 `message user state`

对应实现见 [`pebble_message_state.go`](../internal/store/pebble_message_state.go) 的 `trimMessagesForUserLocked()`。

这条路径已经不再是“每次写入都发生”的主路径问题，但在：

- 超大窗口
- 历史堆积后首次回收
- 快照修复

这些场景下，仍然会是值得继续优化的点。

## 5. 当前阶段计划

### 阶段 A：保持现有 fast path 不回退

目标：

- 不破坏本地消息 loop 的顺序与批处理语义。
- 不破坏 `message sequence` / `cursor` / `pending projection` 的 Pebble 持久化边界。
- 不把成功路径重新引回 SQLite 事务。

### 阶段 B：针对 `force_sync` 单独做 profile

目标：

- 明确 `force_sync` 当前成本究竟主要来自磁盘同步、batch 切分，还是前置元数据读取。
- 评估是否需要额外的批量提交策略，或者仅把 `force_sync` 视为高可靠低吞吐模式而单独监控。

非目标：

- 不为了 benchmark 数字去削弱 `force_sync` 的 durability 语义。

### 阶段 C：评估 inbox fan-out 与混合元数据读取

目标：

- 重点看 channel / 广播 / 大量登录用户在线时的 inbox 写放大。
- 评估是否要继续把更多只读元数据迁离 SQLite，还是先接受混合引擎边界。

### 阶段 D：按需优化 trim / snapshot repair

目标：

- 只在 profile 证明 trim 或 snapshot repair 成为热点时，再考虑更复杂的增量回收策略。
- 在优化前保持当前 `visible window bounded`、`snapshot repair 可收敛` 的行为语义不变。

## 6. 验收与验证

每次继续优化这条路径时，至少应跑：

```bash
go test ./internal/store -count=1
go test ./internal/store -run '^$' -bench 'BenchmarkStore(CreateMessage|CreateMessageSteadyState|CreateMessageParallel|ListMessagesByUser|PruneEventLogOnce)' -benchmem -count=1
```

重点关注：

- `BenchmarkStoreCreateMessage/*/pebble/balanced/no_sync/*`
- `BenchmarkStoreCreateMessage/*/pebble/throughput/no_sync/*`
- `BenchmarkStoreCreateMessage/*/pebble/*/force_sync/*`
- `BenchmarkStoreCreateMessageSteadyState/*/pebble/*`
- `BenchmarkStoreCreateMessageParallel/*/pebble/*`

通过标准：

- `no_sync` 主路径不出现明显回退。
- `force_sync` 的任何优化都必须单独看语义与成本，不能只看吞吐数字。
- `ListMessagesByUser`、`PruneEventLogOnce`、snapshot repair 相关路径不能因为写入优化而出现异常退化。

## 7. 一句话总结

当前 `Pebble` `CreateMessage` 的核心事实已经从“整体慢于 SQLite”变成了：

- 主写路径上的本地 fast path、消息序号去 SQLite 化、body 去重、延迟 trim 都已经落地。
- 现在真正需要继续关注的是 `force_sync`、inbox fan-out、混合元数据读取，以及硬 trim / snapshot repair 的边界成本。
