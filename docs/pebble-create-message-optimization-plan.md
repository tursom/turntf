# Pebble CreateMessage 优化计划

本文档聚焦 `store` 层 `CreateMessage` 在 `Pebble` 模式下的热点问题，目标是解释“为什么当前 `Pebble` 写入仍明显慢于 `SQLite`”，并把后续优化拆成可执行的工作点。

相关基线见 [performance-baseline.md](/root/dev/sys/turntf/turntf/docs/performance-baseline.md)。

## 1. 背景

当前 `BenchmarkStoreCreateMessage` 的结果说明，`Pebble` 的典型消息写入吞吐仍明显落后于 `SQLite`：

- `sqlite / 256B`：约 `0.26ms/op`
- `sqlite / 4KiB`：约 `0.29ms/op`
- `pebble / 256B`：约 `0.88ms/op`
- `pebble / 4KiB`：约 `2.8ms/op`

这不是因为 “`Pebble` 一定比 `SQLite` 慢”，而是因为当前仓库里的 `Pebble` 模式仍然是“SQLite 控制面 + Pebble 事件日志/投影”的混合写路径，而且 `Pebble` 侧存在明显的写放大。

## 2. 当前瓶颈

### 2.1 Pebble 模式并没有绕开 SQLite 热路径

`CreateMessage()` 先进入 SQLite 事务做用户读取、黑名单判断、消息序号分配，然后才进入事件写入和投影阶段，[messages.go](/root/dev/sys/turntf/turntf/internal/store/messages.go:21)。

具体来说：

- `recipient` / `sender` 查询仍走 SQLite。
- `nextMessageSeqTx()` 仍通过 `message_sequence_counters` 和 `messages` 表维护序号，[messages.go](/root/dev/sys/turntf/turntf/internal/store/messages.go:92)。
- 成功写完投影后，仍会额外执行一次 `clearPendingProjection()` 的 SQLite delete，[messages.go](/root/dev/sys/turntf/turntf/internal/store/messages.go:82)。

因此当前 `Pebble` 并不是“单引擎写入”，而是“先做一段 SQLite 事务，再做一段 Pebble 写入”。

### 2.2 Pebble 事件日志本地写入仍然偏重

在 `Pebble` 模式下，`insertEvent()` 直接走 `eventLog.Append()`，[events.go](/root/dev/sys/turntf/turntf/internal/store/events.go:27)。

而 `pebbleEventLogRepository.appendStored()` 当前本地写入仍会做这些事：

- 先对 `originKey` 做一次 `Get` 查重，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:74)。
- 再读取 `meta/event_sequence` 推导下一序号，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:82)。
- 最终一次消息事件会落成 3 个 key：
  - `event/seq/...`
  - `event/origin/...`
  - `meta/event_sequence`
  见 [pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:88)。

对“本地新建消息”来说，前置查重实际上没有必要，因为 `EventID` 本来就是本地新生成的。

### 2.3 消息投影存在明显写放大

`ApplyMessageCreated()` 在 `Pebble` 模式下会：

- 先 `messageExists()` 做一次点查，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:284)。
- 再 `putMessage()` 写入投影，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:289)。
- 最后立即 `trimMessagesForUser()`，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:292)。

其中最重的问题是 `putMessage()`：

- 它会为同一条消息写 3 个 key，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:483)。
- 这 3 个 key 的 value 都是完整 protobuf 消息，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:571)。
- 对 `4KiB` payload，这意味着消息体被重复序列化、重复复制、重复落盘。

这也是为什么 `Pebble / 4KiB` 比 `Pebble / 256B` 恶化得更明显。

### 2.4 Pebble trim 是“写后立刻全量读一遍”

`trimMessagesForUser()` 当前逻辑是：

- 先 `listRawMessagesByUser()` 把该用户的消息全部迭代出来，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:529)。
- 迭代过程中还要把每条记录反序列化成 `Message`，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:511)。
- 再删除超窗消息的 3 组索引 key，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:539)。

也就是说，当前一次写入路径实际上是：

- SQLite 前置检查
- Pebble 事件日志写
- Pebble 投影写
- Pebble 全量读该用户消息
- Pebble 删除超窗索引
- SQLite 清理 pending projection

而 `SQLite` 对应路径只是：

- 一次 SQLite 事务内写事件
- 一次 SQLite 事务内写消息投影
- 一条 SQL 做 trim

见 [sqlite_projection.go](/root/dev/sys/turntf/turntf/internal/store/sqlite_projection.go:212) 和 [sqlite_projection.go](/root/dev/sys/turntf/turntf/internal/store/sqlite_projection.go:698)。

## 3. 优化目标

本轮优化目标：

- 把 `BenchmarkStoreCreateMessage/*/pebble/256B` 压到接近 `0.6ms/op` 量级。
- 把 `BenchmarkStoreCreateMessage/*/pebble/4KiB` 压到接近 `1.2ms/op` 量级。
- 不改变外部 API。
- 不改变“成功返回后消息已本地可见”的语义。

非目标：

- 本轮不先改 `ListMessagesByUser` 聚合读路径。
- 本轮不先改 snapshot message rebuild 的整体结构。
- 本轮不要求一步把所有 SQLite 热路径完全移除。

## 4. 优先级与工作点

建议分两批推进。

### 第一批：直接降低 CreateMessage 写放大

- 工作点 1：消息 body 单副本化
- 工作点 2：Pebble 本地 append 快路径
- 工作点 3：写后立即 trim 改阈值化/延迟化

### 第二批：继续去掉 SQLite 控制面残留

- 工作点 4：成功路径跳过无意义的 pending projection delete
- 工作点 5：消息序号与热点元数据去 SQLite 化

## 5. 工作点 1：消息 body 单副本化

### 目标

- 避免同一条消息在 `Pebble` 中存 3 份完整 value。
- 显著降低 `4KiB` payload 的序列化、复制和写盘成本。

### 当前问题

- `putMessage()` 会把完整消息写入 `message/id`、`message/user`、`message/producer` 三类 key，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:643)。

### 具体改动

- 保留 `message/id/...` 作为完整消息主记录。
- 把 `message/user/...` 和 `message/producer/...` 改成轻量引用值：
  - 只保存 `recipient node_id/user_id + message node_id/seq`
  - 或等价的固定长二进制引用
- `listRawMessagesByUser()` 遍历用户索引时，先读引用，再回查 `message/id/...` 取完整消息。
- `BuildMessageSnapshotRows()` 读取 `message/producer/...` 时，同样通过引用回查主记录。

### 预期收益

- `4KiB` payload 的写放大从 “3 份完整 value” 降到 “1 份完整 value + 2 份小引用”。
- `B/op` 和 `allocs/op` 都会明显下降。

### 风险

- 读取路径会增加一次附加查主记录的 KV 访问。
- 但当前基线显示 `Pebble` 读路径仍优于 `SQLite`，这一点可以接受。

## 6. 工作点 2：Pebble 本地 append 快路径

### 目标

- 去掉本地消息事件写入里的无效查重。
- 减少事件日志 append 的点查和元信息读取次数。

### 当前问题

- `appendStored()` 本地和复制共用同一套逻辑，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:62)。
- 本地消息事件也会先查 `originKey` 是否已存在。

### 具体改动

- 把 `Append()` 和 `AppendReplicated()` 分开：
  - `Append()` 走本地快路径
  - `AppendReplicated()` 保留幂等查重逻辑
- 本地快路径直接：
  - 分配 `EventID`
  - 分配 `Sequence`
  - 组 batch
  - 提交
- 保持现有 `r.mu`，继续保证本地 sequence 单调递增。

### 预期收益

- 降低 `256B` 小消息场景里事件日志 append 的固定成本。
- 对 `BenchmarkStoreCreateMessage/*/pebble/256B` 收益更明显。

### 风险

- 需要确认本地路径没有任何“可能重复提交同一 EventID” 的入口。
- 第一版通过只对 `Append()` 启用快路径规避风险。

## 7. 工作点 3：写后立即 trim 改阈值化/延迟化

### 目标

- 避免每次写入都立即触发“全量读该用户消息 + 删超窗索引”。

### 当前问题

- `ApplyMessageCreated()` 每次写入后都会同步 trim，[pebble_projection.go](/root/dev/sys/turntf/turntf/internal/store/pebble_projection.go:292)。
- trim 前还会全量扫描用户消息列表。

### 具体改动

- 第一版先不做后台 worker，先做低风险阈值化：
  - 增加每用户近似计数或超窗探测
  - 只有当消息数可能超过 `windowSize + 32` 时才真正 trim
- 如果当前计数未超阈值：
  - 直接返回
  - 不触发全量扫描
- 第二版再考虑后台 trim worker，把 trim 从同步写路径里彻底移出。

### 预期收益

- 在消息窗口未逼近上限时，写路径不再携带读放大。
- 对稳定写入的新用户或中小历史用户效果明显。

### 风险

- 短时间内某些用户消息数可能略超窗。
- 控制方式：
  - 允许短暂超窗
  - 读取时仍按最终排序截断

## 8. 工作点 4：成功路径跳过无意义的 pending projection delete

### 目标

- 去掉成功投影后额外的 SQLite delete。

### 当前问题

- `CreateMessage()` 成功执行后总会调用 `clearPendingProjection()`，[messages.go](/root/dev/sys/turntf/turntf/internal/store/messages.go:82)。
- 但大多数成功路径下，根本没有对应的 pending 记录。

### 具体改动

- 第一版改为“仅在投影曾失败过或重试路径里执行 clear”。
- 普通成功路径默认不再发这条 SQLite delete。

### 预期收益

- 每次消息成功写入少一次 SQLite 往返。

### 风险

- 需要确认失败重试路径仍能清掉旧记录。

## 9. 工作点 5：消息序号与热点元数据去 SQLite 化

### 目标

- 进一步减少 `Pebble` 模式下的混合引擎往返。

### 当前问题

- `nextMessageSeqTx()` 完全依赖 SQLite 的 `message_sequence_counters` 和 `messages` 表，[messages.go](/root/dev/sys/turntf/turntf/internal/store/messages.go:100)。

### 具体改动

- 为 `Pebble` 模式增加独立的 `message_sequence` key 空间。
- `CreateMessage()` 在 `Pebble` 模式下直接从 Pebble 获取并推进下一序号。
- 仅保留必要的 SQLite 元数据，不再让消息序号落回 SQL 表。

### 预期收益

- 进一步降低 `CreateMessage()` 的固定 SQLite 开销。

### 风险

- 需要明确与现有 `messages` 表历史数据的兼容策略。
- 建议作为第二批工作推进。

## 10. 验收标准

每完成一个工作点，至少跑以下命令：

```bash
go test ./internal/store -count=1
go test ./internal/store -run '^$' -bench 'BenchmarkStore(CreateMessage|ListMessagesByUser|PruneEventLogOnce)' -benchmem -count=1
```

重点关注：

- `BenchmarkStoreCreateMessage/*/pebble/256B`
- `BenchmarkStoreCreateMessage/*/pebble/4KiB`
- `BenchmarkStoreListMessagesByUser/*/pebble`
- `BenchmarkStorePruneEventLogOnce/*/pebble`

通过标准：

- `CreateMessage/pebble` 明显改善。
- `ListMessagesByUser/pebble` 不出现异常回退。
- `PruneEventLogOnce/pebble` 不出现明显恶化。

## 11. 建议执行顺序

建议按下面顺序推进：

1. 消息 body 单副本化
2. Pebble 本地 append 快路径
3. trim 阈值化
4. 成功路径跳过 pending projection delete
5. 热点元数据去 SQLite 化

原因：

- 前三项都直接命中 `CreateMessage` 主路径。
- 第 1 项最能改善 `4KiB` payload。
- 第 2 项最能改善 `256B` 小消息。
- 第 3 项能降低每次写入绑定的读放大。
- 后两项收益更偏“继续收尾”，可以放在第二轮。
