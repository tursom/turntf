# 集群在线连接容量优化方案

本文档用于校准“当前 `turntf/` 在大规模在线连接下到底测了什么、已经优化到了哪里、剩余瓶颈在哪里”。它不再把已经落地的能力继续写成待办，而是把历史结论、当前实现和后续工作拆开描述。

相关基线与 benchmark 见：

- [performance-baseline.md](/root/dev/sys/turntf/turntf/docs/performance-baseline.md)
- [internal/api/client_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/client_benchmark_test.go)
- [internal/api/client_persistent_benchmark_test.go](/root/dev/sys/turntf/turntf/internal/api/client_persistent_benchmark_test.go)

## 1. 当前结论

当前已经把在线连接基线拆成两组互不替代的 benchmark：

- `BenchmarkClientWebSocketTransientSendMessageAuthenticatedLinearMeshWithOnlineUsers`
- `BenchmarkClientWebSocketPersistentLoginAuthenticated`
- `BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers`

但这组 benchmark 的语义，需要按**当前代码**重新理解：

- 拓扑仍是 `3` 节点 / `7` 节点线性 mesh。
- 当前只跑 `SQLite` 场景。
- 背景在线连接不是“标准 `/ws/client` + 持久化补推 + 持久化消息分发”，而是通过 `dialAndLoginBenchmarkIdleClientWebSocketsWithOptions(..., true)` 建立的 `/ws/realtime` 登录连接；它们会登录、注册在线状态、参与 transient 收包，但不会进入持久化补发路径。
- 被测的前台发送端和接收端使用 `/ws/client`，但登录时显式传 `TransientOnly=true`，同样不会注册持久化消息推送。
- benchmark 在造完用户后会先裁剪一次 event log，并把 event log 上限压到 `32`，目的是尽量把观测重点放在“在线连接和 transient 路径本身”，而不是历史事件积压。

因此，这组 benchmark 当前测到的是：

- **大量 realtime / transient-only 在线会话存在时**
- **一条跨节点 transient `SendMessage` 的前台端到端延迟**

它**不是**下面这些能力的直接容量证明；这些能力现在由两组 full-client benchmark 单独承接：

- 标准 `/ws/client` 持久化客户端的稳态在线上限
- 大量历史消息补发时的登录吞吐
- 大量 broadcast / channel / direct 持久消息 fanout 时的稳态成本

### 1.1 历史样本

以下结果仍可保留为**历史本地样本**，但只代表这一个 benchmark 场景，不应直接当作当前版本的通用对外承诺：

- 采样日期：**2026-04-26**
- CPU：`12th Gen Intel(R) Core(TM) i5-12400`
- `goos=linux`
- `goarch=amd64`

| 场景 | 结果 | accept_ms/op | push_ms/op |
| --- | --- | ---: | ---: |
| `3-nodes / 1000-online / 256B` | 通过 | `0.1137` | `0.2099` |
| `3-nodes / 5000-online / 256B` | 通过 | `0.0971` | `0.2264` |
| `7-nodes / 5000-online / 256B` | 通过 | `0.0717` | `0.2873` |
| `3-nodes / 10000-online / 256B` | 未在 `2m` 内进入稳态 | - | - |
| `7-nodes / 10000-online / 256B` | 未在 `2m` 内进入稳态 | - | - |

### 1.2 当前可用口径

目前更准确的说法应是：

- 这组 benchmark 曾经验证过“`5000` 总在线 realtime / transient-only 会话 + 跨节点 transient 前台路径”这一特定场景。
- 这组 benchmark 还没有把 `10000` 总在线 realtime / transient-only 会话稳定跑进 `2m` 稳态窗口。
- 它不能直接推出“当前 `turntf` 已验证支持 `5000` 或 `10000` 个标准持久化 WebSocket 客户端”。
- 在没有重新采样前，也不应该把这组历史样本升级成今天的官方容量 SLA。

## 2. 当前实现已与旧版分析不同

旧版文档把不少已经落地的优化仍写成“计划中”。下面这些点在当前实现里已经不是未来工作，而是现有基线。

### 2.1 已落地能力

- 已有 `LoginRequest.transient_only`，并且 `/ws/realtime` 会强制采用同样的“只收发 transient、不接持久化推送”语义。
- 持久化客户端登录时，会先读取 `LastEventSequence()` 作为 `afterSequence` 初始水位；不再存在“默认从 `afterSequence = 0` 扫全历史 event log”的行为。
- `HTTP` 已经维护常驻 `onlineUsers` registry，并用 `onlineUserCount` 预估容量；`ListLoggedInUsers()` 不再靠遍历整张 session map 组装在线列表。
- 客户端 session 已经改成 `256` 个 shard 的分片存储，并且每个用户桶维护稳定快照 `snapshot`，本地 transient 投递不再每次复制一份新的 session slice。
- 持久化消息推送已切到节点级共享分发器：后台只启动一个 `runPersistentDispatcher()`。本地或复制消息提交后会立即唤醒 dispatcher；原有 1 秒 ticker 仅作为通知缺失时的兜底。每次触发会连续分页读取 event log 直到追平，再把消息分发给需要持久化推送的 session。
- 黑名单和目标角色保留短 TTL 缓存；频道订阅改由节点本地 `channel -> persistent online sessions` 索引维护，不再按会话执行 TTL 查询。
- 当前协议已经包含 `session_ref`、`resolve_user_sessions` 和 `target_session`，瞬时包可以精确路由到某个在线会话，而不只是“打给这个用户的所有在线节点”。

### 2.2 已过期的旧判断

下面这些旧结论与当前代码已不一致，应视为过期：

- “每个会话都有一个独立 `pushLoop()`，并以 `O(连接数)` 轮询 event log。”
- “普通登录默认从 `afterSequence = 0` 开始追旧事件。”
- “`ListLoggedInUsers()` 仍然动态遍历整张 sessions map。”
- “`ReceiveTransientPacket()` 每次都会复制目标用户的 session slice。”
- “连接模型还没有区分 transient-only 客户端和持久消息客户端。”

## 3. 当前真正的瓶颈

在现状下，在线容量的主要风险点已经从“每连接一条持久化轮询 goroutine”转移到了别处。

### 3.1 full client 路径已经有独立 benchmark，仍需单独采样

最先需要澄清的不是代码，而是结论边界：

- `BenchmarkClientWebSocketPersistentLoginAuthenticated` 以 `0/100/1000` 条历史拆出登录与补发成本。
- `BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers` 使用标准 `/ws/client`，覆盖 3/7 节点、1k/5k/10k 普通会话以及 direct、broadcast、10% channel fanout。
- 稳态容量 fixture 为已创建用户预签发短期 `reconnect_token`，批量连接不执行 bcrypt；初始密码登录成本仍由独立登录 benchmark 承担，不能从容量 setup 推导。
- 两组 full-client benchmark 固定使用 SQLite，与现有 transient-only 在线容量口径保持可比；它们是本地手动基线，不是 CI 硬阈值或生产 SLA。

所以当前最大的“文档风险”是：

- transient 路径样本被误读成整个客户端模型的容量上限。

### 3.2 full client 路径的结构性热点，已经变成共享持久化分发器

当前 `runPersistentDispatcher()` 的复杂度来源主要是：

- 正常路径由可合并的消息提交通知触发 `ListEvents(afterSequence, 100)`；连续满批时会继续读取直到追平，1 秒 ticker 只负责异常兜底，不再构成正常推送的尾延迟下限。
- 对 direct 消息，需要解析候选接收 session。
- 对 broadcast 消息，业务语义要求枚举全部持久化 session；对 channel 消息，候选集直接来自在线订阅索引并合并管理员 session。
- direct 消息仍需为实际接收者检查黑名单；channel 投递不再逐会话查询 SQLite 订阅关系。

2026-08-09 在同一台本地机器上执行 3 节点 SQLite direct 定向单次验证：

- `1000-online`：事件唤醒前 `write_ms/op=0.3121`、`last_push_ms/op=999.4`；事件唤醒后 `write_ms/op=0.2597`、`last_push_ms/op=4.058`。
- `10000-online`：事件唤醒后 `write_ms/op=0.2654`、`last_push_ms/op=2.971`。

这组结果只验证固定 1 秒轮询不再限制 direct 尾延迟；它是 `benchtime=1x` 的本地定向样本，不是完整容量矩阵或生产 SLA。

这意味着当前 full client 路径的热点已经不再是“每连接一个 ticker”，而是：

- **共享 dispatcher 的候选集合构建**
- **高 fanout 持久消息的授权判断**

### 3.3 登录风暴成本仍然真实存在

即便“从 0 扫 event log”的旧问题已经修掉，full client 登录仍然不是零成本：

- 登录成功后仍会执行 `pushInitialMessages()`，最多补发最近 `1000` 条历史消息。
- 会话会注册到本地在线表和 cluster session registry。
- 如果后续存在大量持久化消息，这些会话还会加入共享持久化分发器的候选集。
- 稳态容量 fixture 使用 reconnect token 只是把 bcrypt 从该测量边界中移除，不改变真实首次登录和登录风暴仍需密码认证的事实。

所以“能否快速建立很多连接”与“steady-state 是否稳定”仍然要分开看。

### 3.4 单用户多会话下，本地 transient fanout 仍然线性依赖 bucket 大小

当前 `ReceiveTransientPacket()` 对未指定 `target_session` 的 transient 包仍会：

- 取出目标用户的 `bucket.snapshot`
- 顺序给桶里的每个 session 执行 `pushPacket`

这已经比“复制 slice + 全局锁”轻很多，但如果某一个用户自己挂了很多终端，会话内 fanout 成本仍然与该用户在线 session 数线性相关。

因此在“单用户多终端很多”的业务里，更合适的做法会是：

- 先用 `resolve_user_sessions` 拿到会话列表
- 再通过 `target_session` 精确投递

### 3.5 在线用户查询已经降级为次要问题

`onlineUsers` registry 和 shard 化之后，`ListLoggedInUsers()` 已经不再是最主要的容量瓶颈。它现在仍然会：

- 构造结果 slice
- 最后按 `node_id / user_id` 排序

但这更像是管理查询路径的固定开销，而不是当前 online benchmark 失败与否的首要原因。

### 3.6 在线状态同步已改为增量与分片校验

旧实现每 5 秒调用一次 `ListLoggedInUsers()`，复制、排序并广播完整 session 与登录用户集合，接收端再清空并重建对应 origin 的全部临时状态。10k 在线时，即使业务轮次只投递一个客户端，这项后台工作仍会制造百 MB 级分配和明显 GC 抖动。

当前实现改为：

- 本地注册/注销只把相关用户标记为 dirty，后台等待 50ms 合并窗口；同一用户只发送窗口结束时的最终状态。
- 每条 `DELTA` 最多包含 256 个用户，携带 presence、登录摘要 upsert 或用户墓碑。
- 用户按确定性哈希固定分为 16 个分片；后台每 500ms 构建一个 `AUTHORITATIVE_SHARD`，8 秒完成一轮权威校验。
- 接收端按 `(origin_node_id, runtime_epoch, shard_index, generation)` 去重，并按 origin/shard 维护用户索引；权威分片只替换对应分片。
- HTTP 注册会话时把 `LoggedInUserSummary` 与 session 同次提交给 cluster Manager；周期任务不再调用 `ListLoggedInUsers()` 构建全量副本。
- 增量丢失、乱序或短暂无路由不会永久造成漂移，后续对应权威分片会在最坏 8 秒内修复。

`mesh-v1alpha3` 与旧 `mesh-v1alpha2` 的 presence 线缆语义严格不兼容。发布必须整集群协调升级或蓝绿切换，不能在同一个 mesh 中逐节点混合滚动。

## 4. 计划状态

| 工作点 | 当前状态 | 说明 |
| --- | --- | --- |
| 1. `transient-only` 会话模式 | 已完成 | `LoginRequest.transient_only` 已存在，`/ws/realtime` 也已落地。 |
| 2. 登录默认从当前事件水位起步 | 已完成 | 持久化会话登录时会先读取 `LastEventSequence()`。 |
| 3. 本地在线用户 registry 常驻化 | 已完成 | `onlineUsers` + `onlineUserCount` 已是当前实现。 |
| 4. session registry 分片 | 已完成 | 当前是 `256` shard，并为每个用户维护 `snapshot`。 |
| 5. 节点级 shared event tailer | 已完成 | 当前持久化推送由 `runPersistentDispatcher()` 统一负责；消息提交事件立即唤醒并跨 batch 追平，1 秒 ticker 仅作为兜底。 |
| 6. 可见性判断缓存化 | 已完成 | 黑名单和目标角色保留 TTL 缓存；频道订阅已迁移到在线索引。 |
| 7. transient / persistent 路径拆分 | 部分完成 | `/ws/client` 与 `/ws/realtime` 已分流，但 ZeroMQ 仍复用标准客户端语义，也还没有单独的 edge 接入层。 |
| 8. 接入层与 cluster 节点角色拆分 | 未开始 | 当前节点仍同时承载 API、连接、store 和 mesh。 |
| 9. 用户或 session 粘性放置 | 未开始 | 还没有 consistent hash / sticky routing，但 `session_ref` / `resolve_user_sessions` / `target_session` 已具备前置能力。 |
| 10. 在线连接分级、配额与背压 | 未开始 | 当前代码里还没有明确的每节点连接上限、每用户并发上限或连接爬升限速。 |
| 11. 在线状态增量与分片校验 | 已完成 | 50ms 用户增量、16 分片、500ms 单分片权威校验，8 秒完成一轮自愈。 |
| 12. channel 在线会话索引 | 已完成 | Store 在本地写入、复制和快照提交后同步更新 `channel -> persistent online sessions`，channel 投递不再扫描全部在线会话。 |

## 5. 现在最值得继续做的事

### 5.1 第一优先级：采集并持续对比分层 benchmark

在线容量 benchmark 已经按 realtime 与 full client 分层，后续优化前应先在同一机器重新采样：

- 保留现有 realtime / transient-only 场景，重新采样当前代码。
- 采集 full-client 登录的 `login_ms/op` / `catchup_ms/op`。
- 采集 full-client 稳态分发的 `write_ms/op` / `first_push_ms/op` / `last_push_ms/op`，并保留 `delivered/op` 与 `candidates/op`。

只有实际采样完成后，才能分别回答 transient realtime 与标准持久化客户端的当前容量；benchmark 存在本身不等于容量数字已经成立。

### 5.2 第二优先级：继续优化共享持久化分发器

如果要做下一项代码优化，我会优先投在共享持久化分发器，而不是再回头优化已经不存在的 per-session `pushLoop`。

direct 已按目标用户索引，channel 也已通过在线订阅索引把候选集缩小到订阅会话与管理员会话；benchmark 新增 `candidates/op` 用于直接验证候选规模。broadcast 的业务语义就是投递全部持久化会话，因此 O(N) fanout 仍然存在，不能通过候选索引消除。

后续应继续补充授权 miss 和索引重载的运行时观测，并在同机重新采样后再判断容量变化；索引完成本身不构成性能提升数字或生产 SLA。

### 5.3 第三优先级：补连接治理能力

在还没有 edge 拆层之前，比较务实的增强项是：

- 每节点最大客户端连接数
- 每用户最大并发会话数
- 每连接待发送队列上限
- 短时连接爬升速率限制

这些能力不一定立刻抬高 benchmark 数字，但能显著改善“接近极限时系统怎么退化”。

## 6. 修订后的验收口径

### 6.1 realtime / transient-only 路径

现有 benchmark 可以继续作为 realtime / transient-only 路径的验收基线，但应该单独表述：

- `3-nodes / 1000|5000|10000-online / 256B`
- `7-nodes / 1000|5000|10000-online / 256B`
- 记录：
  - 是否能在 `2m` 内进入稳态
  - `accept_ms/op`
  - `push_ms/op`

### 6.2 full client 路径

标准持久化客户端已有独立验收场景，覆盖：

- 背景连接为 `/ws/client`
- 不启用 `TransientOnly`
- 有历史补发
- 有共享持久化分发器参与，并分别校验 direct、broadcast 和 10% channel fanout 的最后一个目标到达时间

在没有同机实际采样结果前，仍不应把 realtime / transient-only 路径的结果外推成 full client 容量。

### 6.3 周期任务验收

10k full-client direct 的旧式 `-benchtime=10x` 只限定操作次数，实际测量窗口可能短于 5 秒，因此可能完全错过旧在线状态 ticker，也可能恰好把一次全量同步和 GC 集中计入某一轮。周期工作验收必须使用持续时间控制：

```bash
go test ./internal/api -run '^$' -bench '^BenchmarkClientWebSocketPersistentSendMessageAuthenticatedLinearMeshWithOnlineUsers/tmp/sqlite/3-nodes/10000-online/direct/256B$' -benchmem -benchtime=10s -count=3
```

该场景同时记录 `write` 与 `last_push` 的平均值、p95、p99 和 max。发布后还应同时观察 Go heap、GC pause、control-critical 转发字节；`last_push >= 1s` 视为周期抖动未消除。

2026-08-09 同机三轮采集结果：

- `write_ms/op`：`0.6916–0.7125`，p99 `1.604–1.628ms`，max `5.600–18.74ms`。
- `last_push_ms/op`：`4.162–4.236`，p99 `5.740–5.831ms`，max `20.69–77.92ms`。
- 三轮均未出现 `last_push >= 1s`。旧样本只记录到约 `1.4s` 的 `last_push` 而没有 p95/p99；即使用新 max 保守比较，周期相关尾延迟也下降 94% 以上。
- fixture 在计时前先等待在线用户镜像数量收敛，再等待 8.5 秒覆盖一整轮权威 presence 分片；随后要求 durable replication/snapshot 连续 2 秒无 pending、游标/确认/快照活动或本地 snapshot digest 变化，最后清理 setup GC。durable 静默只排除后台残留，不要求不同节点的业务 snapshot digest 完全相同；正式 10 秒窗口仍会覆盖下一整轮 presence 分片校验。

### 6.4 对外承诺

在重新采样之前，这份文档不再给出“`10000` 在线必须达成”或“下一步直接承诺 `15000` / `20000`”这样的刚性目标。

更稳妥的口径是：

- 使用当前实现对应的分层 benchmark 完成同机采样
- 再分别给出 realtime / transient-only 与 full client 的独立容量数字

## 7. 当前最推荐的答案

如果只让我从今天的现状里选一个“最值得继续做”的方向，我会选：

- **先采集 full client 在线容量基线，再围绕共享持久化分发器做优化**

原因：

- `transient_only`、登录水位、在线用户 registry、session shard、shared dispatcher、可见性缓存和 channel 在线会话索引都已经落地。
- full-client benchmark 已经存在，当前最容易误导人的地方变成“把 benchmark 场景存在误写成容量数字已经得到验证”。
- 真正决定 full client 在线上限的，已经是共享持久化分发器及其高 fanout 授权路径，而不是旧版文档中描述的 per-session 轮询模型。
