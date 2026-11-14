# 实时连接调度的 TLA+ 规约

当前模型描述已选定的 **per_stream + reject_request** 调度语义。本目录同时保留上一版模型及全部 27 个历史配置，便于复现旧屏障、满额接纳、同步路由和慢写反例。新模型实际结果见 [RESULTS.md](RESULTS.md)。

这是依据约定行为建立的手工有限抽象，未建立 Go 到 TLA+ 的机器精化关系。这里保存 TLC/Python 的独立模型证据；服务端实现、Go 回归和适用范围见[修复验证记录](../../../docs/realtime-dispatch-isolation-validation-20261114.md)，不能用模型通过代替实现验证。

## 文件与运行

| 文件 | 职责 |
| --- | --- |
| `RealtimeDispatch.tla` | 新调度模型、安全性质、进展性质、错误注入和覆盖见证 |
| `cases.json` | 新模型 68 个配置，schema 3；保留原 56 配置，新增 12 个 lane 配置 |
| `LegacyDispatch.tla` | 上一版模型，仅修改模块名 |
| `legacy-cases.json` | 原 27 个配置，仅增加模型名；其中 `current-*` 是历史命名 |
| `check.py` | 生成配置、执行 TLC、严格分类及保存证据 |
| `audit.py` | 复核产物/hash/状态数/关键见证，兼容原 schema 2 证据，实际执行解析与求值故障检查分类器 |
| `README.md` / `RESULTS.md` | 模型说明 / 实际结果 |

需要 Python 3、Java 和 `tla2tools.jar`。默认 jar 路径是 `/usr/share/java/tla2tools.jar`，可用 `TLA2TOOLS_JAR` 或 `--jar` 指定。仓库不附带 jar，也不自动下载。

从 monorepo 根目录运行；输出目录必须尚不存在，省略 `--output` 时自动新建临时目录：

```sh
python3 turntf/specs/tla/realtime-dispatch/check.py \
  --timeout 60 --output /tmp/turntf-tlc-current
python3 turntf/specs/tla/realtime-dispatch/check.py \
  --legacy --timeout 60 --output /tmp/turntf-tlc-historical
PYTHONDONTWRITEBYTECODE=1 python3 turntf/specs/tla/realtime-dispatch/audit.py \
  --current /tmp/turntf-tlc-current --legacy /tmp/turntf-tlc-historical \
  --output /tmp/turntf-tlc-audit
```

可以用重复的 `--case NAME` 缩小检查范围。`audit.py` 的关键见证复核需要完整两套矩阵。进入服务端仓库运行时，将脚本路径改为 `specs/tla/realtime-dispatch/...`。

历史证据可以只读复用，将 `--legacy` 指向原始历史运行目录即可；这属于 audit，不是本次重跑历史 TLC。当前新模型的完整 68 配置必须实际运行。`check.py` 只运行新 schema 3 或历史 schema 1；schema 2 兼容仅用于 audit 逐字节重建原 56 配置。使用独立输出目录，保留 `/root/.local/state/turntf-realtime-per-stream-20261114` 的原 83 份证据。

每个场景保存规约快照、`model.cfg`、原始 `tlc.log`、`result.json`；存在轨迹时保存 `counterexample.json`，保留原始 TLA 值和循环/停顿信息，不将集合或函数猜测为普通 JSON。顶层保存矩阵和运行器快照、`summary.json`，包括调用命令、工作目录、Java/TLC 版本、jar/规约/矩阵/运行器/配置/日志 SHA-256、退出码、超时标记和状态数。

运行器整体退出 0 表示所有配置符合各自预期，包含有意失败的性质。不变量反例要求退出码 12、唯一匹配的具名不变量、至少两个轨迹状态；时序反例要求退出码 13、时序违例、轨迹，并且配置只检查一个与预期同名的时序性质。TLC 的时序违例正文没有单独打印性质名，因此由单性质配置确定名称。工具故障、OOM、超时、未知退出码、其他名称的违例不能算通过。`classification` 区分 `pass`、`expected_violation`、`tool_failure` 和 `unexpected_result`。

## 新模型的调度边界

| 动作/状态 | 约定行为 |
| --- | --- |
| `Read`、`pc`、`held` | 单 reader 顺序解码有限输入；实时 reader 只做接纳/入队，不等待物理响应写入 |
| `Admit`、`started` | Query/Relay 占后台名额，名额含远端调用、等待响应锁和活动 Send |
| `EnqueueResponse`、`rejected` | 后台满额即拒绝，不启动 handler；原请求 ID 的 `service_unavailable` 与 Ping 共用独立响应队列 |
| `EnqueueStream`、`StartStream`、`Route` | Stream 按完整三元 key 分组，同 key 前项整个 handler 结束后才运行后项；不同 key 独立执行 |
| `StreamFramesFit`、`StreamBytesFit`、`LaneFits` | 独立判定 Stream 帧数、编码权重、新 lane 接纳；`StreamFits` 合取三者 |
| `Lanes`、`BoundedLanes` | 所有 `queued/route/wait/write` Stream 的 distinct `ScheduleKey`；同 key 多帧只占一个 lane，最后一帧到终态才释放 |
| `ResponseFits`、`Overflow` | 响应条数和权重统计活动项及排队项；超限明确关闭，Stream 原因为 `streamItems/streamBytes/streamLanes` |
| `StartResponse`、`responseActive` | 单响应 worker，按队列 FIFO 处理 Ping/拒绝响应，活动项继续占预算 |
| `Acquire`、`writer`、`FinishWrite` | 后台、Stream 和响应 worker 共用一个 session 响应写锁 |
| `armed`、`Expire` | 进入等锁时启动一次预算，获得锁不重置；预算到期关闭会话 |
| `WaitBarrier`、`Barrier` | `state` 等待此前后台、Stream 和 reader 响应（含活动 Send）全部结束 |
| `SerialClient`、`SerialAdvance` | 普通 client 一项完成后才读取下一项，保留串行行为 |
| `CancelHandler`、`CloseTransport`、`FinishClose` | backend/route 观察取消后仍走错误响应写锁路径；Close 中断原 transport 的写入后收敛 |

当前实现的预算是后台 64 项、**Stream 排队+活动共 8192 帧、独立 64 个 lane、8MiB 编码字节**，reader 响应 128 项/1MiB。lane key 为目标用户、目标 session、streamID，不含 epoch；只要该 key 还有排队或活动帧就占 lane。帧缓冲预算与 lane 并发预算分别限制资源。

本模型使用后台容量 1/2、Stream 帧容量 1/2/4、独立 `LaneLimit=1/2/4`、响应容量 1/2，以及 1/2 的有限权重和小字节预算。原 56 场景默认 lane 容量 4，保留原有帧数/字节/慢流/拒绝/写/关闭性质；新增场景单独收紧 lane，帧数与字节仍允许接纳。权重 2 的项使字节超限与条数超限分别可达。这里的“字节”是权重单位，不是实际序列化长度；**提高容量不构成参数化证明**，没有建立到真实 4096/8192 帧、64 lane、128 响应或 MiB 预算的 cutoff/精化证明。

首次发布的生产终验在 kiwi/home 各捕获一次 Stream budget 关闭，主会话已启动全部 6 节点回退到 d0ee 并保留数据（此处记录用户提供的事实，不是本任务执行或复核的部署）。实际 TUN 为 MTU 1400、`send_queue_size=4096`、`stream_window=8MiB`；用户提供的旧代码本地量化结果是在合法 4096 帧突发的第 65 帧、累计 94,690 编码字节即拒绝。旧 64 帧预算未覆盖这种小帧量化工作负载，属于工作负载与容量验证缺口；原模型发现的屏障、满额等待、同步慢路由和慢写反例仍然有效，原证据继续保留。Go 量化突发回归由主会话负责，本目录的有限 TLC 结果不证明真实突发已通过。

每帧记录 `kind/user/session/stream/epoch/weight`，输入位置作为连接内唯一非零 request ID。`user/session` 分别抽象完整的目标 UserKey 和 SessionRef，不单独展开节点 ID 字段。`Key(i)` 为 `(目标用户, 目标 session, stream_id)`，不含 epoch。`MultiFrameInput` 包含 A 的两个不同 epoch 帧、B 的两帧和一个 Ping。`KeyInput` 额外使用相同 stream_id、不同用户或不同 session 的健康流，检查三元 key 的独立性。Stream 的 DATA/ACK 在此合并为同类调度帧，不展开其内容协议。

`state` 是保留排空语义的抽象控制请求，不对应对某个具体实时控制 API 可用性的断言；其处理及完成折叠为一个动作。当前 writer 仅是原会话的响应写入，向 receiver 的工作折叠在路由动作中，不证明 receiver/mesh 的协议与可靠交付。

## 安全、进展与非真空检查

所有新场景默认检查类型、后台/Stream 帧数及字节/独立 lane/响应预算、writer/reader/响应 worker 所有权、响应关联、至多一次、拒绝安全、同 key FIFO/前缀顺序、状态屏障、关闭后无工作以及不进入阻塞接纳。

- `RejectionSafety`：拒绝请求不在 `started` 中，不进入成功响应集合。`AllRequestsResolved` 允许 `done` 或 `rejected`；`AllRequestsSuccessful` 只允许 `done`。
- `BoundedLanes`：`Cardinality({ScheduleKey(i) : i \in Streams}) <= LaneLimit`。`LaneFits` 检查与 incoming key 并集的大小，同 key 跨 epoch 不增加 lane。`Overflow` 先区分帧数、再字节、最后 lane；`streamLanes` 因而只在帧数和字节允许时记录，`stop.lanes` 保存触发时 lane 数。
- `StreamFIFO`：同完整 key 的路由完成日志保持输入顺序。`StreamPrefix` 还要求每个后帧的前帧已路由，防止“首帧永久慢、仅后帧路由”绕过单纯日志排序断言。旧版全连接 `ReaderRouteOrder` 只保留在历史模型中。
- `IOEventuallyReplies`：有限输入的 Stream/Ping 都成功完成响应。用于容量满额但响应队列仍有空间、物理可写、相关路由可返回的环境。
- `HealthyIO`：只要求健康 stream 与 Ping 完成。永久慢 A 不必完成；慢路由正向配置额外检查 `SessionOpen`，整个执行都不能关闭。非关闭见证还同时要求 A 首帧保持 `route`、跨 epoch 后帧保持 `queued`，B 两帧和 Ping 均完成。
- `IOCompletedOrFailed`：全部 IO 成功，或会话明确 `closed`。适用于物理永久不可写；不承诺一定收到 Pong。
- `ClosingDrains`：进入 closing 后最终 closed；`EventuallyClosed` 用于确定触发预算超限的配置，防止仅检查条件性质而没有关闭。

见证断言 `No*Witness` 有意失败，覆盖后台未完成时 IO 成功、原 ID 的拒绝响应、跨 key 超越、同 key 跨 epoch 多帧、各种预算关闭、活动响应占预算、持锁取消、带 Stream 工作取消、空闲取消、等锁者 deadline 及状态屏障完成。`NoCancelRouteWitness` 的快照条件为取消时存在 Stream 工作，不单独区分 queued/route；已进入慢路由的收尾由 `current-cancel-slow-route` 的全状态时序检查与 Go 受控路由回归覆盖。错误注入分别破坏响应 ID（含拒绝响应）、将 epoch 加入调度 key、去掉 FIFO、跳过三类状态屏障及恢复阻塞接纳，必须命中对应的指定断言。

新增 lane 正向配置检查容量 1/2 下独立 lane 超限必关闭并排空，见证分别到达仅 lane 超限的 `closing` 和完全排空的 `closed`；其他正向/见证检查三帧同 key（跨 epoch）共用一个 lane 且 Ping 完成、`wait/write` 活动 handler 仍占 lane、旧 lane 完成后可接纳不同 key 再接纳原 key、容量 4 时四个 distinct key 同时可达。`audit.py` 从实际溢出轨迹复核 frame/byte 尚有余量和 incoming key 未出现，不只接受一个原因字符串。

## 公平性、取消及时间假设

`Spec` 对本地动作、每个有限请求的处理/等锁动作、关闭及到期动作使用弱公平，没有使用强公平。`Cancel` 和 `FailWrite` 没有公平性要求，不能靠“系统迟早主动取消所有连接”证明健康流进展。远端不返回时 `RemoteReady` 禁用；慢 A 的 `Route` 禁用，弱公平不能让它恢复。

writer 可写环境假定响应在写预算内返回；不可写环境 `WriterResponds=FALSE` 禁止正常写完成，并要求已启动预算最终 `Expire`。模型用持续的 `armed` 表示同一次锁等待+Send 预算，不模拟时钟；**不证明真实 10 秒期限、调度延迟上界，也不证明间歇抖动时总耗时符合期限**。关闭 deadline 后的对照配置保留无限停顿反例，避免把到期能力隐含在公平性里。慢路由没有进入响应等锁时不会触发写预算。

关闭收敛明确假设 backend/route 能观察取消，原 transport 的 Close 最终执行并中断它自己的阻塞写入，之后的本地动作继续被调度。模型取消后仍让已接纳工作进入等锁、取得锁、尝试发送再收尾；其中尚未开始的 Stream 和 reader 响应队列项在 Go 实现中会直接清理，模型保留额外的取消写步骤，是收尾工作量的保守抽象，不能解释为实现必须为每个排队项写出错误。已经等待写锁或进入 Send 的实际任务仍需等待原写路径退出。原 transport 真正关闭前可能写出取消响应，关闭后 Send 返回失败。此假设不覆盖任意其他 receiver 的不可取消写入或任意第三方 transport。

空闲取消从入口已安装取消/Close 机制的状态开始。空输入时 reader 只有在 transportClosed 后才能停止，正向与覆盖配置均保留；登录握手和具体 Go Receive 调用不在模型中。

输入是有限 batch，竞争者有限，所以弱公平的锁接纳足以检查这些配置；不推出无限流的无饥饿、有界等待或吞吐量。关闭 TLC 死锁诊断以允许有限终态和环境停顿，通过具名时序性质判断是否违反目标。未使用深度截断、状态筛选或随机模拟。正向配置完成可达状态穷举，负向配置在找到指定违例后停止。

未验证 request_id=0、重复 ID、重连、完整 epoch/session 隔离、Open/Close/Reset/ACK 状态机、SDK pending RPC、游标持久化、完整 mesh/Raft、混合版本及 Go 精化。两份模型的状态数与性质不能相加解释成一个更大协议的证明。
