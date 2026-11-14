# 实时连接调度的 TLC 实际验证结果

本次最终结果：**新模型完整 68/68 配置已实际复跑，历史 27/27 配置复用原始 TLC 运行并通过只读 audit**。新模型为 30 个正向穷举、33 个具名不变量反例、5 个时序反例；历史模型为 11 个正向、16 个反例。新旧共 95 个配置符合各自预期，其中本次实际执行的是 68 个新模型配置，不能写成 95 个全部重跑。反例包含刻意破坏与非真空覆盖，不代表对应的更强性质成立。

原 83 份最终证据（原新模型 56 + 历史 27）另外完整只读审计，548 项检查通过；新模型与历史 27 的最终审计 625 项通过，另有 59 项原 56 配置保留及状态数比较全部通过。原有 56 配置的定义、性质、生成/独立状态数、退出码和轨迹状态数均保持一致，默认增加 `BoundedLanes`，没有抹掉原反例。

新行为的关键结论：满额后台请求被拒绝后，健康 Stream/Ping 仍进展；永久慢的单个 stream 不妨碍其他完整 key 和 Ping，且不关闭连接；物理写永久不可用时保证完成或明确关闭，不保证 Pong。状态请求继续排空此前三类工作，普通 client 继续串行。所有结论受 [README.md](README.md) 的有限抽象与环境假设限制。

新增结论是独立 lane 预算：所有仍排队或活动的 Stream 按 distinct `ScheduleKey` 计数，默认不含 epoch；帧/字节尚有余量时，新 key 超过 `LaneLimit` 仍明确关闭并排空。同 key 多帧跨 epoch 只占一个 lane，`wait/write` 仍占 lane，最后一帧完成后释放。实际实现预算为 **8192 帧 / 64 lane / 8MiB 编码字节**，本次只检查小容量 1/2/4，不构成真实规模或参数化证明。

## 运行输入、工具与证据

最终证据根目录的规范路径：

```text
/root/dev/.local/state/turntf-realtime-lane-budget-20261114/
  current-final/summary.json        新模型实际完整 68 配置
  audit-complete/report.json        新模型 + 历史 27：625 项检查全通过
  audit-complete/BadParse/tlc.log    实际 SANY 解析错误，退出码 150
  audit-complete/BadEval/tlc.log     实际 TLC 求值错误，退出码 75
  original-83-audit/report.json     原 56 + 历史 27：548 项检查全通过
  original-56-final-comparison.json 原 56 配置保留及状态数比较：59 项全通过
  original-evidence-sha256.json      原证据 394 个非状态库文件指纹
  final-manifest.json               最终文档/源码/证据 hash 与原文件未改写检查

/root/dev/.local/state/turntf-realtime-per-stream-20261114/
  current/summary.json              原 56 配置，保留不改写
  historical/summary.json           原 27 配置，本次 audit 的历史来源
  audit/report.json                 原 461 项审计，保留不改写
```

命令中的 `/root/.local/state/...` 在此环境经 `Path.resolve()` 规范化为 `/root/dev/.local/state/...`。每个场景目录包含模型/配置、完整日志、结果以及存在时的 JSON 轨迹；顶层保留矩阵和运行器快照。`summary.json` 中保存精确 Java 命令、版本、时间及 SHA-256。

- TLC：`2.19 of 08 August 2024 (rev: 5a47802)`。
- Java：OpenJDK `21.0.13.0.3`，64 位 Server VM。
- 单 worker、BFS、`-fp 0 -seed 0`、堆上限 512MiB；每场景 timeout 60 秒。
- 新模型本次最终运行 UTC：`2026-11-14T02:37:31.812228–02:38:36.083916`。
- 历史模型原运行 UTC：`2026-11-14T00:50:33.797969–00:50:53.744255`；本次未重新运行历史 TLC。
- 所有最终配置均无超时或工具失败。最大单配置为 `current-cancel-capacity-2`，50,999 个独立状态，约 6.124 秒；新增四 lane 正向配置为 39,780 个独立状态；没有深度截断或状态筛选。

| 输入 | SHA-256 |
| --- | --- |
| `/usr/share/java/tla2tools.jar` | `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88` |
| `RealtimeDispatch.tla` | `20ca2ee6f91da2e881843bb26b8588b6696a64247560e9c9e4173303307f395d` |
| `cases.json` | `d541f3c4ffb44f4c1b911d954d1378e0c6c8d27d331f3c9607619f529ffd101b` |
| `check.py` | `10eef4c1436f9d70af45e834e917f65b3649aa531719a11c7df2cabadc141ad8` |
| `audit.py`（最终） | `4ecfbb1300bd956eec6350b91ac01025b6cc8a70d526d99eaf9faba1712ee5d7` |
| `current-final/summary.json` | `5bbb9817957042b8cb4b5156b738fc65ce9683d727d87aac4d740809050b146f` |
| `LegacyDispatch.tla` | `3405f7f13def3830fcab8b70baeb6f3168a0c36579345499e120a31acf1fd791` |
| `legacy-cases.json` | `ef74feeac254d36014dab91a8694a0d24b881b3b7e6fbc8e1284126fa30cb3bc` |

从 monorepo 根目录实际执行的最终命令如下，三条均退出 0。`current-final` 是实际重跑；两个 audit 复用只读历史结果并各自实际运行分类器故障模型。复跑需要更换为尚不存在的输出目录。

```sh
python3 turntf/specs/tla/realtime-dispatch/check.py \
  --timeout 60 \
  --output /root/.local/state/turntf-realtime-lane-budget-20261114/current-final

PYTHONDONTWRITEBYTECODE=1 python3 turntf/specs/tla/realtime-dispatch/audit.py \
  --current /root/.local/state/turntf-realtime-per-stream-20261114/current \
  --legacy /root/.local/state/turntf-realtime-per-stream-20261114/historical \
  --previous /root/dev/.local/state/turntf-realtime-idle-fix-20261114T001504Z/tlc-results \
  --output /root/.local/state/turntf-realtime-lane-budget-20261114/original-83-audit

PYTHONDONTWRITEBYTECODE=1 python3 turntf/specs/tla/realtime-dispatch/audit.py \
  --current /root/.local/state/turntf-realtime-lane-budget-20261114/current-final \
  --legacy /root/.local/state/turntf-realtime-per-stream-20261114/historical \
  --previous /root/dev/.local/state/turntf-realtime-idle-fix-20261114T001504Z/tlc-results \
  --output /root/.local/state/turntf-realtime-lane-budget-20261114/audit-complete
```

每个配置展开后的 TLC 命令形如：

```text
java -XX:+UseParallelGC -Xmx512m -cp /usr/share/java/tla2tools.jar \
  tlc2.TLC -workers 1 -fp 0 -seed 0 -cleanup \
  -metadir <场景目录>/states -config model.cfg RealtimeDispatch
```

历史模型末尾为 `LegacyDispatch`。具名时序反例各只配置一个时序性质；不变量反例要求退出 12 且名称一致；时序反例要求退出 13、轨迹与单性质配置一致。不能用一般非零退出码代替反例。

## 独立 lane 预算的新增 12 配置

全部默认检查 `BoundedLanes` 以及既有安全不变量。以下为本次真实 TLC 计数；反例配置找到指定见证后停止，状态数不是全空间大小。生成状态数与独立状态数均保存于各场景 `result.json` 及原始日志。

| 配置 | 生成状态 | 独立状态 | 退出码 | 轨迹状态 | 指定结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| `current-lane-shared-epochs` | 31 | 22 | 0 | 0 | 始终 open，健康 Ping 完成 |
| `witness-lane-shared-epochs` | 30 | 22 | 12 | 13 | `NoSharedLaneWitness` |
| `current-lane-overflow-1` | 115 | 55 | 0 | 0 | `EventuallyClosed` / `ClosingDrains` |
| `witness-lane-overflow-1` | 12 | 10 | 12 | 7 | `NoLaneOverflowWitness` |
| `current-lane-overflow-2` | 541 | 221 | 0 | 0 | `EventuallyClosed` / `ClosingDrains` |
| `witness-lane-closed` | 528 | 217 | 12 | 21 | `NoLaneClosedWitness` |
| `current-lane-active-write` | 797 | 347 | 0 | 0 | `EventuallyClosed` / `ClosingDrains` |
| `witness-lane-active-write` | 343 | 170 | 12 | 12 | `NoActiveLaneWitness` |
| `current-lane-reuse` | 98 | 66 | 0 | 0 | 始终 open，`AllRequestsSuccessful` |
| `witness-lane-reuse` | 97 | 65 | 12 | 28 | `NoLaneReuseWitness` |
| `current-lane-capacity-4` | 73,933 | 39,780 | 0 | 0 | 始终 open，`IOEventuallyReplies` |
| `witness-lane-capacity-4` | 52 | 34 | 12 | 9 | `NoFourLanesWitness` |

独立 lane 关闭见证的关键实际值：

- 容量 1：`phase=<<"queued","queued","held">>`，两帧 A 的 epoch 不同，共占 1 lane；B 是新 key。`stop.cause="streamLanes"`，`stop.streams=2 < StreamLimit=4`，编码权重 `2+1 <= StreamBytes=8`。尚未启动 worker 时排队 key 就占预算。
- 容量 2 的关闭见证：A 两帧跨 epoch、只改变 user 的第 3 帧已接纳；只改变 session 的第 4 帧构成第 3 个 distinct key，被 lane 预算关闭。最终 `connection="closed"`、无剩余工作，非仅到达条件前件。
- 活动写见证：`phase=<<"write","wait","held">>`（前两项分别持锁写和等锁），没有排队帧；前两 key 仍占满 2 lane。第 3 个 key 来临时只有 2 帧/2 权重，仍因 lane 而关闭。audit 同时检查持锁者非零及 `wait/write` 两状态。
- 共用 lane 见证：`phase=<<"route","queued","queued","done">>`，三帧相同 key（跨 epoch）仅占一个 lane，Ping 已成功且会话 open。完成释放见证的 `routed=<<1,3,5>>`，A → B → A 在单 lane 容量下全部完成。

三个 lane 超限正向场景均禁用外部取消；活动写/容量 2 场景还禁用 deadline，不能由其他关闭路径冒充 lane 溢出。lane 见证的 audit 从原始 phase、输入 key、held 与预算值重算 distinct key 和余量；帧数/字节超限仍保留原独立正向及见证。

## 原有 56 配置中的正向穷举

以下全部同时检查默认安全不变量。慢路由的两个正向配置还检查 `SessionOpen`，不允许任何关闭路径。

| 配置 | 独立状态 | TLC 退出码 | 检查结论 |
| --- | ---: | ---: | --- |
| `current-isolation-1` | 34 | 0 | 慢 Query 不妨碍 IO |
| `current-isolation-2` | 34 | 0 | 同上，后台容量 2 |
| `current-relay-isolation` | 34 | 0 | 慢 Relay 不妨碍 IO |
| `current-capacity-1` | 90 | 0 | 后台满额拒绝后 IO 完成 |
| `current-capacity-2` | 92 | 0 | 同上，后台容量 2 |
| `current-normal-1` | 1,071 | 0 | 有限请求最终 done 或 rejected |
| `current-normal-2` | 1,398 | 0 | 同上，后台容量 2 |
| `current-multi-frame-order` | 2,382 | 0 | 两流各多帧、跨 epoch 同 key 顺序与 IO 完成 |
| `current-slow-route` | 146 | 0 | 慢 A 阻塞时 B 两帧及 Ping 完成，始终 open |
| `current-full-key-isolation` | 454 | 0 | 不同目标用户/session 的同 stream_id 独立进展，始终 open |
| `current-slow-write` | 1,314 | 0 | IO 完成或 closed，关闭最终收敛 |
| `current-response-state-barrier` | 56 | 0 | reader 响应也被状态屏障等待，不可写时关闭收敛 |
| `current-client-serial-normal` | 28 | 0 | 普通 client 串行完成全部请求 |
| `current-cancel-pending` | 2,304 | 0 | 后台、Stream 和响应取消收敛 |
| `current-cancel-capacity-2` | 50,999 | 0 | 满额与拒绝响应存在时取消收敛 |
| `current-cancel-slow-route` | 189 | 0 | 慢路由及同流排队取消收敛 |
| `current-cancel-blocked-write` | 848 | 0 | 不依赖 deadline，外部取消及 transport Close 解除阻塞写 |
| `current-idle-cancel` | 5 | 0 | 空输入取消收敛 |
| `current-stream-items` | 55 | 0 | Stream 总项数超限必关闭并收敛 |
| `current-stream-bytes` | 30 | 0 | Stream 权重预算超限必关闭并收敛 |
| `current-response-items` | 48 | 0 | 响应总项数超限必关闭并收敛 |
| `current-response-bytes` | 30 | 0 | 响应权重预算超限必关闭并收敛 |
| `current-stream-capacity-1` | 30 | 0 | Stream 容量 1 超限关闭 |
| `current-reject-response-overflow` | 471 | 0 | 拒绝响应占满容量 1，后续响应超限明确关闭 |

## 原有 56 配置中保留的边界反例

这些配置不要求不可能的更强进展性质。时序轨迹状态数包含 TLC 输出的停顿状态。

| 配置 | 独立状态 | 退出码 | 轨迹状态 | 指定违例及解释 |
| --- | ---: | ---: | ---: | --- |
| `slow-write-does-not-guarantee-pong` | 576 | 13 | 22 | `IOEventuallyReplies`：写不可用时允许关闭，无必收 Pong 保证 |
| `without-deadline-no-progress` | 23 | 13 | 12 | `IOCompletedOrFailed`：禁用 deadline 后可无限停顿 |
| `current-state-barrier` | 5 | 13 | 6 | `IOEventuallyReplies`：慢后台前的 state 仍会阻止后续读取 |
| `current-stream-state-barrier` | 8 | 13 | 7 | `IOEventuallyReplies`：慢 Stream 前的 state 仍会等待 |
| `current-client-serial-slow` | 3 | 13 | 4 | `IOEventuallyReplies`：普通 client 继续等待慢请求 |

`state` 反例说明保留屏障与“所有后续 IO 都独立进展”不能同时成立。慢路由的正向配置没有在慢流与健康输入之间插入这种屏障。

## 原有 56 配置中的非真空见证与错误注入

以下均返回指定的不变量违例，退出码全部为 12。

| 配置 | 独立状态 | 轨迹状态 | 指定不变量 |
| --- | ---: | ---: | --- |
| `witness-isolated-progress` | 33 | 14 | `NoIsolationWitness` |
| `witness-rejected-request` | 88 | 19 | `NoRejectedWitness` |
| `witness-healthy-without-close` | 144 | 23 | `NoHealthyWitness` |
| `witness-full-key-without-close` | 443 | 23 | `NoHealthyWitness` |
| `witness-cross-key-overtake` | 19 | 9 | `NoCrossKeyWitness` |
| `witness-multi-frame-epochs` | 2,253 | 30 | `NoMultiFrameWitness` |
| `witness-cancel-pending` | 186 | 10 | `NoCancelWitness` |
| `witness-cancel-writer` | 212 | 10 | `NoCancelWriterWitness` |
| `witness-cancel-route` | 107 | 10 | `NoCancelRouteWitness` |
| `witness-write-failure` | 315 | 10 | `NoWriteFailureWitness` |
| `witness-idle-cancel` | 5 | 5 | `NoIdleCancelWitness` |
| `witness-deadline` | 448 | 16 | `NoDeadlineWitness` |
| `witness-lock-wait-deadline` | 1,046 | 16 | `NoLockDeadlineWitness` |
| `witness-stream-items` | 53 | 16 | `NoStreamItemsWitness` |
| `witness-stream-bytes` | 29 | 11 | `NoStreamBytesWitness` |
| `witness-response-items` | 47 | 16 | `NoResponseItemsWitness` |
| `witness-response-bytes` | 29 | 11 | `NoResponseBytesWitness` |
| `witness-active-response-counted` | 48 | 16 | `NoActiveResponseWitness` |
| `witness-state-completed` | 1,048 | 24 | `NoBarrierWitness` |
| `mutation-response-correlation` | 19 | 9 | `ResponseCorrelation` |
| `mutation-rejected-correlation` | 15 | 8 | `ResponseCorrelation` |
| `mutation-epoch-key` | 19 | 7 | `StreamPrefix` |
| `mutation-stream-fifo` | 19 | 7 | `StreamPrefix` |
| `mutation-background-barrier` | 5 | 5 | `StateBarrier` |
| `mutation-stream-barrier` | 6 | 5 | `StateBarrier` |
| `mutation-response-barrier` | 7 | 5 | `StateBarrier` |
| `mutation-blocking-admission` | 5 | 5 | `NoBlockingAdmission` |

非关闭健康见证的实际末状态为：

```text
connection = "open"
phase = <<"route", "queued", "done", "done", "done">>
routed = <<3, 4>>
```

其中 A1 与 A2 具有相同目标三元 key、不同 epoch；A1 永久慢，A2 不能越过它。B1/B2 和 Ping 全部完成。完整 key 见证也达到同一 phase 布局，但第 3/4 帧分别只改变 user/session。`audit.py` 对两条轨迹实际核对了这些值，健康进展没有借助关闭全部连接。

满额拒绝见证的实际末状态为：

```text
connection = "open"
started = {1}
rejected = {2}
phase = <<"remote", "rejected", "done", "done">>
```

第 2 个 Query 未执行 handler，响应为同 ID 的 `service_unavailable`；Stream 和 Ping 已成功，拒绝请求没有被计入成功。

`witness-cancel-writer` 的实际动作顺序已核对为 `Cancel → CloseTransport → FinishWrite`，没有用写入自行失败代替外部取消。等锁 deadline 见证的 `stop.expired` 非零且不同于 `stop.writer`，说明预算可在尚未取得锁时触发。活动响应覆盖确认溢出时 `responseActive` 非零、活动项计入 `responses=2`。

`mutation-epoch-key`/`mutation-stream-fifo` 仅为定位首个指定违例而省略 `StreamFIFO`，仍保留更强的 `StreamPrefix` 检查；其他安全断言继续运行。它们展示后帧先路由时立即失败，不必等到慢首帧也完成。

## 历史 27 模型原结果复核

历史模型与原路径 `/root/dev/.local/state/turntf-realtime-idle-fix-20261114T001504Z/tlc-results` 的模型相比，仅修改模块名；恢复原模块名后的 SHA-256 仍为 `3fbe75f2141ae1ae77e0bffeb63a254a4888087bfee67c1ae0a5efcd726bd59c`。本次 audit 重建的 27 份 `model.cfg` 与原始产物逐字节相同，模型/矩阵与仓库保留版本 hash 相同。以下均为原运行计数，本次已按原日志核验，不宣称重新执行历史 TLC。

| 历史配置 | 独立状态 | 退出码 | 结果 |
| --- | ---: | ---: | --- |
| `current-isolation-1` | 18 | 0 | 通过 |
| `current-isolation-2` | 18 | 0 | 通过 |
| `current-relay-isolation` | 18 | 0 | 通过 |
| `legacy-query-barrier` | 5 | 12 | `NoQueryDrainForIO` |
| `legacy-query-progress` | 5 | 13 | `IOEventuallyReplies` |
| `legacy-eventual-query-barrier` | 6 | 12 | `NoQueryDrainForIO` |
| `current-capacity-1` | 5 | 13 | `IOEventuallyReplies` |
| `current-capacity-2` | 7 | 13 | `IOEventuallyReplies` |
| `current-slow-write` | 16 | 13 | `IOEventuallyReplies` |
| `current-slow-route` | 5 | 13 | `IOEventuallyReplies` |
| `current-state-barrier` | 5 | 13 | `IOEventuallyReplies` |
| `current-normal-1` | 94 | 0 | 通过 |
| `current-normal-2` | 398 | 0 | 通过 |
| `current-multi-frame-order` | 33 | 0 | 通过 |
| `current-cancel-2` | 1,944 | 0 | 通过 |
| `current-cancel-capacity-1` | 75 | 0 | 通过 |
| `current-cancel-capacity-2` | 318 | 0 | 通过 |
| `current-cancel-blocked-write` | 780 | 0 | 通过 |
| `witness-isolated-progress` | 18 | 12 | `NoIsolationWitness` |
| `witness-cancel-pending` | 148 | 12 | `NoCancelWitness` |
| `witness-cancel-writer` | 236 | 12 | `NoCancelWriterWitness` |
| `witness-cancel-admission` | 74 | 12 | `NoCancelAdmissionWitness` |
| `witness-cancel-route` | 127 | 12 | `NoCancelRouteWitness` |
| `witness-write-failure` | 155 | 12 | `NoWriteFailureWitness` |
| `current-idle-cancel` | 6 | 0 | 通过 |
| `witness-idle-cancel` | 6 | 12 | `NoIdleCancelWitness` |
| `mutation-response-correlation` | 8 | 12 | `ResponseCorrelation` |

旧满额输入卡在 `WaitAdmission`，新模型在容量 1/2 下同结构输入均转成正向；旧同步慢路由阻塞 reader，新模型用多帧、多个完整 key 检查健康进展；旧全局 reader 路由次序不再是新保证，跨 key 超越已有 open 见证。

## 工具失败分类与复核

`audit-complete/report.json` 的 625 项全部通过，包括：规约/配置/日志/矩阵/运行器 hash 与快照一致、生成/独立状态数及轨迹计数与原日志一致、保存的轨迹与日志一致、指定反例重新分类、旧模型/配置保留，以及上述关键终态和动作顺序。`original-83-audit/report.json` 的 548 项另外核验了原 83 配置；原目录 394 个非状态库文件在本次前后指纹一致，未改写。

最终审计实际生成并执行了非法语法模型 `BadParse`（150）和空序列越界模型 `BadEval`（75），全部 54 个负向配置均拒绝把两者当作预期反例。另对每个负向配置检查缺失轨迹、错误退出码、超时，以及错误不变量名或不匹配的时序配置，全部拒绝。原 83 配置审计中的 48 个负向配置也通过相同检查。

原证据根目录下的 `initial/`、`new-initial/`、`new-second/`、`legacy/`、`audit-initial/` 继续保留此前开发过程。此前曾将拒绝响应误计入后台名额，并在空闲取消中访问 ID 0，分别被标记为不匹配和工具失败；这些不是最终原 83 配置的结果，也没有将求值错误包装成预期反例。

本次独立证据目录的 `current/`、`audit/`、`audit-final/` 保留中间结果。首轮 audit 的一项失败是额外要求 lane 超限见证首帧已处于 `route`，实际最短轨迹为两帧均 `queued`；修正审计为核对真实排队计数。进一步复核发现，原活动写见证可以在 Overflow 后的取消收尾阶段才进入 `wait/write`，因此收紧为溢出快照已有持锁者、所有相关 Stream 已在 open 时完成 Route，并直接核验 Overflow 当步均为 `wait/write`。收紧后完整重跑全部 68 配置至 `current-final/`，最终 `audit-complete` 的 625 项全通过。未覆盖或删除中间记录。

## 剩余边界

此处结果只验证约定的调度模型，不构成 Go 精化证明；实现及 Go 验证另见[服务端修复记录](../../../docs/realtime-dispatch-isolation-validation-20261114.md)。取消模型保守地为尚未开始的队列项保留错误写步骤，Go 则直接释放这些未执行项；此差异不意味着实现需要为每项取消写出响应。

首次发布预算失败是量化工作负载缺口：按用户提供的生产事实，kiwi/home 各捕获一次 Stream budget 关闭，主会话已启动全部 6 节点回退到 d0ee 并保留数据；实际 TUN 为 MTU 1400、`send_queue_size=4096`、`stream_window=8MiB`，旧代码在合法 4096 帧突发的第 65 帧、累计 94,690 编码字节即拒绝。小容量 TLA 抽象没有建立真实帧分布与旧 64 帧预算适用性的量化关系；本次分离 frame/lane 预算，保留原模型反例和原始证据，不把提高容量解释为消除反例。这里未执行或验证回退部署。

容量为 1/2/必要的 4，字节为有限权重；没有真实 4096 帧突发、8192 帧/64 lane/8MiB、128 响应或参数化 cutoff 证明，没有真实时钟与真实 10 秒证明。Go 量化突发回归由主会话执行，本任务没有运行 Go 测试。正常 writer 场景假定预算内返回，不可写场景假定到期和 transport Close 最终生效。取消依赖 backend/route 可观察取消以及原 transport Close 可中断自己的写入。

没有证明完整 mesh、epoch/session 协议、可靠投递、重连、SDK、无限输入公平性或任意第三方 transport。慢 stream 本身可以一直不完成，跨 key 健康进展也不跨越显式 state 屏障。
