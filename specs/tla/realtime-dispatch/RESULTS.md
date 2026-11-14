# 实时连接调度的 TLC 实际验证结果

本次最终运行：**新模型 56/56、历史模型 27/27，合计 83 个配置均符合各自预期**。新模型为 24 个正向穷举、27 个具名不变量反例、5 个时序反例；历史模型为 11 个正向、16 个反例。反例包含刻意破坏与非真空覆盖，不代表对应的更强性质成立。

新行为的关键结论：满额后台请求被拒绝后，健康 Stream/Ping 仍进展；永久慢的单个 stream 不妨碍其他完整 key 和 Ping，且不关闭连接；物理写永久不可用时保证完成或明确关闭，不保证 Pong。状态请求继续排空此前三类工作，普通 client 继续串行。所有结论受 [README.md](README.md) 的有限抽象与环境假设限制。

## 运行输入、工具与证据

最终证据根目录的规范路径：

```text
/root/dev/.local/state/turntf-realtime-per-stream-20261114/
  current/summary.json       新模型最终 56 配置
  historical/summary.json    历史模型最终 27 配置
  audit/report.json          最终 461 项证据与分类检查
  audit/BadParse/tlc.log     实际 SANY 解析错误，退出码 150
  audit/BadEval/tlc.log      实际 TLC 求值错误，退出码 75
```

命令中的 `/root/.local/state/...` 在此环境经 `Path.resolve()` 规范化为 `/root/dev/.local/state/...`。每个场景目录包含模型/配置、完整日志、结果以及存在时的 JSON 轨迹；顶层保留矩阵和运行器快照。`summary.json` 中保存精确 Java 命令、版本、时间及 SHA-256。

- TLC：`2.19 of 08 August 2024 (rev: 5a47802)`。
- Java：OpenJDK `21.0.13.0.3`，64 位 Server VM。
- 单 worker、BFS、`-fp 0 -seed 0`、堆上限 512MiB；每场景 timeout 60 秒。
- 新模型运行 UTC：`2026-11-14T00:49:43.649975–00:50:33.723071`。
- 历史模型运行 UTC：`2026-11-14T00:50:33.797969–00:50:53.744255`。
- 所有最终配置均无超时或工具失败。最大单配置为 `current-cancel-capacity-2`，50,999 个独立状态，约 5.974 秒；没有海量状态或深度截断。

| 输入 | SHA-256 |
| --- | --- |
| `/usr/share/java/tla2tools.jar` | `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88` |
| `RealtimeDispatch.tla` | `66519a58e92913afe5f6fb537db75612edaf9328a29f604e865e45abcd423199` |
| `cases.json` | `fdff6244a70dd0a8950944592723c2a77c6fdabbd2cf1c9a8d13936bdc69b199` |
| `check.py` | `9eb69e17bfd54bce641099e467f956e95cade47e7f383f2a1e40b6d30948f749` |
| `LegacyDispatch.tla` | `3405f7f13def3830fcab8b70baeb6f3168a0c36579345499e120a31acf1fd791` |
| `legacy-cases.json` | `ef74feeac254d36014dab91a8694a0d24b881b3b7e6fbc8e1284126fa30cb3bc` |

从 monorepo 根目录实际执行的最终命令如下，三条均退出 0。复跑需要更换为尚不存在的输出目录。

```sh
python3 turntf/specs/tla/realtime-dispatch/check.py \
  --timeout 60 \
  --output /root/.local/state/turntf-realtime-per-stream-20261114/current

python3 turntf/specs/tla/realtime-dispatch/check.py \
  --legacy --timeout 60 \
  --output /root/.local/state/turntf-realtime-per-stream-20261114/historical

PYTHONDONTWRITEBYTECODE=1 python3 turntf/specs/tla/realtime-dispatch/audit.py \
  --current /root/.local/state/turntf-realtime-per-stream-20261114/current \
  --legacy /root/.local/state/turntf-realtime-per-stream-20261114/historical \
  --previous /root/dev/.local/state/turntf-realtime-idle-fix-20261114T001504Z/tlc-results \
  --output /root/.local/state/turntf-realtime-per-stream-20261114/audit
```

每个配置展开后的 TLC 命令形如：

```text
java -XX:+UseParallelGC -Xmx512m -cp /usr/share/java/tla2tools.jar \
  tlc2.TLC -workers 1 -fp 0 -seed 0 -cleanup \
  -metadir <场景目录>/states -config model.cfg RealtimeDispatch
```

历史模型末尾为 `LegacyDispatch`。具名时序反例各只配置一个时序性质；不变量反例要求退出 12 且名称一致；时序反例要求退出 13、轨迹与单性质配置一致。不能用一般非零退出码代替反例。

## 新模型的正向穷举

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

## 新模型保留的边界反例

这些配置不要求不可能的更强进展性质。时序轨迹状态数包含 TLC 输出的停顿状态。

| 配置 | 独立状态 | 退出码 | 轨迹状态 | 指定违例及解释 |
| --- | ---: | ---: | ---: | --- |
| `slow-write-does-not-guarantee-pong` | 576 | 13 | 22 | `IOEventuallyReplies`：写不可用时允许关闭，无必收 Pong 保证 |
| `without-deadline-no-progress` | 23 | 13 | 12 | `IOCompletedOrFailed`：禁用 deadline 后可无限停顿 |
| `current-state-barrier` | 5 | 13 | 6 | `IOEventuallyReplies`：慢后台前的 state 仍会阻止后续读取 |
| `current-stream-state-barrier` | 8 | 13 | 7 | `IOEventuallyReplies`：慢 Stream 前的 state 仍会等待 |
| `current-client-serial-slow` | 3 | 13 | 4 | `IOEventuallyReplies`：普通 client 继续等待慢请求 |

`state` 反例说明保留屏障与“所有后续 IO 都独立进展”不能同时成立。慢路由的正向配置没有在慢流与健康输入之间插入这种屏障。

## 新模型的非真空见证与错误注入

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

## 历史模型复现

历史模型与原路径 `/root/dev/.local/state/turntf-realtime-idle-fix-20261114T001504Z/tlc-results` 的模型相比，仅修改模块名；恢复原模块名后的 SHA-256 仍为 `3fbe75f2141ae1ae77e0bffeb63a254a4888087bfee67c1ae0a5efcd726bd59c`。本次展开的 27 份 `model.cfg` 与原始产物逐字节相同，已写入 audit 检查结果。

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

`audit/report.json` 的 461 项全部通过，包括：规约/配置/日志/矩阵/运行器 hash 与快照一致、保存的轨迹与日志一致、指定反例重新分类、旧模型/配置保留，以及上述关键终态和动作顺序。

审计实际生成并执行了非法语法模型 `BadParse`（150）和空序列越界模型 `BadEval`（75），全部 48 个负向配置均拒绝把两者当作预期反例。另对每个负向配置检查缺失轨迹、错误退出码、超时，以及错误不变量名或不匹配的时序配置，全部拒绝。

中间的 `initial/`、`new-initial/`、`new-second/`、`legacy/`、`audit-initial/` 目录保留开发过程。首轮新模型曾将拒绝响应误计入后台名额，并在空闲取消中访问 ID 0，分别被标记为不匹配和工具失败；修复后完整重跑。它们不参与最终 83 配置的通过计数，也没有将求值错误包装成预期反例。

## 剩余边界

此处结果只验证约定的调度模型，不构成 Go 精化证明；实现及 Go 验证另见[服务端修复记录](../../../docs/realtime-dispatch-isolation-validation-20261114.md)。取消模型保守地为尚未开始的队列项保留错误写步骤，Go 则直接释放这些未执行项；此差异不意味着实现需要为每项取消写出响应。

容量为 1/2/必要的 4，字节为有限权重；没有全规模 64/128 项与 MiB 预算证明，没有真实时钟与真实 10 秒证明。正常 writer 场景假定预算内返回，不可写场景假定到期和 transport Close 最终生效。取消依赖 backend/route 可观察取消以及原 transport Close 可中断自己的写入。

没有证明完整 mesh、epoch/session 协议、可靠投递、重连、SDK、无限输入公平性或任意第三方 transport。慢 stream 本身可以一直不完成，跨 key 健康进展也不跨越显式 state 屏障。
