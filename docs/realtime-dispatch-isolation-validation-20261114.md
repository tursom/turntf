# 实时调度修复验证（2026-11-14）

## 本地变更

基于服务端 `d0ee5177f1494e34ac8dafc1ed2d4293cca42166`，完成用户确认的同流 FIFO、跨流并行和后台满额拒绝，增加有界 reader 响应及实时写锁等待预算。协议字段、SDK、普通客户端路径和生产配置未修改。本文记录发布前本地验证，生产部署结果以独立发布记录为准。

主要实现与预算见 [修复说明](realtime-dispatch-isolation-plan.md)，故障注入见 [回归说明](realtime-stream-isolation.md)。

- 后台：64 个查询/定向 Relay；未接纳请求按原 ID 返回 `service_unavailable`。
- Stream：完整目标身份下单 lane FIFO，epoch 不拆分；当前活动及排队共 8192 项、8MiB 编码大小、最多 64 个活动 lane，超限关闭，不静默丢零 ID 帧。初次发布使用的 64 帧限制已由生产突发反例否定，修正和验证见下文。
- reader 响应：128 项、1MiB，含正在写入项，单 worker；满额、异步失败明确关闭。
- 实时写：共享原 session 写锁，锁等待和 Send 共 10 秒预算，超期 Close 并等待已启动回调。
- 控制屏障：等待此前后台、Stream 和 reader 响应完成；普通客户端串行。

## Go 验证

完整仓库普通检查已通过：

```sh
go test -p 1 ./... -count=1 -timeout=15m
```

定向实时调度、普通串行对照、写期限及真实 WebSocket 回归通过。新 pipeline 测试包含 9 个测试、22 个场景，单独在 race 下重复 3 次通过；最终完整 API race 通过（269.847 秒），smoke 通过：

```sh
go test -race ./internal/api -count=1 -timeout=10m
./scripts/smoke.sh
```

完整普通检查开始后追加一处空 lane 记账修正：释放最后一个活动项时同时退役空 lane，避免未调度的空队列积累。该内部修正由后续最终 API race 检查覆盖；其他包的普通检查不受影响。

## 反向变异检查

仅通过 Go overlay 改变验证进程看到的文件，不修改工作区或生产：

| 定向回退变异 | 对应回归 | 结果 |
| --- | --- | --- |
| 删除满额拒绝，恢复等待 slot | `TestRealtimePipelineBackgroundSaturation` | 按预期实际断言失败 |
| Stream 恢复 reader 同步调用 | `TestRealtimePipelineLaneFIFOAcrossEpochs/stream_id` | 按预期检出 B/Ping 被阻塞 |
| Pong 恢复 reader 同步发送 | `TestRealtimePipelineBlockedBackgroundWriteStillReads` | 按预期检出无法读取 Ping 后标记 |
| 普通写路径替代实时预算 wrapper | `TestRealtimeWriteUsesFiniteTotalBudget` | 按预期检出缺少期限 |

以上四项均运行到了指定测试的断言失败，不是编译失败，也不是历史完整提交复测。`mutations/mutations.exit=0` 表示四种有意引入的回退都被检出，不表示变异实现可用。

## 独立审查

Go 实现经过独立只读审查，未发现阻断项。重点包括唯一 reader 下 Add/Wait、队列清理与预算、lane 退役、跨 epoch FIFO、过载关联、watchdog 的解锁后 join 顺序及普通连接兼容。独立补跑锁等待期限的 race 回归通过。

收尾仍依赖 backend/route 遵守 context 或有限等待，以及 transport Close 能并发调用并解除 I/O。10 秒不是全部跨节点 join 的总预算。持续超过资源预算会中断整个连接；同一不可写 socket 没有 Pong 物理发送优先级。

## TLA

初次发布前模型的 56 个配置全部符合预期：24 个正向穷举、27 个具名不变量反例、5 个时序反例。历史 27 个配置完整保留并复现原结果；当时两套合计 83 配置符合预期，证据审计的 461 项检查通过。容量修正后新增独立 lane 预算的 12 个配置，本次实际重跑当前全部 68 配置，并复用原始历史 27 配置做只读审计；共 95 配置符合预期、625 项审计通过。原 83 配置的输入、计数和反例另行保留核对，包括真实工具失败和不匹配的违例名称不会被当作预期反例。

关键正向性质包括后台满额拒绝后 IO 进展、慢 A 阻塞及同流新 epoch 排队期间健康 B 两帧与 Ping 在连接保持 open 时完成、同 key FIFO/前缀顺序、资源上限和关闭收敛。不可写 socket 的目标是完成或明确 closed，而非一定收到 Pong；显式状态屏障和普通 client 串行的边界反例仍保留。

规约快照、完整矩阵、实际状态数和复跑命令见 [TLA 记录](../specs/tla/realtime-dispatch/RESULTS.md)。模型检查不等同于 Go 精化证明、实际 ACK/心跳时间证明、全规模并发或整个 mesh 正确性证明。模型对尚未开始的取消队列项保留了额外错误写步骤，Go 直接清理；这项保守抽象已记载。

## 证据

受限 Go 证据目录：`/root/dev/.local/state/turntf-perstream-fix-20261114T005436Z/`。完整 Go、API race、smoke 日志、输入散列、四种 overlay 与各自失败样本保存在该目录。

TLC 证据目录：`/root/dev/.local/state/turntf-realtime-per-stream-20261114/`，含 current、historical、audit 三套结果。模型/运行器/配置与实际执行快照的 SHA-256 已核对，主实现的最终快照和验证汇总保存在 Go 证据目录的 `final-manifest.json`。

## 生产突发反例与容量修正

首版 `2ab9ae4f188dbf57b1eb1b39264b6b522993455c` 的六节点滚动升级和业务探测成功，但最终完整日志门禁检出 kiwi、home 各一次 Stream 预算超限断连。该版本未通过发布验收，六节点已逐台回退到原固定镜像 `d0ee5177f1494e34ac8dafc1ed2d4293cca42166`；回退仅替换 core 镜像，不恢复数据库。

只读核对生产 TUN 的 MTU 为 1400，发送队列为 4096 包，流窗口为 8MiB。本地让首帧路由暂时等待后提交同一流的小包，第 65 帧即在累计约 94KiB 编码数据时触发原数量限制。这个反例说明小容量模型中的有界性不能替代生产突发容量验证；它不证明两次线上超限的具体流量组成完全相同。

修正将数量上限调整为 8192 帧，同时单独保留 64 个活动 lane 和原 8MiB 编码字节上限，避免增加并行 handler 数。超限错误包含一致快照下的帧数、lane 数及编码字节计量，便于区分限制来源。

新增受控路由和真实 WebSocket 的 4096×1400 字节突发回归：路由等待期间整批接纳，Ping 可先完成，释放后全部帧交付，状态屏障后的 Pong 确认排空。另验证第 65 个独立 lane 被明确拒绝，帧数和字节数预算仍分别触发。接纳调用和 WS 写入均有有限测试等待。旧 64 帧实现实际在指定断言失败；修正后定向回归、完整 Go、完整 API race 和 smoke 均通过。原始失败、修正后日志与生产数值 profile 保存在 `/root/dev/.local/state/turntf-perstream-burst-fix-20261114T022812Z/`。

原 83 配置的模型结果保留为历史快照。新增 lane 独立预算的模型结果以 [TLA 记录](../specs/tla/realtime-dispatch/RESULTS.md) 为准；两者均不证明 8192 帧、64 lane 或真实 10 秒调度期限的全规模行为。
