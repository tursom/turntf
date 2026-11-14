# 实时调度隔离、故障注入与边界

## 目标

router 故障曾同时出现会话查询超时、Stream ACK 停滞和 WebSocket 重连。受控 resolver 和真实本地 WebSocket 已复现一个确定的放大路径：先前慢查询/定向 Relay 的排空屏障阻止后续 Stream/Ping。该局部复现不证明它是线上唯一触发因素。

首轮修复解除该排空依赖后，TLA 继续给出了满额接纳、同步路由和共享响应写入的阻塞反例。用户已明确选择同流有序、跨流并行及满额单请求拒绝，本次据此修复这些 reader 阻塞；同时保留资源预算、状态屏障和明确失败。

## 行为

- 后台查询/定向 transient Relay 合计 64 个名额，非阻塞接纳；满额请求不执行，按原 request ID 返回既有 `service_unavailable`。
- 专用 Stream 以目标用户、完整目标 session、stream ID 为身份，同身份 FIFO，epoch 不分 lane；不同身份并行。最多 64 个活动 lane，活动与排队合计不超过 8192 帧、8MiB Protobuf 请求编码大小。小帧缓冲数量与并行 worker 数分别限制；任一预算超限关闭会话，不能静默丢零 request ID 的帧。
- reader 的 Pong、过载错误和无效帧错误由单个响应 worker 处理，活动与排队总计不超过 128 项、1MiB。reader 不等物理写入；排队成功不等于消息交付。
- 所有实际写入仍由原 session 写锁串行化。实时写的锁等待和 Send 共用 10 秒期限，过期 Close 原 transport，函数返回前等待已启动的关闭回调。普通 session 保持原路径。
- 状态请求、持久 AckMessage、重复 Login 等仍等待此前后台、stream 和 reader 响应完成；Pong 仍不是全连接排空屏障。
- 实时 readLoop 入口安装取消关闭回调，覆盖尚无查询/Relay 的空闲连接；关闭时取消、清理未执行队列并等待 worker，保留首个失败。

协议字段不变，逐帧鉴权、session/epoch/offset 验证和业务错误映射仍使用原 handler。响应与不同 stream 可以重排，调用方按 request ID 关联。

## Go 故障注入

| 测试 | 关键断言 |
| --- | --- |
| `TestRealtimeSlowRequestsDoNotBlockStreamAndPing` | 慢查询、Relay 及已接纳满额时，Stream/Ping 仍进展 |
| `TestRealtimePipelineBackgroundSaturation` | 混合占满 64 项后查询和 Relay 均明确拒绝，handler 不多执行，后续 IO 继续 |
| `TestRealtimePipelineLaneFIFOAcrossEpochs` | A1 卡住时 A2 不越序，B 和 Ping 完成；包括 stream、目标 session、目标用户的身份区分 |
| `TestRealtimePipelineBlockedBackgroundWriteStillReads` | 响应 Send 卡住时 reader 仍收到 Ping 后的标记，不把不可写网络假定为能交付 Pong |
| `TestRealtimePipelineStreamQuotaCloses` | 数量、编码字节及零 request ID 超限均关闭；活动项参与预算 |
| `TestRealtimePipelineZeroIDFramesAreDelivered` | 无超限时零 ID 帧正常投递，不伪造响应 |
| `TestRealtimePipelineReaderReplyQuotaCloses` | reader 响应队列数量/字节超限后关闭，清理预算并 join |
| `TestRealtimePipelineStateBarriers` | 后台、stream 和 reader 响应未完成时，控制屏障不越过工作 |
| `TestRealtimePipelineWebSocketSlowLane` | 真实本地 WebSocket 中慢 A、健康 B 与 Pong 并存，同流结果保持顺序 |
| `TestRealtimeWrite*` | 实际传入有限 deadline；忽略 ctx 的 Send 被 Close 释放；锁等待过期不再发第二次 Send |
| `TestRealtimeIdleCancellationClosesTransport` | 首次请求前、仅 Ping 后的读取期间不观察 ctx，只由 Close 中断；修改前两场景均失败 |

旧查询屏障的受控 resolver、真实 WS、错误 request ID、普通连接串行、业务错误/传输错误、关闭回调等待回归继续保留。原有同流顺序测试明确命名为 `TestRealtimeStreamFramesPreserveStreamOrder`，不再把跨流全序当作新承诺。

在服务端模块运行：

```sh
go test ./internal/api -run 'TestRealtime|TestOrdinarySlowLookupStillOrdersStreamAndPing' -count=1 -timeout=150s
go test -race ./internal/api -count=1 -timeout=10m
go test -p 1 ./... -count=1 -timeout=15m
./scripts/smoke.sh
```

测试依靠 channel/Close 控制进入、阻塞和释放，清理时取消、关闭并有界等待；不通过固定生产延时、放大 timeout 或遗留测试 goroutine 获得通过。

## TLA 与范围

规约、配置矩阵、严格运行器和实际结果位于 [TLA 目录](../specs/tla/realtime-dispatch/README.md)。旧行为配置应继续出现原反例；新实现检查同流顺序、健康流进展、拒绝关联、预算、取消/关闭和可达覆盖。永久不可写时的目标是明确失败，而不是一定交付；永久慢 A 时也不能仅靠关闭整个连接冒充健康 B 已进展。

这是一份手工抽象，不是 Go 精化证明。小容量、有限输入、弱公平和逻辑期限不证明真实 ACK/心跳毫秒上界、64 并发全规模或无限输入无饥饿。具体字节阈值由 Go 边界回归校验。

## 仍需保留的边界

显式全连接状态屏障仍会阻止后续读取；单慢 lane 的大量排队仍可能耗尽全连接预算并导致断线。不可写的同一 socket 不能被另一条 Pong 抢占；期限只保证在 transport Close 能中断发送、本地动作继续被调度时的失败收尾。

发送者取消不代表另一个 receiver 的所有写入立即结束；另一接收 session、普通 client 写路径、mesh 和 backend 各有生命周期。一个写期限不是全部路由/后台任务 join 的总预算。这里没有增加节点全局限流，也没有修改 SDK、设备写回调、拥塞算法或生产部署。

最终 Go/TLC 检查、反向故障注入及独立复核的实际结果另存验证记录；不由一次短期成功请求推断长期网络稳定。
