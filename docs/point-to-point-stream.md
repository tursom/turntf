# Point-to-point stream protocol

点对点字节流使用独立的 `StreamFrameRequest` / `StreamFramePushed`，不会调用 `SendMessage`、`TransientAccepted` 或 `TransientPacket`。客户端 realtime transport 只负责承载帧；服务端鉴权后将帧交给 core stream router。

## 帧语义

- `Open` 建立 `(sender, recipient, stream_id)` 逻辑流，目标以 `OpenAck` 应答。
- `Data` 使用字节 `offset`，接收端只按连续累计 offset 交付；重叠重传只交付尚未确认的后缀。
- `Ack` 的 `offset` 是累计确认位置，`window` 是接收端允许的绝对窗口上限。发送端不得使未确认字节超过窗口。
- `Resume` 必须携带新 `epoch` 和当前累计 offset。新 epoch 的 Data 只能在 Resume 完成后接受。
- 旧 epoch 的帧在 core registry、SDK receiver 和 TUN receiver 都被丢弃，旧 path 只允许排空到旧 epoch，不得污染新 path。
- `Close` 删除目标端的逻辑流状态。

## Mesh

core 将帧编码为 `MeshStreamFrame`，分类为 `TRAFFIC_POINT_TO_POINT_STREAM`，沿 mesh envelope forwarding path 转发。该消息使用 `stream_id`、`epoch`、`offset` 和目标 `SessionRef` 标识逻辑流；不填充 `TransientPacket`。目标节点的 registry 先过滤旧 epoch，再注入指定客户端会话。

目标节点存在多条已建立的物理邻接时，direct stream fast path 将 `(target, stream_id, epoch)` 固定到一个邻接。接收端会丢弃乱序 `Data`，因此换路必须保证顺序：

- 本节点在该 epoch 发出的有序帧全部被对端确认后（`Open` 已收到 `OpenAck`、`Resume` 已收到同 epoch 的 `Ack`、累计 `Ack` 覆盖已发 `Data` 末尾），下一帧 `Data`、`Ack` 或 `OpenAck` 可以直接改走其他邻接。
- 仍有未确认 `Data` 时，换路先排空：后续 `Data`（及 `Close`）暂存在本节点，等旧邻接上已发数据全部被确认后，再按原顺序写到新邻接。3 秒内未排空则放弃换路，暂存帧按序写回旧邻接。暂存量受发送端 stream 窗口约束，另有 8 MiB 上限。

选路依据实测投递速率：stream 在途数据不少于 128 KiB 时，每 0.5 秒按累计 `Ack` 采样一次速率，记在所用邻接上，取 1–2 个 10 秒窗口的最大值；超过 90 秒无样本视为未测。Ping 的 RTT/jitter 不能可靠比较路径：承载流量的邻接 Ping 会排在自身数据后面，低延迟路径的拥塞丢包也只在有负载时出现。规则如下：

- 当前邻接与候选都有实测值时，候选需高出 30% 才切换；当前邻接已有实测值时，仅有 RTT/jitter 的候选不会触发切换。
- 都没有实测值时（冷启动、低流量），按 RTT+jitter 成本选路，候选需优于当前 max(25ms, 20%)，且只在静止时切换。
- 繁忙且当前邻接已实测的 stream 会试探未实测的候选（按 RTT+jitter 取最优），每个目标节点每 5 分钟最多一次；每次换路后至少停留 5 秒以完成测量，之后按实测值决定留下或切回。
- `Open`、`Resume` 优先选择实测速率最高的邻接，没有实测值时按 RTT+jitter。规划器指定直连传输时只在该传输内选择，否则 TCP 邻接优先。

固定邻接失效或发送失败时错误直接返回，由 TUN 发起 `Resume` 切换 epoch；当前 epoch 不回退到其他邻接。`Close`、runtime 关闭会清理 affinity（排空中的 `Close` 在暂存帧写出后清理），运行时同时设置固定容量上限，避免缺失 `Close` 时状态无界增长。未携带 `stream_id` 的兼容帧保持原有路由行为。

## TUN

`turntf-tun` 的 `stream` 模式在发送 IP packet 前等待 `OpenAck`，发送窗口由累计 ACK 和 credit 控制。握手、发送或目标会话不可用时回到原有 Relay 模式；Relay 仍保持原有兼容行为。
