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

目标节点存在多条已建立的物理邻接时，direct stream fast path 只在 `Open` 首次选择邻接，并将 `(target, stream_id, epoch)` 固定到该连接；同 epoch 的 `Data`、`Ack` 和其他流帧不因 RTT/jitter 分数变化而换路。`Resume` 进入新 epoch 后才允许重新选择路径。固定邻接失效或发送失败时错误直接返回，由 TUN 发起 `Resume` 切换 epoch；当前 epoch 不回退到其他邻接。`Close`、runtime 关闭会清理 affinity，运行时同时设置固定容量上限，避免缺失 `Close` 时状态无界增长。未携带 `stream_id` 的兼容帧保持原有路由行为。

## TUN

`turntf-tun` 的 `stream` 模式在发送 IP packet 前等待 `OpenAck`，发送窗口由累计 ACK 和 credit 控制。握手、发送或目标会话不可用时回到原有 Relay 模式；Relay 仍保持原有兼容行为。
