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

目标节点存在多条已建立的物理邻接时，direct stream fast path 在 `Open` 首次选择邻接，并将 `(target, stream_id, epoch)` 固定到该连接。接收端会丢弃乱序 `Data`，因此只有当本节点在该 epoch 发出的有序帧全部被对端确认后（`Open` 已收到 `OpenAck`、`Resume` 已收到同 epoch 的 `Ack`、累计 `Ack` 覆盖已发 `Data` 末尾），下一帧 `Data`、`Ack` 或 `OpenAck` 才会按与 `Open` 相同的规则重新选择邻接；仍有未确认数据时不换路。stream 选路（`Open`、`Resume` 和静止后的重选）按邻接的窗口最小 RTT 加发送失败、探测超时惩罚排序：探测 Ping 与业务数据共用连接，承载流量的邻接 RTT EWMA 会被自身排队抬高，若按 EWMA 选路会把流推到空闲但更慢的路径（如经 CDN 的 WSS）。最小 RTT 取最近一到两个 2 分钟窗口的样本，尚无样本时沿用 RTT+jitter 成本。最小 RTT 不反映丢包，因此另统计空闲探测（探测往返期间该邻接收发不超过 64 KiB）中 RTT 超过最小值 max(50ms, 最小值/2) 的尖峰比例（EWMA，α=0.1），尖峰多为丢包后的 TCP 重传；分数加上尖峰比例 × 500ms，使低延迟但高丢包的直连让位于较慢但干净的路径（如经 CDN 的 WSS）。忙碌期间的慢探测只反映自身排队，不计入；转发规划器的链路成本不变。新邻接的分数需比当前低至少 25ms 或当前分数的 1/5（取较大者）才会切换，避免在相近路径间抖动。本节点作为接收端只发送可乱序的累计 `Ack`，每帧都可按该规则选路。转发路径上的 affinity 在静止后也可切到规划器选中的直连邻接。`Close` 沿用当前邻接。`Resume` 进入新 epoch 时总是重新选择路径。固定邻接失效或发送失败时错误直接返回，由 TUN 发起 `Resume` 切换 epoch；当前 epoch 不回退到其他邻接。`Close`、runtime 关闭会清理 affinity，运行时同时设置固定容量上限，避免缺失 `Close` 时状态无界增长。未携带 `stream_id` 的兼容帧保持原有路由行为。


### 负载感知选路与中转

stream 除直连外还可经一个邻居中转：首跳为到中转节点 X 的本地邻接，第二跳为 X 公告的 X→目标链路（与首跳同一传输，X 不桥接），X 须开启转发且不拒绝 stream 流量。中转帧包装为 `ForwardedPacket`（`last_hop` 为本节点、TTL 已减一）发给 X，由 X 的转发引擎送达目标；首跳仍是固定邻接，第二跳链路失效时 `Data` 返回错误由 TUN `Resume`，累计 `Ack` 直接改走其他路径。

每条邻接在探测周期读取 TCP_INFO：发送侧重传率（累计至少 50 个报文段的增量比例，EWMA α=0.3）与容量（BBR 取 pacing 速率 EWMA，其他算法取送达速率的衰减最大值）。二者量化后（重传率 5‰ 一档、容量变化超过 25%，每条邻接至多 10 秒一次）写入链路公告的 `loss_permille`、`capacity_kbps`，只用于 stream 选路，转发规划器成本不变。

候选路径的稳定性代价 = 首跳上述 stream 分数 + 第二跳公告成本与抖动 + 合成丢包率 × 3ms/‰ + 中转节点转发惩罚 + 10ms；中转容量取两段较小值。按本 stream 的发送速率（0.5 秒窗口 EWMA）区分负载：超过约 4 Mbps 进入重负载，低于约 1 Mbps 退出。

- 轻负载：选稳定性代价最低的路径，沿用 25ms/五分之一的换路余量。入境丢包重的直连（如 kiwi→home 8%）会让位于经低丢包节点的中转。
- 重负载：只有中转容量估计不低于直连 1.5 倍时才走中转，已在中转上时不低于直连 1.1 倍才保留；直连或中转容量未知时走直连，保证中转不会降低重负载带宽。走直连时仍按稳定性在直连邻接间选择。

全部确认时可直接换路；有在途 `Data` 时只有 `Data` 帧能发起换路（限每秒一次）：进入排空，该流后续帧在 core 内按序排队并立即返回（不阻塞客户端会话，同会话的反向 `Ack` 照常发送），等旧路径数据全部确认后从新路径按序发出；5 秒未确认或排队超过 16 MiB 时放弃换路、排队帧发回旧路径，并退避 30 秒。排队中的 `Close` 发出后清理 affinity；被新 epoch 取代的排队帧丢弃，由端点恢复重传。

`/ops/status` 的 `mesh.adjacencies[]` 给出 `loss_permille`、`capacity_kbps` 估计，`mesh.stream_paths[]` 给出每条流的目标、`via_node_id`（中转）、传输、`heavy`、`rate_kbps` 及换路、排空、放弃计数。

## TUN

`turntf-tun` 的 `stream` 模式在发送 IP packet 前等待 `OpenAck`，发送窗口由累计 ACK 和 credit 控制。握手、发送或目标会话不可用时回到原有 Relay 模式；Relay 仍保持原有兼容行为。
