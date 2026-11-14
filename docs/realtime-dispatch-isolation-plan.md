# 实时调度反例的修复

## 已确认的行为

用户已明确选择：同一 stream 有序、不同 stream 并行；后台查询/定向 Relay 满额时，只拒绝未接纳请求并按原 `request_id` 返回现有 `service_unavailable`。此变更仅涉及服务端实现和文档，不新增协议字段。

状态 RPC、持久 `AckMessage`、重复 Login 等显式控制屏障仍保留。普通 `/ws/client` 继续同步执行。跨 stream 的结果和 Pong 可能重排，调用方按 `request_id` 关联；Pong 不是排空先前工作的屏障。

## 反例与实现

| 反例 | 原路径 | 修复 |
| --- | --- | --- |
| 第 65 项后台请求 | reader 等待名额，后续 IO 尚未读取 | 非阻塞接纳；满额错误入有界 reader 响应队列，请求不执行 |
| 慢 Stream 路由 | reader 同步等待其他 stream 和 Ping | 每个完整 stream 身份单 worker FIFO，不同 lane 并行 |
| 共享慢响应写入 | reader 的 Pong/过载错误等待写锁 | reader 只排入有界响应队列，单独 worker 等待实际写入 |
| 锁等待不计入写期限 | 接收端 mutex 等待可能累积 | 实时写入的锁等待与 Send 共用 10 秒预算；超时 Close 原 transport |
| 首个后台请求前空闲取消 | 尚未安装取消关闭回调 | 实时 readLoop 入口建立唯一并发组，先安装回调再 Receive |

## 接纳和资源预算

后台查询和定向 transient Relay 合计最多 64 项。满额时不运行 handler，不缓存鉴权或路由结果，而是排入带原 request ID 的 `service_unavailable`。若错误响应也无法接纳，则明确关闭过载连接。

专用 Stream 的活动与排队帧合计最多 64 项、8MiB。字节数按 `proto.Size(StreamFrameRequest)` 计量，并包含正在路由或等待响应写入的帧；不是进程 RSS 上限。lane 的 key 包含目标用户、目标 session 和 stream ID，发送连接的 source session 是固定身份。epoch 不划分 lane，完整身份相同的跨 epoch 帧不能互相绕行。lane 清空后在提交锁内退役，后续同 key worker 只有在旧 handler 全部结束后才能运行。

超过帧数或字节预算会失败并关闭会话，不静默丢弃 `request_id=0` 的帧，也不伪造接纳成功。每帧沿用原鉴权、session/epoch/offset 校验和错误映射。

reader 响应队列由一个 worker 消费，合计最多 128 项、1MiB，包括正在发送的一项。这里只容纳 Pong、满额拒绝和无效 transport/protobuf 帧错误；后台与 stream handler 仍在各自 worker 中等待原写路径完成。因此不需要再为所有登录、推送和普通 RPC 建立一套全流量发送队列。

## 实际写入与关闭

所有物理写入仍由原 session `writeMu` 串行化。实时写入在尝试获得该锁之前开始 10 秒期限，将等待锁和 `Send` 纳入同一预算。期限到达时关闭该接收连接的 transport，使持锁发送返回、等待者获得锁后观察到过期。函数返回前等待已启动的关闭回调，避免把卡住的发送 goroutine 留在后台。

reader 不等待物理发送，不等于消息已送达。响应 worker 的异步失败会记录失败、取消会话、关闭原 transport 并清理剩余队列。完全不可写的共享 socket 上不能保证收到 Pong；此时承诺明确失败和有限资源，而不是无限缓存。有限输入中各写入竞争者依赖运行时调度继续执行，不据此证明无限流中的严格优先级或有界公平等待。

普通客户端保留原写路径。发往另一个 receiver 的操作仍受该 receiver、mesh 及 backend 的生命周期约束；发送者取消不自动意味着另一个接收连接应被关闭。实时接收端的写预算改进了其等待，但不是全部跨节点任务 join 的总时限。

## 屏障和收尾

控制请求在 reader 中依次等待此前的后台请求、Stream 工作及 reader 响应全部完成，再检查取消并执行。工作中的实际写入仍是 handler 完成条件，未将排队成功冒充物理发送成功。

reader 是唯一提交者，进入屏障或收尾后不再新增工作。取消先关闭 transport，再等待后台任务、stream worker 与响应 worker 退出；排队但尚未处理的帧会释放数量/字节预算和 WaitGroup 计数。返回首个观察到的传输或资源失败。收尾仍以 backend 能响应取消、Close 能中断原 transport 及本地动作能继续被调度为前提。

## 验证

- 后台满额：混合占满 64 个查询/Relay，第 65 项明确拒绝，后续 Stream/Ping 仍进展且关联正确。
- 路由隔离：A1 阻塞，A2 不越序；B 与 Ping 完成。分别覆盖不同 stream、目标 session、目标用户和同 lane 换 epoch。
- 写入隔离：后台响应 Send 卡住时，reader 仍读到后续 Stream、Ping 及 Ping 后的标记；不要求不可写 socket 交付 Pong。
- 数量与字节限制：活动项参与预算；超限含零 request ID 均关闭；取消后计数归零。
- 显式屏障：AckMessage/状态请求等待先前后台、stream、reader 响应，再观察后续消息。
- 写期限：测试实际传入的有限 deadline、忽略 context 的 Send 被 Close 中断，以及锁等待过期后不再尝试第二次物理 Send。
- 真实 WebSocket：慢 A、健康 B 和 Ping，与同流结果顺序。
- 规约：保留旧行为反例、新实现同流顺序/健康流进展、饱和拒绝、资源有界、关闭及非真空见证；不宣称完整 mesh、真实时间或 Go 精化证明。

完整 Go、API race、smoke 和 TLC 的实际结果见验证记录；生产发布结果以独立发布记录为准。
