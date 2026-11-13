# 慢会话查询阻塞实时 Stream 的复现与修复

## 复现范围

router 故障曾同时出现会话查询响应超时、Stream ACK 停滞和 WebSocket 自动重连。本回归复现其中一个确定的服务端放大机制：先前的慢会话查询或定向 Relay 发送，会让同连接后续专用 StreamFrame 和 Ping 等待。

这个局部复现不需要制造公网丢包、修改生产超时或重启节点，也不证明线上最初的超时一定来自该机制。外层网络退化、慢客户端写入和同步读回调仍可能是触发因素。

## 可重复的故障注入

`internal/api/client_realtime_stream_isolation_test.go` 使用通道控制 resolver 的进入和释放时机：

1. 在已登录的 `/ws/realtime` 连接发起 `ResolveUserSessions`，或发起会执行会话校验的定向 Relay 请求。
2. 等待请求已进入 resolver，然后保持它不返回，模拟远端会话查询变慢。
3. 在同一连接继续发送 Stream DATA、Ping、另一条 Stream 的 ACK。
4. 要求在 resolver 释放前收到相应 Stream result 和 Pong；若它们必须等待先前查询，测试失败。
5. 取消连接，确认尚未完成的查询能够停止并回收。另测释放后错误仍按原 `request_id` 对应，不被先完成的 Stream 结果覆盖。

同一回归还覆盖 64 个已接纳请求占满并发组的情形，并通过真实本地 WebSocket 重复慢查询→Stream DATA/ACK→Ping 的过程，校验帧的顺序及响应。真实 WebSocket 测试不依赖生产账号，也不修改网络规则。

在服务端模块目录执行：

```sh
go test ./internal/api \
  -run 'TestRealtime(SlowRequestsDoNotBlockStreamAndPing|LookupErrorAfterStreamKeepsRequestID|WebSocketSlowLookupAllowsStreamAndPing)$|TestOrdinarySlowLookupStillOrdersStreamAndPing$' \
  -count=1 -timeout=90s
```

将同一测试加入修复前的 `87849bc` 时，三个慢请求子场景和错误返回场景均因收不到 Stream 响应失败，真实 WebSocket 场景发生读取超时。普通 `/ws/client` 的串行对照通过。修复后以上用例全部应通过。

## 最小修复

原 `client_dispatch.go` 在处理不是并发 Relay/Lookup 的请求前调用 `sends.pending.Wait()`，专用 StreamFrame 和 Ping 也落在这个分支：

```text
慢 Resolve / Relay → pending.Wait → Stream DATA/ACK 等待
                            └→ Ping 等待 → 后续帧继续排队
```

修复只对 `/ws/realtime` 的 StreamFrame 和 Ping 跳过这次排空等待。二者继续由同一读取循环同步处理，不增加 goroutine 或发送队列，因此保留 Stream 帧的连接内顺序。逐帧鉴权、session、epoch、offset 验证、路由错误及响应关联保持原路径。

Ping 也必须豁免：只放行 StreamFrame 会让下一次 Ping 再次停在排空等待，阻挡其后的 DATA/ACK。相应地，实时连接上的 Pong 仅说明该心跳得到处理，不能作为“先前发送和查询已经全部完成”的屏障。

持久消息的 AckMessage、重复 Login、未定向发送及状态 RPC 仍保留排空屏障，普通 `/ws/client` 仍串行执行。协议字段与 SDK API 均不需要修改。

## 验证与剩余边界

应继续运行已有的 Stream 连接内顺序、AckMessage 状态屏障、接纳上限、逐请求鉴权、响应写失败及断线取消测试，并完成：

```sh
go test ./... -count=1
go test -race ./internal/api -count=1
./scripts/smoke.sh
```

本次解除的是等待已有并发组全部结束的屏障，并非保证同连接上所有慢操作完全隔离：

- 当读取循环正等待第 65 个查询或定向发送的接纳名额时，后面尚未读取的 Stream/Ping 仍受背压。测试中的 64 个已接纳请求与第 65 个正在等待名额的请求是不同边界。
- 慢 Stream 路由自身仍同步执行，可能挡住其后的帧；普通状态屏障也会阻挡后续读取。
- 共享响应写锁、慢接收端、SDK 同步回调与设备写入仍可能传播背压。

这些边界需要分别故障注入，不能通过放大超时、建立无界队列或并发发送有序 DATA 来规避。线上验证应在隔离环境先模拟查询延迟，再按节点灰度并保留失败样本，观察业务请求、ACK 进度和重连，而不仅检查集群 connected。本次代码修复本身不包含生产发布。

## 本次验证记录

- 将最终回归测试配合 `go test -overlay` 指向 `87849bc` 的旧 `client_dispatch.go`，上述五个阻塞子场景稳定失败；真实 WS 测试失败清理能够释放注入查询并等待 handler 退出。
- 最终代码的故障注入回归通过，完整 API `-race` 检查通过；补充的传输失败及测试清理变更也单独通过 `-race` 回归。
- 最终完整测试 `go test -p 1 ./... -count=1 -timeout=15m` 通过，`./scripts/smoke.sh` 通过。API、cluster、mesh、store 均完成实际测试，没有跳过存储或启动路径。
- 首轮与竞态检查并行运行全量测试时，未改动的 `TestPebbleBackgroundTrimEventuallyUpdatesMessageUserState` 出现一次 1 秒条件等待失败；原始失败保留，单独复查及最终串行全量复跑通过，没有放宽该测试期限或修改存储代码。仅凭复跑通过不确定这次偶发失败的原因。
- 独立只读审查的测试清理问题已修正，复查未发现剩余阻塞项。上述记录对应发布前本地验证；生产部署结果应以单独的发布记录为准。
