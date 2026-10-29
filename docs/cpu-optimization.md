# Mesh 路由规划 CPU 优化

发布 Dockerfile 的 `CGO_CFLAGS` 曾覆盖掉默认的 `-O2`，该构建问题及修复后的独立测量见 [发布构建的 CPU 优化检查](build-cpu-optimization.md)。本文的路由基准使用本机默认 C 优化参数，不包含该构建问题的影响。

## 改动和适用范围

`Planner.Compute` 使用 Dijkstra 算法搜索 `(node, transport)` 状态。此前每扩展一个状态，`expand` 都从空切片开始追加转移，产生短命的临时分配。现在一次 `Compute` 调用复用一个转移缓冲区，前 8 个转移使用栈空间，邻接度更高时仍由 `append` 自动扩容。

转移在更新 `best` 和加入优先队列时已按值复制，因此下一次扩展可以覆盖缓冲区。缓冲区不保留在 `Planner` 中，并发调用之间独立。选路比较、转移遍历顺序、桥接和中转策略、TCP mTLS 直连优先、TTL、去重与网络发送均沿用原实现。

收益主要作用于实际执行 Dijkstra 搜索的多跳或混合传输路径。已经提前返回的 TCP mTLS 直连优先路径不经过这段代码。不能将路由规划的降幅等同于整个服务 CPU 的降幅。

## 测量方法

环境：Linux amd64，Intel Core i5-12400，Go `go1.27.3-X:nodwarf5`。本机还有其他工作负载，性能结果使用相同源码状态、相同参数、前后版本交替运行的方式比较。

在当前工作树生成两个测试二进制；基线通过 Go `-overlay` 仅恢复修改前的 `internal/mesh/planner.go`，其余实现、已有工作树改动和新增 benchmark 完全一致。每组测量 6 对，每对交换运行顺序，以减小频率、温度和背景负载变化的偏差。全量功能回归先于这组性能测量结束。

- 路由微基准：`BenchmarkPlannerCPU`，`-test.cpu=1 -test.benchtime=500ms`。覆盖三节点、七节点线性拓扑，以及 16 条两级分支汇聚到同一目标的拓扑。邻接索引在计时前构建，与生产只读快照一致。
- 吞吐：`BenchmarkMeshTransientPointToPointThroughput/websocket/linear-7nodes/(256B|4KiB)`，`-test.cpu=4 -test.benchtime=100000x`。每种 payload 完成固定 100000 条消息，并校验目标收到的 ID、正文和 TTL。
- CPU：上述吞吐二进制由独立子进程运行，使用 Linux `getrusage(RUSAGE_CHILDREN)` 的 user + system CPU 秒。两个 payload 合计 200000 次正式投递；进程 CPU 指标还包括相同的夹具 setup、warmup、清理和 benchmark 启动成本，因此不是纯生产进程的稳态 CPU profile。
- 延迟：`BenchmarkMeshTransientPointToPointLatency`，`-test.cpu=4 -test.benchtime=5000x`。使用同一七节点 WebSocket 路径，逐条发送并等待目标校验完成，分别记录 p50、p95 和 p99；采样数组在计时前分配。它验证无额外并发压力的端到端延迟，不代表生产饱和负载下的尾延迟。

已有 profile 中，七节点 WebSocket transient 转发的 `Planner.Compute` 累计占 14.90 / 72.63 CPU 秒（20.51%）。该 profile 包含夹具和测试进程开销，仅用于定位热点。

## 本机结果

以下取前后各轮结果的中位数。

| 路由规划场景 | B/op 前 → 后 | allocs/op 前 → 后 |
| --- | ---: | ---: |
| 三节点线性 | 352 → 224（-36.4%） | 7 → 5（-28.6%） |
| 七节点线性 | 864 → 480（-44.4%） | 15 → 9（-40.0%） |
| 16 分支汇聚 | 13,512 → 10,616（-21.4%） | 84 → 48（-42.9%） |

自适应微基准的耗时受后台负载影响，六轮中位数下降 2.3%–3.5%，但单对样本波动明显。为单独核验 CPU，另以七节点微基准、`-test.cpu=1 -test.benchtime=1000000x`、前后交替各 6 轮测量进程 CPU 时间。每百万次规划的进程 CPU 中位数从 **1.411849s 降到 1.233945s（-12.6%）**；此值包含相同的测试断言、夹具及进程固定开销，不包含构建时间。

七节点吞吐测试中，每个进程合计完成 200000 次正式投递（256B、4KiB 各 100000 次），进程 CPU 中位数从 **37.6975s 降到 36.6464s（-2.8%）**。这是整个测试进程的结果，不能用路由微基准的 12.6% 代替它。

| 首轮七节点吞吐矩阵 | ns/op 前 | ns/op 后 | 变化 |
| --- | ---: | ---: | ---: |
| 256B | 39,490 | 42,896 | +8.6% |
| 4KiB | 68,250 | 63,809 | -6.5% |

首轮 256B 结果没有满足“吞吐不下降”的目标，因此另以相同二进制、相同七节点路径、`-test.cpu=4 -test.benchtime=50000x` 只测 256B，交替运行前后各 8 轮：

| 256B 定向复测 | 优化前中位数 | 优化后中位数 | 变化 |
| --- | ---: | ---: | ---: |
| ns/op | 52,476.5 | 43,693.0 | -16.7% |
| 每 50000 次正式投递的进程 CPU | 7.8510s | 7.1868s | -8.5% |

定向复测没有复现首轮的小包吞吐下降。不过，两组吞吐测试的绝对耗时也显示出明显的背景负载波动，因此不把复测的 16.7% wall time 改善当作稳定收益承诺。当前结论是 CPU 开销和分配量下降，未观察到稳定的吞吐退化；不能仅凭分配量下降推定任意负载的吞吐或尾延迟。

| 七节点逐条确认延迟 | 平均值 前 → 后 | p95 前 → 后 | p99 前 → 后 |
| --- | ---: | ---: | ---: |
| 256B | 140.95 → 136.61µs | 240.55 → 239.05µs | 469.60 → 470.45µs |
| 4KiB | 253.14 → 246.42µs | 512.65 → 489.10µs | 1170.00 → 1041.75µs |

256B 的 p99 变化为 +0.18%，4KiB 为 -11.0%。这些数据只能说明当前测量口径下的表现，不证明所有生产负载均无退化，尤其没有覆盖饱和负载下的尾延迟、其他集群传输或磁盘持久化链路。

## 复测命令

在服务端仓库中执行：

```bash
go test ./internal/mesh -run '^$' -bench '^BenchmarkPlannerCPU$' -cpu=1 -benchmem -benchtime=500ms -count=6
go test ./internal/cluster -run '^$' -bench '^BenchmarkMeshTransientPointToPointThroughput/websocket/linear-7nodes/(256B|4KiB)$' -cpu=4 -benchmem -benchtime=100000x -count=6
go test ./internal/cluster -run '^$' -bench '^BenchmarkMeshTransientPointToPointLatency$' -cpu=4 -benchmem -benchtime=5000x -count=6
go test ./internal/mesh -run '^$' -bench '^BenchmarkPlannerCPU/linear-7$' -cpu=1 -benchmem -benchtime=1000000x -count=6
go test ./internal/cluster -run '^$' -bench '^BenchmarkMeshTransientPointToPointThroughput/websocket/linear-7nodes/256B$' -cpu=4 -benchmem -benchtime=50000x -count=8
```

前后比较应先分别 `go test -c`，再交替执行两个二进制；不要把构建时间混入 CPU 指标，也不要同时运行性能测试和功能回归。

行为检查：

```bash
go test ./... -count=1
go test -race ./internal/mesh -count=1
./scripts/smoke.sh
```

`TestPlannerHighFanoutThenDeadEnd` 验证超过栈缓冲容量的拓扑仍能选择预期路径，并在删除目标的所有入边后正确返回不可达。现有 planner / forwarding 测试继续覆盖拒绝中转、桥接、不同流量策略和回环处理。

本轮全量测试、mesh race 检查和 smoke 均通过，所有基准子进程也均成功完成。独立只读审查未发现此次缓冲区复用引入选路、并发或生命周期回归。

本次工作区的原始记录保存在 `/tmp/turntf-cpu-review/`：`paired-results.json` 包含首轮六对微基准、吞吐及延迟结果，`planner-cpu-results.json` 包含固定次数路由 CPU 复测，`small-packet-results.json` 包含八对小包吞吐复测；记录包含原始输出、参数和各进程 CPU 时间。该目录为本地临时产物，不是仓库内的长期性能基线。
