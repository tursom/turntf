# 持久化与复制路径 CPU 优化实验

本轮以 `86f4b26` 为基线，在保留已有路由缓冲复用和发布构建 `CGO_CFLAGS=-O2` 修复的前提下，分别试验 SQL、编码、复制、收件箱读取和去重优化。保留判断同时看完整操作耗时、分配和延迟；局部 CPU 收益不能抵消已观察到的吞吐或尾延迟回退。

## 实现边界

- SQLite 在初始化身份的事务提交后预编译固定 SQL，通过 `Tx.StmtContext` 在当前事务复用，重复初始化和关闭时释放语句。语句不保存用户、序列号或窗口的事实值。
- SQLite 写消息只读取权限判断需要的角色及原读取仍需校验的整数、HLC 字段，省去完整 `User` 构造及 profile、密码、用户名读取。角色、删除、黑名单仍在当前事务判断；保留损坏 HLC 的错误行为。
- 持久化分发没有任何候选会话时立即完成该事件，外层游标继续推进；候选仍包含管理员。需要发送时，同步编码借用正文，返回的 payload 独立持有数据。
- 复制事件在多 peer 入队前只转换一次 protobuf、计算一次大小和 HLC，各 peer 保持自己的批次边界，编码期间共享对象只读。
- Pebble 收件箱按顺序逐条解码，找到足够可见消息便停止。先创建 iterator 固定收件箱消息视图，再读取黑名单和订阅；不完整时的 legacy 回退独立重读权限。原始扫描窗口、广播合并、排序和去重仍保留。
- Pebble 本地原子批次内仍逐条更新计数、判断 trim 门槛，只把同一用户的最终状态写入 batch 一次；消息索引、事件、状态和序列仍在同一次 Commit，提交失败不发布内存状态。
- Snapshot hash 每次调用复用编码缓冲，哈希仍包含原来的确定性 protobuf 和长度分帧。没有引入跨写入摘要缓存或失效机制。

## 测量方法

环境为 Linux amd64、Intel Core i5-12400，Go `go1.26.2`，`GOWORK=off`，`CGO_CFLAGS='-O2 -D_LARGEFILE64_SOURCE -D_GNU_SOURCE'`。数据库临时目录统一设为 `/var/tmp/turntf-cpu-trials`，位于本机 SSD ext4；基线源码 worktree 在 tmpfs，但数据库不放在该 worktree 下。

先各自 `go test -c`，之后顺序运行测试二进制，构建、功能测试与性能测量不并发。基线通过 overlay 只加入同一份独立 benchmark；SQLite 三个候选、Pebble 元数据复用、流式读取和批写先做单因素对照，再复测最终组合。以下报告使用每组多次运行的中位数；短微基准一般三轮，需核查的批写另做五轮。

- 微基准 `-test.cpu=1`，每子场景计时 300ms～1s，报告 `ns/op`、`B/op`、`allocs/op`。
- HTTP handler 基准包括认证、JSON 和实际存储写入，不含真实 HTTP 网络；先填满消息窗口，再固定次数逐条写入，采样数组在计时外分配。
- 进程 CPU 使用独立子进程的 `getrusage(RUSAGE_CHILDREN)` user + system 秒。固定次数才能比较；该值还包含夹具创建、预热、清理和 benchmark 启动开销，不当作生产稳态 CPU profile。
- 集群投递基准包含真实 WebSocket 连接及目标 ID、正文、TTL 校验；延迟从测试辅助函数调用到确认完成，包含 waiter、复制和计时等辅助设施，不是纯线路延迟。
- Pebble 的 128 条原子批次基准把清理放在计时外，包含真实 WAL/Commit；不代表后台 trim 的稳态表现。另用既有并发写入基准核查 NoSync、ForceSync。

## 撤回的候选

| 实验 | 获益 | 撤回依据 |
| --- | --- | --- |
| SQLite 复用 trim 边界再执行范围删除 | 普通完整写入单因素快约 5% | 交错生产者投影、128/1024 条窗口分别慢约 11%/30%；叠加其他优化后 1024 窗口仍慢约 12%。恢复原来的 OFFSET 删除集合，仅保留语句预编译。 |
| Mesh 去重到期 FIFO，加乱序时间堆 | 1000 包/秒、四个 TTL 窗口的每次处理从 667ns 降到 173ns；无到期扫描明显减少 | 普通插入从 128ns 增至 144ns；262144 条集中到期从 11.75ms 增至 35.42ms；同规模 GC 后持有内存从 29.38MB 增至 42.49MB，撤销重插更高。已恢复原去重实现。 |
| Pebble inbox 与 legacy 回退共享权限读取 | 不完整收件箱单因素快约 5% | 独立审查发现，先读权限再读更新后的消息，或把旧订阅用于回退，可能返回拉黑/退订后才产生的消息。恢复原读取顺序和回退重读，仅保留按需解码。新增两个确定性插入并发写入的测试覆盖该边界。 |

去重实验仍按原清理时点、严格 `age > TTL`、运行时 epoch 和撤销重试语义实现，失败原因是性能取舍。保留独立 benchmark 便于以后尝试其他算法；实验实现和实现专属测试不留在运行时代码中。

## 保留项与完整路径结果

以下为各组中位数。SQL 行使用撤回 trim 边界实验后的组合；收件箱行使用恢复权限读取顺序后的最终版本。

| 场景 | 优化前 | 保留版本 | 变化 |
| --- | ---: | ---: | ---: |
| SQLite 完整写入，收发不同用户、普通 profile | 226.04µs/op | 167.78µs/op | -25.8% |
| SQLite 完整写入，收发不同用户、16KiB profile | 241.51µs/op | 169.12µs/op | -30.0% |
| SQLite 消息投影，128 / 1024 条窗口 | 88.94 / 132.49µs/op | 70.13 / 112.51µs/op | -21.1% / -15.1% |
| 无候选会话的持久化分发，4KiB 正文 | 3701ns、9464B、12 alloc/op | 105ns、0B、0 alloc/op | 耗时 -97.2% |
| 持久化消息编码，4KiB 正文 | 3511ns、9456B、11 alloc/op | 2214ns、5352B、5 alloc/op | 耗时 -36.9% |
| 复制准备及入队、刷新，32 事件、4KiB、4 peer | 119.93µs/op | 52.73µs/op | -56.0% |
| 同上，16 peer | 407.95µs/op | 137.20µs/op | -66.4% |
| Pebble 128 条热点用户批写，balanced / throughput | 1078.10 / 1009.04µs/op | 1006.43 / 927.98µs/op | -6.6% / -8.0% |
| Pebble 128 个不同用户批写，balanced / throughput | 1448.45 / 1556.04µs/op | 1443.86 / 1506.80µs/op | -0.3% / -3.2% |
| Snapshot hash，500 条 × 4KiB | 1.680ms、2432032B/op | 1.194ms、10272B/op | 耗时 -28.9%，分配字节 -99.6% |
| 完整 Snapshot digest，SQLite / Pebble | 5.578 / 4.120ms/op | 5.135 / 3.904ms/op | -7.9% / -5.2% |

复制准备基准包含入队、flush、封包和 channel drain，但没有网络和存储，不能把其 66% 改善当作整个集群的改善。Snapshot 完整 digest 仍重新读取所有行，收益低于纯 hash 微基准。

最终收件箱复测也按 AB、BA 交替，前后各四轮，历史 256 条、请求 20 条。没有把已撤回的权限复用收益算入下表。

| Pebble 查询场景 | balanced 前 → 后（µs/op） | throughput 前 → 后（µs/op） |
| --- | ---: | ---: |
| 完整 inbox | 530.12 → 65.27（-87.7%） | 279.84 → 47.49（-83.0%） |
| 强过滤 | 528.62 → 481.03（-9.0%） | 287.08 → 269.06（-6.3%） |
| inbox 不完整、回退 | 663.63 → 671.73（+1.2%） | 402.58 → 396.63（-1.5%） |
| 广播与频道混合 | 601.38 → 113.63（-81.1%） | 450.39 → 95.57（-78.8%） |
| 只有本地消息、回退 | 641.93 → 613.19（-4.5%） | 368.90 → 359.08（-2.7%） |

balanced 完整 inbox 的分配从 231291B、2407 次降至 22238B、282 次；不完整收件箱的分配字节下降约 10%，但耗时没有稳定收益。对回退路径不承诺加速。

### 完整请求、CPU 与延迟

最终 HTTP 复测按 AB、BA、AB、BA 顺序，前后各四轮，每轮分别固定 20000 次写入，`-test.cpu=4`。SQLite 的进程 CPU 中位数从 **6.327700s 降至 4.126215s（-34.8%）**；Pebble 从 **1.709738s 到 1.716674s（+0.4%）**。后者没有明显 CPU 收益，也没有稳定回退的证据；本地批写优化主要作用于并发聚合形成的多消息批次。

| HTTP handler，256B | 平均值 前 → 后 | p95 前 → 后 | p99 前 → 后 |
| --- | ---: | ---: | ---: |
| SQLite | 300.93 → 214.19µs | 461.60 → 322.70µs | 3491.0 → 3370.0µs |
| Pebble balanced NoSync | 46.86 → 47.12µs | 48.63 → 49.88µs | 178.65 → 176.35µs |

SQLite 平均耗时下降 28.8%，p95 下降 30.1%，p99 下降 3.5%；分配从 28113.5B、495 次降至 26139.5B、430 次。Pebble 平均值变化 +0.6%，p95 +2.6%，p99 -1.3%，与首轮观察到的小幅变化一起如实保留，不宣称该单请求路径有收益。

既有 Pebble 并发热点写入基准每轮固定 20000 条、4 个并发执行者：NoSync 从 26.71 降至 25.03µs/op（-6.3%），ForceSync 从 229.30 到 230.99µs/op（+0.7%）。ForceSync 受磁盘同步影响，当前样本不能支持改善的结论。

真实三节点、1000 在线用户、SQLite、256B 持久化 direct 投递，每轮固定 1000 条：平均完成时间从 4.139 降至 3.978ms，p95 从 5.378 降至 4.971ms，p99 从 11.80 降至 11.17ms；进程 CPU 从 2.734 降至 2.639s。样本包含一次优化版本的额外后台分配异常，因此没有把这条复杂链路的分配量当作稳定收益。

七节点 WebSocket transient 路径也做了旁路回归：256B / 4KiB 并发投递耗时变化 -1.9% / +0.5%；逐条确认 p99 变化 -7.1% / +1.2%。去重实验已撤回，该路径不是本轮主要获益对象，微小变化只用于检查明显回退。

这些结果支持保留上述改动；不构成对任意生产负载、饱和压力、所有磁盘和网络条件的零回退保证。没有修改协议格式、ack 时机、批次等待时间或持久化模式。

### 行为验证

新增回归覆盖语句初始化失败、单连接及重复初始化生命周期；事务内角色/删除/黑名单变化；损坏 HLC；trim 删除集合、保存点回滚与重放；编码 wire 完全一致和正文所有权；共享复制事件并发编码、批次条数/字节/等待时间边界；Pebble 不同读取/过滤/回退场景及批写失败、重启后的状态；Snapshot 哈希与独立参考实现一致。

权限读取顺序的两个测试采用仓库接口包装，在“读取旧权限”和后续消息投影之间精确插入拉黑或退订：在已撤回的复用候选上均失败，在 `86f4b26` 原实现和最终流式实现上均通过。

最终检查命令如下，沿用上述 Go/CGO 环境；性能测量结束后才运行。

```bash
go test ./... -count=1 -timeout=5m
go test -race ./internal/store ./internal/mesh -count=1 -timeout=3m
go test -race ./internal/api ./internal/cluster -run 'Test(Persistent|Replication|Prepared|SnapshotHash)' -count=1 -timeout=3m
GOCACHE=/root/.cache/go-build ./scripts/smoke.sh
git diff --check
```

上述检查均通过：全量测试中 API 包约 161 秒，包含原有长超时用例；store/mesh 全包 race、API/cluster 的持久化及复制定向 race 均通过；smoke 完成健康检查、登录、用户及消息读写流程。两次独立只读审查中，发现的权限顺序回归已修正并补充复现测试，去重索引的资源保留问题随候选撤回一并消除。

## 复测入口

在服务端模块中使用相同工具链和 C 优化参数：

```bash
export GOWORK=off GOTOOLCHAIN=go1.26.2
export CGO_CFLAGS='-O2 -D_LARGEFILE64_SOURCE -D_GNU_SOURCE'
export TMPDIR=/var/tmp/turntf-cpu-trials
mkdir -p "$TMPDIR"

go test ./internal/store -run '^$' -bench '^BenchmarkSQLiteCPU(CreateMessage|ProjectionTrim)$' -cpu=1 -benchmem -benchtime=500ms -count=3
go test ./internal/api -run '^$' -bench '^BenchmarkPersistent(DispatchEmptyCandidates|MessageEncoding)$' -cpu=1 -benchmem -benchtime=300ms -count=3
go test ./internal/cluster -run '^$' -bench '^BenchmarkReplicationPrepareFanout$' -cpu=1 -benchmem -benchtime=300ms -count=3
go test ./internal/store -run '^$' -bench '^BenchmarkPebbleCPU(ListMessages|LocalMessageBatch)$' -cpu=1 -benchmem -benchtime=1s -count=5
go test ./internal/store -run '^$' -bench '^BenchmarkSnapshot(HashRows|DigestCPU)$' -cpu=1 -benchmem -benchtime=300ms -count=3
go test ./internal/api -run '^$' -bench '^BenchmarkHTTPCreateMessageCPULatency$' -cpu=4 -benchmem -benchtime=10000x -count=3
go test ./internal/mesh -run '^$' -bench '^BenchmarkMeshDedup(Insert|NoExpirySweep|Sustained|ExpiryBurst)$' -cpu=1 -benchmem -benchtime=300ms -count=3
go test ./internal/mesh -run '^$' -bench '^BenchmarkMeshDedupRetainedMemory$' -cpu=1 -benchmem -benchtime=1x -count=3
```

正式前后比较应先编译两个测试二进制，交替运行，避免将构建和测试夹具开销混入计时。`TMPDIR` 应按复测机器选择相同的存储介质，不能把 tmpfs 数据库和磁盘数据库互相比。

本机原始记录保存在 `/tmp/turntf-cpu-trials/`：`initial-results.json` 为独立候选试验，`pebble-results.json` 为读写拆分试验，`retained-results.json` 为撤回 trim/去重后的组合与集群测试，`final-results.json` 为权限顺序修正后的最终 HTTP/收件箱交替复测。每条记录包含原始输出、参数、CPU 和 wall 秒。前几组短测量的双版本顺序固定为先基线、后候选，最终 HTTP/收件箱四轮复测修正为 AB、BA 交替；不要把短测量中的微小耗时差异当作稳定收益。原始记录与实验源码快照是本地临时产物，不是长期性能基线。
