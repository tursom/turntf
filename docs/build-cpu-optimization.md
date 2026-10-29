# 发布构建的 CPU 优化检查

## 修复：恢复 CGO 的 C 编译优化

Dockerfile 原来使用：

```bash
CGO_CFLAGS="-D_LARGEFILE64_SOURCE -D_GNU_SOURCE"
```

设置 `CGO_CFLAGS` 会替换 Go 默认的 `-O2 -g`，而非在默认值后追加参数。`github.com/mattn/go-sqlite3` 的包级 `#cgo CFLAGS` 只设置标准、功能宏和警告选项，没有补充 `-O` 优化级别。因此，这条发布构建路径中 SQLite 的 C 源码没有获得 `-O2`，由 GCC 按默认的 `-O0` 编译。

已修改为：

```bash
CGO_CFLAGS="-O2 -D_LARGEFILE64_SOURCE -D_GNU_SOURCE"
```

保留两个原有兼容性宏，恢复 C 编译优化。`-O2` 通过环境变量传给 C 编译器，不是 Go 编译器的参数。没有提高目标 CPU 指令集要求，也没有修改 SQLite 的持久化、同步或消息语义。

前一轮源码优化使用本机 `go test` 默认 C 编译参数（含 `-O2 -g`），没有复现 Dockerfile 覆盖默认参数的问题。因此，那轮结果只能比较源码优化，不能回答原发布构建中 SQLite 未优化的额外成本。

## SQLite 对比结果

环境为 Intel Core i5-12400、Linux amd64、Go 1.26.2、本机 GCC / glibc。Go 版本属于发布 Dockerfile 使用的 1.26 系列；未构建和测量 Alpine / musl 的最终镜像，也未验证带 ZeroMQ tag 的容器构建。

同一工作树生成两个测试二进制，显式关闭 PGO，只改变 `CGO_CFLAGS` 中是否包含 `-O2`。二进制交替运行前后各 6 轮，每轮固定执行 5000 次 `BenchmarkStoreListMessagesByUser/tmp/sqlite/history-1000`，`-test.cpu=1`。夹具准备 1000 条历史，按现有消息窗口保留历史，正式查询请求 50 条。

| 指标，中位数 | 原构建参数 | 补回 -O2 | 变化 |
| --- | ---: | ---: | ---: |
| 正式查询 ns/op | 274,083.5 | 160,374.0 | -41.5% |
| 整个测试进程 CPU，user + system | 2.128378s | 1.060391s | -50.2% |

进程 CPU 通过 `getrusage(RUSAGE_CHILDREN)` 测量，包含相同的测试启动、夹具写入、预热、5000 次查询和清理，不包含构建时间。该指标不能解释为整个 turntf 服务的稳态 CPU 降幅；正式计时的 `ns/op` 则排除了夹具准备。此次结果验证了 SQLite 查询路径的改善，没有测生产饱和负载下的尾延迟。

构建命令：

```bash
GOTOOLCHAIN=go1.26.2 CGO_CFLAGS='-D_LARGEFILE64_SOURCE -D_GNU_SOURCE' \
  go test -c -pgo=off -o /tmp/turntf-cgo-before.test ./internal/store
GOTOOLCHAIN=go1.26.2 CGO_CFLAGS='-O2 -D_LARGEFILE64_SOURCE -D_GNU_SOURCE' \
  go test -c -pgo=off -o /tmp/turntf-cgo-after.test ./internal/store
```

交替执行前后两个二进制，重复 6 对：

```bash
/tmp/turntf-cgo-before.test -test.run='^$' \
  -test.bench='^BenchmarkStoreListMessagesByUser/tmp/sqlite/history-1000$' \
  -test.cpu=1 -test.benchtime=5000x -test.benchmem -test.count=1
/tmp/turntf-cgo-after.test -test.run='^$' \
  -test.bench='^BenchmarkStoreListMessagesByUser/tmp/sqlite/history-1000$' \
  -test.cpu=1 -test.benchtime=5000x -test.benchmem -test.count=1
```

本地原始输出、参数和进程 CPU 保存在 `/tmp/turntf-build-cpu/cgo/results.json`，该临时目录不是长期性能基线。

验证已通过：

```bash
GOTOOLCHAIN=go1.26.2 CGO_CFLAGS='-O2 -D_LARGEFILE64_SOURCE -D_GNU_SOURCE' \
  go test ./... -count=1
GOTOOLCHAIN=go1.26.2 CGO_CFLAGS='-O2 -D_LARGEFILE64_SOURCE -D_GNU_SOURCE' \
  GOCACHE="$(go env GOCACHE)" ./scripts/smoke.sh
```

两个对照二进制的 `go version -m` 元数据也确认了工具链均为 Go 1.26.2、`GOAMD64=v1`，`CGO_CFLAGS` 的实质差异仅为是否包含 `-O2`。

## 其他构建参数的检查

- Go 编译器默认开启优化；当前 `GOFLAGS` 为空，没有 `-N -l` 关闭优化。不能把缺少额外参数等同于 Go 代码未优化。
- 现有 `-trimpath` 用于删除构建路径，`-ldflags="-s -w"` 用于去掉符号和调试信息，均不能据此承诺运行时 CPU 改善。
- 未找到主程序的 `default.pgo` 或显式的 PGO profile 配置，当前发布构建没有利用业务采样进行 PGO。
- 本机 `GOAMD64` 默认是 `v1`。提高到 `v3` 会提高目标机器的指令集要求，不能直接替换面向通用 amd64 的发布产物。

另以 Go 1.27 本机工具链和之前的七节点 WebSocket CPU profile 做了探索性 PGO 测量，保持源码一致：六轮中位数下，固定一百万次七节点路由规划的进程 CPU 从 1.4882s 降到 1.4001s（-5.9%）；固定五万次七节点 256B 转发的进程 CPU 从 6.0527s 降到 5.8889s（-2.7%），ns/op 从 32606.5 降到 31795.5（-2.5%）。单独提高到 `GOAMD64=v3` 未在该路由微基准中观察到收益。这些结果不等于 Go 1.26 容器发布结果，benchmark profile 也不代表所有生产流量，尚未将它固化到发布构建。
