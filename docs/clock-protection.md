# 时钟保护算法

本文档整理当前代码已经实现的时钟保护机制。它描述现状，不是未来设计草案；如果实现发生变化，应同步更新本文档。

## 目标

- 保护字段级 `LWW` 合并语义，避免明显错误的本地系统时钟污染 HLC。
- 在集群模式下阻止未完成可信校时的节点继续产生本地写入。
- 对时钟偏差过大、校时质量过低、未来 HLC 事件或快照分片做拒绝或降级处理。
- 在多 peer 场景下用可信校时样本修正本地 HLC 的物理时间偏移。

## 相关组件

- HLC 实现在 [internal/clock/hlc.go](/root/dev/sys/turntf/internal/clock/hlc.go)。`Clock.Now` 和 `Clock.Observe` 使用“本地物理时钟 + 当前 offset”生成或推进 HLC，并保证单调递增。
- 集群校时和保护状态机主要在 [internal/cluster/clock_guard.go](/root/dev/sys/turntf/internal/cluster/clock_guard.go) 和 [internal/cluster/manager.go](/root/dev/sys/turntf/internal/cluster/manager.go)。
- 配置默认值在 [internal/cluster/config.go](/root/dev/sys/turntf/internal/cluster/config.go)。
- 本地写闸门通过 `api.WriteGate` 接入写接口，见 [internal/api/service.go](/root/dev/sys/turntf/internal/api/service.go)。

## 校时采样

peer 连接完成 `Hello` 后，节点会先执行一次校时；连接保持期间，每 30 秒周期性再次校时。校时失败或被拒绝会影响 peer 的时钟状态，严重时关闭 session。

单轮校时使用 `TimeSyncRequest` 和 `TimeSyncResponse`，采集四个时间点：

- `clientSendTimeMs`：本节点发送请求时的本地物理时间。
- `serverReceiveTimeMs`：peer 收到请求时的物理时间。
- `serverSendTimeMs`：peer 发送响应时的物理时间。
- `clientReceiveTimeMs`：本节点收到响应时的本地物理时间。

offset 和 RTT 按近似 NTP 公式计算：

```text
offsetMs = ((serverReceiveTimeMs - clientSendTimeMs) + (serverSendTimeMs - clientReceiveTimeMs)) / 2
rttMs = clientReceiveTimeMs - clientSendTimeMs - (serverSendTimeMs - serverReceiveTimeMs)
```

默认每次校时连续采集 7 个 round trip，并选择 RTT 最小的样本作为本次 best sample。该样本的不确定性按以下公式估算：

```text
uncertaintyMs = max(bestRTT / 2, jitter / 2) + 50
jitter = maxRTT - minRTT
```

如果样本 RTT 不超过 `cluster.clock.credible_rtt_ms`，它被认为是可信样本；否则只是慢样本，peer 会进入观察状态，不参与 offset 修正。

## Peer 状态机

每个 peer 都维护独立时钟状态：

| 状态 | 含义 |
| --- | --- |
| `probing` | 已知 peer，但还没有可信校时样本。 |
| `trusted` | 最近有可信校时样本，且当前 session 可作为 offset 来源。 |
| `observing` | peer 仍可连接，但校时质量、偏差边界或 session 状态需要观察。 |
| `rejected` | 连续校时失败或确认偏差超限，复制事件和快照流量会被保护逻辑拒绝。 |

可信样本进入后会更新 `clockOffsetMs`、`clockUncertaintyMs` 和 `lastCredibleClockSync`。慢样本不会更新可信 offset，只会让 peer 保持或进入 `observing`。

当 `cluster.clock.max_skew_ms > 0` 时，会按 offset 的不确定区间判断偏差：

- `abs(offsetMs) - uncertaintyMs > max_skew_ms`：偏差下界已经超过阈值，记为确认超限；连续达到 `reject_after_skew_samples` 次后，peer 进入 `rejected`。
- `abs(offsetMs) + uncertaintyMs > max_skew_ms`：偏差上界可能超过阈值，但下界尚未确认超限，peer 进入 `observing`。
- 区间未超过阈值：样本健康；如果 peer 正在恢复，需要连续达到 `clock_recover_after_healthy_samples` 次健康样本后才回到 `trusted`。

校时请求失败会累加 `clockFailureStreak`。如果连续失败达到 `clock_reject_after_failures`，peer 进入 `rejected`。

## 节点聚合状态机

节点还维护一个聚合时钟状态，用来决定本地写入和部分复制流量是否允许继续。

| 状态 | 进入条件 | 写入 |
| --- | --- | --- |
| `trusted` | 单节点模式，或存在新鲜可信 peer 校时。 | 允许 |
| `observing` | 最近有可信校时，但已经超过 `clock_trusted_fresh_ms`，仍处于观察宽限期。 | 允许 |
| `degraded` | 可信校时进一步过期，但还未超过写闸门宽限。 | 拒绝本地写入 |
| `unwritable` | 最近可信校时超过 `clock_write_gate_grace_ms`。 | 拒绝本地写入 |
| `unsynced` | 集群模式下从未获得可信校时。 | 拒绝本地写入 |

当前实现中，本地写闸门只允许 `trusted` 和 `observing`。写接口遇到未同步状态会返回 `app.ErrClockNotSynchronized`，HTTP 层映射为 `503 Service Unavailable`。

复制事件应用的保护略有不同：单节点模式直接允许；如果节点状态为 `unwritable`，或来源 peer 是 `rejected`，则拒绝应用事件。快照流量更严格，只允许节点状态为 `trusted` 或 `observing`，且 peer 未被 `rejected`。

## Offset 聚合

每次 peer 状态变化后，Manager 会重新计算本节点 HLC offset：

1. 收集所有 `trustedSession != nil` 的 peer offset。
2. 如果没有可信 peer，设置本地 HLC offset 为 `0`。
3. 如果存在可信 peer，对 offset 排序并取中位数。
4. 将该中位数写入 `clock.Clock.SetOffsetMs`，之后本地 HLC 的 `Now`、`Observe` 和 `WallTimeMs` 都基于该 offset。

这个策略避免单个极端 peer 直接决定本地时间，同时比平均值更不容易被异常样本拖偏。

## 未来 HLC 拒绝

即使 peer 当前未被拒绝，事件和快照仍会做未来时间戳检查。

事件批次检查：

- `Envelope.sent_at_hlc` 必须存在并能解析。
- `sent_at_hlc.wall_time_ms` 不能超过本地修正 wall time 加 `max_skew_ms`。
- 每个 replicated event 的 HLC wall time 不能超过同一未来时间边界。
- `sent_at_hlc` 不能早于批次内最大 event HLC。

快照 chunk 检查：

- 应用快照前会扫描分片内最大 HLC。
- 如果最大 HLC 超过本地修正 wall time 加 `max_skew_ms`，该 chunk 会被拒绝。

当 `cluster.clock.max_skew_ms = 0` 时，当前实现关闭超限拒绝和未来 HLC 上界检查；但集群模式下首次可信校时前的本地写闸门仍然存在。

## 默认参数

| 配置项 | 默认值 | 作用 |
| --- | ---: | --- |
| `cluster.clock.max_skew_ms` | `1000` | 允许的最大时钟偏差；`0` 表示关闭超限拒绝。 |
| `cluster.clock.sync_timeout_ms` | `8000` | 单轮校时等待响应的超时时间。 |
| `cluster.clock.credible_rtt_ms` | `4000` | 校时样本可被视为可信的最大 RTT。 |
| `cluster.clock.trusted_fresh_ms` | `60000` | 聚合状态保持 `trusted` 的新鲜窗口。 |
| `cluster.clock.observe_grace_ms` | `180000` | 从可信过期到 `observing` 的宽限窗口。 |
| `cluster.clock.write_gate_grace_ms` | `300000` | 可信校时过期后允许继续复制事件应用的最大窗口。 |
| `cluster.clock.reject_after_failures` | `3` | 连续校时失败多少次后拒绝 peer。 |
| `cluster.clock.reject_after_skew_samples` | `3` | 连续确认偏差超限多少次后拒绝 peer。 |
| `cluster.clock.recover_after_healthy_samples` | `2` | 从观察状态恢复到可信所需的连续健康样本数。 |

## 观测与排查

`GET /ops/status` 会返回节点聚合状态和每个 peer 的时钟信息：

- `write_gate_ready`：当前是否允许本地写入。
- `clock_state`、`clock_reason`、`last_trusted_clock_sync`：节点聚合状态、原因和最近可信校时。
- peer 级 `clock_state`、`clock_offset_ms`、`clock_uncertainty_ms`、`clock_failures`、`last_clock_error`、`last_clock_sync`、`last_credible_clock_sync`、`trusted_for_offset`。

Prometheus 指标包括：

- `notifier_write_gate_ready`
- `notifier_clock_state`
- `notifier_clock_offset_ms`
- `notifier_peer_clock_state`
- `notifier_peer_clock_uncertainty_ms`
- `notifier_peer_clock_failures_total`
- `notifier_clock_state_transitions_total`

常见判断：

- `write_gate_ready = false` 且 `clock_state = unsynced`：节点启动后尚未完成首次可信校时，或所有 peer 都不可达。
- peer `clock_state = observing` 且 reason 为 `slow_time_sync_sample`：网络 RTT 太高，样本不可信，优先检查网络延迟和代理链路。
- peer `clock_state = observing` 且 reason 为 `clock_skew_near_limit`：offset 不确定区间触及阈值，优先检查 NTP 和宿主机时钟。
- peer `clock_state = rejected` 且 reason 为 `clock_skew_rejected`：连续确认偏差超限，需先修正系统时间，再等待重新校时或重连恢复。
- 日志中出现 future HLC 拒绝：对端可能系统时间超前，或本节点 offset 已被错误样本修正，应同时检查两端 `/ops/status` 中的 offset 和状态。

## 当前边界

- 该机制保护 HLC 和 LWW 语义，但不是强一致时钟协议，也不提供全局线性化提交。
- offset 修正来自 peer 间校时，不替代生产环境 NTP；所有节点仍应使用可靠系统时间源。
- `observing` 是一个可写宽限状态，用于避免短暂校时波动直接造成不可用；它不表示时钟完全健康。
- `cluster.clock.max_skew_ms = 0` 只关闭超限拒绝和未来 HLC 上界检查，不关闭首次可信校时前的写闸门。
