# 分布式系统未来演进路线图

本文档记录当前分布式通知服务在未来几个版本中的重点演进方向，用于指导后续 issue 拆分、里程碑排期和设计评审。它不是实现细节设计稿，也不直接定义最终协议，而是明确“为什么做、先做什么、依赖什么、验收到什么程度才算完成”。

说明：部分工作点已经在当前仓库完成第一轮落地。本文会同时记录“当前已实现基线”和“下一步演进方向”，避免把已上线能力继续误写成纯未来计划。

当前基线能力如下：

- 节点间通过 mesh runtime 在 WebSocket、libp2p 或 ZeroMQ transport 上复制，`peer` 关系来自静态 `cluster.peers` 和自动发现
- 写入先本地提交，再通过 `EventBatch` 异步广播，对端应用后返回 `Ack`
- 断线后按 `origin_progress` 与 `origin_cursors` 自动补拉缺失事件
- 运行期通过快照摘要比对和分片快照修复做反熵
- 用户数据按字段级 `LWW`、删除墓碑和幂等事件最终收敛
- 消息按每节点 `message_window_size` 窗口收敛
- 瞬时包通过 mesh 策略路由与逐跳转发尽力送达，旧 `RoutingUpdate` 不再承担生产路由语义
- 集群内部消息当前使用共享 `cluster.secret` 做 HMAC 鉴权，并已落地 peer / 节点两级时钟状态机与写闸门保护 `HLC`

## 演进原则

- 不把当前 turntf 服务 演进成通用分布式数据库；所有设计仍服务于通知、消息和用户关系这类业务模型。
- 优先保持最终一致模型可解释，避免在没有文档和测试约束前引入无法描述的隐式语义。
- 任何更强的安全、时钟或冲突机制，都必须先定义迁移期、兼容边界和失败时的退化行为。
- 优先补齐复制语义文档和分布式测试，再放大协议复杂度或替换底层传输。
- 对已接入的 `libp2p` 和 `zeromq` 保持克制：它们可以继续扩展承载能力和观测，但不能反向改变现有复制语义。

## Phase 1：基础语义与安全补强

### 1. 复制语义文档维护（第一阶段已完成）

当前状态：

- [README.md](/root/dev/sys/turntf/turntf/README.md)、[docs/replication-semantics.md](/root/dev/sys/turntf/turntf/docs/replication-semantics.md) 和 [docs/operations.md](/root/dev/sys/turntf/turntf/docs/operations.md) 已经收敛了 `Ack`、补拉、反熵、消息窗口、黑名单、瞬时包和写闸门等当前语义
- 测试结果文档也已开始直接复用这套术语，例如 [docs/distributed-test-framework-enhancement-results.md](/root/dev/sys/turntf/turntf/docs/distributed-test-framework-enhancement-results.md) 已明确把回归断言对齐到复制语义边界

问题：

- 复制语义文档已经不是“缺失”，但后续黑名单、mTLS、自动发现、向量时钟或 mixed transport 能力继续演进时，仍然容易出现 README、专题文档、运维手册和测试断言漂移
- 如果路线图继续把这项工作写成“未来新增一份文档”，会掩盖当前仓库其实已经把第一阶段落地完成

目标能力：

- 维持“先收紧当前承诺，再扩展实现”的文档纪律，而不是重复新建平行说明
- 新分布式特性落地前，先明确以下边界：
  - 本地写入成功与集群收敛成功的区别
  - `Ack`、补拉、快照和瞬时包分别承诺什么
  - 哪些行为是当前保证，哪些仍属于未定义或不保证

分阶段落地：

1. 新增或调整分布式语义时，优先更新 [docs/replication-semantics.md](/root/dev/sys/turntf/turntf/docs/replication-semantics.md)，把当前保证与非保证写清楚。
2. 再同步 [README.md](/root/dev/sys/turntf/turntf/README.md) 与 [docs/operations.md](/root/dev/sys/turntf/turntf/docs/operations.md) 中的入口说明、排障语义和观测约定。
3. 测试与执行结果文档继续复用同一套术语，避免断言和文档各说各话。

验收标准：

- 新成员只读 README、复制语义专题文档和运维手册，就能判断某个行为是 bug、允许的暂态，还是尚未定义
- 新特性测试命名和断言可以直接引用文档中的语义术语，而不是重新发明口径

风险与兼容性：

- 文档收紧会暴露现有实现里的模糊区，需要先承认“不承诺的行为”，避免文档写得比实现更强

### 2. 用户拒收（黑名单）能力补完

当前状态：

- 系统已经支持 `user_blacklist` 持久化关系、HTTP 管理接口、事件复制、快照修复和查询
- 当前语义已经明确：黑名单阻止后续新的普通用户直发持久消息与瞬时包，不回溯删除历史消息，`channel`、`broadcast` 和 `node` 入口消息不受影响

问题：

- 第一版黑名单已经落地，但后续若扩展批量管理、可观测性或更细粒度拒收策略，仍必须保持写侧校验、读侧可见性和复制语义一致
- 这项能力已经不再适合被描述为“尚无持久化模型”的未来计划

目标能力：

- 在不改变当前语义承诺的前提下，补齐黑名单的管理、观测和性能演进空间
- 继续把管理员能力、广播地址、`channel` 地址、节点入口瞬时包边界保持显式定义

分阶段落地：

1. 把当前已实现语义持续固定在 [docs/replication-semantics.md](/root/dev/sys/turntf/turntf/docs/replication-semantics.md)、[README.md](/root/dev/sys/turntf/turntf/README.md) 和回归测试中。
2. 如有需要，再补批量查询、批量管理、更细粒度指标或缓存优化，但不能悄悄扩大语义作用面。
3. 若未来扩展到新的地址类型或投递规则，必须先补文档和测试，再改写路径。

验收标准：

- 对任意来源节点，黑名单关系在补拉、反熵和重启后仍保持一致
- 黑名单命中后，接口返回行为、日志、指标和复制语义都有明确说明

风险与兼容性：

- 如果黑名单影响复制应用而不是仅影响发送路径，必须非常小心历史消息可见性和窗口裁剪行为
- 对 `role=broadcast`、`role=channel`、`role=node` 的影响不应默认继承普通用户语义

### 3. 分布式测试框架维护（第一轮已完成）

详细实现计划见 [docs/distributed-test-framework-enhancement.md](/root/dev/sys/turntf/turntf/docs/distributed-test-framework-enhancement.md)。
当前执行结果见 [docs/distributed-test-framework-enhancement-results.md](/root/dev/sys/turntf/turntf/docs/distributed-test-framework-enhancement-results.md)。

当前状态：

- 三层测试策略、统一节点夹具、固定回归集、故障注入场景驱动器和扩展点测试已经落地
- 当前仓库已经不再只有“点状单测”，而是具备可以承载后续黑名单、自动发现、时钟和 mixed transport 演进的回归骨架

问题：

- 测试框架本身已完成第一轮建设，但后续新能力仍有可能绕开既有夹具，重新堆出一次性测试路径
- mixed transport、自动发现和未来身份治理会继续扩大状态机，如果不强制纳入统一回归，测试风格会再次发散

目标能力：

- 维持已经落地的三层测试策略：
  - `store` 层收敛测试
  - `cluster manager` 协议测试
  - 多节点仿真测试
- 要求后续特性优先复用现有夹具、驱动器和语义断言，而不是重新拼装一次性 helper

分阶段落地：

1. 新增分布式能力时，先声明它会落在哪一层测试，再补实现。
2. 把已支持的 transport、membership 和路由边界继续收编到固定回归集，而不是只留在临时验证里。
3. 为后续黑名单增强、mTLS、自动发现扩展或新的冲突元数据继续复用现有扩展点。

验收标准：

- 新增协议特性时，可以先补测试夹具再落实现，而不是每次单独拼装临时测试
- 已有复制语义、自动发现和 mixed transport 边界不会因为新能力接入而失去固定回归覆盖

风险与兼容性：

- 测试框架本身不要绑死某一种 transport 或当前 build tag 组合，否则后续 mixed transport 演进时仍需大改

## Phase 2：时钟治理与集群身份强化

### 4. 时钟保护与告警治理

当前状态：

- 当前已经实现 peer 级 `probing/trusted/observing/rejected` 与节点级 `trusted/observing/degraded/unwritable/unsynced` 状态机
- 本地写闸门、未来 HLC 拒绝、offset 中位数聚合、`/ops/status` 与 Prometheus 指标也已落地，详见 [docs/clock-protection.md](/root/dev/sys/turntf/turntf/docs/clock-protection.md)

问题：

- 这项能力已经不再是“单次校时 + 阈值拒绝”的草案；后续重点转为阈值调优、告警分级、恢复策略和跨传输运行经验沉淀
- 如果路线图仍按旧表述描述，会低估当前实现和运维边界

目标能力：

- 在现有状态机基础上继续收紧运维可解释性，而不是重新定义一套并不存在的时钟模型
- 明确不同状态下写入、事件复制、快照和 peer 接纳的运维判断约定

分阶段落地：

1. 基于现有状态机持续校准阈值、日志事件和告警文案，保证 `trusted/observing/degraded/unwritable/unsynced` 的语义稳定。
2. 当 mixed transport、自动发现或身份治理继续扩展时，保持校时与写闸门规则跨传输一致。
3. 如未来需要更强策略，再在 [docs/clock-protection.md](/root/dev/sys/turntf/turntf/docs/clock-protection.md) 和测试中先定义恢复/降级规则，再调整实现。

验收标准：

- 节点在时钟风险上升时行为可预测，且日志与指标能准确暴露当前状态
- 任何基于时间的拒绝都能在文档中找到对应规则

风险与兼容性：

- 更严格的时钟保护会主动牺牲一部分可用性，必须在文档中明确这属于有意取舍

### 5. 集群双向证书验证

当前状态：

- 当前集群内部逻辑身份和复制鉴权仍主要依赖共享 `cluster.secret` 做 HMAC
- WebSocket/ZeroMQ 可以分别通过 `wss` 或 ZeroMQ `curve` 提供链路安全能力，但还没有把证书身份与 `node_id` 绑定成统一的 cluster mTLS 模型

问题：

- 共享密钥适合小规模集群，但对节点身份区分、证书轮换、细粒度信任和链路机密性支持有限

目标能力：

- 从共享密钥模式演进到双向 TLS 证书验证
- 把证书身份与 `node_id` 或 peer 身份绑定，防止“拿到密钥即可冒充任意节点”

分阶段落地：

1. 先定义身份模型：
   - 证书中的哪一项绑定 `node_id`
   - 多地址、多连接和证书轮换时如何认定同一 peer
2. 设计迁移期：
   - `HMAC + mTLS` 双栈兼容
   - 仅 mTLS
3. 扩展配置、日志、运维手册和证书失效观测能力。

验收标准：

- 未持有受信证书的节点无法加入集群
- 证书身份与逻辑节点身份的绑定关系有明确校验规则
- 轮换过程不会要求整集群同时停机切换

风险与兼容性：

- mTLS 不是简单叠加；它会影响握手、运维部署、自动发现和未来的多传输层治理

## Phase 3：成员管理与 membership 演进

### 6. peer 自动发现与 membership 稳定化

当前状态：

- 当前已经支持基于 membership update 的 peer 自动发现、`discovered_peers` 持久化恢复，以及 `GET /ops/status` / `/metrics` 观测
- 自动发现与静态 `cluster.peers` 并存，并已支持 WebSocket、ZeroMQ 和 libp2p 候选地址传播，详见 [docs/peer-discovery.md](/root/dev/sys/turntf/turntf/docs/peer-discovery.md)

问题：

- 当前第一版自动发现主要依赖集群内广告和本地持久化；外部 discovery backend、专门运维 API 和更激进的地址治理仍未形成统一方案
- 如果仍把这项能力描述成“完全依赖静态 peer”，会掩盖当前 membership 基线和后续真正剩余的工作

目标能力：

- 在现有 discovery 基线上继续完善 membership 稳定性、地址治理和运维可控性
- 保持“静态 peer + 自动发现”并存，且 discovery 不能绕过身份校验

分阶段落地：

1. 继续稳定当前 advertisement、候选筛选、去重、过期和动态拨号策略。
2. 视需要补充 discovered peer 的运维接口、失效治理、地址迁移和更多观测维度。
3. 如果未来引入外部 discovery backend，先明确它只提供候选地址，不替代 `Hello`、HMAC、校时和 peer identity 绑定。

验收标准：

- 节点无需全量人工改配置即可发现新 peer
- 同一逻辑节点的多地址和地址漂移不会导致重复 peer 身份

风险与兼容性：

- 自动发现不能绕过 mTLS 或身份校验，否则只会把“谁能进集群”问题放大
- membership 扩展后，运维接口和指标必须能区分“已发现”“已连接”“已认证”“可信复制中”

## Phase 4：冲突语义升级

### 7. 向量时钟的受限引入

当前状态：

- 当前主要依赖 `HLC`、字段级 `LWW`、删除墓碑和幂等事件收敛

问题：

- 对真正的并发写冲突，`LWW` 会把因果关系压平，适合简单字段，但不适合需要表达并发意图的对象

目标能力：

- 向量时钟只用于少数确实需要保留并发信息的实体或字段，不全局替换 HLC 排序
- 保持现有事件日志、快照和协议仍以当前 Envelope 结构为主，只在需要处承载额外冲突元数据

分阶段落地：

1. 先选定使用范围，例如用户特定资料字段或未来更复杂的关系对象。
2. 定义事件格式和快照承载方式，明确与 HLC 的边界：
   - HLC 继续承担排序、日志和时钟保护的基础角色
   - 向量时钟只承担“是否并发”的判定
3. 定义冲突暴露和合并策略，避免引入向量时钟后仍悄悄按 LWW 覆盖。

验收标准：

- 新增并发冲突表达能力后，文档、接口和测试都能区分“先后覆盖”和“并发冲突待合并”

风险与兼容性：

- 向量时钟会带来存储和快照体积膨胀，也会抬高接口复杂度
- 如果没有先收紧复制语义文档，向量时钟会把现有模糊边界进一步放大

## Phase 5：多传输层稳定化与协议边界

### 8. 多传输层稳定化与 mixed transport 演进

当前状态：

- `TransportConn` / mesh runtime 抽象已经存在，WebSocket、ZeroMQ 和 libp2p 都可以承载当前 cluster protocol
- 当前仓库已经有 mixed transport 集成测试、mesh bridge 行为验证和 benchmark 基线；ZeroMQ 仍受 `zeromq` build tag 与 libzmq 环境约束，libp2p 仍只服务节点间通信

问题：

- 这项工作已经不再是“先抽象接口再试点接入”；后续重点是 mixed transport 的长期边界、观测和 rollout 纪律
- 不同 transport 的能力差异可能诱发“某个 transport 反向定义上层语义”的风险

目标能力：

- 保持复制语义高于 transport 选择，继续稳定跨 transport 的路由、发现、观测和性能基线
- 明确哪些跨 transport 组合已支持，哪些仍显式不支持或只作为实验路径

分阶段落地：

1. 维持当前 transport 抽象与 mesh traffic class 边界稳定，避免新 transport 直接侵入上层语义。
2. 持续补 mixed transport 的回归、benchmark 和观测，特别是 bridge、route fallback、snapshot / replication no-route 等显式边界。
3. 若未来继续扩 transport 能力，先更新文档和测试，再决定是否扩大默认 rollout 范围。

验收标准：

- 不改复制语义的前提下，跨 transport 已支持的路径都有测试与文档，未支持的路径也有明确拒绝或降级说明

风险与兼容性：

- 新传输层不能反向定义上层语义
- mixed transport 扩容会放大观测与排障成本，必须与测试、文档和 rollout 纪律同步推进

## Phase 6：分布式测试体系完善

### 9. 分布式逻辑测试覆盖清单

当前回归体系已经覆盖其中大部分场景；后续新增能力或扩大 rollout 时，至少继续覆盖以下场景：

- 网络分区与分区恢复
- 乱序到达、重复投递与补拉重放
- 实时广播与补拉并发
- 快照请求、快照应用与事件流并发
- 时钟超限、未来时间戳、连续校时失败
- 证书失效、证书轮换和身份不匹配
- membership 抖动、重复地址、自动发现与静态 peer 并存
- 黑名单复制、重启恢复和跨节点一致性

测试层次要求：

- `store` 层验证收敛与冲突处理
- `cluster manager` 层验证协议状态机与错误路径
- 多节点仿真验证网络故障、重连、反熵和 membership 变更

验收标准：

- 每个新能力在设计评审时就能指出将落在哪一层测试
- 回归测试能稳定覆盖“语义正确性”而不是只覆盖单条代码路径

## 预期接口与配置变化

以下内容属于未来可能新增的公共面，用于帮助设计评审时提前留意兼容边界，不代表这些接口已经定版：

- `cluster` 配置可能继续扩展外部 discovery 后端、TLS/mTLS、证书路径、信任根和更细粒度时钟治理策略
- 用户关系模型可能扩展黑名单的批量管理接口、查询能力或更细粒度拒收策略
- 集群协议可能继续扩展证书身份信息、冲突元数据和向量时钟字段；成员发现与 transport hint 已有基线
- 运维和观测面可能在现有时钟状态、membership 状态和黑名单计数基础上继续细化指标与告警

所有新增公共面都应优先采用“兼容扩展、双栈迁移、先文档后落地”的策略，避免在未来版本中做一次性破坏性切换。

## 文档与里程碑要求

- README 持续维护“当前已实现边界”，不直接承载过长的未来规划
- 本路线图用于同时记录“已落地基线 + 下一步阶段目标 + 关键依赖关系”，避免现状与规划脱节
- 复制语义专题文档用于承载当前系统的规范语义
- [docs/peer-discovery.md](/root/dev/sys/turntf/turntf/docs/peer-discovery.md) 和 [docs/clock-protection.md](/root/dev/sys/turntf/turntf/docs/clock-protection.md) 负责记录当前 membership / 时钟保护实现边界
- 运维手册负责记录上线、证书、时钟治理、告警和排障策略

里程碑顺序建议如下：

1. 持续维护复制语义文档、黑名单语义和分布式测试基线
2. 时钟治理调优与 mTLS 双向认证
3. 自动发现稳定化与成员管理扩展
4. 向量时钟的受限引入
5. 多传输层稳定化、mixed transport 观测与 rollout 边界收紧

关键依赖关系如下：

- 向量时钟依赖复制语义文档先收紧
- 更激进的自动发现扩展依赖身份认证与 peer 身份绑定更明确
- mixed transport 的进一步放量依赖既有语义边界、观测能力和回归覆盖持续稳定

## 本文档的使用方式

- 新增长期特性前，先确认其属于哪个阶段、依赖是否满足、是否需要先更新复制语义文档
- 做方案评审时，优先补“当前问题、目标能力、验收标准、风险与兼容性”
- 拆分 issue 时，以本文档的阶段顺序为默认顺序；若要跳阶段，必须说明为什么现有依赖已被替代或提前解决
