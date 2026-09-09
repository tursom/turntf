# 会话查询失败诊断

`ResolveUserSessions` 先查询 presence 候选节点，再在没有结果时查询其余已知节点。候选节点失败后，空且成功的 fallback 不能证明候选节点没有会话，必须保留候选错误。fallback 自身失败时仍返回 fallback 错误；成功找到会话时保持原有成功语义，不改变部分结果策略。发送阶段的上下文取消保留 `context.Canceled`，不再包装成节点不可达。

远端会话查询失败时，现有集群 logger 输出 warn 事件 `session_lookup_failed`：

- `local_node_id`、`query_request_id`：关联发起节点内的查询。查询 ID 不是客户端 RPC ID，也不保证跨进程重启唯一，需结合时间窗口。
- `target_node_id`：本次查询目标节点。
- `elapsed_ms`：本次远端查询耗时，使用现有 zerolog duration 输出约定（默认毫秒）。
- `error_kind`、`error_type`：白名单语义类别和原始 Go 错误类型。
- `error`：仅白名单 sentinel 的可读错误文本。不会输出任意 wrapped error 或远端错误消息，未知错误只输出类别和类型。

日志不包含 SessionUUID、session_ref、用户标识、请求 payload 或认证信息。成功及正常空结果不新增日志。单个节点失败日志不代表最终解析失败，因为 fallback 仍可能成功；最终返回错误和空成功分别表示查询失败与正常空结果。不要仅凭没有失败日志推断查询一定成功，也不要把保留的部分结果误解成所有节点均已验证。

本修复不改变公开协议、配置或超时，不缓存会话、不跳过验证。Go proto 未改，无须重新生成。已证实的本地错误吞失不能证明历史 TLS EOF 的完整因果；仍需未来真实 canary 的 app 关闭原因、对应 RPC 和 session-query 事件来定位。
