# Raft 数据库 Web 接口

本文描述服务端 `/kv` HTTP JSON 接口。需启用 KV 共识并提供有效登录凭据；KV 始终要求可识别的用户，即使关闭全局认证也不会隐式获得管理员权限。Web 管理平台若通过 `/api` 代理访问，应在下列路径前加 `/api`。

## 身份、所有者和权限

数据库名称在整个 KV 状态机中全局唯一，不按用户建立独立命名空间。`POST /kv/databases` 只接收 `name`，创建者的认证 subject（`nodeID:userID`）成为 owner，并初始获得权限 `7`。管理员创建数据库时 owner 仍是管理员本人，不能通过请求指定其他 owner。

系统角色 `admin`、`super_admin` 可跨用户操作已有数据库的键值、事务和 ACL。服务端从已认证用户的角色构造管理员身份，请求参数或 ACL subject 名称不能设置该身份。跨用户操作、grant 和 revoke 均不会改变 owner；当前没有转移 owner 或删除数据库的接口。

ACL 权限是独立的位标志，多个权限按位或组合：

| 值 | 权限 | 允许的操作 |
| --- | --- | --- |
| 1 | read | get、list、watch |
| 2 | write | put、delete、txn |
| 4 | admin | 查询 ACL、grant、revoke |
| 7 | 全部 | 上述全部操作 |

ACL 的 `*` 项适用于所有已认证用户。普通用户通过自己的 ACL 项或 `*` 项获得对应操作的权限。数据库 admin 不隐含 read/write；write 也不隐含 read。grant 覆盖目标原有权限，revoke 删除目标 ACL 项。

owner 是创建者元数据，不是永久授权：撤销 owner 的 ACL 后，该用户仍能在数据库列表中看到自有库，但读写和 ACL 管理仍须通过权限检查。系统管理员可恢复授权。数据库 ACL 的 admin 权限仅适用于该数据库，不会变成系统管理员。

## 用户选库和权限查询

1. 调用 `GET /kv/databases` 获取当前用户可见的数据库。响应按名称升序排列，仅含名称和 owner，不含键值或 ACL。
2. 管理员在界面中选择用户后，调用 `GET /kv/databases?owner=nodeID%3AuserID` 列出该用户创建的库。owner 参数必须是有效的 `nodeID:userID`，服务端会规范化；格式错误返回 400。普通用户也可传 owner，但该筛选不会扩大可见范围。
3. 选择数据库后，有数据库 admin 权限或系统管理员身份的用户可调用 `GET /kv/{database}/acl` 查看授权。只读、只写或仅有 owner 元数据的用户不能读取 ACL，返回 403；界面不应因此隐藏其已有的读写功能。

普通用户的可见集合是自有库、自己的 ACL 非零或 `*` ACL 非零的库；系统管理员可见全部库。空列表是 `[]`，owner 筛选结果可以为空。列表中的出现不等于拥有 read 权限，数据库列表也不返回当前用户权限。管理界面应根据 ACL 查询能力展示授权管理入口，实际操作仍由服务端鉴权。

```json
{"items":[{"name":"settings","owner":"1:42"}],"revision":12}
```

ACL 响应示例：

```json
{"database":{"name":"settings","owner":"1:42","acl":{"1:42":7,"1:99":1},"permission":7},"revision":12}
```

`permission` 是查询者自身 ACL 与 `*` ACL 的按位或；系统管理员固定返回 `7`。`acl` 是独立副本，调用方修改响应不会改变状态机，修改授权必须调用 grant/revoke。缺失数据库的 ACL 查询当前返回 403。

## 接口一览

| 方法和路径 | 请求内容 | 成功响应 |
| --- | --- | --- |
| `POST /kv/databases` | `{"name":"settings"}` | 201，Result |
| `GET /kv/databases?owner=1%3A42` | owner 可省略 | 200，`{"items":[...],"revision":12}` |
| `GET /kv/{database}/acl` | 无 | 200，`{"database":{...},"revision":12}` |
| `GET /kv/{database}/keys/{key...}` | key 支持多段路径 | 200，`{"entry":{...},"revision":12}` |
| `GET /kv/{database}/list?prefix=cfg%2F` | prefix 可省略 | 200，`{"items":{"cfg/a":{...}},"revision":12}` |
| `PUT /kv/{database}/keys/{key...}` | `{"value":"aGVsbG8="}` | 200，Result |
| `DELETE /kv/{database}/keys/{key...}` | 无 | 200，Result |
| `POST /kv/{database}/txn` | compare、puts、deletes，见下文 | 200，Result，包括比较失败 |
| `PUT /kv/{database}/acl/{principal}` | `{"permission":1}` | 200，Result |
| `DELETE /kv/{database}/acl/{principal}` | 无 | 200，Result |
| `GET /kv/{database}/watch?prefix=cfg%2F` | prefix 可省略 | 200，NDJSON 变更流 |

路径和查询参数必须按 URL 规则编码。grant 的 ACL principal 使用有效的正 int64 `nodeID:userID` 或 `*`；服务端会把 `01:002` 规范化为 `1:2`，非法 ID 或权限位返回 400。revoke 按授权列表中的原始 principal 精确删除，以便清理历史非规范条目。list 的 items 是以键名为属性的对象，不是数组。不存在的键在有读权限时返回 404；数据库权限不足返回 403，未认证返回 401，KV 未启用或写入不可用通常返回 503。当前重复创建同名数据库返回 503，而非 409。

## 本地读和事务

数据库列表、ACL、get 和 list 均读取接收请求节点的本地 FSM，没有 Raft ReadIndex 或 leader barrier，不保证线性一致。在副本滞后时可能读到旧数据、旧 ACL 或旧数据库列表，响应 revision 表示该节点当前状态机版本，不是全局最新版本的证明。写请求提交给本地 Raft，当前 Manager 不自动把 follower 上的写请求转发给 leader；对 follower 写入可能返回 503。

事务将全部比较条件和写入作为一次 Raft 操作执行。compare 支持 `exists` 和 `revision`：非零 revision 比较键的 `mod_revision`，`revision:0` 表示不检查版本。若二者同时提供，则必须同时匹配。全部条件满足后执行 puts，再执行 deletes，因此同一键同时出现在两者中时最终被删除。

```json
{
  "compare":[{"key":"cfg/a","revision":12,"exists":true}],
  "puts":[{"key":"cfg/a","value":"bmV3"}],
  "deletes":["cfg/obsolete"]
}
```

比较失败不修改任何键，但会推进状态机 revision，并正常返回 **HTTP 200**：

```json
{"Revision":13,"Entries":null,"Succeeded":false}
```

客户端必须检查大写字段 `Succeeded`，不能只根据 HTTP 2xx 判断事务已写入，也不能期待这种冲突返回 HTTP 409。成功的修改返回相同结构，`Succeeded:true`；当前 Entries 通常为 null。事务失败后应重新读取并由业务决定是否重试。版本号不是连续成功写入的计数。

## JSON 编码

- 键是字符串，值是字节数组。JSON 请求和响应中的 `value` 使用标准 base64，例如 UTF-8 的 `hello` 编码为 `aGVsbG8=`；不能直接发送普通文本作为 value。空字节值的响应可能为 null，客户端应按空字节处理。
- Entry 使用小写下划线字段：`{"value":"aGVsbG8=","create_revision":10,"mod_revision":12}`。更新已有键保留 create_revision，并更新 mod_revision。
- Result 使用 Go 默认的大写字段 `Revision`、`Entries`、`Succeeded`。watch 的 Change 同样使用 `Revision`、`Database`、`Key`、`Op`、`Value`，其中 Value 也是 base64。
- 所有 revision 在服务端是 `uint64`，JSON 线上类型为数字，不是字符串。JavaScript 原生 `JSON.parse` 对超过 `Number.MAX_SAFE_INTEGER` 的值可能丢失精度；需要精确 CAS 的客户端应使用可保留大整数的 JSON 解析和序列化方案，不能先转成 Number 再转 BigInt。发送 compare.revision 时仍须按 JSON 整数字面量编码。

watch 读取权限在建立连接时检查，当前不是带历史游标和断线补发保证的事件日志；不要仅依赖 watch 实现可靠同步。
