# Go 通知服务

一个基于 Go + SQLite 的简单通知服务，支持：

- 用户登录查看自己的通知
- 首次公开注册自动创建管理员账号
- 管理员创建普通用户或其他管理员
- 管理员管理用户角色、密码和删除用户
- 为每个账号生成唯一 `push_id`
- 其他服务通过 `push_id` 向指定用户推送通知
- 推送接口使用服务 API Key 鉴权
- 提供 Web UI 登录页与用户 Dashboard

## 技术栈

- Go 1.26
- SQLite（本地文件数据库）
- `github.com/mattn/go-sqlite3`
- 标准库 `net/http`

## 项目结构

```text
.
├── cmd/notifier/main.go         # 启动入口与 CLI
├── internal/config              # 配置读取
├── internal/httpapi             # HTTP 路由与处理器
├── internal/security            # 密码与 token 工具
├── internal/store               # SQLite 数据访问与迁移
└── README.md
```

## 启动

```bash
cp config.example.toml config.toml
go run ./cmd/notifier serve
```

启动后可直接访问：

- `http://localhost:8080/login`：登录页
- `http://localhost:8080/dashboard`：登录后的 Dashboard

默认配置：

- 服务地址：`127.0.0.1:8080`（实际监听 `:8080`）
- SQLite 文件：`./data/notifier.db`
- 用户登录态有效期：24 小时

示例配置文件：

```toml
addr = ":8080"
db_path = "./data/notifier.db"
user_session_ttl_hours = 24
```

也可以指定配置文件路径：

```bash
go run ./cmd/notifier serve -config ./config.toml
```

## 数据库迁移

服务启动时会自动执行数据库迁移：

- 使用 `kv` 表保存 `db_version`
- 新库会按版本依次建表
- 旧库会自动补齐 `users.role`
- 如果旧库存在用户但没有管理员，会把 `id` 最小的用户提升为管理员

## 用户与角色

系统固定支持两种角色：

- `admin`：管理员
- `user`：普通用户

注册规则：

- `POST /api/v1/users/register` 只允许在系统还没有任何用户时调用
- 第一个注册成功的用户会自动成为 `admin`
- 之后公开注册关闭，后续用户必须由管理员创建

## 创建服务 API Key

其他业务系统在调用推送接口前，需要先创建一个服务 API Key：

```bash
go run ./cmd/notifier apikey create orders-service
```

会输出类似：

```text
service api key created
name: orders-service
created_at: 2026-03-20T00:00:00Z
api_key: ntfsk_xxx
use header: X-API-Key: ntfsk_xxx
```

查看已有 key：

```bash
go run ./cmd/notifier apikey list
```

## API

### 1. 首次注册管理员

`POST /api/v1/users/register`

请求：

```json
{
  "username": "alice",
  "password": "password123"
}
```

返回：

```json
{
  "id": 1,
  "username": "alice",
  "push_id": "push_xxx",
  "role": "admin",
  "created_at": "2026-03-20T00:00:00Z"
}
```

说明：

- 当系统已有用户时，此接口返回 `403`
- 错误信息：`public registration is closed`

### 2. 用户登录

`POST /api/v1/users/login`

请求：

```json
{
  "username": "alice",
  "password": "password123"
}
```

返回：

```json
{
  "token": "ntfus_xxx",
  "expires_at": "2026-03-21T00:00:00Z",
  "user": {
    "id": 1,
    "username": "alice",
    "push_id": "push_xxx",
    "role": "admin",
    "created_at": "2026-03-20T00:00:00Z"
  }
}
```

### 3. 查看当前用户信息

`GET /api/v1/users/me`

请求头：

```text
Authorization: Bearer ntfus_xxx
```

返回体包含当前用户的 `role`。

### 4. 管理员查看用户列表

`GET /api/v1/admin/users`

请求头：

```text
Authorization: Bearer ntfus_xxx
```

返回：

```json
{
  "count": 2,
  "items": [
    {
      "id": 1,
      "username": "alice",
      "push_id": "push_xxx",
      "role": "admin",
      "created_at": "2026-03-20T00:00:00Z"
    }
  ]
}
```

### 5. 管理员创建用户

`POST /api/v1/admin/users`

请求头：

```text
Authorization: Bearer ntfus_xxx
Content-Type: application/json
```

请求体：

```json
{
  "username": "bob",
  "password": "password456",
  "role": "user"
}
```

`role` 只允许：

- `admin`
- `user`

### 6. 管理员修改用户角色或密码

`PATCH /api/v1/admin/users/{id}`

请求体可以包含以下任意字段：

```json
{
  "role": "admin",
  "password": "newpassword456"
}
```

说明：

- 至少要传 `role` 或 `password` 其中一个
- 如果目标用户是最后一个管理员，降级会返回 `409`

### 7. 管理员删除用户

`DELETE /api/v1/admin/users/{id}`

说明：

- 如果目标用户是最后一个管理员，删除会返回 `409`

### 8. 推送通知

`POST /api/v1/push`

请求头：

```text
X-API-Key: ntfsk_xxx
Content-Type: application/json
```

请求体：

```json
{
  "push_id": "push_xxx",
  "sender": "orders-service",
  "title": "订单更新",
  "body": "您的订单已发货",
  "metadata": {
    "order_id": "A1001"
  }
}
```

返回：

```json
{
  "status": "accepted",
  "notification_id": 1,
  "push_id": "push_xxx",
  "created_at": "2026-03-20T00:00:00Z"
}
```

### 9. 查看通知列表

`GET /api/v1/notifications?limit=20`

请求头：

```text
Authorization: Bearer ntfus_xxx
```

## Web UI

### 登录页

- `GET /login`
- 表单提交到 `POST /login`

### Dashboard

- `GET /dashboard`
- 登录成功后，服务会写入 Cookie
- 页面会展示：
  - 当前用户名
  - 当前角色
  - 用户 `push_id`
  - 最近通知列表

### 退出登录

- `POST /logout`

## curl 示例

### 首次注册管理员

```bash
curl -X POST http://localhost:8080/api/v1/users/register \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"password123"}'
```

### 登录

```bash
curl -X POST http://localhost:8080/api/v1/users/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"alice","password":"password123"}'
```

### 管理员创建普通用户

```bash
curl -X POST http://localhost:8080/api/v1/admin/users \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer ntfus_xxx' \
  -d '{"username":"bob","password":"password456","role":"user"}'
```

### 管理员提升用户为管理员

```bash
curl -X PATCH http://localhost:8080/api/v1/admin/users/2 \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer ntfus_xxx' \
  -d '{"role":"admin"}'
```

### 推送

```bash
curl -X POST http://localhost:8080/api/v1/push \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: ntfsk_xxx' \
  -d '{
    "push_id":"push_xxx",
    "sender":"orders-service",
    "title":"订单更新",
    "body":"您的订单已发货",
    "metadata":{"order_id":"A1001"}
  }'
```

### 拉取通知

```bash
curl http://localhost:8080/api/v1/notifications \
  -H 'Authorization: Bearer ntfus_xxx'
```

## 后续可扩展方向

- 接入真实推送通道（APNs / FCM / WebSocket / 短信 / 邮件）
- 增加服务 API Key 的吊销能力
- 增加消息状态机（queued / delivered / failed）
- 增加管理员 Web 管理后台
