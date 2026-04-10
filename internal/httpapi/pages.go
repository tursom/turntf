package httpapi

import "html/template"

const sessionCookieName = "notifier_session"

var pageTemplates = template.Must(template.New("pages").Parse(loginPageTemplate + dashboardPageTemplate))

const loginPageTemplate = `
{{define "login"}}
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Notifier Login</title>
  <style>
    :root { color-scheme: light dark; }
    body { margin: 0; font-family: Arial, sans-serif; background: #0f172a; color: #e2e8f0; }
    .wrap { min-height: 100vh; display: grid; place-items: center; padding: 24px; }
    .card { width: 100%; max-width: 420px; background: #111827; border: 1px solid #334155; border-radius: 16px; padding: 28px; box-sizing: border-box; }
    h1 { margin: 0 0 8px; font-size: 28px; }
    p { color: #94a3b8; margin: 0 0 24px; }
    label { display: block; margin: 0 0 8px; font-size: 14px; color: #cbd5e1; }
    input { width: 100%; box-sizing: border-box; padding: 12px 14px; margin: 0 0 16px; border-radius: 10px; border: 1px solid #475569; background: #0f172a; color: #e2e8f0; }
    button { width: 100%; border: 0; border-radius: 10px; padding: 12px 14px; font-size: 15px; font-weight: 700; cursor: pointer; background: #2563eb; color: white; }
    .error { margin: 0 0 16px; padding: 10px 12px; border-radius: 10px; background: #7f1d1d; color: #fecaca; }
    .tip { margin-top: 16px; font-size: 13px; color: #94a3b8; }
    code { color: #bfdbfe; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>登录</h1>
      <p>登录后查看你的通知 Dashboard。</p>
      {{if .Error}}<div class="error">{{.Error}}</div>{{end}}
      <form method="post" action="/login">
        <label for="username">用户名</label>
        <input id="username" name="username" type="text" value="{{.Username}}" autocomplete="username" required>
        <label for="password">密码</label>
        <input id="password" name="password" type="password" autocomplete="current-password" required>
        <button type="submit">登录</button>
      </form>
      <div class="tip">如需创建账号，可先调用 <code>POST /api/v1/users/register</code>。</div>
    </div>
  </div>
</body>
</html>
{{end}}
`

const dashboardPageTemplate = `
{{define "dashboard"}}
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Notifier Dashboard</title>
  <style>
    :root { color-scheme: light dark; }
    body { margin: 0; font-family: Arial, sans-serif; background: #020617; color: #e2e8f0; }
    .page { max-width: 1000px; margin: 0 auto; padding: 24px; }
    .topbar { display: flex; gap: 16px; align-items: center; justify-content: space-between; margin-bottom: 24px; }
    .title { margin: 0; font-size: 28px; }
    .subtitle { margin: 6px 0 0; color: #94a3b8; }
    .logout button { border: 0; border-radius: 10px; padding: 10px 14px; background: #1e293b; color: #e2e8f0; cursor: pointer; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background: #0f172a; border: 1px solid #1e293b; border-radius: 16px; padding: 18px; }
    .label { color: #94a3b8; font-size: 13px; margin-bottom: 8px; }
    .value { word-break: break-all; font-size: 18px; font-weight: 700; }
    .list { display: grid; gap: 14px; }
    .item { background: #0f172a; border: 1px solid #1e293b; border-radius: 16px; padding: 18px; }
    .item h3 { margin: 0 0 8px; font-size: 18px; }
    .meta { color: #94a3b8; font-size: 13px; margin-bottom: 12px; }
    .body { white-space: pre-wrap; line-height: 1.5; }
    pre { margin: 12px 0 0; padding: 12px; overflow: auto; border-radius: 12px; background: #020617; color: #cbd5e1; }
    .empty { color: #94a3b8; text-align: center; padding: 32px 16px; background: #0f172a; border: 1px dashed #334155; border-radius: 16px; }
  </style>
</head>
<body>
  <div class="page">
    <div class="topbar">
      <div>
        <h1 class="title">欢迎，{{.User.Username}}</h1>
        <p class="subtitle">这是你的通知 Dashboard。当前角色：{{if eq .User.Role "admin"}}管理员{{else}}普通用户{{end}}</p>
      </div>
      <form class="logout" method="post" action="/logout">
        <button type="submit">退出登录</button>
      </form>
    </div>

    <div class="grid">
      <div class="card">
        <div class="label">Push ID</div>
        <div class="value">{{.User.PushID}}</div>
      </div>
      <div class="card">
        <div class="label">通知数</div>
        <div class="value">{{len .Notifications}}</div>
      </div>
      <div class="card">
        <div class="label">角色</div>
        <div class="value">{{.User.Role}}</div>
      </div>
      <div class="card">
        <div class="label">注册时间</div>
        <div class="value">{{.User.CreatedAt.Format "2006-01-02 15:04:05 MST"}}</div>
      </div>
    </div>

    {{if .Notifications}}
      <div class="list">
        {{range .Notifications}}
          <div class="item">
            <h3>{{.Title}}</h3>
            <div class="meta">来自 {{.Sender}} · {{.CreatedAt.Format "2006-01-02 15:04:05 MST"}}</div>
            <div class="body">{{.Body}}</div>
            {{if .Metadata}}<pre>{{.Metadata}}</pre>{{end}}
          </div>
        {{end}}
      </div>
    {{else}}
      <div class="empty">暂时还没有通知。</div>
    {{end}}
  </div>
</body>
</html>
{{end}}
`
