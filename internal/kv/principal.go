package kv

// Principal 仅由服务端认证层构造；SystemAdmin 不从 KV 请求参数或数据库 ACL 推导。
// 保留 Subject，使管理员操作不会改变数据库的所有者或借用其他用户的身份。
type Principal struct {
	Subject     string
	SystemAdmin bool
}

// DatabaseInfo 不包含数据库值，仅用于按所有者选择数据库。
type DatabaseInfo struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

type DatabaseAccess struct {
	Name       string                `json:"name"`
	Owner      string                `json:"owner"`
	ACL        map[string]Permission `json:"acl"`
	Permission Permission            `json:"permission"`
}
