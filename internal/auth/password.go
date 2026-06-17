// Package auth 提供用户认证相关的核心功能，包括密码哈希与校验、
// 以及基于 HMAC-SHA256 的令牌签发与验证。
// 令牌格式为 Base64URL(JSON payload) "." Base64URL(HMAC-SHA256 签名)，
// 采用对称验证模型：Signer 使用密钥签发令牌，Verify 使用相同密钥验证。
// 与 store 层的集成方式为：密码哈希值存储在持久化存储中供 VerifyPassword 使用，
// 令牌作为无状态凭证在客户端与服务端之间传递，服务端通过 Signer.Verify 验证其完整性。
package auth

import (
	"fmt"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// HashPassword 使用 bcrypt 算法对明文密码进行哈希处理。
// 参数 password: 用户提供的明文密码。
// 返回 (string, error): 成功时返回 bcrypt 哈希值的字符串表示，
// 失败时返回包含上下文信息的错误。
// bcrypt 会自动生成随机盐值（salt）并嵌入到输出字符串中，因此
// 即使对同一密码多次调用 HashPassword，每次返回的哈希值都不同。
// 使用 bcrypt.DefaultCost（当前为 10）作为计算成本系数，在安全性与
// 响应速度之间取得平衡。成本系数越高，暴力破解的计算代价越大。
// 安全注意: 空密码或仅含空白字符的密码将被拒绝，防止意外使用弱凭证。
func HashPassword(password string) (string, error) {
	if strings.TrimSpace(password) == "" {
		return "", fmt.Errorf("password cannot be empty")
	}
	hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hash password: %w", err)
	}
	return string(hashed), nil
}

// VerifyPassword 校验明文密码是否与给定的 bcrypt 哈希值匹配。
// 参数 passwordHash: 之前由 HashPassword 生成的 bcrypt 哈希字符串。
// 参数 password: 待校验的明文密码。
// 返回 error: 匹配时返回 nil，不匹配时返回 bcrypt 的内部错误信息。
// bcrypt.CompareHashAndPassword 内部使用恒定时间比较（constant-time comparison），
// 防止攻击者通过测量响应时间的差异来逐字符猜测密码（时序旁路攻击）。
// 安全注意: 两个参数均会检查空值，因为空哈希值或空密码在校验场景中
// 通常表明上游存在逻辑错误，应尽早暴露而非静默失败。
func VerifyPassword(passwordHash, password string) error {
	if strings.TrimSpace(passwordHash) == "" {
		return fmt.Errorf("password hash cannot be empty")
	}
	if strings.TrimSpace(password) == "" {
		return fmt.Errorf("password cannot be empty")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(passwordHash), []byte(password)); err != nil {
		return err
	}
	return nil
}
