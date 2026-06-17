package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidToken 表示令牌格式无效或签名验证失败。
// 在以下场景返回此错误：
// - 令牌格式错误（不是两段式 "payload.signature" 结构）
// - HMAC 签名不匹配（可能因密钥不同、令牌被篡改或 payload 损坏）
// - Base64URL 解码失败
// - JSON 反序列化失败
// 注意: 出于安全考虑，所有上述失败情况统一返回 ErrInvalidToken，
// 不区分具体失败原因，防止攻击者通过错误信息推断内部状态。
var ErrInvalidToken = errors.New("invalid token")

// Claims 表示令牌中包含的声明（claims）信息，采用标准 JWT 字段命名以便于理解。
// Subject:   令牌主体的标识符，如用户 ID、节点 ID 等唯一标识。
// Issuer:    令牌签发者的标识符，在多服务架构中用于区分不同签发方。
// IssuedAt:  令牌签发时间的 Unix 时间戳（秒）。用于计算令牌的"年龄"。
// ExpiresAt: 令牌过期时间的 Unix 时间戳（秒）。签发者应在验证时检查此字段。
// Metadata:  可选的附加元数据键值对，用于传递角色、权限等额外上下文。
// 设计说明：Claims 不严格遵循标准 JWT 注册声明命名（如 sub、iss），
// 而是使用 Go 的 JSON 标签映射到标准名称，
// 这样在 Go 代码中可以获得更好的可读性和 IDE 支持。
type Claims struct {
	Subject   string            `json:"sub"`
	Issuer    string            `json:"iss"`
	IssuedAt  int64             `json:"iat"`
	ExpiresAt int64             `json:"exp"`
	Metadata  map[string]string `json:"meta,omitempty"`
}

// Signer 是基于 HMAC-SHA256 算法的令牌签发与验证器。
// 使用对称密钥模型：签发（Sign）和验证（Verify）使用同一个 secret。
// 重要安全注意:
//   - 由于 HMAC 的对称性，持有 secret 的任何一方都能签发有效令牌，
//     因此 secret 必须严格保密，不应在多个互不信任的服务间共享。
//   - secret 建议使用至少 32 字节的高熵随机值。
//   - 若 secret 泄露，应立即轮换密钥并重新签发所有有效令牌。
type Signer struct {
	secret []byte
}

// NewSigner 创建一个新的 Signer 实例。
// 参数 secret: 用于 HMAC 签名的密钥字符串。不能为空或纯空白字符。
// 返回 (*Signer, error): 成功时返回 Signer 指针，失败时返回描述错误。
// secret 会被转换为 []byte 并存储在 Signer 中，原始字符串不会被保留。
func NewSigner(secret string) (*Signer, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("secret cannot be empty")
	}
	return &Signer{secret: []byte(secret)}, nil
}

// Sign 使用 HMAC-SHA256 算法对 Claims 进行签名，生成令牌字符串。
// 令牌格式: Base64URL(JSON payload) "." Base64URL(HMAC-SHA256 签名)
//
// 实现步骤:
// 1. 将 Claims 结构体序列化为 JSON 字节序列
// 2. 对 JSON 进行 RawURL 编码的 Base64（无填充字符），得到 payloadPart
// 3. 使用 HMAC-SHA256 以 Signer.secret 为密钥对 payloadPart 计算签名
// 4. 对签名结果同样进行 RawURL 编码的 Base64，得到 signaturePart
// 5. 最终令牌为 "payloadPart.signaturePart" 的拼接
//
// 选择 HMAC-SHA256 而非 RSA/ECDSA 的原因:
// - 本系统为单服务或可信内部网络场景，无需非对称签名的密钥分发能力
// - HMAC-SHA256 计算开销低、实现简单
// - SHA256 提供 256 位安全强度，在合理密钥长度下满足内部认证需求
//
// 使用 Base64URL（RawURLEncoding）而非标准 Base64 的原因:
// - 标准 Base64 包含 '+' 和 '/' 字符，在 URL 中需要额外转义
// - Base64URL 使用 '-' 和 '_' 替代，确保令牌可直接放在 URL 和 HTTP 头部中
// - RawURLEncoding（无填充）避免 padding 字符 '='，使令牌长度更紧凑
//
// 参数 claims: 待签名的声明信息。
// 返回 (string, error): 成功时返回令牌字符串，失败时返回 JSON 序列化错误。
func (s *Signer) Sign(claims Claims) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}

	payloadPart := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(payloadPart))
	signaturePart := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payloadPart + "." + signaturePart, nil
}

// Verify 验证令牌字符串的签名完整性并解析出 Claims。
//
// 验证步骤:
//  1. 按 "." 分割令牌为 payload 和 signature 两部分。若非恰好两段则拒绝。
//  2. 使用 HMAC-SHA256 以 Signer.secret 为密钥重新计算 payloadPart 的期望签名。
//  3. 使用 hmac.Equal 将期望签名与令牌携带的签名进行恒定时间比较，
//     防止基于时序的旁路攻击（不匹配的分支执行时间相同）。
//  4. 对 payload 部分进行 Base64URL 解码。
//  5. 将解码后的 JSON 反序列化为 Claims 结构体。
//
// 验证顺序的设计考量:
// - 先验证签名再解析 payload：在无效令牌上尽早返回，避免不必要的高开销操作。
// - 签名比较失败直接返回，不提供更具体的错误信息。
//
// 与标准 JWT 的差异:
//   - 本实现使用两段式结构 Payload.Signature，而非标准 JWT 的三段式
//     Header.Payload.Signature。省略 Header 是因为本系统仅使用 HMAC-SHA256，
//     无需算法协商字段，使用固定算法减少了攻击面。
//   - 未使用标准 JWT 库，自行实现以保持依赖最小化。
//   - 由于以上差异，本令牌被称为 "JWT 风格" 而非标准 JWT。
//
// 注意: 此函数仅验证签名的完整性和令牌格式的正确性，
// 不会检查 ExpiresAt 是否已过期、IssuedAt 是否在合理范围内等业务逻辑。
// 调用方应在获取 Claims 后自行校验这些时间字段，例如:
//
//	if claims.ExpiresAt > 0 && time.Now().Unix() > claims.ExpiresAt {
//	    return auth.ErrInvalidToken
//	}
//
// 参数 token: 待验证的令牌字符串（格式为 "payload.signature"）。
// 返回 (Claims, error): 成功时返回解析后的 Claims，失败时返回 ErrInvalidToken。
func (s *Signer) Verify(token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return Claims{}, ErrInvalidToken
	}

	expectedMAC := hmac.New(sha256.New, s.secret)
	_, _ = expectedMAC.Write([]byte(parts[0]))
	expectedSignature := expectedMAC.Sum(nil)

	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !hmac.Equal(signature, expectedSignature) {
		return Claims{}, ErrInvalidToken
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, ErrInvalidToken
	}

	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return Claims{}, ErrInvalidToken
	}
	return claims, nil
}
