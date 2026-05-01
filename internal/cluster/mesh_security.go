package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
)

// meshMarshalOptions 使用确定性序列化，确保HMAC签名一致。
var meshMarshalOptions = proto.MarshalOptions{Deterministic: true}

// meshEnvelopeHMACFieldNumber 是ClusterEnvelope中hmac字段的protobuf字段编号。
const meshEnvelopeHMACFieldNumber = 14

// meshEnvelopeAuthenticator 实现mesh.Signer和mesh.Verifier接口。
// 使用HMAC-SHA256对网格信封进行签名和验证。
// 与传统信封签名不同，此实现使用protowire级别操作，
// 在已序列化的字节流上附加/剥离HMAC字段，避免重新序列化。
type meshEnvelopeAuthenticator struct {
	secret []byte
}

// newMeshEnvelopeAuthenticator 创建网格信封认证器。
// 如果密钥为空则返回nil。
func newMeshEnvelopeAuthenticator(secret string) *meshEnvelopeAuthenticator {
	trimmed := strings.TrimSpace(secret)
	if trimmed == "" {
		return nil
	}
	return &meshEnvelopeAuthenticator{secret: []byte(trimmed)}
}

// Sign 对网格信封进行签名。encoded参数是已序列化（不含HMAC）的字节。
// 如果未提供，则从envelope重新序列化。
// 返回在末尾附加了HMAC字段的完整字节。
func (a *meshEnvelopeAuthenticator) Sign(envelope *mesh.ClusterEnvelope, encoded []byte) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("mesh envelope cannot be nil")
	}
	if len(encoded) == 0 {
		var err error
		encoded, err = meshEnvelopeBytes(envelope)
		if err != nil {
			return nil, err
		}
	}
	signature, err := a.signatureForPayload(encoded)
	if err != nil {
		return nil, err
	}
	signed := make([]byte, len(encoded), len(encoded)+protowire.SizeTag(meshEnvelopeHMACFieldNumber)+protowire.SizeBytes(len(signature)))
	copy(signed, encoded)
	signed = protowire.AppendTag(signed, meshEnvelopeHMACFieldNumber, protowire.BytesType)
	signed = protowire.AppendBytes(signed, signature)
	return signed, nil
}

// Verify 验证网格信封的HMAC签名。
// 从raw字节中剥离HMAC字段，重新计算签名并进行常量时间比较。
func (a *meshEnvelopeAuthenticator) Verify(envelope *mesh.ClusterEnvelope, raw []byte) error {
	if envelope == nil {
		return errors.New("mesh envelope cannot be nil")
	}
	encoded, signature, err := stripMeshEnvelopeHMAC(raw)
	if err != nil {
		return err
	}
	expected, err := a.signatureForPayload(encoded)
	if err != nil {
		return err
	}
	if !hmac.Equal(signature, expected) {
		return errors.New("mesh envelope hmac mismatch")
	}
	return nil
}

// signatureFor 为给定的网格信封计算HMAC签名。
func (a *meshEnvelopeAuthenticator) signatureFor(envelope *mesh.ClusterEnvelope) ([]byte, error) {
	if a == nil {
		return nil, errors.New("mesh authenticator cannot be nil")
	}
	clone, ok := proto.Clone(envelope).(*mesh.ClusterEnvelope)
	if !ok {
		return nil, errors.New("clone mesh envelope")
	}
	clone.Hmac = nil
	payload, err := meshMarshalOptions.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("marshal mesh envelope for hmac: %w", err)
	}
	return a.signatureForPayload(payload)
}

// signatureForPayload 为给定的字节负载计算HMAC-SHA256签名。
func (a *meshEnvelopeAuthenticator) signatureForPayload(payload []byte) ([]byte, error) {
	if a == nil {
		return nil, errors.New("mesh authenticator cannot be nil")
	}
	mac := hmac.New(sha256.New, a.secret)
	if _, err := mac.Write(payload); err != nil {
		return nil, fmt.Errorf("write mesh envelope hmac: %w", err)
	}
	return mac.Sum(nil), nil
}

// meshEnvelopeBytes 将信封序列化为不含HMAC的原始字节。
func meshEnvelopeBytes(envelope *mesh.ClusterEnvelope) ([]byte, error) {
	clone, ok := proto.Clone(envelope).(*mesh.ClusterEnvelope)
	if !ok {
		return nil, errors.New("clone mesh envelope")
	}
	clone.Hmac = nil
	encoded, err := meshMarshalOptions.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("marshal mesh envelope: %w", err)
	}
	return encoded, nil
}

// stripMeshEnvelopeHMAC 从序列化的网格信封中剥离HMAC字段。
// 使用protowire按字段解析，将HMAC字段提取出来，其余字段保持不变。
// 这避免了反序列化和重新序列化整个消息。
func stripMeshEnvelopeHMAC(raw []byte) ([]byte, []byte, error) {
	if len(raw) == 0 {
		return nil, nil, errors.New("mesh envelope raw bytes cannot be empty")
	}
	stripped := make([]byte, 0, len(raw))
	var signature []byte
	for len(raw) > 0 {
		fieldNum, wireType, tagLen := protowire.ConsumeTag(raw)
		if tagLen < 0 {
			return nil, nil, fmt.Errorf("consume mesh envelope tag: %v", protowire.ParseError(tagLen))
		}
		fieldLen := protowire.ConsumeFieldValue(fieldNum, wireType, raw[tagLen:])
		if fieldLen < 0 {
			return nil, nil, fmt.Errorf("consume mesh envelope field %d: %v", fieldNum, protowire.ParseError(fieldLen))
		}
		totalLen := tagLen + fieldLen
		fieldBytes := raw[:totalLen]
		if fieldNum == meshEnvelopeHMACFieldNumber {
			if signature != nil {
				return nil, nil, errors.New("mesh envelope hmac cannot repeat")
			}
			if wireType != protowire.BytesType {
				return nil, nil, errors.New("mesh envelope hmac must use bytes wire type")
			}
			value, valueLen := protowire.ConsumeBytes(raw[tagLen:])
			if valueLen < 0 {
				return nil, nil, fmt.Errorf("consume mesh envelope hmac: %v", protowire.ParseError(valueLen))
			}
			if len(value) == 0 {
				return nil, nil, errors.New("mesh envelope hmac cannot be empty")
			}
			signature = append([]byte(nil), value...)
		} else {
			stripped = append(stripped, fieldBytes...)
		}
		raw = raw[totalLen:]
	}
	if len(signature) == 0 {
		return nil, nil, errors.New("mesh envelope hmac cannot be empty")
	}
	return stripped, signature, nil
}
