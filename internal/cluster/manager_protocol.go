package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

// marshalSignedEnvelope 对信封进行序列化并用HMAC-SHA256签名。
// 签名过程：先克隆信封（不含HMAC字段），计算HMAC，再将签名写入克隆体。
// 使用确定性序列化确保相同的消息产生一致的签名。
func (m *Manager) marshalSignedEnvelope(envelope *internalproto.Envelope) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("envelope cannot be nil")
	}
	clone, ok := proto.Clone(envelope).(*internalproto.Envelope)
	if !ok {
		return nil, errors.New("clone envelope")
	}
	signature, err := m.envelopeHMAC(clone)
	if err != nil {
		return nil, err
	}
	clone.Hmac = signature
	return marshalOptions.Marshal(clone)
}

// verifyEnvelope 验证信封的HMAC签名是否有效。
// 重新计算HMAC并与信封中的签名进行常量时间比较。
func (m *Manager) verifyEnvelope(envelope *internalproto.Envelope) error {
	if envelope == nil {
		return errors.New("envelope cannot be nil")
	}
	if len(envelope.Hmac) == 0 {
		return errors.New("envelope hmac cannot be empty")
	}
	expected, err := m.envelopeHMAC(envelope)
	if err != nil {
		return err
	}
	if !hmac.Equal(envelope.Hmac, expected) {
		return errors.New("envelope hmac mismatch")
	}
	return nil
}

// envelopeHMAC 计算信封的HMAC-SHA256签名。
// 克隆信封、清除HMAC字段、确定性序列化，然后用集群密钥计算HMAC。
func (m *Manager) envelopeHMAC(envelope *internalproto.Envelope) ([]byte, error) {
	clone, ok := proto.Clone(envelope).(*internalproto.Envelope)
	if !ok {
		return nil, errors.New("clone envelope")
	}
	clone.Hmac = nil
	payload, err := marshalOptions.Marshal(clone)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope for hmac: %w", err)
	}
	mac := hmac.New(sha256.New, []byte(m.cfg.ClusterSecret))
	if _, err := mac.Write(payload); err != nil {
		return nil, fmt.Errorf("write envelope hmac: %w", err)
	}
	return mac.Sum(nil), nil
}

// validatePeerEnvelope 验证信封的node_id是否与会话的对等节点ID匹配。
// 该检查防止一个节点伪装成另一个节点发送消息。
func validatePeerEnvelope(sess *session, envelope *internalproto.Envelope) error {
	envelopeNodeID := envelope.NodeId
	if envelopeNodeID <= 0 {
		return errors.New("envelope node id cannot be empty")
	}
	if sess.peerID == 0 {
		return errors.New("session has not completed hello")
	}
	if envelopeNodeID != sess.peerID {
		return fmt.Errorf("envelope node id mismatch: got %d want %d", envelopeNodeID, sess.peerID)
	}
	return nil
}
