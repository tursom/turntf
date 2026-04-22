package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	internalproto "github.com/tursom/turntf/internal/proto"
)

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
