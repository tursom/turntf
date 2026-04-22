package cluster

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/tursom/turntf/internal/mesh"
)

var meshMarshalOptions = proto.MarshalOptions{Deterministic: true}

type meshEnvelopeAuthenticator struct {
	secret []byte
}

func newMeshEnvelopeAuthenticator(secret string) *meshEnvelopeAuthenticator {
	trimmed := strings.TrimSpace(secret)
	if trimmed == "" {
		return nil
	}
	return &meshEnvelopeAuthenticator{secret: []byte(trimmed)}
}

func (a *meshEnvelopeAuthenticator) Sign(envelope *mesh.ClusterEnvelope, _ []byte) ([]byte, error) {
	if envelope == nil {
		return nil, errors.New("mesh envelope cannot be nil")
	}
	clone, ok := proto.Clone(envelope).(*mesh.ClusterEnvelope)
	if !ok {
		return nil, errors.New("clone mesh envelope")
	}
	signature, err := a.signatureFor(clone)
	if err != nil {
		return nil, err
	}
	clone.Hmac = signature
	return meshMarshalOptions.Marshal(clone)
}

func (a *meshEnvelopeAuthenticator) Verify(envelope *mesh.ClusterEnvelope, _ []byte) error {
	if envelope == nil {
		return errors.New("mesh envelope cannot be nil")
	}
	if len(envelope.GetHmac()) == 0 {
		return errors.New("mesh envelope hmac cannot be empty")
	}
	expected, err := a.signatureFor(envelope)
	if err != nil {
		return err
	}
	if !hmac.Equal(envelope.GetHmac(), expected) {
		return errors.New("mesh envelope hmac mismatch")
	}
	return nil
}

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
	mac := hmac.New(sha256.New, a.secret)
	if _, err := mac.Write(payload); err != nil {
		return nil, fmt.Errorf("write mesh envelope hmac: %w", err)
	}
	return mac.Sum(nil), nil
}
