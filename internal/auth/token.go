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

var ErrInvalidToken = errors.New("invalid token")

type Claims struct {
	Subject   string            `json:"sub"`
	Issuer    string            `json:"iss"`
	IssuedAt  int64             `json:"iat"`
	ExpiresAt int64             `json:"exp"`
	Metadata  map[string]string `json:"meta,omitempty"`
}

type Signer struct {
	secret []byte
}

func NewSigner(secret string) (*Signer, error) {
	if strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("secret cannot be empty")
	}
	return &Signer{secret: []byte(secret)}, nil
}

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
