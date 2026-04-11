package auth

import "testing"

func TestSignerSignAndVerify(t *testing.T) {
	t.Parallel()

	signer, err := NewSigner("secret")
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}

	token, err := signer.Sign(Claims{
		Subject:   "1",
		Issuer:    "node-a",
		IssuedAt:  100,
		ExpiresAt: 200,
		Metadata: map[string]string{
			"role": "admin",
		},
	})
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	claims, err := signer.Verify(token)
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if claims.Subject != "1" || claims.Issuer != "node-a" || claims.Metadata["role"] != "admin" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestSignerRejectsTamperedToken(t *testing.T) {
	t.Parallel()

	signer, err := NewSigner("secret")
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}

	token, err := signer.Sign(Claims{Subject: "1"})
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	if _, err := signer.Verify(token + "x"); err == nil {
		t.Fatalf("expected tampered token to fail verification")
	}

	otherSigner, err := NewSigner("other-secret")
	if err != nil {
		t.Fatalf("new other signer: %v", err)
	}
	if _, err := otherSigner.Verify(token); err == nil {
		t.Fatalf("expected token signed with another secret to fail verification")
	}
}
