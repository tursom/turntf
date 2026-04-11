package auth

import "testing"

func TestHashPasswordAndVerify(t *testing.T) {
	t.Parallel()

	hash, err := HashPassword("secret")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if hash == "" || hash == "secret" {
		t.Fatalf("unexpected hash: %q", hash)
	}
	if err := VerifyPassword(hash, "secret"); err != nil {
		t.Fatalf("verify password: %v", err)
	}
}

func TestVerifyPasswordRejectsWrongPassword(t *testing.T) {
	t.Parallel()

	hash, err := HashPassword("secret")
	if err != nil {
		t.Fatalf("hash password: %v", err)
	}
	if err := VerifyPassword(hash, "wrong"); err == nil {
		t.Fatalf("expected wrong password verification to fail")
	}
}
