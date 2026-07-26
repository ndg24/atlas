package auth_test

import (
	"testing"

	"atlas/coordinator/internal/auth"
)

func TestHashAndVerifyPassword_RoundTrips(t *testing.T) {
	hash, err := auth.HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if hash == "correct horse battery staple" {
		t.Fatal("HashPassword returned the plaintext unchanged")
	}
	if err := auth.VerifyPassword(hash, "correct horse battery staple"); err != nil {
		t.Fatalf("VerifyPassword with the correct password: %v", err)
	}
}

func TestVerifyPassword_RejectsWrongPassword(t *testing.T) {
	hash, err := auth.HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if err := auth.VerifyPassword(hash, "wrong password"); err == nil {
		t.Fatal("VerifyPassword should have rejected a wrong password")
	}
}
