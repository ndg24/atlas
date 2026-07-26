package auth

import (
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

// HashPassword bcrypt-hashes a plaintext password for storage in
// users.password_hash. Never store or log the plaintext itself.
func HashPassword(plain string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", fmt.Errorf("hashing password: %w", err)
	}
	return string(hash), nil
}

// VerifyPassword returns nil if plain matches hash (a users.password_hash
// value produced by HashPassword), or an error otherwise — including a
// wrong password, so callers should treat any non-nil error as "invalid
// credentials" without inspecting it further.
func VerifyPassword(hash, plain string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain))
}
