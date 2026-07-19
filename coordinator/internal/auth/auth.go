// Package auth implements the coordinator's JWT-based auth: signing and
// validating bearer tokens that carry a user id and workspace id. This is
// groundwork, not enforcement — no login/signup flow exists yet (tokens are
// minted by the dev-facing cmd/tokengen CLI against a shared secret), and no
// catalog query is filtered by workspace yet. See
// docs/atlas-implementation-spec.md's cross-cutting auth note: "add the
// users/workspaces tables and JWT middleware... even if initially permissive
// (single default workspace)".
package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// DefaultWorkspaceID is the single workspace seeded by
// migrations/0007_workspaces_users.up.sql.
const DefaultWorkspaceID = "00000000-0000-0000-0000-000000000001"

// Claims is the JWT payload: a user id and workspace id alongside the
// standard registered claims (expiry, issued-at).
type Claims struct {
	jwt.RegisteredClaims
	UserID      string `json:"user_id"`
	WorkspaceID string `json:"workspace_id"`
}

// Generate signs a token for userID/workspaceID, valid for ttl.
func Generate(secret []byte, userID, workspaceID string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
		UserID:      userID,
		WorkspaceID: workspaceID,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		return "", fmt.Errorf("signing token: %w", err)
	}
	return signed, nil
}

// Parse validates tokenString against secret and returns its claims.
// Rejects tokens signed with an unexpected algorithm, expired tokens, and
// malformed tokens.
func Parse(secret []byte, tokenString string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("parsing token: %w", err)
	}
	if !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	return claims, nil
}
