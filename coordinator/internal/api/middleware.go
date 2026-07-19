package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"atlas/coordinator/internal/auth"
)

type ctxKey int

const claimsCtxKey ctxKey = iota

// claimsFromContext returns the bearer token's claims stashed by
// authMiddleware, if any.
func claimsFromContext(ctx context.Context) (*auth.Claims, bool) {
	claims, ok := ctx.Value(claimsCtxKey).(*auth.Claims)
	return claims, ok
}

// authMiddleware requires a valid "Authorization: Bearer <token>" header,
// signed against secret, on every request it wraps. On success, the parsed
// claims are stashed in the request context for handlers to read via
// claimsFromContext.
func authMiddleware(secret []byte) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			token, ok := strings.CutPrefix(header, "Bearer ")
			if !ok || token == "" {
				writeError(w, http.StatusUnauthorized, fmt.Errorf(`missing or malformed "Authorization: Bearer <token>" header`))
				return
			}

			claims, err := auth.Parse(secret, token)
			if err != nil {
				writeError(w, http.StatusUnauthorized, err)
				return
			}

			ctx := context.WithValue(r.Context(), claimsCtxKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
