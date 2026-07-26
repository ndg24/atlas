package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"atlas/coordinator/internal/accounts"
	"atlas/coordinator/internal/auth"
)

// tokenTTL matches cmd/tokengen's own default — signup/login are the
// user-facing replacement for that dev CLI, so the tokens they mint should
// behave the same way.
const tokenTTL = 24 * time.Hour

type authRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	// WorkspaceName is signup-only: if set, a workspace by that name is
	// created (or reused, if it already exists) for the new user; if empty,
	// the user is created in the seeded default workspace.
	WorkspaceName string `json:"workspace_name,omitempty"`
}

type authResponse struct {
	Token string `json:"token"`
}

// handleSignup creates a new user (in a named or the default workspace)
// and returns a bearer token for it — the same auth.Generate call
// cmd/tokengen makes, so the token this mints is indistinguishable from one
// minted by the dev CLI.
func (s *Server) handleSignup(w http.ResponseWriter, r *http.Request) {
	var req authRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decoding request body: %w", err))
		return
	}
	if req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf(`both "email" and "password" are required`))
		return
	}

	ctx := r.Context()
	workspaceID := auth.DefaultWorkspaceID
	if req.WorkspaceName != "" {
		id, err := s.accounts.EnsureWorkspace(ctx, req.WorkspaceName)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		workspaceID = id
	}

	passwordHash, err := auth.HashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	user, err := s.accounts.CreateUser(ctx, workspaceID, req.Email, passwordHash)
	if err != nil {
		// The only expected failure here is users.email's UNIQUE constraint —
		// there's no pre-check-then-insert race to close, so the constraint
		// violation itself is the signal.
		writeError(w, http.StatusConflict, fmt.Errorf("creating user (email may already be taken): %w", err))
		return
	}

	token, err := auth.Generate(s.authSecret, user.ID, user.WorkspaceID, tokenTTL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusCreated, authResponse{Token: token})
}

// handleLogin verifies email/password against the stored bcrypt hash and
// returns a bearer token on success. Both "no such user" and "wrong
// password" report the same 401 with the same message, so a caller can't
// use this endpoint to enumerate registered emails.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req authRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decoding request body: %w", err))
		return
	}
	if req.Email == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf(`both "email" and "password" are required`))
		return
	}

	user, err := s.accounts.GetUserByEmail(r.Context(), req.Email)
	if errors.Is(err, accounts.ErrNotFound) {
		writeError(w, http.StatusUnauthorized, fmt.Errorf("invalid email or password"))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if err := auth.VerifyPassword(user.PasswordHash, req.Password); err != nil {
		writeError(w, http.StatusUnauthorized, fmt.Errorf("invalid email or password"))
		return
	}

	token, err := auth.Generate(s.authSecret, user.ID, user.WorkspaceID, tokenTTL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, authResponse{Token: token})
}
