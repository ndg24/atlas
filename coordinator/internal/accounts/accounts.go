// Package accounts implements user signup/login persistence — creating a
// user (with a bcrypt-hashed password, see internal/auth) and looking one
// up by email at login, plus ensuring a named workspace exists. This is the
// direct-Postgres counterpart to catalog.Service's dataset/snapshot RPCs,
// kept in the coordinator itself (like internal/history's query_history
// access) rather than added to the catalog service's gRPC surface, since
// nothing outside the coordinator ever needs to create a user or workspace.
package accounts

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type User struct {
	ID           string
	WorkspaceID  string
	Email        string
	PasswordHash string
}

// ErrNotFound is returned by GetUserByEmail instead of a raw pgx.ErrNoRows,
// so callers (the /auth/login handler) don't need to import pgx just to
// check this one case.
var ErrNotFound = errors.New("not found")

// EnsureWorkspace returns the id of the workspace named name, creating it
// first if it doesn't exist yet — idempotent, so it's safe to call on every
// signup that names a workspace.
func (s *Store) EnsureWorkspace(ctx context.Context, name string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO workspaces (name) VALUES ($1)
		 ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id::text`,
		name,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("ensuring workspace %q: %w", name, err)
	}
	return id, nil
}

// CreateUser inserts a new user row. passwordHash must already be hashed
// (auth.HashPassword) — this method never sees or stores a plaintext
// password.
func (s *Store) CreateUser(ctx context.Context, workspaceID, email, passwordHash string) (*User, error) {
	var u User
	err := s.pool.QueryRow(ctx,
		`INSERT INTO users (workspace_id, email, password_hash) VALUES ($1, $2, $3)
		 RETURNING id::text, workspace_id::text, email, password_hash`,
		workspaceID, email, passwordHash,
	).Scan(&u.ID, &u.WorkspaceID, &u.Email, &u.PasswordHash)
	if err != nil {
		return nil, fmt.Errorf("creating user %q: %w", email, err)
	}
	return &u, nil
}

// GetUserByEmail looks up a user for login, returning ErrNotFound if no
// user has that email.
func (s *Store) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	var u User
	err := s.pool.QueryRow(ctx,
		`SELECT id::text, workspace_id::text, email, password_hash FROM users WHERE email = $1`,
		email,
	).Scan(&u.ID, &u.WorkspaceID, &u.Email, &u.PasswordHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting user %q: %w", email, err)
	}
	return &u, nil
}
