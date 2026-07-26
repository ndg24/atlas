package api_test

// Signup/login need a real *accounts.Store (bcrypt hash stored in and read
// back from Postgres), so unlike the fully in-memory tests in
// server_test.go, this one needs a real Postgres testcontainer — the same
// Docker-gated pattern coordinator/internal/catalog's tests already use.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"atlas/coordinator/internal/accounts"
	"atlas/coordinator/internal/api"
	"atlas/coordinator/internal/auth"
	"atlas/coordinator/internal/catalog"
)

func newTestServerWithAccounts(t *testing.T) *api.Server {
	t.Helper()
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("atlas"),
		tcpostgres.WithUsername("atlas"),
		tcpostgres.WithPassword("atlas"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminating postgres container: %v", err)
		}
	})

	databaseURL, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("getting connection string: %v", err)
	}
	if err := catalog.RunMigrations(databaseURL); err != nil {
		t.Fatalf("running migrations: %v", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("creating pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	accountsStore := accounts.NewStore(pool)
	return api.NewServer(&fakeCatalog{}, nil, nil, nil, accountsStore, nil, testSecret)
}

func decodeToken(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding token response: %v\nbody: %s", err, rec.Body.String())
	}
	if resp.Token == "" {
		t.Fatalf("expected a non-empty token, got body: %s", rec.Body.String())
	}
	return resp.Token
}

func TestSignupThenLogin_MintsValidTokensForTheSameUser(t *testing.T) {
	server := newTestServerWithAccounts(t)

	signupReq := httptest.NewRequest(http.MethodPost, "/auth/signup",
		strings.NewReader(`{"email":"alice@example.com","password":"hunter22","workspace_name":"acme"}`))
	signupRec := httptest.NewRecorder()
	server.Routes().ServeHTTP(signupRec, signupReq)
	if signupRec.Code != http.StatusCreated {
		t.Fatalf("signup: expected 201, got %d: %s", signupRec.Code, signupRec.Body.String())
	}
	signupClaims, err := auth.Parse(testSecret, decodeToken(t, signupRec))
	if err != nil {
		t.Fatalf("parsing signup token: %v", err)
	}
	if signupClaims.UserID == "" || signupClaims.WorkspaceID == "" {
		t.Fatalf("signup token missing claims: %+v", signupClaims)
	}

	loginReq := httptest.NewRequest(http.MethodPost, "/auth/login",
		strings.NewReader(`{"email":"alice@example.com","password":"hunter22"}`))
	loginRec := httptest.NewRecorder()
	server.Routes().ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("login: expected 200, got %d: %s", loginRec.Code, loginRec.Body.String())
	}
	loginClaims, err := auth.Parse(testSecret, decodeToken(t, loginRec))
	if err != nil {
		t.Fatalf("parsing login token: %v", err)
	}
	if loginClaims.UserID != signupClaims.UserID || loginClaims.WorkspaceID != signupClaims.WorkspaceID {
		t.Fatalf("login token claims %+v don't match signup token claims %+v", loginClaims, signupClaims)
	}
}

func TestLogin_RejectsWrongPassword(t *testing.T) {
	server := newTestServerWithAccounts(t)

	signupReq := httptest.NewRequest(http.MethodPost, "/auth/signup",
		strings.NewReader(`{"email":"bob@example.com","password":"correct-password"}`))
	signupRec := httptest.NewRecorder()
	server.Routes().ServeHTTP(signupRec, signupReq)
	if signupRec.Code != http.StatusCreated {
		t.Fatalf("signup: expected 201, got %d: %s", signupRec.Code, signupRec.Body.String())
	}

	loginReq := httptest.NewRequest(http.MethodPost, "/auth/login",
		strings.NewReader(`{"email":"bob@example.com","password":"wrong-password"}`))
	loginRec := httptest.NewRecorder()
	server.Routes().ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusUnauthorized {
		t.Fatalf("login with wrong password: expected 401, got %d: %s", loginRec.Code, loginRec.Body.String())
	}
}

func TestSignup_RejectsDuplicateEmail(t *testing.T) {
	server := newTestServerWithAccounts(t)

	body := `{"email":"carol@example.com","password":"whatever123"}`
	first := httptest.NewRecorder()
	server.Routes().ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/auth/signup", strings.NewReader(body)))
	if first.Code != http.StatusCreated {
		t.Fatalf("first signup: expected 201, got %d: %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	server.Routes().ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/auth/signup", strings.NewReader(body)))
	if second.Code != http.StatusConflict {
		t.Fatalf("duplicate signup: expected 409, got %d: %s", second.Code, second.Body.String())
	}
}
