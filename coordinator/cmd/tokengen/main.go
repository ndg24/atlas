// Command tokengen mints a dev JWT for the coordinator's REST API. There is
// no login/signup flow (see docs/atlas-implementation-spec.md's
// cross-cutting auth note) — this is the stand-in for one, signing tokens
// against the same JWT_SECRET the coordinator validates against.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"atlas/coordinator/internal/auth"
)

func main() {
	userID := flag.String("user-id", "dev-user", "user id to embed in the token's claims")
	workspaceID := flag.String("workspace-id", auth.DefaultWorkspaceID, "workspace id to embed in the token's claims")
	ttl := flag.Duration("ttl", 24*time.Hour, "token lifetime")
	flag.Parse()

	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		log.Fatal("JWT_SECRET is required — generate one and export it, or set it in deploy/docker-compose.yml for local dev")
	}

	token, err := auth.Generate([]byte(secret), *userID, *workspaceID, *ttl)
	if err != nil {
		log.Fatalf("generating token: %v", err)
	}
	fmt.Println(token)
}
