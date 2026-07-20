// Command coordinator serves the REST API (docs/atlas-implementation-spec.md
// Phase 3): it never executes a query itself, only compiles it via a worker,
// fans partial tasks out across the registered workers, and merges the
// result — kept as its own binary, separate from the catalog service, per
// the architecture's active-scheduler vs. passive-metadata split.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"atlas/coordinator/internal/api"
	aipb "atlas/coordinator/internal/aipb"
	"atlas/coordinator/internal/cache"
	catalogpb "atlas/coordinator/internal/catalogpb"
	"atlas/coordinator/internal/history"
	"atlas/coordinator/internal/observability"
	"atlas/coordinator/internal/scheduler"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := envOr("DATABASE_URL", "postgres://atlas:atlas@localhost:5432/atlas")
	catalogAddr := envOr("CATALOG_ADDR", "127.0.0.1:9091")
	aiServiceAddr := envOr("AI_SERVICE_ADDR", "127.0.0.1:9092")
	listenAddr := envOr("COORDINATOR_ADDR", ":8080")
	workerAddrs := strings.Split(envOr("WORKER_ADDRS", "127.0.0.1:9100"), ",")
	redisURL := envOr("REDIS_URL", "redis://127.0.0.1:6379")

	// Unlike every other config value here, JWT_SECRET has no dev fallback:
	// it gates the entire REST API, so a hardcoded default would let anyone
	// reading the source mint valid tokens for any workspace.
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		return fmt.Errorf("JWT_SECRET is required — generate one and export it, or set it in deploy/docker-compose.yml for local dev")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracer, err := observability.InitTracer(ctx, "coordinator")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracer(context.Background()) }()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	catalogConn, err := grpc.NewClient(catalogAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return err
	}
	defer catalogConn.Close()
	catalogClient := catalogpb.NewCatalogServiceClient(catalogConn)

	// Same construction as the catalog client — otelgrpc's stats handler is
	// what propagates the coordinator's trace id into the AI service's
	// spans, no extra code needed beyond this dial option.
	aiConn, err := grpc.NewClient(aiServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return err
	}
	defer aiConn.Close()
	aiClient := aipb.NewAIServiceClient(aiConn)

	registry, err := scheduler.NewRegistry(workerAddrs)
	if err != nil {
		return err
	}
	defer registry.Close()
	registry.StartHeartbeats(ctx, 2*time.Second)

	coordinator := &scheduler.Coordinator{Registry: registry}
	historyStore := history.NewStore(pool)

	// A Redis connectivity problem should never take the coordinator down —
	// cache.Get/Set failures are swallowed by the API layer and treated as
	// misses, so it's safe to always construct the client here even if
	// Redis isn't reachable yet.
	resultCache, err := cache.New(redisURL, 5*time.Minute)
	if err != nil {
		return err
	}
	defer resultCache.Close()

	server := api.NewServer(catalogClient, coordinator, aiClient, historyStore, resultCache, []byte(jwtSecret))

	httpServer := &http.Server{Addr: listenAddr, Handler: server.Routes()}
	go func() {
		<-ctx.Done()
		log.Println("shutting down coordinator")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("coordinator REST API listening on %s (workers: %s)", listenAddr, workerAddrs)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
