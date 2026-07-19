// Command catalog serves CatalogService (proto/catalog.proto) standalone on
// port 9091 — kept as its own binary, separate from the coordinator, because
// the catalog is passive metadata storage while the coordinator is an active
// scheduler (see docs/atlas-implementation-spec.md's architecture notes).
package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"atlas/coordinator/internal/catalog"
	pb "atlas/coordinator/internal/catalogpb"
	"atlas/coordinator/internal/observability"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://atlas:atlas@localhost:5432/atlas"
	}
	addr := os.Getenv("CATALOG_ADDR")
	if addr == "" {
		addr = ":9091"
	}
	// 9095, not 9092 — docs/atlas-implementation-spec.md §1.4 already
	// reserves 9092 for the (not yet built) AI service's gRPC port.
	metricsAddr := os.Getenv("CATALOG_METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":9095"
	}

	if err := catalog.RunMigrations(databaseURL); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracer, err := observability.InitTracer(ctx, "catalog")
	if err != nil {
		return err
	}
	defer func() { _ = shutdownTracer(context.Background()) }()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(observability.GRPCMetricsInterceptor()),
	)
	pb.RegisterCatalogServiceServer(server, catalog.NewService(pool))

	metricsServer := &http.Server{Addr: metricsAddr, Handler: promhttp.Handler()}
	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	go func() {
		<-ctx.Done()
		log.Println("shutting down catalog service")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
		server.GracefulStop()
	}()

	log.Printf("catalog service listening on %s (metrics on %s)", addr, metricsAddr)
	return server.Serve(lis)
}
