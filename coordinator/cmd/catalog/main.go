// Command catalog serves CatalogService (proto/catalog.proto) standalone on
// port 9091 — kept as its own binary, separate from the coordinator, because
// the catalog is passive metadata storage while the coordinator is an active
// scheduler (see docs/atlas-implementation-spec.md's architecture notes).
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"

	"atlas/coordinator/internal/catalog"
	pb "atlas/coordinator/internal/catalogpb"
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

	if err := catalog.RunMigrations(databaseURL); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	pb.RegisterCatalogServiceServer(server, catalog.NewService(pool))

	go func() {
		<-ctx.Done()
		log.Println("shutting down catalog service")
		server.GracefulStop()
	}()

	log.Printf("catalog service listening on %s", addr)
	return server.Serve(lis)
}
