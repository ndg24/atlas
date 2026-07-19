package observability

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

var (
	grpcRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "atlas_grpc_server_request_duration_seconds",
		Help: "Duration of unary gRPC requests handled by this server, by method and status code.",
	}, []string{"method", "code"})

	grpcRequestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "atlas_grpc_server_requests_total",
		Help: "Count of unary gRPC requests handled by this server, by method and status code.",
	}, []string{"method", "code"})
)

// GRPCMetricsInterceptor records request duration/count per RPC method and
// status code. Unary-only: every RPC on CatalogService (the one Go gRPC
// server this is used on) is unary.
func GRPCMetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		resp, err := handler(ctx, req)

		code := status.Code(err).String()
		grpcRequestDuration.WithLabelValues(info.FullMethod, code).Observe(time.Since(started).Seconds())
		grpcRequestTotal.WithLabelValues(info.FullMethod, code).Inc()

		return resp, err
	}
}
