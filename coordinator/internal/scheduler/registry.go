// Package scheduler holds the coordinator's worker registry and query
// dispatch logic (docs/atlas-implementation-spec.md Phase 3): which workers
// are alive, which one gets the next task, and how a query's partial tasks
// and combine step get run against them with retries.
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "atlas/coordinator/internal/workerpb"
)

type workerState struct {
	client pb.WorkerServiceClient
	conn   *grpc.ClientConn
	alive  bool
	misses int
	// inFlight is a coordinator-side reservation count (incremented by
	// PickAndReserve, decremented by Release), not the worker's
	// self-reported Heartbeat count — that keeps least-loaded picking
	// race-free under concurrent dispatch instead of depending on the
	// heartbeat poll interval to observe a task that was just assigned.
	inFlight int32
}

// Registry tracks every configured worker's liveness and current
// reservation count, guarded by a single mutex — there are only ever a
// handful of workers, so fine-grained per-worker locking isn't worth the
// complexity.
type Registry struct {
	mu      sync.Mutex
	workers map[string]*workerState
}

func NewRegistry(addrs []string) (*Registry, error) {
	workers := make(map[string]*workerState, len(addrs))
	for _, addr := range addrs {
		// otelgrpc's client stats handler injects the current trace context
		// (a W3C traceparent header) into every outgoing call's gRPC
		// metadata — this is what lets atlas-worker's telemetry module
		// (engine/crates/atlas-worker/src/telemetry.rs) pick up the
		// coordinator's trace id and attach its own spans to the same trace.
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		)
		if err != nil {
			return nil, fmt.Errorf("dialing worker %s: %w", addr, err)
		}
		workers[addr] = &workerState{
			client: pb.NewWorkerServiceClient(conn),
			conn:   conn,
			alive:  true, // assumed alive until the first heartbeat fails 3 times
		}
	}
	return &Registry{workers: workers}, nil
}

func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, w := range r.workers {
		_ = w.conn.Close()
	}
}

// StartHeartbeats polls every worker's Heartbeat RPC on `interval` until ctx
// is done, marking a worker dead after 3 consecutive missed/failed
// heartbeats and alive again the moment one succeeds.
func (r *Registry) StartHeartbeats(ctx context.Context, interval time.Duration) {
	r.mu.Lock()
	addrs := make([]string, 0, len(r.workers))
	for addr := range r.workers {
		addrs = append(addrs, addr)
	}
	r.mu.Unlock()

	for _, addr := range addrs {
		go r.heartbeatLoop(ctx, addr, interval)
	}
}

func (r *Registry) heartbeatLoop(ctx context.Context, addr string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			w, ok := r.workers[addr]
			r.mu.Unlock()
			if !ok {
				return
			}

			hctx, cancel := context.WithTimeout(ctx, interval)
			_, err := w.client.Heartbeat(hctx, &pb.HeartbeatRequest{})
			cancel()

			r.mu.Lock()
			if err != nil {
				w.misses++
				if w.misses >= 3 {
					w.alive = false
				}
			} else {
				w.misses = 0
				w.alive = true
			}
			r.mu.Unlock()
		}
	}
}

// PickAndReserve returns the least-loaded live worker not in `exclude`,
// incrementing its reservation count atomically with the pick so two
// concurrent calls never both pick the same idle worker. Call Release with
// the same address once the task against it finishes (success or failure).
func (r *Registry) PickAndReserve(exclude map[string]bool) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	best := ""
	var bestLoad int32 = -1
	for addr, w := range r.workers {
		if exclude[addr] || !w.alive {
			continue
		}
		if bestLoad == -1 || w.inFlight < bestLoad {
			best, bestLoad = addr, w.inFlight
		}
	}
	if best == "" {
		return "", false
	}
	r.workers[best].inFlight++
	return best, true
}

func (r *Registry) Release(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.workers[addr]; ok {
		w.inFlight--
	}
}

// MarkDead immediately marks a worker dead — e.g. after an ExecuteTask RPC
// itself fails, not just a missed heartbeat — so subsequent picks within the
// same query skip it without waiting out the 3-miss heartbeat grace period.
// The heartbeat loop marks it alive again on its own once it responds.
func (r *Registry) MarkDead(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.workers[addr]; ok {
		w.alive = false
	}
}

func (r *Registry) client(addr string) (pb.WorkerServiceClient, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.workers[addr]
	if !ok {
		return nil, false
	}
	return w.client, true
}
