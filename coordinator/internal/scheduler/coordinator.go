package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/errgroup"

	"atlas/coordinator/internal/planjson"
	pb "atlas/coordinator/internal/workerpb"
)

var (
	taskDispatchDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "atlas_task_dispatch_duration_seconds",
		Help: "Duration of dispatching one task (including retries) to a worker, by outcome.",
	}, []string{"outcome"})

	taskDispatchTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "atlas_task_dispatch_total",
		Help: "Count of task dispatches to workers, by outcome.",
	}, []string{"outcome"})
)

// Manifest is the subset of a catalog manifest the scheduler needs to
// dispatch a scan task against it.
type Manifest struct {
	FilePath string
	Format   string
}

// QueryResult is a fully-executed query: zero or more self-contained Arrow
// IPC streams (see proto/worker.proto's ResultBatch) ready to be returned to
// the caller or decoded by a client that understands Arrow.
type QueryResult struct {
	ArrowIPCBatches [][]byte
}

// Coordinator dispatches a compiled query's partial tasks across the
// registry's live workers and, if the query needs it, runs the combine step
// that folds every partial result into the final answer.
type Coordinator struct {
	Registry *Registry
}

// Compile parses `sql` against `schemaJSON` on any live worker, returning
// the plan already split into the partial (per-partition) and combine
// (once, over the union) halves distributed execution needs. Retries on a
// different live worker, like runTaskWithRetry, if the RPC itself fails —
// but not if the worker responds with a compile error, since a bad query
// fails identically on every worker.
func (c *Coordinator) Compile(ctx context.Context, sql, schemaJSON string) (*pb.CompileResponse, error) {
	const maxAttempts = 3
	tried := map[string]bool{}
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		addr, ok := c.Registry.PickAndReserve(tried)
		if !ok {
			if lastErr != nil {
				return nil, fmt.Errorf("no more live workers to retry compiling on: %w", lastErr)
			}
			return nil, fmt.Errorf("no live workers available to compile query")
		}
		tried[addr] = true

		client, _ := c.Registry.client(addr)
		resp, err := client.Compile(ctx, &pb.CompileRequest{Sql: sql, SchemaJson: schemaJSON})
		c.Registry.Release(addr)
		if err != nil {
			lastErr = fmt.Errorf("worker %s: %w", addr, err)
			c.Registry.MarkDead(addr)
			continue
		}
		if resp.GetError() != "" {
			return nil, fmt.Errorf("compiling query: %s", resp.GetError())
		}
		return resp, nil
	}
	return nil, fmt.Errorf("compiling query: failed after %d attempts: %w", maxAttempts, lastErr)
}

// RunCompiled dispatches one partial task per manifest, then — if the
// compiled query needs it — folds every partial result together with a
// single combine task. Manifests must all belong to the same dataset
// snapshot the plan was compiled against.
func (c *Coordinator) RunCompiled(ctx context.Context, compiled *pb.CompileResponse, manifests []Manifest) (*QueryResult, error) {
	if len(manifests) == 0 {
		return &QueryResult{}, nil
	}

	// Phase 4 column pruning: the plan's Scan node already carries the
	// pruned column list (atlas-optimizer's ColumnPruningRule, applied on
	// the worker during Compile) — thread it into each FileSource so
	// atlas_format::read_atlas_file actually skips unrequested columns'
	// byte ranges instead of reading everything. Best-effort: a plan we
	// can't parse just falls back to reading every column, never fails the
	// query outright over an optimization detail.
	columns, _ := planjson.ExtractScanColumns(compiled.GetPartialPlanJson())

	partials := make([][]byte, len(manifests))
	g, gctx := errgroup.WithContext(ctx)
	for i, m := range manifests {
		g.Go(func() error {
			req := &pb.TaskRequest{
				TaskId:   fmt.Sprintf("partial-%d", i),
				PlanJson: compiled.GetPartialPlanJson(),
				Source:   &pb.TaskRequest_File{File: &pb.FileSource{FilePath: m.FilePath, Columns: columns, Format: m.Format}},
			}
			result, err := c.runTaskWithRetry(gctx, req.GetTaskId(), req)
			if err != nil {
				return err
			}
			partials[i] = result
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	if !compiled.GetNeedsCombine() {
		return &QueryResult{ArrowIPCBatches: partials}, nil
	}

	combineReq := &pb.TaskRequest{
		TaskId:   "combine",
		PlanJson: compiled.GetCombinePlanJson(),
		Source:   &pb.TaskRequest_Inline{Inline: &pb.InlineSource{ArrowIpcBatches: partials}},
	}
	final, err := c.runTaskWithRetry(ctx, "combine", combineReq)
	if err != nil {
		return nil, err
	}
	return &QueryResult{ArrowIPCBatches: [][]byte{final}}, nil
}

// RunQuery is Compile followed by RunCompiled — a convenience for callers
// (tests, and any future direct caller) that don't need to record the
// compiled plan in between, unlike the REST handler which does.
func (c *Coordinator) RunQuery(ctx context.Context, sql, schemaJSON string, manifests []Manifest) (*QueryResult, error) {
	compiled, err := c.Compile(ctx, sql, schemaJSON)
	if err != nil {
		return nil, err
	}
	return c.RunCompiled(ctx, compiled, manifests)
}

func (c *Coordinator) executeOnWorker(ctx context.Context, addr string, req *pb.TaskRequest) ([]byte, error) {
	client, ok := c.Registry.client(addr)
	if !ok {
		return nil, fmt.Errorf("unknown worker %s", addr)
	}
	stream, err := client.ExecuteTask(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("starting task: %w", err)
	}
	msg, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("receiving task result: %w", err)
	}
	return msg.GetArrowIpc(), nil
}

// runTaskWithRetry dispatches a task to the least-loaded live worker,
// retrying on a different live worker up to 3 attempts total if the RPC
// itself fails (the assigned worker errored mid-task or is unreachable) —
// so one dead worker never fails the whole query.
func (c *Coordinator) runTaskWithRetry(ctx context.Context, taskID string, req *pb.TaskRequest) ([]byte, error) {
	started := time.Now()
	result, err := c.doRunTaskWithRetry(ctx, taskID, req)

	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	taskDispatchDuration.WithLabelValues(outcome).Observe(time.Since(started).Seconds())
	taskDispatchTotal.WithLabelValues(outcome).Inc()

	return result, err
}

func (c *Coordinator) doRunTaskWithRetry(ctx context.Context, taskID string, req *pb.TaskRequest) ([]byte, error) {
	const maxAttempts = 3
	tried := map[string]bool{}
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		addr, ok := c.Registry.PickAndReserve(tried)
		if !ok {
			if lastErr != nil {
				return nil, fmt.Errorf("task %s: no more live workers to retry on: %w", taskID, lastErr)
			}
			return nil, fmt.Errorf("task %s: no live workers available", taskID)
		}
		tried[addr] = true

		result, err := c.executeOnWorker(ctx, addr, req)
		c.Registry.Release(addr)
		if err == nil {
			return result, nil
		}
		lastErr = fmt.Errorf("worker %s: %w", addr, err)
		c.Registry.MarkDead(addr)
	}
	return nil, fmt.Errorf("task %s: failed after %d attempts: %w", taskID, maxAttempts, lastErr)
}
