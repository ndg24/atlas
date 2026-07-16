// Package history records every query the coordinator runs into the
// catalog's query_history table (docs/atlas-implementation-spec.md §1.3): a
// row inserted as "running" when the query starts, updated with its
// terminal status/duration/error when it finishes — for both successful and
// failed queries, so GET /history is accurate either way.
package history

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Start inserts a "running" query_history row and returns its id.
// logicalPlanJSON may be a placeholder ("{}") if the plan isn't known yet —
// the column is NOT NULL, and SetPlan can fill in the real one once
// compiled.
func (s *Store) Start(ctx context.Context, source, rawInput, logicalPlanJSON string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO query_history (source, raw_input, logical_plan_json, status)
		 VALUES ($1, $2, $3, 'running') RETURNING id::text`,
		source, rawInput, logicalPlanJSON,
	).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("starting query_history row: %w", err)
	}
	return id, nil
}

// SetPlan records the actual compiled plan against an already-started row.
func (s *Store) SetPlan(ctx context.Context, id, logicalPlanJSON string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE query_history SET logical_plan_json = $1 WHERE id = $2`, logicalPlanJSON, id,
	)
	if err != nil {
		return fmt.Errorf("updating query_history plan for %s: %w", id, err)
	}
	return nil
}

// Finish sets a query_history row's terminal state. queryErr is stored as
// NULL when empty (a successful query).
func (s *Store) Finish(ctx context.Context, id, status string, durationMs int, queryErr string) error {
	var errArg any
	if queryErr != "" {
		errArg = queryErr
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE query_history SET status = $1, duration_ms = $2, error = $3 WHERE id = $4`,
		status, durationMs, errArg, id,
	)
	if err != nil {
		return fmt.Errorf("finishing query_history row %s: %w", id, err)
	}
	return nil
}

type Entry struct {
	ID          string  `json:"id"`
	SubmittedAt string  `json:"submitted_at"`
	Source      string  `json:"source"`
	RawInput    string  `json:"raw_input"`
	Status      string  `json:"status"`
	DurationMs  *int    `json:"duration_ms"`
	Error       *string `json:"error"`
}

// List returns the most recent `limit` query_history rows, newest first.
func (s *Store) List(ctx context.Context, limit int) ([]Entry, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id::text, submitted_at::text, source, raw_input, status, duration_ms, error
		 FROM query_history ORDER BY submitted_at DESC LIMIT $1`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("listing query history: %w", err)
	}
	defer rows.Close()

	out := []Entry{}
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.SubmittedAt, &e.Source, &e.RawInput, &e.Status, &e.DurationMs, &e.Error); err != nil {
			return nil, fmt.Errorf("scanning query_history row: %w", err)
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating query_history rows: %w", err)
	}
	return out, nil
}
