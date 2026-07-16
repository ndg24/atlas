// Package cache is the coordinator's Redis-backed result cache (Phase 4):
// keyed on the hash of a query's *optimized* logical plan plus the dataset
// snapshot it ran against, so two differently-worded but equivalent queries
// share a cache entry, and a cache entry becomes a miss the instant its
// dataset's current_snapshot_id advances — checked on read, rather than
// requiring an active invalidation sweep on every ingest.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const keyPrefix = "atlas:query:"

// Entry is a cached query result: the Arrow IPC batches it produced, tagged
// with the dataset snapshot they were computed against.
type Entry struct {
	SnapshotID      string   `json:"snapshot_id"`
	ArrowIPCBatches [][]byte `json:"arrow_ipc_batches"`
}

type ResultCache struct {
	rdb *redis.Client
	ttl time.Duration
}

// New builds a ResultCache against redisURL (e.g. "redis://localhost:6379").
// ttl is how long a cache entry lives before it expires regardless of
// snapshot staleness; pass 0 for the 5-minute default.
func New(redisURL string, ttl time.Duration) (*ResultCache, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parsing REDIS_URL: %w", err)
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &ResultCache{rdb: redis.NewClient(opts), ttl: ttl}, nil
}

// NewWithClient builds a ResultCache around an already-constructed redis
// client — used by tests to point at an in-memory miniredis instance.
func NewWithClient(rdb *redis.Client, ttl time.Duration) *ResultCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &ResultCache{rdb: rdb, ttl: ttl}
}

func (c *ResultCache) Close() error {
	return c.rdb.Close()
}

// Key hashes the optimized plan JSON plus the snapshot id into a cache key.
// Deliberately not the raw SQL string, so equivalent-but-differently-worded
// queries that optimize to the same plan share an entry.
func Key(optimizedPlanJSON, snapshotID string) string {
	sum := sha256.Sum256([]byte(optimizedPlanJSON + "|" + snapshotID))
	return keyPrefix + hex.EncodeToString(sum[:])
}

// Get returns the cached entry for key, and whether it's a usable hit —
// present in Redis AND still fresh (its stored SnapshotID matches the
// dataset's current one; a re-ingest since caching makes it stale even
// though it hasn't expired).
func (c *ResultCache) Get(ctx context.Context, key, currentSnapshotID string) (*Entry, bool, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading cache key %s: %w", key, err)
	}
	var entry Entry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return nil, false, fmt.Errorf("decoding cached entry: %w", err)
	}
	if entry.SnapshotID != currentSnapshotID {
		return nil, false, nil
	}
	return &entry, true, nil
}

// Set stores batches under key, tagged with snapshotID for staleness checks.
func (c *ResultCache) Set(ctx context.Context, key, snapshotID string, batches [][]byte) error {
	raw, err := json.Marshal(Entry{SnapshotID: snapshotID, ArrowIPCBatches: batches})
	if err != nil {
		return fmt.Errorf("encoding cache entry: %w", err)
	}
	if err := c.rdb.Set(ctx, key, raw, c.ttl).Err(); err != nil {
		return fmt.Errorf("writing cache key %s: %w", key, err)
	}
	return nil
}
