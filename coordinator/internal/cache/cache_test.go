package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestCache(t *testing.T) *ResultCache {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("starting miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return NewWithClient(rdb, time.Minute)
}

func TestGetMissWhenAbsent(t *testing.T) {
	c := newTestCache(t)
	_, hit, err := c.Get(context.Background(), Key("plan", "snap-1"), "snap-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hit {
		t.Fatal("expected a miss for an absent key")
	}
}

func TestSetThenGetHits(t *testing.T) {
	c := newTestCache(t)
	ctx := context.Background()
	key := Key("optimized-plan-json", "snap-1")
	want := [][]byte{[]byte("batch-1"), []byte("batch-2")}

	if err := c.Set(ctx, key, "snap-1", want); err != nil {
		t.Fatalf("Set: %v", err)
	}
	entry, hit, err := c.Get(ctx, key, "snap-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !hit {
		t.Fatal("expected a hit after Set")
	}
	if len(entry.ArrowIPCBatches) != 2 || string(entry.ArrowIPCBatches[0]) != "batch-1" {
		t.Fatalf("unexpected cached batches: %v", entry.ArrowIPCBatches)
	}
}

func TestGetMissWhenSnapshotAdvanced(t *testing.T) {
	c := newTestCache(t)
	ctx := context.Background()
	key := Key("optimized-plan-json", "snap-1")

	if err := c.Set(ctx, key, "snap-1", [][]byte{[]byte("stale")}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// Same key, but the dataset was re-ingested since caching (snapshot
	// advanced) — must be treated as a miss even though the Redis key still
	// exists and hasn't expired.
	_, hit, err := c.Get(ctx, key, "snap-2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if hit {
		t.Fatal("expected a miss once the current snapshot id has advanced")
	}
}

func TestKeyIsStableForSamePlanAndSnapshot(t *testing.T) {
	a := Key("same-plan", "snap-1")
	b := Key("same-plan", "snap-1")
	if a != b {
		t.Fatalf("expected identical keys, got %s vs %s", a, b)
	}
	if c := Key("different-plan", "snap-1"); c == a {
		t.Fatal("expected different plans to hash to different keys")
	}
}
