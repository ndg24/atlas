package scheduler

import (
	"encoding/json"
	"testing"

	catalogpb "atlas/coordinator/internal/catalogpb"
)

func exprJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("building expr fixture: %v", err)
	}
	return json.RawMessage(b)
}

func eqPredicate(t *testing.T, column string, intLit int) json.RawMessage {
	return exprJSON(t, map[string]any{"Binary": map[string]any{
		"left":  map[string]any{"Column": column},
		"op":    "Eq",
		"right": map[string]any{"Literal": map[string]any{"Int": intLit}},
	}})
}

func manifestWithPartition(t *testing.T, filePath string, partitionValues map[string]any) *catalogpb.Manifest {
	t.Helper()
	pv, err := json.Marshal(partitionValues)
	if err != nil {
		t.Fatalf("marshaling partition values: %v", err)
	}
	return &catalogpb.Manifest{
		FilePath:            filePath,
		PartitionValuesJson: string(pv),
		ColumnStatsJson:     "{}",
	}
}

func TestPrunePartitions_ExactPartitionValueEquality(t *testing.T) {
	manifests := []*catalogpb.Manifest{
		manifestWithPartition(t, "year=2020/part-0.atlas", map[string]any{"year": 2020}),
		manifestWithPartition(t, "year=2021/part-0.atlas", map[string]any{"year": 2021}),
		manifestWithPartition(t, "year=2022/part-0.atlas", map[string]any{"year": 2022}),
		manifestWithPartition(t, "year=2023/part-0.atlas", map[string]any{"year": 2023}),
		manifestWithPartition(t, "year=2024/part-0.atlas", map[string]any{"year": 2024}),
	}
	predicate := eqPredicate(t, "year", 2024)

	survivors := PrunePartitions(predicate, manifests)

	if len(survivors) != 1 {
		t.Fatalf("expected exactly 1 surviving manifest, got %d: %v", len(survivors), survivors)
	}
	if survivors[0].GetFilePath() != "year=2024/part-0.atlas" {
		t.Fatalf("expected the year=2024 manifest to survive, got %s", survivors[0].GetFilePath())
	}
}

func TestPrunePartitions_UnknownColumnPrunesNothing(t *testing.T) {
	manifests := []*catalogpb.Manifest{
		manifestWithPartition(t, "a.atlas", map[string]any{"year": 2020}),
		manifestWithPartition(t, "b.atlas", map[string]any{"year": 2024}),
	}
	// "hospital" isn't a partition column or in column_stats for either
	// manifest — nothing to reason about, so nothing should be pruned.
	predicate := eqPredicate(t, "hospital", 1)

	survivors := PrunePartitions(predicate, manifests)
	if len(survivors) != len(manifests) {
		t.Fatalf("expected all %d manifests to survive an unreasonable predicate, got %d", len(manifests), len(survivors))
	}
}

func TestPrunePartitions_NoPredicateKeepsEverything(t *testing.T) {
	manifests := []*catalogpb.Manifest{
		manifestWithPartition(t, "a.atlas", map[string]any{"year": 2020}),
		manifestWithPartition(t, "b.atlas", map[string]any{"year": 2024}),
	}
	survivors := PrunePartitions(nil, manifests)
	if len(survivors) != len(manifests) {
		t.Fatalf("expected all manifests to survive a nil predicate, got %d", len(survivors))
	}
}

func TestPrunePartitions_ColumnStatsRangeCheck(t *testing.T) {
	manifestFor := func(min, max int) *catalogpb.Manifest {
		stats, _ := json.Marshal(map[string]columnStats{
			"age": {Min: float64(min), Max: float64(max)},
		})
		return &catalogpb.Manifest{
			FilePath:            "part.atlas",
			PartitionValuesJson: "{}",
			ColumnStatsJson:     string(stats),
		}
	}

	// WHERE age > 50: a manifest whose max age is 40 can't possibly match.
	predicate := exprJSON(t, map[string]any{"Binary": map[string]any{
		"left":  map[string]any{"Column": "age"},
		"op":    "Gt",
		"right": map[string]any{"Literal": map[string]any{"Int": 50}},
	}})

	tooYoung := manifestFor(10, 40)
	overlapping := manifestFor(30, 60)

	survivors := PrunePartitions(predicate, []*catalogpb.Manifest{tooYoung, overlapping})
	if len(survivors) != 1 || survivors[0] != overlapping {
		t.Fatalf("expected only the overlapping-range manifest to survive, got %d manifests", len(survivors))
	}
}

func TestPrunePartitions_AndCombination(t *testing.T) {
	manifests := []*catalogpb.Manifest{
		manifestWithPartition(t, "match.atlas", map[string]any{"year": 2024, "region": "east"}),
		manifestWithPartition(t, "wrong-year.atlas", map[string]any{"year": 2020, "region": "east"}),
		manifestWithPartition(t, "wrong-region.atlas", map[string]any{"year": 2024, "region": "west"}),
	}
	predicate := exprJSON(t, map[string]any{"Binary": map[string]any{
		"left":  eqExprMap("year", 2024),
		"op":    "And",
		"right": eqExprMapStr("region", "east"),
	}})

	survivors := PrunePartitions(predicate, manifests)
	if len(survivors) != 1 || survivors[0].GetFilePath() != "match.atlas" {
		t.Fatalf("expected only match.atlas to survive AND(year=2024, region=east), got %v", survivors)
	}
}

func TestPrunePartitions_FormatAgnostic(t *testing.T) {
	// Same partition_values/column_stats, different `format` — pruning must
	// reach the identical keep/prune decision regardless of file format,
	// since PrunePartitions only ever reasons about partition_values/
	// column_stats/file_path, never format.
	atlasManifest := manifestWithPartition(t, "year=2024/part-0.atlas", map[string]any{"year": 2024})
	atlasManifest.Format = "atlas"
	parquetManifest := manifestWithPartition(t, "year=2024/part-0.parquet", map[string]any{"year": 2024})
	parquetManifest.Format = "parquet"
	prunedManifest := manifestWithPartition(t, "year=2020/part-0.parquet", map[string]any{"year": 2020})
	prunedManifest.Format = "parquet"

	predicate := eqPredicate(t, "year", 2024)
	survivors := PrunePartitions(predicate, []*catalogpb.Manifest{atlasManifest, parquetManifest, prunedManifest})

	if len(survivors) != 2 {
		t.Fatalf("expected both year=2024 manifests (atlas and parquet) to survive, got %d: %v", len(survivors), survivors)
	}
	survivingPaths := map[string]bool{}
	for _, m := range survivors {
		survivingPaths[m.GetFilePath()] = true
	}
	if !survivingPaths["year=2024/part-0.atlas"] || !survivingPaths["year=2024/part-0.parquet"] {
		t.Fatalf("expected both formats' year=2024 manifests to survive, got %v", survivors)
	}
}

func eqExprMap(column string, intLit int) map[string]any {
	return map[string]any{"Binary": map[string]any{
		"left":  map[string]any{"Column": column},
		"op":    "Eq",
		"right": map[string]any{"Literal": map[string]any{"Int": intLit}},
	}}
}

func eqExprMapStr(column string, strLit string) map[string]any {
	return map[string]any{"Binary": map[string]any{
		"left":  map[string]any{"Column": column},
		"op":    "Eq",
		"right": map[string]any{"Literal": map[string]any{"Str": strLit}},
	}}
}
