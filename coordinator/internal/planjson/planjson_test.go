package planjson

import (
	"encoding/json"
	"testing"
)

// Fixtures below match the real serde JSON shape produced by
// atlas_query::LogicalPlan (verified against actual `serde_json::to_string`
// output during development) — see plan.rs for the Rust type definitions.
// Built via json.Marshal of nested maps rather than hand-written literals,
// so brace-matching mistakes are impossible.

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("building fixture: %v", err)
	}
	return string(b)
}

func projectFilterScan(t *testing.T) string {
	scan := map[string]any{"Scan": map[string]any{
		"dataset": "t", "columns": []string{"age", "diagnosis"}, "snapshot_id": "",
	}}
	predicate := map[string]any{"Binary": map[string]any{
		"left":  map[string]any{"Column": "age"},
		"op":    "Gt",
		"right": map[string]any{"Literal": map[string]any{"Int": 50}},
	}}
	filter := map[string]any{"Filter": map[string]any{"input": scan, "predicate": predicate}}
	project := map[string]any{"Project": map[string]any{
		"input":   filter,
		"exprs":   []any{map[string]any{"Column": "diagnosis"}},
		"aliases": []string{"diagnosis"},
	}}
	return mustJSON(t, project)
}

func bareScan(t *testing.T) string {
	return mustJSON(t, map[string]any{"Scan": map[string]any{
		"dataset": "t", "columns": []string{}, "snapshot_id": "",
	}})
}

func limitSortAggregateFilterScan(t *testing.T) string {
	scan := map[string]any{"Scan": map[string]any{
		"dataset": "t", "columns": []string{"diagnosis"}, "snapshot_id": "",
	}}
	predicate := map[string]any{"Binary": map[string]any{
		"left":  map[string]any{"Column": "year"},
		"op":    "Eq",
		"right": map[string]any{"Literal": map[string]any{"Int": 2024}},
	}}
	filter := map[string]any{"Filter": map[string]any{"input": scan, "predicate": predicate}}
	aggregate := map[string]any{"Aggregate": map[string]any{
		"input":      filter,
		"group_by":   []any{map[string]any{"Column": "diagnosis"}},
		"aggregates": []any{map[string]any{"func": "Count", "arg": nil, "alias": "n"}},
	}}
	sort := map[string]any{"Sort": map[string]any{
		"input": aggregate,
		"keys":  []any{map[string]any{"expr": map[string]any{"Column": "n"}, "descending": true}},
	}}
	limit := map[string]any{"Limit": map[string]any{"input": sort, "n": 5}}
	return mustJSON(t, limit)
}

func TestExtractScanColumns(t *testing.T) {
	cols, err := ExtractScanColumns(projectFilterScan(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) != 2 || cols[0] != "age" || cols[1] != "diagnosis" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestExtractScanColumnsEmptyMeansAll(t *testing.T) {
	cols, err := ExtractScanColumns(bareScan(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) != 0 {
		t.Fatalf("expected empty columns, got %v", cols)
	}
}

func TestExtractScanColumnsThroughMultipleWrappers(t *testing.T) {
	cols, err := ExtractScanColumns(limitSortAggregateFilterScan(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cols) != 1 || cols[0] != "diagnosis" {
		t.Fatalf("unexpected columns: %v", cols)
	}
}

func TestExtractFilterPredicateFound(t *testing.T) {
	predicate, ok, err := ExtractFilterPredicate(projectFilterScan(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected a predicate to be found")
	}
	if got := string(predicate); got == "" {
		t.Fatal("expected non-empty predicate JSON")
	}
}

func TestExtractFilterPredicateNoneWhenNoFilter(t *testing.T) {
	_, ok, err := ExtractFilterPredicate(bareScan(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected no predicate to be found")
	}
}

func TestExtractFilterPredicateThroughMultipleWrappers(t *testing.T) {
	predicate, ok, err := ExtractFilterPredicate(limitSortAggregateFilterScan(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected a predicate to be found")
	}
	if got := string(predicate); got == "" {
		t.Fatal("expected non-empty predicate JSON")
	}
}

func TestDecodeRejectsMultiKeyObject(t *testing.T) {
	if _, err := ExtractScanColumns(`{"Scan":{},"Filter":{}}`); err == nil {
		t.Fatal("expected an error for a malformed multi-key plan node")
	}
}
