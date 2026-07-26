package api

// Unit tests for the heuristic column pickers behind handleInsights'
// outlier/trend wiring (docs/atlas-implementation-spec.md Phase 7) — these
// are unexported, so this file lives in package api rather than api_test.

import "testing"

func strp(s string) *string { return &s }

func TestPickGroupColumn(t *testing.T) {
	t.Run("prefers the lowest-cardinality qualifying string column", func(t *testing.T) {
		columns := []columnStat{
			{Name: "notes", DataType: "Utf8", NullRate: 0.1, DistinctCountEstimate: 9000},
			{Name: "hospital", DataType: "Utf8", NullRate: 0.1, DistinctCountEstimate: 5},
			{Name: "diagnosis", DataType: "Utf8", NullRate: 0.1, DistinctCountEstimate: 12},
			{Name: "age", DataType: "Int64", NullRate: 0.1, DistinctCountEstimate: 3},
		}
		got, ok := pickGroupColumn(columns)
		if !ok || got != "hospital" {
			t.Fatalf("expected hospital, got %q (ok=%v)", got, ok)
		}
	})

	t.Run("skips mostly-null and single-value columns", func(t *testing.T) {
		columns := []columnStat{
			{Name: "sparse", DataType: "Utf8", NullRate: 0.9, DistinctCountEstimate: 3},
			{Name: "constant", DataType: "Utf8", NullRate: 0.0, DistinctCountEstimate: 1},
			{Name: "unbounded", DataType: "Utf8", NullRate: 0.0, DistinctCountEstimate: 5000},
		}
		if _, ok := pickGroupColumn(columns); ok {
			t.Fatal("expected no qualifying group column")
		}
	})

	t.Run("no string columns means no pick", func(t *testing.T) {
		columns := []columnStat{
			{Name: "age", DataType: "Int64", NullRate: 0.0, DistinctCountEstimate: 3},
		}
		if _, ok := pickGroupColumn(columns); ok {
			t.Fatal("expected no group column among purely numeric columns")
		}
	})
}

func TestPickValueColumn(t *testing.T) {
	t.Run("skips zero-variance and all-null numeric columns", func(t *testing.T) {
		columns := []columnStat{
			{Name: "flag", DataType: "Int64", NullRate: 0.0, Min: strp("1"), Max: strp("1")},
			{Name: "empty", DataType: "Float64", NullRate: 1.0, Min: strp("0"), Max: strp("100")},
			{Name: "cost", DataType: "Float64", NullRate: 0.1, Min: strp("5"), Max: strp("500")},
		}
		got, ok := pickValueColumn(columns)
		if !ok || got != "cost" {
			t.Fatalf("expected cost, got %q (ok=%v)", got, ok)
		}
	})

	t.Run("ignores non-numeric columns", func(t *testing.T) {
		columns := []columnStat{
			{Name: "hospital", DataType: "Utf8", NullRate: 0.0, Min: strp("A"), Max: strp("Z")},
		}
		if _, ok := pickValueColumn(columns); ok {
			t.Fatal("expected no value column among purely string columns")
		}
	})
}

func TestPickTimeColumn(t *testing.T) {
	t.Run("picks a Date32 column, ignoring mostly-null ones", func(t *testing.T) {
		columns := []columnStat{
			{Name: "sparse_date", DataType: "Date32", NullRate: 0.9},
			{Name: "admit_date", DataType: "Date32", NullRate: 0.1},
			{Name: "age", DataType: "Int64", NullRate: 0.0},
		}
		got, ok := pickTimeColumn(columns)
		if !ok || got != "admit_date" {
			t.Fatalf("expected admit_date, got %q (ok=%v)", got, ok)
		}
	})

	t.Run("no date columns means no pick", func(t *testing.T) {
		columns := []columnStat{
			{Name: "age", DataType: "Int64", NullRate: 0.0},
		}
		if _, ok := pickTimeColumn(columns); ok {
			t.Fatal("expected no time column without a Date32 field")
		}
	})
}

func TestOutlierAndTrendQuerySQL(t *testing.T) {
	if got, want := outlierQuerySQL("hospital", "cost"), `SELECT "hospital", AVG("cost") AS "value" FROM t GROUP BY "hospital"`; got != want {
		t.Fatalf("outlierQuerySQL:\n got:  %s\n want: %s", got, want)
	}
	if got, want := trendQuerySQL("admit_date", "cost"), `SELECT "admit_date", AVG("cost") AS "value" FROM t GROUP BY "admit_date" ORDER BY "admit_date"`; got != want {
		t.Fatalf("trendQuerySQL:\n got:  %s\n want: %s", got, want)
	}
}
