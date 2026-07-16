package scheduler

// Partition pruning (docs/atlas-implementation-spec.md Phase 4): drops
// manifests that cannot possibly satisfy a query's filter predicate, using
// each manifest's exact partition_values (from Hive-style partitioning) and
// per-column min/max stats (from the .atlas footer, see proto/format.proto).
//
// This is the one place the coordinator inspects predicate structure at all
// — narrowly, via `planjson.ExtractFilterPredicate`'s raw Expr JSON — rather
// than general plan-awareness. Every check here is conservative: anything
// it can't confidently reason about (an unknown column, an unsupported
// operator/shape, incomparable types) returns "could match" so a manifest is
// never wrongly dropped.

import (
	"encoding/json"

	catalogpb "atlas/coordinator/internal/catalogpb"
)

// PrunePartitions returns the subset of manifests that could possibly
// satisfy predicate. A nil/empty predicate (no Filter in the query) means
// every manifest survives.
func PrunePartitions(predicate json.RawMessage, manifests []*catalogpb.Manifest) []*catalogpb.Manifest {
	if len(predicate) == 0 {
		return manifests
	}
	survivors := make([]*catalogpb.Manifest, 0, len(manifests))
	for _, m := range manifests {
		partitionValues, stats := decodeManifestStats(m)
		if couldMatch(predicate, partitionValues, stats) {
			survivors = append(survivors, m)
		}
	}
	return survivors
}

type columnStats struct {
	Min       any `json:"min"`
	Max       any `json:"max"`
	NullCount any `json:"null_count"`
}

func decodeManifestStats(m *catalogpb.Manifest) (partitionValues map[string]any, stats map[string]columnStats) {
	if s := m.GetPartitionValuesJson(); s != "" {
		_ = json.Unmarshal([]byte(s), &partitionValues)
	}
	if s := m.GetColumnStatsJson(); s != "" {
		_ = json.Unmarshal([]byte(s), &stats)
	}
	return partitionValues, stats
}

// couldMatch conservatively evaluates whether predicate (a serde-JSON
// atlas_query::Expr) could be true for some row in a manifest with the given
// exact partition values and per-column [min, max] stats.
func couldMatch(predicate json.RawMessage, partitionValues map[string]any, stats map[string]columnStats) bool {
	var node map[string]json.RawMessage
	if json.Unmarshal(predicate, &node) != nil {
		return true
	}
	binBody, ok := node["Binary"]
	if !ok {
		// A bare Column/Literal isn't a valid boolean predicate on its own;
		// be conservative rather than guess.
		return true
	}
	var bin struct {
		Left  json.RawMessage `json:"left"`
		Op    string          `json:"op"`
		Right json.RawMessage `json:"right"`
	}
	if json.Unmarshal(binBody, &bin) != nil {
		return true
	}

	switch bin.Op {
	case "And":
		return couldMatch(bin.Left, partitionValues, stats) && couldMatch(bin.Right, partitionValues, stats)
	case "Or":
		return couldMatch(bin.Left, partitionValues, stats) || couldMatch(bin.Right, partitionValues, stats)
	case "Eq", "NotEq", "Lt", "LtEq", "Gt", "GtEq":
		return couldMatchComparison(bin.Left, bin.Op, bin.Right, partitionValues, stats)
	default:
		// Add/Sub/Mul/Div etc: not a boolean predicate we can prune on.
		return true
	}
}

func couldMatchComparison(left json.RawMessage, op string, right json.RawMessage, partitionValues map[string]any, stats map[string]columnStats) bool {
	col, lit, effectiveOp, ok := columnAndLiteral(left, op, right)
	if !ok {
		return true
	}
	if pv, present := partitionValues[col]; present {
		return evalExact(pv, effectiveOp, lit)
	}
	if st, present := stats[col]; present {
		return evalRange(st, effectiveOp, lit)
	}
	return true
}

type literalValue struct {
	kind string // "num" or "str" (bool literals are folded into "num" as 0/1)
	num  float64
	str  string
}

func asColumn(e json.RawMessage) (string, bool) {
	var m map[string]json.RawMessage
	if json.Unmarshal(e, &m) != nil {
		return "", false
	}
	body, ok := m["Column"]
	if !ok {
		return "", false
	}
	var name string
	if json.Unmarshal(body, &name) != nil {
		return "", false
	}
	return name, true
}

func asLiteral(e json.RawMessage) (literalValue, bool) {
	var m map[string]json.RawMessage
	if json.Unmarshal(e, &m) != nil {
		return literalValue{}, false
	}
	body, ok := m["Literal"]
	if !ok {
		return literalValue{}, false
	}
	var lit map[string]json.RawMessage
	if json.Unmarshal(body, &lit) != nil {
		return literalValue{}, false
	}
	if v, ok := lit["Int"]; ok {
		var i int64
		if json.Unmarshal(v, &i) == nil {
			return literalValue{kind: "num", num: float64(i)}, true
		}
	}
	if v, ok := lit["Float"]; ok {
		var f float64
		if json.Unmarshal(v, &f) == nil {
			return literalValue{kind: "num", num: f}, true
		}
	}
	if v, ok := lit["Str"]; ok {
		var s string
		if json.Unmarshal(v, &s) == nil {
			return literalValue{kind: "str", str: s}, true
		}
	}
	if v, ok := lit["Bool"]; ok {
		var b bool
		if json.Unmarshal(v, &b) == nil {
			n := 0.0
			if b {
				n = 1.0
			}
			return literalValue{kind: "num", num: n}, true
		}
	}
	return literalValue{}, false
}

// columnAndLiteral normalizes `col op lit` or `lit op col` into (col,
// effectiveOp, lit) oriented as "col <effectiveOp> lit" — flipping
// Lt/LtEq/Gt/GtEq when the literal appeared on the left.
func columnAndLiteral(left json.RawMessage, op string, right json.RawMessage) (string, literalValue, string, bool) {
	if col, ok := asColumn(left); ok {
		if lit, ok := asLiteral(right); ok {
			return col, lit, op, true
		}
	}
	if col, ok := asColumn(right); ok {
		if lit, ok := asLiteral(left); ok {
			return col, lit, flipOp(op), true
		}
	}
	return "", literalValue{}, "", false
}

func flipOp(op string) string {
	switch op {
	case "Lt":
		return "Gt"
	case "LtEq":
		return "GtEq"
	case "Gt":
		return "Lt"
	case "GtEq":
		return "LtEq"
	default:
		return op // Eq/NotEq are symmetric
	}
}

func evalExact(pv any, op string, lit literalValue) bool {
	cmp, ok := compareValues(pv, lit)
	if !ok {
		return true
	}
	return applyOp(op, cmp)
}

// evalRange conservatively checks whether some value in [min, max] could
// satisfy `col op lit`.
func evalRange(st columnStats, op string, lit literalValue) bool {
	minCmp, minOk := compareValues(st.Min, lit)
	maxCmp, maxOk := compareValues(st.Max, lit)
	if !minOk || !maxOk {
		return true
	}
	switch op {
	case "Eq":
		return minCmp <= 0 && maxCmp >= 0
	case "NotEq":
		// Only unsatisfiable if min == max == lit; too narrow a win to
		// bother pruning on — stay conservative.
		return true
	case "Lt":
		return minCmp < 0
	case "LtEq":
		return minCmp <= 0
	case "Gt":
		return maxCmp > 0
	case "GtEq":
		return maxCmp >= 0
	default:
		return true
	}
}

// compareValues returns -1/0/1 for value compared to lit, and false if the
// two aren't comparable (type mismatch or unrecognized shape).
func compareValues(value any, lit literalValue) (int, bool) {
	switch v := value.(type) {
	case float64:
		if lit.kind == "num" {
			return compareFloat(v, lit.num), true
		}
	case string:
		if lit.kind == "str" {
			return compareStr(v, lit.str), true
		}
	case bool:
		if lit.kind == "num" {
			n := 0.0
			if v {
				n = 1.0
			}
			return compareFloat(n, lit.num), true
		}
	}
	return 0, false
}

func compareFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareStr(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func applyOp(op string, cmp int) bool {
	switch op {
	case "Eq":
		return cmp == 0
	case "NotEq":
		return cmp != 0
	case "Lt":
		return cmp < 0
	case "LtEq":
		return cmp <= 0
	case "Gt":
		return cmp > 0
	case "GtEq":
		return cmp >= 0
	default:
		return true
	}
}
