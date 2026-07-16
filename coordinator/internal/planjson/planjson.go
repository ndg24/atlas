// Package planjson decodes just enough of a serde-JSON-serialized
// atlas_query::LogicalPlan (engine/crates/atlas-query/src/plan.rs) for the
// coordinator's Phase 4 needs: the leaf Scan's pruned column list, and the
// top-level Filter's predicate (for partition pruning against manifest
// stats). This is deliberately narrow — two fields, not general
// plan-awareness — the coordinator otherwise never inspects plan structure
// (see proto/worker.proto's package doc comment).
//
// LogicalPlan's serde representation is externally tagged: every node is a
// single-key JSON object, e.g. `{"Scan": {...}}`, `{"Filter": {"input":
// <LogicalPlan>, "predicate": <Expr>}}`. Every non-Scan node has an "input"
// field wrapping the next node down, so finding a target node is a matter of
// repeatedly unwrapping "input" until the right key appears.
package planjson

import (
	"encoding/json"
	"fmt"
)

// wrapperKeys are the node kinds whose JSON shape is {"input": <LogicalPlan>, ...}
// — every node except Scan.
var wrapperKeys = []string{"Filter", "Project", "Aggregate", "Sort", "Limit"}

type rawNode map[string]json.RawMessage

func decode(planJSON string) (rawNode, error) {
	var node rawNode
	if err := json.Unmarshal([]byte(planJSON), &node); err != nil {
		return nil, fmt.Errorf("decoding plan JSON: %w", err)
	}
	if len(node) != 1 {
		return nil, fmt.Errorf("expected exactly one variant key in plan JSON, got %d", len(node))
	}
	return node, nil
}

// descend walks one level down a wrapper node's "input" field.
func descend(node rawNode) (rawNode, error) {
	for _, key := range wrapperKeys {
		body, ok := node[key]
		if !ok {
			continue
		}
		var wrapper struct {
			Input json.RawMessage `json:"input"`
		}
		if err := json.Unmarshal(body, &wrapper); err != nil {
			return nil, fmt.Errorf("decoding %s node: %w", key, err)
		}
		var inner rawNode
		if err := json.Unmarshal(wrapper.Input, &inner); err != nil {
			return nil, fmt.Errorf("decoding %s input: %w", key, err)
		}
		return inner, nil
	}
	return nil, fmt.Errorf("unrecognized plan node keys: %v", keysOf(node))
}

func keysOf(node rawNode) []string {
	keys := make([]string, 0, len(node))
	for k := range node {
		keys = append(keys, k)
	}
	return keys
}

// ExtractScanColumns finds the plan's (single) Scan node and returns its
// `columns` list. An empty list means "all columns" — the same convention
// the Rust side uses.
func ExtractScanColumns(planJSON string) ([]string, error) {
	node, err := decode(planJSON)
	if err != nil {
		return nil, err
	}
	for {
		if scanBody, ok := node["Scan"]; ok {
			var scan struct {
				Columns []string `json:"columns"`
			}
			if err := json.Unmarshal(scanBody, &scan); err != nil {
				return nil, fmt.Errorf("decoding Scan node: %w", err)
			}
			return scan.Columns, nil
		}
		node, err = descend(node)
		if err != nil {
			return nil, err
		}
	}
}

// ExtractFilterPredicate finds the plan's Filter node (there is at most one,
// always sitting directly over Scan per atlas_query's SQL builder) and
// returns its predicate as raw JSON. ok is false if the plan has no Filter
// at all — nothing to prune partitions on.
func ExtractFilterPredicate(planJSON string) (predicate json.RawMessage, ok bool, err error) {
	node, err := decode(planJSON)
	if err != nil {
		return nil, false, err
	}
	for {
		if filterBody, present := node["Filter"]; present {
			var filter struct {
				Predicate json.RawMessage `json:"predicate"`
			}
			if err := json.Unmarshal(filterBody, &filter); err != nil {
				return nil, false, fmt.Errorf("decoding Filter node: %w", err)
			}
			return filter.Predicate, true, nil
		}
		if _, isScan := node["Scan"]; isScan {
			return nil, false, nil
		}
		node, err = descend(node)
		if err != nil {
			return nil, false, err
		}
	}
}
