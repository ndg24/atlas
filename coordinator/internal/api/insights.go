// Phase 7 (AI Analyst): POST /datasets/{name}/summary and
// POST /datasets/{name}/insights (docs/atlas-implementation-spec.md Phase 7,
// tasks 2 and 4). Both run a fixed set of queries through the unmodified
// engine, dispatch the resulting batches to a worker's Analyze RPC (the one
// place Arrow bytes get interpreted for this path — see
// engine/crates/atlas-worker/src/service.rs's run_analyze), and only then,
// for /insights, hand the already-computed findings to the AI service for
// narration. No number in the response is ever invented outside the engine.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	aipb "atlas/coordinator/internal/aipb"
	catalogpb "atlas/coordinator/internal/catalogpb"
	workerpb "atlas/coordinator/internal/workerpb"
)

// schemaField mirrors one entry of the dataset's Arrow schema JSON
// (arrow-schema's own Field serde shape — see docs/atlas-implementation-spec.md
// §1.1 and ai-service/atlas_ai/plan/schema.py's identical mirror).
type schemaField struct {
	Name     string `json:"name"`
	DataType string `json:"data_type"`
}

func parseSchemaFields(schemaJSON string) ([]schemaField, error) {
	var parsed struct {
		Fields []schemaField `json:"fields"`
	}
	if err := json.Unmarshal([]byte(schemaJSON), &parsed); err != nil {
		return nil, fmt.Errorf("parsing schema_json: %w", err)
	}
	return parsed.Fields, nil
}

// rawColumnStat mirrors one entry of a manifest's column_stats_json map
// (proto/catalog.proto's Manifest.column_stats_json: "{column: {min, max,
// null_count}}", written by atlas-cli's column_stats_by_name) — min/max are
// base64-encoded bytes in atlas_format::stats's own little-endian encoding,
// passed straight through to atlas_insights without ever being decoded here
// (docs/atlas-implementation-spec.md's own rationale: numeric byte order
// doesn't sort like numbers, so that decode has to happen in Rust, next to
// the encoder it matches).
type rawColumnStat struct {
	Min                   *string `json:"min"`
	Max                   *string `json:"max"`
	DistinctCountEstimate uint64  `json:"distinct_count_estimate"`
}

// mergedColumnStat mirrors atlas_insights::MergedColumnStats — one entry per
// (manifest, column) pair, unreduced; the "summary" Analyze call does the
// cross-manifest min/max/distinct-count reduction in Rust.
type mergedColumnStat struct {
	Name                  string  `json:"name"`
	DataType              string  `json:"data_type"`
	DistinctCountEstimate uint64  `json:"distinct_count_estimate"`
	MinBase64             *string `json:"min_base64,omitempty"`
	MaxBase64             *string `json:"max_base64,omitempty"`
}

// buildMergedColumnStats flattens every manifest's column_stats_json into
// one (manifest, column) list, tagging each entry with its column's
// data_type from the dataset schema — the catalog's own per-manifest stats
// carry no type information, which is why this can't just forward the raw
// JSON as-is.
func buildMergedColumnStats(fields []schemaField, manifests []*catalogpb.Manifest) []mergedColumnStat {
	dataTypeByName := make(map[string]string, len(fields))
	for _, f := range fields {
		dataTypeByName[f.Name] = f.DataType
	}

	var out []mergedColumnStat
	for _, m := range manifests {
		var perColumn map[string]rawColumnStat
		if s := m.GetColumnStatsJson(); s != "" {
			_ = json.Unmarshal([]byte(s), &perColumn)
		}
		for name, stat := range perColumn {
			dataType, ok := dataTypeByName[name]
			if !ok {
				continue
			}
			out = append(out, mergedColumnStat{
				Name:                  name,
				DataType:              dataType,
				DistinctCountEstimate: stat.DistinctCountEstimate,
				MinBase64:             stat.Min,
				MaxBase64:             stat.Max,
			})
		}
	}
	return out
}

// countQuerySQL builds `SELECT COUNT(*) AS total_rows, COUNT("col") AS
// "col_non_null", ... FROM t` — the one aggregate query
// atlas_insights::build_summary expects (its own doc comment names this
// exact shape). Bare Aggregate plans always combine to a single final batch
// (engine/crates/atlas-worker/src/split.rs's split_for_distribution), so
// RunCompiled is guaranteed to return exactly one ArrowIPCBatches entry here.
func countQuerySQL(fields []schemaField) string {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	sort.Strings(names)

	parts := make([]string, 0, len(names)+1)
	parts = append(parts, "COUNT(*) AS total_rows")
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("COUNT(%q) AS %q", name, name+"_non_null"))
	}
	return "SELECT " + strings.Join(parts, ", ") + " FROM t"
}

// sampleQuerySQL is quality's bounded row sample for duplicate-row detection
// (atlas_insights::detect_data_quality_issues needs actual rows, not just
// stats). LIMIT makes this a wrapped (non-Aggregate) plan, which
// split_for_distribution still combines to a single final batch — so this
// too is guaranteed exactly one ArrowIPCBatches entry, never a per-partition
// list the coordinator would have to merge itself.
const sampleQuerySQL = "SELECT * FROM t LIMIT 10000"

// buildSummary runs the fixed set of aggregate queries through the
// unmodified engine (docs/atlas-implementation-spec.md Phase 7, task 2),
// merges them with catalog stats via the worker's Analyze RPC, and returns
// the resulting DatasetSummary JSON verbatim (summaryJSON) plus its decoded
// `columns` array on its own (columnsJSON) — handleInsights reuses columnsJSON
// as the "quality" Analyze call's input without re-parsing summaryJSON.
func (s *Server) buildSummary(ctx context.Context, dq *datasetQuery) (summaryJSON string, columnsJSON string, err error) {
	fields, err := parseSchemaFields(dq.dataset.GetSchemaJson())
	if err != nil {
		return "", "", err
	}
	if len(fields) == 0 {
		return "", "", fmt.Errorf("dataset %q has no columns in its schema", dq.dataset.GetName())
	}

	compiled, err := s.coordinator.Compile(ctx, countQuerySQL(fields), dq.dataset.GetSchemaJson())
	if err != nil {
		return "", "", fmt.Errorf("compiling summary count query: %w", err)
	}
	countResult, err := s.coordinator.RunCompiled(ctx, compiled, toSchedulerManifests(dq.manifests))
	if err != nil {
		return "", "", fmt.Errorf("running summary count query: %w", err)
	}
	if len(countResult.ArrowIPCBatches) != 1 {
		return "", "", fmt.Errorf("summary count query: expected exactly 1 combined batch, got %d", len(countResult.ArrowIPCBatches))
	}

	statsJSON, err := json.Marshal(buildMergedColumnStats(fields, dq.manifests))
	if err != nil {
		return "", "", fmt.Errorf("serializing manifest column stats: %w", err)
	}

	resp, err := s.coordinator.Analyze(ctx, &workerpb.AnalyzeRequest{
		ArrowIpc:        countResult.ArrowIPCBatches[0],
		Kind:            "summary",
		ColumnStatsJson: string(statsJSON),
	})
	if err != nil {
		return "", "", fmt.Errorf("analyzing dataset summary: %w", err)
	}

	var parsed struct {
		Columns json.RawMessage `json:"columns"`
	}
	if err := json.Unmarshal([]byte(resp.GetFindingsJson()), &parsed); err != nil {
		return "", "", fmt.Errorf("parsing summary findings: %w", err)
	}
	return resp.GetFindingsJson(), string(parsed.Columns), nil
}

// handleSummary is pure engine — no LLM call — matching the spec's Phase 7
// task 2 boundary exactly.
func (s *Server) handleSummary(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("dataset name is required"))
		return
	}

	ctx := r.Context()
	dq, err := s.lookupDataset(ctx, name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	summaryJSON, _, err := s.buildSummary(ctx, dq)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(summaryJSON))
}

type insightsResponse struct {
	Summary            json.RawMessage `json:"summary"`
	QualityFindings    json.RawMessage `json:"quality_findings"`
	Narrative          string          `json:"narrative"`
	SuggestedQuestions []string        `json:"suggested_questions"`
}

// handleInsights orchestrates Phase 7's AI Analyst pipeline: engine summary
// (buildSummary) -> engine quality checks (Analyze kind="quality") ->
// AIService.NarrateFindings -> AIService.SuggestQuestions
// (docs/atlas-implementation-spec.md Phase 7, task 4). Outlier and trend
// detection (also implemented in atlas-insights) aren't wired in here yet —
// both need a group/value/time column choice this fixed, dataset-agnostic
// pipeline has no principled way to make, unlike null-rate/zero-variance/
// duplicate checks, which apply uniformly to every column.
func (s *Server) handleInsights(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("dataset name is required"))
		return
	}

	ctx := r.Context()
	dq, err := s.lookupDataset(ctx, name)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	summaryJSON, columnsJSON, err := s.buildSummary(ctx, dq)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	sampleCompiled, err := s.coordinator.Compile(ctx, sampleQuerySQL, dq.dataset.GetSchemaJson())
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("compiling sample query: %w", err))
		return
	}
	sampleResult, err := s.coordinator.RunCompiled(ctx, sampleCompiled, toSchedulerManifests(dq.manifests))
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("running sample query: %w", err))
		return
	}
	var sampleIPC []byte
	if len(sampleResult.ArrowIPCBatches) > 0 {
		sampleIPC = sampleResult.ArrowIPCBatches[0]
	}

	qualityResp, err := s.coordinator.Analyze(ctx, &workerpb.AnalyzeRequest{
		Kind:            "quality",
		ColumnStatsJson: columnsJSON,
		SampleArrowIpc:  sampleIPC,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("analyzing data quality: %w", err))
		return
	}

	findingsForNarration, err := json.Marshal(map[string]json.RawMessage{
		"summary":          json.RawMessage(summaryJSON),
		"quality_findings": json.RawMessage(qualityResp.GetFindingsJson()),
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	narrateResp, err := s.ai.NarrateFindings(ctx, &aipb.NarrateFindingsRequest{FindingsJson: string(findingsForNarration)})
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("narrating findings: %w", err))
		return
	}

	suggestResp, err := s.ai.SuggestQuestions(ctx, &aipb.SuggestQuestionsRequest{
		SchemaJson:  dq.dataset.GetSchemaJson(),
		SummaryJson: summaryJSON,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("suggesting questions: %w", err))
		return
	}

	writeJSON(w, http.StatusOK, insightsResponse{
		Summary:            json.RawMessage(summaryJSON),
		QualityFindings:    json.RawMessage(qualityResp.GetFindingsJson()),
		Narrative:          narrateResp.GetNarrative(),
		SuggestedQuestions: suggestResp.GetQuestions(),
	})
}
