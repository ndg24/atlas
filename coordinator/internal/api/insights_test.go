package api_test

// Exercises POST /datasets/{name}/summary and POST /datasets/{name}/insights
// end to end against fake in-process Worker and AI gRPC servers plus a fake
// CatalogServiceClient — no Docker/Postgres/real LLM needed, mirroring
// server_test.go's approach for handleExplain. The fake worker's Compile/
// ExecuteTask responses are opaque to the coordinator (it never decodes
// Arrow), so only Analyze's canned findings_json needs to look like real
// atlas_insights output; everything upstream of it is plumbing.

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc"

	aipb "atlas/coordinator/internal/aipb"
	"atlas/coordinator/internal/api"
	catalogpb "atlas/coordinator/internal/catalogpb"
	"atlas/coordinator/internal/scheduler"
	pb "atlas/coordinator/internal/workerpb"
)

// --- fake worker: canned Compile (always needs combine), ExecuteTask, and
// Analyze responses, keyed off AnalyzeRequest.Kind. ---

type fakeInsightsWorker struct {
	pb.UnimplementedWorkerServiceServer
	analyzeCalls []*pb.AnalyzeRequest
	// summaryFindingsJSON overrides the canned "summary" Analyze response
	// below when non-empty — tests that need pickGroupColumn/pickValueColumn/
	// pickTimeColumn to find candidates set this to a summary carrying the
	// columns they want picked.
	summaryFindingsJSON string
}

func (w *fakeInsightsWorker) Compile(ctx context.Context, req *pb.CompileRequest) (*pb.CompileResponse, error) {
	return &pb.CompileResponse{
		LogicalPlanJson:   "{}",
		OptimizedPlanJson: "{}",
		PartialPlanJson:   "{}",
		CombinePlanJson:   "{}",
		NeedsCombine:      true,
	}, nil
}

func (w *fakeInsightsWorker) ExecuteTask(req *pb.TaskRequest, stream grpc.ServerStreamingServer[pb.ResultBatch]) error {
	return stream.Send(&pb.ResultBatch{ArrowIpc: []byte("fake-batch")})
}

func (w *fakeInsightsWorker) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	return &pb.HeartbeatResponse{Alive: true}, nil
}

func (w *fakeInsightsWorker) Analyze(ctx context.Context, req *pb.AnalyzeRequest) (*pb.AnalyzeResponse, error) {
	w.analyzeCalls = append(w.analyzeCalls, req)
	switch req.GetKind() {
	case "summary":
		if w.summaryFindingsJSON != "" {
			return &pb.AnalyzeResponse{FindingsJson: w.summaryFindingsJSON}, nil
		}
		return &pb.AnalyzeResponse{FindingsJson: `{"row_count":5,"columns":[{"name":"age","data_type":"Int64","null_rate":0.2,"distinct_count_estimate":3,"min":"1","max":"90"}]}`}, nil
	case "quality":
		return &pb.AnalyzeResponse{FindingsJson: `[{"kind":"HighNullRate","column":"age","null_rate":0.2}]`}, nil
	case "outlier":
		return &pb.AnalyzeResponse{FindingsJson: `[{"group":"General Hospital","value":0.42,"group_mean":0.1,"group_stddev":0.05,"z_score":6.4,"group_col":"hospital","value_col":"value"}]`}, nil
	case "trend":
		return &pb.AnalyzeResponse{FindingsJson: `{"time_col":"admit_date","value_col":"value","slope":1.5,"direction":"increasing","r_squared":0.9}`}, nil
	default:
		return &pb.AnalyzeResponse{Error: "unsupported analyze kind in test fake: " + req.GetKind()}, nil
	}
}

func startFakeInsightsWorker(t *testing.T) (string, *fakeInsightsWorker) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for fake worker: %v", err)
	}
	worker := &fakeInsightsWorker{}
	srv := grpc.NewServer()
	pb.RegisterWorkerServiceServer(srv, worker)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), worker
}

// --- fake AI service client: a plain struct satisfying the generated
// AIServiceClient interface directly, no network involved. ---

type fakeAI struct {
	narrateCalls  []*aipb.NarrateFindingsRequest
	suggestCalls  []*aipb.SuggestQuestionsRequest
	researchCalls []*aipb.ResearchRequest
	narrative     string
	questions     []string
	// researchResp is returned verbatim by Research when set; researchErr, if
	// set, is returned as the transport error instead.
	researchResp *aipb.ResearchResponse
	researchErr  error
}

func (f *fakeAI) NLToQuery(ctx context.Context, in *aipb.NLRequest, opts ...grpc.CallOption) (*aipb.NLResponse, error) {
	return nil, nil
}
func (f *fakeAI) Explain(ctx context.Context, in *aipb.ExplainRequest, opts ...grpc.CallOption) (*aipb.ExplainResponse, error) {
	return nil, nil
}
func (f *fakeAI) NarrateFindings(ctx context.Context, in *aipb.NarrateFindingsRequest, opts ...grpc.CallOption) (*aipb.NarrateFindingsResponse, error) {
	f.narrateCalls = append(f.narrateCalls, in)
	return &aipb.NarrateFindingsResponse{Narrative: f.narrative}, nil
}
func (f *fakeAI) SuggestQuestions(ctx context.Context, in *aipb.SuggestQuestionsRequest, opts ...grpc.CallOption) (*aipb.SuggestQuestionsResponse, error) {
	f.suggestCalls = append(f.suggestCalls, in)
	return &aipb.SuggestQuestionsResponse{Questions: f.questions}, nil
}
func (f *fakeAI) Research(ctx context.Context, in *aipb.ResearchRequest, opts ...grpc.CallOption) (*aipb.ResearchResponse, error) {
	f.researchCalls = append(f.researchCalls, in)
	if f.researchErr != nil {
		return nil, f.researchErr
	}
	if f.researchResp != nil {
		return f.researchResp, nil
	}
	return &aipb.ResearchResponse{}, nil
}

func newInsightsTestServer(t *testing.T, catalog *fakeCatalog, workerAddr string, ai *fakeAI) *api.Server {
	t.Helper()
	registry, err := scheduler.NewRegistry([]string{workerAddr})
	if err != nil {
		t.Fatalf("creating registry: %v", err)
	}
	t.Cleanup(registry.Close)
	coordinator := &scheduler.Coordinator{Registry: registry}
	return api.NewServer(catalog, coordinator, ai, nil, nil, nil, testSecret)
}

func ageDatasetCatalog() *fakeCatalog {
	return &fakeCatalog{
		schemaJSON: `{"fields":[{"name":"age","data_type":"Int64"}]}`,
		snapshotID: "snap-1",
		manifests: []*catalogpb.Manifest{
			{
				FilePath:        "part-0.atlas",
				ColumnStatsJson: `{"age":{"min":"AQAAAAAAAAA=","max":"WgAAAAAAAAA=","null_count":2,"distinct_count_estimate":3}}`,
			},
		},
	}
}

func TestHandleSummary_ReturnsEngineComputedFindingsVerbatim(t *testing.T) {
	workerAddr, worker := startFakeInsightsWorker(t)
	server := newInsightsTestServer(t, ageDatasetCatalog(), workerAddr, &fakeAI{})

	req := authedRequest(t, http.MethodPost, "/datasets/patients/summary", stringsReader(""))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		RowCount int64 `json:"row_count"`
		Columns  []struct {
			Name string `json:"name"`
		} `json:"columns"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v\nbody: %s", err, rec.Body.String())
	}
	if resp.RowCount != 5 {
		t.Fatalf("expected row_count=5 (from the fake worker's summary finding), got %d", resp.RowCount)
	}
	if len(resp.Columns) != 1 || resp.Columns[0].Name != "age" {
		t.Fatalf("expected the age column summary to pass through unmodified, got %+v", resp.Columns)
	}

	var sawSummaryAnalyze bool
	for _, call := range worker.analyzeCalls {
		if call.GetKind() == "summary" {
			sawSummaryAnalyze = true
			var stats []map[string]any
			if err := json.Unmarshal([]byte(call.GetColumnStatsJson()), &stats); err != nil {
				t.Fatalf("column_stats_json sent to Analyze isn't valid JSON: %v", err)
			}
			if len(stats) != 1 || stats[0]["name"] != "age" || stats[0]["data_type"] != "Int64" {
				t.Fatalf("expected one merged column stat for age tagged with its schema data_type, got %+v", stats)
			}
		}
	}
	if !sawSummaryAnalyze {
		t.Fatal("expected handleSummary to call worker.Analyze with kind=\"summary\"")
	}
}

func TestHandleInsights_OrchestratesSummaryQualityAndNarration(t *testing.T) {
	workerAddr, worker := startFakeInsightsWorker(t)
	ai := &fakeAI{narrative: "Age is missing 20% of the time.", questions: []string{"What is the average age?"}}
	server := newInsightsTestServer(t, ageDatasetCatalog(), workerAddr, ai)

	req := authedRequest(t, http.MethodPost, "/datasets/patients/insights", stringsReader(""))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Summary            json.RawMessage `json:"summary"`
		QualityFindings    json.RawMessage `json:"quality_findings"`
		Narrative          string          `json:"narrative"`
		SuggestedQuestions []string        `json:"suggested_questions"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v\nbody: %s", err, rec.Body.String())
	}
	if resp.Narrative != "Age is missing 20% of the time." {
		t.Fatalf("expected the AI service's narrative to pass through, got %q", resp.Narrative)
	}
	if len(resp.SuggestedQuestions) != 1 || resp.SuggestedQuestions[0] != "What is the average age?" {
		t.Fatalf("expected the AI service's suggested questions to pass through, got %v", resp.SuggestedQuestions)
	}
	if len(resp.Summary) == 0 || len(resp.QualityFindings) == 0 {
		t.Fatal("expected both summary and quality_findings to be populated")
	}

	var sawQualityAnalyze bool
	for _, call := range worker.analyzeCalls {
		if call.GetKind() == "quality" {
			sawQualityAnalyze = true
			if len(call.GetSampleArrowIpc()) == 0 {
				t.Fatal("expected the quality Analyze call to carry a non-empty row sample")
			}
		}
	}
	if !sawQualityAnalyze {
		t.Fatal("expected handleInsights to call worker.Analyze with kind=\"quality\"")
	}

	if len(ai.narrateCalls) != 1 {
		t.Fatalf("expected exactly one NarrateFindings call, got %d", len(ai.narrateCalls))
	}
	var findings map[string]json.RawMessage
	if err := json.Unmarshal([]byte(ai.narrateCalls[0].GetFindingsJson()), &findings); err != nil {
		t.Fatalf("findings_json sent to NarrateFindings isn't valid JSON: %v", err)
	}
	if _, ok := findings["summary"]; !ok {
		t.Fatal("expected findings_json to include a \"summary\" key")
	}
	if _, ok := findings["quality_findings"]; !ok {
		t.Fatal("expected findings_json to include a \"quality_findings\" key")
	}

	if len(ai.suggestCalls) != 1 {
		t.Fatalf("expected exactly one SuggestQuestions call, got %d", len(ai.suggestCalls))
	}
	if ai.suggestCalls[0].GetSchemaJson() == "" {
		t.Fatal("expected SuggestQuestions to receive the dataset's schema_json")
	}
}

// richDatasetCatalog has a low-cardinality string column (hospital), a
// non-constant numeric column (cost), and a Date32 column (admit_date) —
// exactly the shape pickGroupColumn/pickValueColumn/pickTimeColumn are
// looking for, so handleInsights should call Analyze with kind="outlier"
// and kind="trend" against it.
func richDatasetCatalog() *fakeCatalog {
	return &fakeCatalog{
		schemaJSON: `{"fields":[{"name":"hospital","data_type":"Utf8"},{"name":"cost","data_type":"Float64"},{"name":"admit_date","data_type":"Date32"}]}`,
		snapshotID: "snap-1",
		manifests: []*catalogpb.Manifest{
			{FilePath: "part-0.atlas"},
		},
	}
}

const richSummaryFindingsJSON = `{"row_count":100,"columns":[` +
	`{"name":"hospital","data_type":"Utf8","null_rate":0.0,"distinct_count_estimate":5,"min":"A","max":"E"},` +
	`{"name":"cost","data_type":"Float64","null_rate":0.0,"distinct_count_estimate":80,"min":"10","max":"5000"},` +
	`{"name":"admit_date","data_type":"Date32","null_rate":0.0,"distinct_count_estimate":30,"min":"0","max":"29"}` +
	`]}`

func TestHandleInsights_WiresOutlierAndTrendWhenColumnsQualify(t *testing.T) {
	workerAddr, worker := startFakeInsightsWorker(t)
	worker.summaryFindingsJSON = richSummaryFindingsJSON
	ai := &fakeAI{narrative: "General Hospital is a cost outlier.", questions: []string{"What drives cost?"}}
	server := newInsightsTestServer(t, richDatasetCatalog(), workerAddr, ai)

	req := authedRequest(t, http.MethodPost, "/datasets/patients/insights", stringsReader(""))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		OutlierFindings json.RawMessage `json:"outlier_findings"`
		TrendFinding    json.RawMessage `json:"trend_finding"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v\nbody: %s", err, rec.Body.String())
	}
	if len(resp.OutlierFindings) == 0 {
		t.Fatal("expected outlier_findings to be populated for a schema with a qualifying group/value column pair")
	}
	if len(resp.TrendFinding) == 0 {
		t.Fatal("expected trend_finding to be populated for a schema with a qualifying time/value column pair")
	}

	var sawOutlier, sawTrend bool
	for _, call := range worker.analyzeCalls {
		switch call.GetKind() {
		case "outlier":
			sawOutlier = true
			if call.GetGroupByColumn() != "hospital" {
				t.Fatalf("expected outlier analyze to use hospital as the group column, got %q", call.GetGroupByColumn())
			}
		case "trend":
			sawTrend = true
			if call.GetTimeColumn() != "admit_date" {
				t.Fatalf("expected trend analyze to use admit_date as the time column, got %q", call.GetTimeColumn())
			}
		}
	}
	if !sawOutlier {
		t.Fatal("expected handleInsights to call worker.Analyze with kind=\"outlier\"")
	}
	if !sawTrend {
		t.Fatal("expected handleInsights to call worker.Analyze with kind=\"trend\"")
	}

	var findings map[string]json.RawMessage
	if err := json.Unmarshal([]byte(ai.narrateCalls[0].GetFindingsJson()), &findings); err != nil {
		t.Fatalf("findings_json sent to NarrateFindings isn't valid JSON: %v", err)
	}
	if _, ok := findings["outlier_findings"]; !ok {
		t.Fatal("expected findings_json sent for narration to include outlier_findings")
	}
	if _, ok := findings["trend_finding"]; !ok {
		t.Fatal("expected findings_json sent for narration to include trend_finding")
	}
}
