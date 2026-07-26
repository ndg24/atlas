package api_test

// Exercises POST /research end to end against a fake AIServiceClient (no real
// ai-service, no HTTP loopback) — the coordinator's own job here is just
// resolving the dataset's schema and forwarding the caller's bearer token, so
// these tests only need to prove that wiring, not the Python-side pipeline
// (which has its own tests in ai-service/tests/agents).

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	aipb "atlas/coordinator/internal/aipb"
	"atlas/coordinator/internal/auth"
)

func TestHandleResearch_ForwardsSchemaAndBearerTokenReturnsReportAndState(t *testing.T) {
	workerAddr, _ := startFakeInsightsWorker(t)
	ai := &fakeAI{
		researchResp: &aipb.ResearchResponse{
			Report:    "General Hospital sees the most patients. [data]",
			StateJson: `{"question":"which hospital sees the most patients?"}`,
		},
	}
	server := newInsightsTestServer(t, ageDatasetCatalog(), workerAddr, ai)

	body := stringsReader(`{"question": "which hospital sees the most patients?", "dataset": "patients", "corpus_id": "papers"}`)
	req := authedRequest(t, http.MethodPost, "/research", body)
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Report string          `json:"report"`
		State  json.RawMessage `json:"state"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decoding response: %v\nbody: %s", err, rec.Body.String())
	}
	if resp.Report != "General Hospital sees the most patients. [data]" {
		t.Fatalf("expected the AI service's report to pass through, got %q", resp.Report)
	}
	if len(resp.State) == 0 {
		t.Fatal("expected the AI service's state_json to pass through as the response's state")
	}

	if len(ai.researchCalls) != 1 {
		t.Fatalf("expected exactly one Research call, got %d", len(ai.researchCalls))
	}
	call := ai.researchCalls[0]
	if call.GetSchemaJson() == "" {
		t.Fatal("expected Research to receive the dataset's schema_json")
	}
	if call.GetCorpusId() != "papers" {
		t.Fatalf("expected corpus_id to pass through, got %q", call.GetCorpusId())
	}

	token, err := auth.Parse(testSecret, call.GetAuthToken())
	if err != nil {
		t.Fatalf("expected Research to receive the request's own bearer token, but it didn't parse: %v", err)
	}
	if token.UserID != "test-user" {
		t.Fatalf("expected the forwarded token to belong to test-user, got %q", token.UserID)
	}
}

func TestHandleResearch_MissingFieldsReturns400WithoutCallingAI(t *testing.T) {
	workerAddr, _ := startFakeInsightsWorker(t)
	ai := &fakeAI{}
	server := newInsightsTestServer(t, ageDatasetCatalog(), workerAddr, ai)

	req := authedRequest(t, http.MethodPost, "/research", stringsReader(`{"dataset": "patients"}`))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a missing question, got %d: %s", rec.Code, rec.Body.String())
	}
	if len(ai.researchCalls) != 0 {
		t.Fatal("expected s.ai.Research to never be called when validation fails")
	}
}

func TestHandleResearch_RequiresAuth(t *testing.T) {
	workerAddr, _ := startFakeInsightsWorker(t)
	server := newInsightsTestServer(t, ageDatasetCatalog(), workerAddr, &fakeAI{})

	req := httptest.NewRequest(http.MethodPost, "/research", stringsReader(`{"question": "x", "dataset": "patients"}`))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without a bearer token, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestHandleResearch_AIServiceErrorBecomes500(t *testing.T) {
	workerAddr, _ := startFakeInsightsWorker(t)
	ai := &fakeAI{researchErr: errors.New("ai-service unreachable")}
	server := newInsightsTestServer(t, ageDatasetCatalog(), workerAddr, ai)

	req := authedRequest(t, http.MethodPost, "/research", stringsReader(`{"question": "x", "dataset": "patients"}`))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 when the AI service call fails, got %d: %s", rec.Code, rec.Body.String())
	}
}
