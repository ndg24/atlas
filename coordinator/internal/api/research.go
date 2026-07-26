package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	aipb "atlas/coordinator/internal/aipb"
)

// researchRequest/researchResponse back POST /research (Phase 8, slice 1):
// the coordinator's only job here is resolving the dataset's schema and
// forwarding the caller's own bearer token, so the AI service's Research RPC
// can call back into this same coordinator's POST /query/nl for each
// structured sub-question without needing its own auth story.
type researchRequest struct {
	Question string `json:"question"`
	Dataset  string `json:"dataset"`
	CorpusID string `json:"corpus_id"`
}

type researchResponse struct {
	Report string          `json:"report"`
	State  json.RawMessage `json:"state"`
}

// handleResearch orchestrates Phase 8's multi-agent research pipeline by
// delegating entirely to AIService.Research (ai-service/atlas_ai/agents/pipeline.py)
// — unlike /query and /query/nl, the coordinator does not compile or run
// anything itself here; the pipeline's ExecutionAgent reuses /query/nl over
// HTTP for each structured sub-question instead.
func (s *Server) handleResearch(w http.ResponseWriter, r *http.Request) {
	var req researchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Question == "" || req.Dataset == "" {
		writeError(w, http.StatusBadRequest, errors.New("question and dataset are required"))
		return
	}

	ctx := r.Context()
	dq, err := s.lookupDataset(ctx, req.Dataset)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// There's no context-stashed raw token today (authMiddleware only keeps
	// parsed *auth.Claims) — the header itself is still on the request, so
	// reading it directly here is the minimal way to forward it.
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")

	resp, err := s.ai.Research(ctx, &aipb.ResearchRequest{
		Question:   req.Question,
		Dataset:    req.Dataset,
		SchemaJson: dq.dataset.GetSchemaJson(),
		CorpusId:   req.CorpusID,
		AuthToken:  token,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if resp.GetError() != "" {
		writeError(w, http.StatusInternalServerError, errors.New(resp.GetError()))
		return
	}

	writeJSON(w, http.StatusOK, researchResponse{
		Report: resp.GetReport(),
		State:  json.RawMessage(resp.GetStateJson()),
	})
}
