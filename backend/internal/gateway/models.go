package gateway

import "net/http"

type modelEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

// handleListModels reports the public route IDs callable by this API key.
func (s *Server) handleListModels(w http.ResponseWriter, r *http.Request) {
	key, _ := authedKey(r.Context())
	allowed, err := s.identity.Keys().GetAllowedModels(r.Context(), key.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list models")
		return
	}

	data := make([]modelEntry, 0)
	seen := make(map[string]struct{})
	appendRoute := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		data = append(data, modelEntry{ID: id, Object: "model", OwnedBy: "nvrouter"})
	}

	if len(allowed) > 0 {
		for _, pattern := range allowed {
			if pattern != "" && pattern[len(pattern)-1] != '*' {
				appendRoute(pattern)
			}
		}
	}

	appendVisibleRoute := func(id string) {
		if len(allowed) == 0 || modelMatchesAny(id, allowed) {
			appendRoute(id)
		}
	}
	if s.aliases != nil {
		if aliases, err := s.aliases.List(r.Context()); err == nil {
			for _, alias := range aliases {
				appendVisibleRoute(alias.Alias)
			}
		}
	}
	if s.chains != nil {
		if chains, err := s.chains.ListByTenant(r.Context(), tenantOf(key)); err == nil {
			for _, chain := range chains {
				appendVisibleRoute(chain.Name)
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// Public model discovery is route-based for every kind; provider capabilities
// remain internal routing details.
func (s *Server) handleListModelsByKind(w http.ResponseWriter, r *http.Request) {
	s.handleListModels(w, r)
}

// Provider/model metadata is intentionally unavailable on the public API.
func (s *Server) handleModelInfo(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotFound)
}
