package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/store"
)

func TestMediaOptionsAuthorizesPublicRouteBeforeResolution(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "restricted")
	createPublicRoutes(t, db)

	for _, route := range []string{"fast", "resilient"} {
		t.Run(route, func(t *testing.T) {
			require.NoError(t, db.APIKeys().SetAllowedModels(context.Background(), keys[0].Record.ID, []string{route}))
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req = req.WithContext(context.WithValue(req.Context(), apiKeyCtxKey, keys[0].Record))

			opts, err := gw.mediaOptions(req, route)
			require.NoError(t, err)
			require.NotEmpty(t, opts.Targets)
		})
	}
}

func TestMediaOptionsPreservesPublicRouteWildcard(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "restricted")
	createPublicRoutes(t, db)
	require.NoError(t, db.APIKeys().SetAllowedModels(context.Background(), keys[0].Record.ID, []string{"res*"}))
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req = req.WithContext(context.WithValue(req.Context(), apiKeyCtxKey, keys[0].Record))

	opts, err := gw.mediaOptions(req, "resilient")
	require.NoError(t, err)
	require.Len(t, opts.Targets, 2)
}

func TestForbiddenRequestedRouteReturns403AcrossPublicEndpoints(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "restricted")
	require.NoError(t, db.APIKeys().SetAllowedModels(context.Background(), keys[0].Record.ID, []string{"fast"}))

	tests := []struct {
		name string
		path string
		body string
	}{
		{"chat", "/v1/chat/completions", `{"model":"not-a-route","messages":[{"role":"user","content":"hello"}]}`},
		{"responses", "/v1/responses", `{"model":"not-a-route","input":"hello"}`},
		{"count tokens", "/v1/messages/count_tokens", `{"model":"not-a-route","messages":[{"role":"user","content":"hello"}]}`},
		{"gemini", "/v1beta/models/not-a-route:generateContent", `{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`},
		{"embeddings", "/v1/embeddings", `{"model":"not-a-route","input":"hello"}`},
		{"images", "/v1/images/generations", `{"model":"not-a-route","prompt":"hello"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer "+keys[0].Plaintext)
			req.Header.Set("Content-Type", "application/json")
			gw.Handler().ServeHTTP(rec, req)

			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "not permitted")
		})
	}
}

func TestMediaOptionsUnrestrictedKeyStillResolves(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "unrestricted")
	createPublicRoutes(t, db)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req = req.WithContext(context.WithValue(req.Context(), apiKeyCtxKey, store.APIKey{
		ID: keys[0].Record.ID, TenantID: store.DefaultTenantID,
	}))

	opts, err := gw.mediaOptions(req, "resilient")
	require.NoError(t, err)
	require.Len(t, opts.Targets, 2)
}
