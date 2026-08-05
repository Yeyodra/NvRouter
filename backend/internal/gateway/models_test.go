package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/config"
	"github.com/mydisha/keirouter/backend/internal/identity"
	"github.com/mydisha/keirouter/backend/internal/store"
	"github.com/mydisha/keirouter/backend/internal/transform"
)

func TestListModelsUsesEffectiveKeyVisibility(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "first", "second")
	createPublicRoutes(t, db)
	require.NoError(t, db.APIKeys().SetAllowedModels(context.Background(), keys[0].Record.ID, []string{"fast"}))
	require.NoError(t, db.APIKeys().SetAllowedModels(context.Background(), keys[1].Record.ID, []string{"safe"}))

	first := getAuthedJSON(t, gw, keys[0].Plaintext, "/v1/models")
	second := getAuthedJSON(t, gw, keys[1].Plaintext, "/v1/models")

	require.Equal(t, []string{"fast"}, modelIDsFromResponse(t, first))
	require.Equal(t, []string{"safe"}, modelIDsFromResponse(t, second))
	assertPublicModelEntries(t, first)
	assertPublicModelEntries(t, second)
}

func TestListModelsRestrictedReturnsExactPublicIDsWithoutRegisteredRoutes(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "restricted")
	allowed := []string{"auto", "openai/gpt-5.6-sol", "claude-opus-4.8"}
	require.NoError(t, db.APIKeys().SetAllowedModels(context.Background(), keys[0].Record.ID, allowed))

	body := getAuthedJSON(t, gw, keys[0].Plaintext, "/v1/models")
	require.ElementsMatch(t, allowed, modelIDsFromResponse(t, body))
	assertPublicModelEntries(t, body)
}

func TestListModelsUnrestrictedShowsOnlyPublicRoutes(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "unrestricted")
	createPublicRoutes(t, db)

	body := getAuthedJSON(t, gw, keys[0].Plaintext, "/v1/models")
	models := modelIDsFromResponse(t, body)
	for _, route := range []string{"fast", "safe", "resilient"} {
		require.Contains(t, models, route)
	}
	assertPublicModelEntries(t, body)

	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "openai")
	require.NotContains(t, string(encoded), "anthropic")
	require.NotContains(t, string(encoded), "internal-")
}

func TestEnterCanonicalMigrationRegistersBarePublicRoutes(t *testing.T) {
	gw, _, keys := newPublicModelsTestGateway(t, "unrestricted")
	body := getAuthedJSON(t, gw, keys[0].Plaintext, "/v1/models")
	models := modelIDsFromResponse(t, body)
	require.Contains(t, models, "gpt-5.6-sol")
	require.Contains(t, models, "claude-opus-4.6")
	require.NotContains(t, models, "openai/gpt-5.6-sol")
	require.NotContains(t, models, "anthropic/claude-opus-4.6")
}

func TestListModelsByKindCannotBypassVisibility(t *testing.T) {
	gw, db, keys := newPublicModelsTestGateway(t, "restricted")
	createPublicRoutes(t, db)
	require.NoError(t, db.APIKeys().SetAllowedModels(context.Background(), keys[0].Record.ID, []string{"fast"}))

	for _, path := range []string{"/v1/models/llm", "/v1/models/embedding", "/v1/models/chains"} {
		t.Run(path, func(t *testing.T) {
			body := getAuthedJSON(t, gw, keys[0].Plaintext, path)
			require.Equal(t, []string{"fast"}, modelIDsFromResponse(t, body))
			assertPublicModelEntries(t, body)
		})
	}
}

func TestModelInfoDoesNotExposeProviderMetadata(t *testing.T) {
	gw, _, keys := newPublicModelsTestGateway(t, "public")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/models/info?id=openai/gpt-4o", nil)
	req.Header.Set("Authorization", "Bearer "+keys[0].Plaintext)

	gw.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "openai")
	require.NotContains(t, rec.Body.String(), "gpt-4o")
}

func newPublicModelsTestGateway(t *testing.T, names ...string) (*Server, *store.DB, []identity.Issued) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, config.DatabaseConfig{Driver: "sqlite", DSN: ":memory:"}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, db.Migrate(ctx))
	require.NoError(t, db.Tenants().EnsureDefault(ctx))
	t.Cleanup(func() { _ = db.Close() })

	idSvc := identity.New(db.APIKeys())
	keys := make([]identity.Issued, 0, len(names))
	for _, name := range names {
		issued, err := idSvc.Create(ctx, store.DefaultTenantID, "", name)
		require.NoError(t, err)
		keys = append(keys, issued)
	}

	return New(Deps{
		Config:   config.Default(),
		DB:       db,
		Identity: idSvc,
		Chains:   db.Chains(),
		Aliases:  db.Aliases(),
		Codecs:   transform.DefaultRegistry(),
	}), db, keys
}

func createPublicRoutes(t *testing.T, db *store.DB) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, db.Aliases().Set(ctx, "fast", "openai/internal-fast"))
	require.NoError(t, db.Aliases().Set(ctx, "safe", "anthropic/internal-safe"))
	require.NoError(t, db.Chains().Create(ctx, store.Chain{
		ID:       "chain-resilient",
		TenantID: store.DefaultTenantID,
		Name:     "resilient",
		Strategy: "fallback",
		Steps: []store.ChainStep{
			{ID: "step-primary", Position: 0, Provider: "openai", Model: "internal-primary", CreatedAt: time.Now()},
			{ID: "step-fallback", Position: 1, Provider: "anthropic", Model: "internal-fallback", CreatedAt: time.Now()},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}))
}

func assertPublicModelEntries(t *testing.T, body map[string]any) {
	t.Helper()
	items, ok := body["data"].([]any)
	require.True(t, ok)
	for _, item := range items {
		entry, ok := item.(map[string]any)
		require.True(t, ok)
		require.Len(t, entry, 3)
		require.Equal(t, "model", entry["object"])
		require.Equal(t, "nvrouter", entry["owned_by"])
		require.NotContains(t, entry, "provider")
	}
}

func getAuthedJSON(t *testing.T, gw *Server, apiKey, path string) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	gw.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body
}

func modelIDsFromResponse(t *testing.T, body map[string]any) []string {
	t.Helper()
	items, ok := body["data"].([]any)
	require.True(t, ok)
	out := make([]string, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		require.True(t, ok)
		id, ok := entry["id"].(string)
		require.True(t, ok)
		out = append(out, id)
	}
	return out
}
