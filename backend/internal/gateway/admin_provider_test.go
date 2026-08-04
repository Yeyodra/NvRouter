package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/store"
	"github.com/mydisha/keirouter/backend/internal/vault"
	"github.com/stretchr/testify/require"
)

func TestProviderAccountMetadataSpecialProviders(t *testing.T) {
	cf, ok := connectors.SpecByID("cloudflare-ai")
	require.True(t, ok)
	_, err := providerAccountMetadata(cf, providerMetadataInput{})
	require.Error(t, err)

	meta, err := providerAccountMetadata(cf, providerMetadataInput{AccountID: "acct-123"})
	require.NoError(t, err)
	require.Equal(t, "acct-123", meta["accountId"])

	azure, ok := connectors.SpecByID("azure")
	require.True(t, ok)
	meta, err = providerAccountMetadata(azure, providerMetadataInput{
		AzureEndpoint:   "https://example.openai.azure.com/",
		AzureDeployment: "prod-gpt",
		AzureAPIVersion: "2024-10-01-preview",
	})
	require.NoError(t, err)
	require.Equal(t, "https://example.openai.azure.com", meta["azure_endpoint"])
	require.Equal(t, "prod-gpt", meta["deployment"])
	require.Equal(t, "2024-10-01-preview", meta["api_version"])

	custom, ok := connectors.SpecByID("custom-openai")
	require.True(t, ok)
	_, err = providerAccountMetadata(custom, providerMetadataInput{})
	require.Error(t, err)
	meta, err = providerAccountMetadata(custom, providerMetadataInput{BaseURL: "https://llm.example.com/v1"})
	require.NoError(t, err)
	require.Equal(t, "https://llm.example.com/v1", meta["base_url"])
}

func TestAccountAuthKindNoAuthProvider(t *testing.T) {
	spec, ok := connectors.SpecByID("searxng")
	require.True(t, ok)
	require.Equal(t, store.AuthNone, accountAuthKind(spec, ""))
	require.Equal(t, store.AuthAPIKey, accountAuthKind(spec, "optional-key"))
}

type countingModelSource struct{ calls atomic.Int32 }

func (s *countingModelSource) ListModels(context.Context, core.Credentials) ([]connectors.ModelSpec, error) {
	s.calls.Add(1)
	return []connectors.ModelSpec{{ID: "live-only", Name: "Live Only", Kind: core.ServiceLLM}}, nil
}

func TestAdminProviderModelsDoesNotBlockOnLiveDiscovery(t *testing.T) {
	s, db := newBulkTestServer(t)
	const providerID = "custom-openai-catalog-read"
	connectors.RegisterDynamicProvider(connectors.DynamicProvider{
		ID: providerID, DisplayName: "Catalog Read", Alias: providerID,
		Dialect: core.DialectOpenAI, BaseURL: "https://example.invalid/v1",
	})
	t.Cleanup(func() { connectors.UnregisterDynamicProvider(providerID) })
	connectors.SetDynamicModels(providerID, []connectors.ModelSpec{{ID: "static-model", Name: "Static Model", Kind: core.ServiceLLM}})
	t.Cleanup(func() { connectors.SetDynamicModels(providerID, nil) })

	now := time.Now()
	acc := store.Account{ID: "catalog-account", TenantID: store.DefaultTenantID, Provider: providerID, Label: "test", AuthKind: store.AuthAPIKey, CreatedAt: now, UpdatedAt: now}
	require.NoError(t, s.vault.Seal(&acc, vault.NewSecret{APIKey: "test-key", Metadata: map[string]string{}}))
	require.NoError(t, db.Accounts().Create(context.Background(), acc))

	source := &countingModelSource{}
	connectors.RegisterLiveModelSource(providerID, source)
	t.Cleanup(func() { connectors.RegisterLiveModelSource(providerID, nil) })

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", providerID)
	req := httptest.NewRequest(http.MethodGet, "/providers/"+providerID+"/models", nil).WithContext(context.WithValue(context.Background(), chi.RouteCtxKey, rctx))
	rec := httptest.NewRecorder()
	s.adminProviderModels(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var body struct {
		Models []struct {
			ID string `json:"id"`
		} `json:"models"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Equal(t, []string{"static-model"}, []string{body.Models[0].ID})
	require.Zero(t, source.calls.Load(), "catalog reads must not call upstream live discovery")
}
