package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/config"
	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/crypto"
	"github.com/mydisha/keirouter/backend/internal/dispatch"
	"github.com/mydisha/keirouter/backend/internal/pipeline"
	"github.com/mydisha/keirouter/backend/internal/store"
	"github.com/mydisha/keirouter/backend/internal/vault"
)

type modelTestConnector struct {
	provider string
	mu       sync.Mutex
	calls    []string
	requests []*core.ChatRequest
	fail     map[string]error
}

func (c *modelTestConnector) ID() string            { return c.provider }
func (c *modelTestConnector) Dialect() core.Dialect { return core.DialectOpenAI }
func (c *modelTestConnector) Chat(_ context.Context, req *core.ChatRequest, creds core.Credentials) (*core.ChatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, creds.AccountID)
	c.requests = append(c.requests, req)
	if err := c.fail[creds.AccountID]; err != nil {
		return nil, err
	}
	return &core.ChatResponse{
		Model:   req.Model,
		Message: core.Message{Role: core.RoleAssistant, Content: []core.ContentPart{{Type: core.PartText, Text: "OK"}}},
		Usage:   core.Usage{PromptTokens: 4, CompletionTokens: 1, TotalTokens: 5},
	}, nil
}
func (c *modelTestConnector) Stream(context.Context, *core.ChatRequest, core.Credentials, core.StreamConfig) (<-chan core.StreamChunk, error) {
	return nil, nil
}

func newModelTestServer(t *testing.T, provider string, accounts ...store.Account) (*Server, *modelTestConnector) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, config.DatabaseConfig{Driver: "sqlite", DSN: ":memory:"}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, db.Migrate(ctx))
	require.NoError(t, db.Tenants().EnsureDefault(ctx))
	t.Cleanup(func() { _ = db.Close() })

	mk, err := crypto.GenerateMasterKey()
	require.NoError(t, err)
	sealer, err := crypto.NewSealer(mk)
	require.NoError(t, err)
	v := vault.New(sealer)
	for i := range accounts {
		accounts[i].TenantID = store.DefaultTenantID
		accounts[i].Provider = provider
		accounts[i].AuthKind = store.AuthAPIKey
		accounts[i].CreatedAt = time.Now()
		accounts[i].UpdatedAt = time.Now()
		require.NoError(t, v.Seal(&accounts[i], vault.NewSecret{APIKey: "secret"}))
		require.NoError(t, db.Accounts().Create(ctx, accounts[i]))
	}

	conn := &modelTestConnector{provider: provider, fail: map[string]error{}}
	registry := connectors.NewRegistry(conn)
	dispatcher := dispatch.New(registry, db.Accounts(), v)
	return &Server{
		db:       db,
		accounts: db.Accounts(),
		conns:    registry,
		pipeline: pipeline.New(pipeline.Deps{Dispatcher: dispatcher}),
	}, conn
}

func postModelTest(t *testing.T, s *Server, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/models/test", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.adminTestModel(rec, req)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return rec.Code, out
}

func TestAdminTestModelSuccess(t *testing.T) {
	s, conn := newModelTestServer(t, "openai", store.Account{ID: "acc-1", Priority: 1})

	status, out := postModelTest(t, s, `{"provider":"openai","model":"gpt-4o"}`)

	require.Equal(t, http.StatusOK, status)
	require.Equal(t, true, out["ok"])
	require.Equal(t, "gpt-4o", out["requested_model"])
	require.Equal(t, "openai", out["provider"])
	require.Equal(t, "gpt-4o", out["actual_model"])
	require.Equal(t, "acc-1", out["account_id"])
	require.Equal(t, "OK", out["content"])
	require.GreaterOrEqual(t, out["latency_ms"].(float64), float64(0))
	require.Equal(t, map[string]any{"prompt_tokens": float64(4), "completion_tokens": float64(1), "total_tokens": float64(5)}, out["usage"])
	require.Equal(t, "Reply with exactly: OK", conn.requests[0].Messages[0].TextContent())
	require.Equal(t, core.DialectOpenAI, conn.requests[0].Metadata.SourceDialect)
	require.Equal(t, adminTenant, conn.requests[0].Metadata.TenantID)
	require.False(t, conn.requests[0].Stream)
	require.Equal(t, 64, *conn.requests[0].MaxTokens)
}

func TestAdminTestModelUsesProviderAccountFallback(t *testing.T) {
	s, conn := newModelTestServer(t, "openai",
		store.Account{ID: "acc-1", Priority: 1},
		store.Account{ID: "acc-2", Priority: 2},
	)
	conn.fail["acc-1"] = &core.ProviderError{Kind: core.ErrAuth, Message: "rejected credential"}

	status, out := postModelTest(t, s, `{"provider":"openai","model":"gpt-4o","prompt":"test"}`)

	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "acc-2", out["account_id"])
	require.Equal(t, []string{"acc-1", "acc-2"}, conn.calls)
	require.Equal(t, "test", conn.requests[0].Messages[0].TextContent())
}

func TestAdminTestModelPinsAccount(t *testing.T) {
	s, conn := newModelTestServer(t, "openai",
		store.Account{ID: "acc-1", Priority: 1},
		store.Account{ID: "acc-2", Priority: 2},
	)

	status, out := postModelTest(t, s, `{"provider":"openai","model":"gpt-4o","account_id":"acc-2"}`)

	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "acc-2", out["account_id"])
	require.Equal(t, []string{"acc-2"}, conn.calls)
}

func TestAdminTestModelValidation(t *testing.T) {
	s, conn := newModelTestServer(t, "openai", store.Account{ID: "acc-1", Priority: 1})
	require.NoError(t, s.db.Accounts().Create(context.Background(), store.Account{
		ID: "other-provider", TenantID: store.DefaultTenantID, Provider: "anthropic", Label: "other",
		AuthKind: store.AuthAPIKey, Priority: 1, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}))

	for _, tc := range []struct {
		name string
		body string
	}{
		{"missing provider", `{"model":"gpt-4o"}`},
		{"unknown provider", `{"provider":"missing","model":"gpt-4o"}`},
		{"unknown model", `{"provider":"openai","model":"missing"}`},
		{"non llm", `{"provider":"openai","model":"text-embedding-3-small"}`},
		{"mismatched account", `{"provider":"openai","model":"gpt-4o","account_id":"other-provider"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, out := postModelTest(t, s, tc.body)
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, false, out["ok"])
		})
	}
	require.Empty(t, conn.calls)
}

func TestAdminTestModelSanitizesProviderError(t *testing.T) {
	s, conn := newModelTestServer(t, "openai", store.Account{ID: "acc-1", Priority: 1})
	conn.fail["acc-1"] = &core.ProviderError{
		Kind: core.ErrBadRequest, StatusCode: http.StatusBadRequest,
		Message: "upstream body contains sk-secret",
	}

	status, out := postModelTest(t, s, `{"provider":"openai","model":"gpt-4o"}`)

	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, false, out["ok"])
	require.Equal(t, "upstream provider rejected the request", out["error"])
	require.Equal(t, "bad_request", out["error_type"])
	require.Equal(t, float64(http.StatusBadRequest), out["upstream_status"])
	require.NotContains(t, out["error"], "sk-secret")
}

func TestAdminTestModelAcceptsCustomModel(t *testing.T) {
	connectors.SetDynamicModels("openai", []connectors.ModelSpec{{ID: "custom-llm", Name: "Custom", Kind: core.ServiceLLM}})
	t.Cleanup(func() { connectors.SetDynamicModels("openai", nil) })
	s, _ := newModelTestServer(t, "openai", store.Account{ID: "acc-1", Priority: 1})

	status, out := postModelTest(t, s, `{"provider":"openai","model":"custom-llm"}`)

	require.Equal(t, http.StatusOK, status)
	require.Equal(t, true, out["ok"])
}
