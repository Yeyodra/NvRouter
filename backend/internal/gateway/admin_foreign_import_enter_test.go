package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/connectors"
)

func TestEnterAdminValidationReturnsAndCreatePersistsResolvedWorkspace(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/workspaces":
			_, _ = w.Write([]byte(`{"data":{"workspaces":[{"id":"ws-resolved"}]}}`))
		case "/ai-capability/models":
			_, _ = w.Write([]byte(`{"data":{"models":[]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	s, db := newBulkTestServer(t)
	s.conns = connectors.NewRegistry(connectors.NewEnterConverge(upstream.URL))

	validate := httptest.NewRecorder()
	s.adminValidateKey(validate, httptest.NewRequest(http.MethodPost, "/admin/validate", strings.NewReader(`{"provider":"enter-converge","api_key":"ek_secret"}`)))
	require.Equal(t, http.StatusOK, validate.Code)
	require.JSONEq(t, `{"status":"ok","workspace_id":"ws-resolved"}`, validate.Body.String())
	require.NotContains(t, validate.Body.String(), "ek_secret")

	create := httptest.NewRecorder()
	s.adminCreateAccount(create, httptest.NewRequest(http.MethodPost, "/admin/accounts", strings.NewReader(`{"provider":"enter-converge","api_key":"ek_secret"}`)))
	require.Equal(t, http.StatusCreated, create.Code, create.Body.String())
	accounts, err := db.Accounts().ListByProvider(context.Background(), adminTenant, "enter-converge")
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	creds, err := s.vault.Open(accounts[0])
	require.NoError(t, err)
	require.Equal(t, "ws-resolved", creds.Extra["workspace_id"])
}

func TestForeignEnterImportPreservesWorkspaceAndLiteralModelLocks(t *testing.T) {
	s, db := newBulkTestServer(t)
	payload := []byte(`[{"id":"ec-1","provider":"enter-converge","authType":"apiKey","name":"Farm","apiKey":"ek_secret","priority":7,"isActive":true,"providerSpecificData":{"workspaceId":"ws-9"},"modelLock_openai/gpt-5.6.sol":1893456000000}]`)
	result := &foreignImportResult{}
	s.importN9routerConnections(context.Background(), map[string]json.RawMessage{"providerConnections": payload}, result, nil)
	require.Equal(t, 1, result.Accounts, result.Errors)
	accounts, err := db.Accounts().ListByProvider(context.Background(), adminTenant, "enter-converge")
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	creds, err := s.vault.Open(accounts[0])
	require.NoError(t, err)
	require.Equal(t, "ek_secret", creds.APIKey)
	require.Equal(t, "ws-9", creds.Extra["workspace_id"])
	active, err := db.Routing().IsModelCooldownActive(context.Background(), accounts[0].ID, "openai/gpt-5.6.sol")
	require.NoError(t, err)
	require.True(t, active)
}

func TestForeignEnterImportRejectsMalformedSecretAndLock(t *testing.T) {
	s, _ := newBulkTestServer(t)
	payload := []byte(`[{"id":"bad-key","provider":"enter-converge","authType":"apiKey","apiKey":"sk_bad"},{"id":"bad-lock","provider":"enter-converge","authType":"apiKey","apiKey":"ek_ok","modelLock_openai/gpt":"never"}]`)
	result := &foreignImportResult{}
	s.importN9routerConnections(context.Background(), map[string]json.RawMessage{"providerConnections": payload}, result, nil)
	require.Equal(t, 1, result.Accounts)
	require.NotEmpty(t, result.Errors)
}

func TestN9routerEnterExportRequiresSecretAcknowledgementAndEmitsLocks(t *testing.T) {
	s, db := newBulkTestServer(t)
	payload := []byte(`[{"id":"ec-1","provider":"enter-converge","authType":"apiKey","name":"Farm","apiKey":"ek_secret","providerSpecificData":{"workspace_id":"ws-9"}}]`)
	result := &foreignImportResult{}
	s.importN9routerConnections(context.Background(), map[string]json.RawMessage{"providerConnections": payload}, result, nil)
	accounts, _ := db.Accounts().ListByProvider(context.Background(), adminTenant, "enter-converge")
	require.NoError(t, db.Routing().SetModelCooldown(context.Background(), accounts[0].ID, "vendor/model.with.dot", time.Now().Add(time.Hour)))

	denied := httptest.NewRecorder()
	s.adminExportDatabase(denied, httptest.NewRequest("GET", "/admin/export?format=9router", nil))
	require.Equal(t, 400, denied.Code)
	require.NotContains(t, denied.Body.String(), "ek_secret")

	passphraseDenied := httptest.NewRecorder()
	s.adminExportDatabase(passphraseDenied, httptest.NewRequest("GET", "/admin/export?format=9router&passphrase=not-encryption", nil))
	require.Equal(t, 400, passphraseDenied.Code)
	require.NotContains(t, passphraseDenied.Body.String(), "ek_secret")

	rec := httptest.NewRecorder()
	s.adminExportDatabase(rec, httptest.NewRequest("GET", "/admin/export?format=9router&include_secrets=true", nil))
	require.Equal(t, 200, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, `"apiKey":"ek_secret"`)
	require.Contains(t, body, `"workspaceId":"ws-9"`)
	require.Contains(t, body, `"modelLock_vendor/model.with.dot"`)
	require.Contains(t, body, "T")
	require.NotContains(t, body, `"modelLock_vendor/model.with.dot":1`)
	require.True(t, strings.HasPrefix(body, "{"))
}
