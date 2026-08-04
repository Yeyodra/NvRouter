package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestTaskletAccountMetadata(t *testing.T) {
	spec, ok := connectors.SpecByID("tasklet")
	require.True(t, ok)
	meta, err := providerAccountMetadata(spec, providerMetadataInput{WorkspaceID: " ws-1 ", OrganizationID: " org-1 "})
	require.NoError(t, err)
	require.Equal(t, "ws-1", meta["workspace_id"])
	require.Equal(t, "org-1", meta["organization_id"])
}

type taskletMetadataConnector struct {
	validated core.Credentials
}

func (c *taskletMetadataConnector) ID() string            { return "tasklet" }
func (c *taskletMetadataConnector) Dialect() core.Dialect { return core.DialectOpenAI }
func (c *taskletMetadataConnector) Chat(context.Context, *core.ChatRequest, core.Credentials) (*core.ChatResponse, error) {
	return nil, nil
}
func (c *taskletMetadataConnector) Stream(context.Context, *core.ChatRequest, core.Credentials, core.StreamConfig) (<-chan core.StreamChunk, error) {
	return nil, nil
}
func (c *taskletMetadataConnector) Validate(_ context.Context, creds core.Credentials) error {
	c.validated = creds
	return nil
}

func TestTaskletCanonicalMetadataFlowsThroughValidateAndCreate(t *testing.T) {
	s, db := newBulkTestServer(t)
	conn := &taskletMetadataConnector{}
	s.conns = connectors.NewRegistry(conn)
	body := `{"provider":"tasklet","api_key":"session-token","workspace_id":"ws-1","organization_id":"org-1"}`

	validate := httptest.NewRecorder()
	s.adminValidateKey(validate, httptest.NewRequest(http.MethodPost, "/admin/validate", strings.NewReader(body)))
	require.Equal(t, http.StatusOK, validate.Code)
	require.JSONEq(t, `{"status":"ok"}`, validate.Body.String())
	require.Equal(t, "ws-1", conn.validated.Extra["workspace_id"])
	require.Equal(t, "org-1", conn.validated.Extra["organization_id"])

	create := httptest.NewRecorder()
	s.adminCreateAccount(create, httptest.NewRequest(http.MethodPost, "/admin/accounts", strings.NewReader(body)))
	require.Equal(t, http.StatusCreated, create.Code, create.Body.String())
	accounts, err := db.Accounts().ListByProvider(context.Background(), adminTenant, "tasklet")
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	creds, err := s.vault.Open(accounts[0])
	require.NoError(t, err)
	require.Equal(t, "ws-1", creds.Extra["workspace_id"])
	require.Equal(t, "org-1", creds.Extra["organization_id"])
}

func TestForeignTaskletImportExportRoundTrip(t *testing.T) {
	s, db := newBulkTestServer(t)
	payload := []byte(`[{"id":"tl-1","provider":"tasklet","authType":"apiKey","name":"Tasklet","apiKey":"session-token","providerSpecificData":{"workspaceId":"ws-1","organizationId":"org-1"}}]`)
	result := &foreignImportResult{}
	s.importN9routerConnections(context.Background(), map[string]json.RawMessage{"providerConnections": payload}, result, nil)
	require.Equal(t, 1, result.Accounts, result.Errors)
	accounts, err := db.Accounts().ListByProvider(context.Background(), adminTenant, "tasklet")
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	creds, err := s.vault.Open(accounts[0])
	require.NoError(t, err)
	require.Equal(t, "ws-1", creds.Extra["workspace_id"])
	require.Equal(t, "org-1", creds.Extra["organization_id"])

	rec := httptest.NewRecorder()
	s.adminExportDatabase(rec, httptest.NewRequest("GET", "/admin/export?format=9router&include_secrets=true", nil))
	require.Equal(t, 200, rec.Code)
	require.Contains(t, rec.Body.String(), `"provider":"tasklet"`)
	require.Contains(t, rec.Body.String(), `"apiKey":"session-token"`)
	require.Contains(t, rec.Body.String(), `"workspaceId":"ws-1"`)
	require.Contains(t, rec.Body.String(), `"organizationId":"org-1"`)
}

func TestForeignTaskletRejectsMissingOrIncompatibleCredential(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"missing token", `[{"id":"tl-empty","provider":"tasklet","authType":"apiKey","apiKey":""}]`},
		{"oauth token", `[{"id":"tl-oauth","provider":"tasklet","authType":"oauth","accessToken":"session-token"}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, db := newBulkTestServer(t)
			result := &foreignImportResult{}
			s.importN9routerConnections(context.Background(), map[string]json.RawMessage{"providerConnections": []byte(tt.payload)}, result, nil)
			require.Zero(t, result.Accounts)
			require.Equal(t, 1, result.Skipped)
			accounts, err := db.Accounts().ListByProvider(context.Background(), adminTenant, "tasklet")
			require.NoError(t, err)
			require.Empty(t, accounts)
			require.NotEmpty(t, result.Errors)
		})
	}
}
