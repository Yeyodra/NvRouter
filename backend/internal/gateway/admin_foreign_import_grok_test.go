package gateway

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestForeignGrokCLIImportPreservesOAuthMetadata(t *testing.T) {
	s, db := newBulkTestServer(t)
	payload := []byte(`[{"id":"grok-1","provider":"grok-cli","authType":"oauth","email":"user@example.com","accessToken":"access-secret","refreshToken":"refresh-secret","expiresAt":"2030-01-02T03:04:05Z","providerSpecificData":{"userId":"uid-99","deviceId":"dev-123"}}]`)
	result := &foreignImportResult{}
	s.importN9routerConnections(context.Background(), map[string]json.RawMessage{"providerConnections": payload}, result, nil)
	require.Equal(t, 1, result.Accounts, result.Errors)

	accounts, err := db.Accounts().ListByProvider(context.Background(), adminTenant, "grok-cli")
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	creds, err := s.vault.Open(accounts[0])
	require.NoError(t, err)
	require.Equal(t, "access-secret", creds.AccessToken)
	refresh, err := s.vault.OpenRefreshToken(accounts[0])
	require.NoError(t, err)
	require.Equal(t, "refresh-secret", refresh)
	require.Equal(t, "user@example.com", creds.Extra["email"])
	require.Equal(t, "uid-99", creds.Extra["userId"])
	require.Equal(t, "dev-123", creds.Extra["deviceId"])
	require.NotNil(t, accounts[0].TokenExpiresAt)
}

func TestForeignGrokCLIImportRejectsIncompleteOAuth(t *testing.T) {
	s, _ := newBulkTestServer(t)
	payload := []byte(`[{"id":"missing-refresh","provider":"grok-cli","authType":"oauth","accessToken":"access-secret"},{"id":"wrong-kind","provider":"grok-cli","authType":"apiKey","apiKey":"not-oauth"}]`)
	result := &foreignImportResult{}
	s.importN9routerConnections(context.Background(), map[string]json.RawMessage{"providerConnections": payload}, result, nil)
	require.Zero(t, result.Accounts)
	require.Len(t, result.Errors, 2)
}
