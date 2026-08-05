package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/config"
	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/store"
)

type countingQuotaSource struct{ calls atomic.Int32 }

func (s *countingQuotaSource) FetchQuota(context.Context, core.Credentials) (*connectors.QuotaResult, error) {
	s.calls.Add(1)
	return &connectors.QuotaResult{}, nil
}

func TestAdminQuotaUsage_IsDBOnlyAndIncludesAccountsBeyond48(t *testing.T) {
	ctx := context.Background()
	db, err := store.Open(ctx, config.DatabaseConfig{Driver: "sqlite", DSN: ":memory:"}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, db.Migrate(ctx))
	require.NoError(t, db.Tenants().EnsureDefault(ctx))
	t.Cleanup(func() { _ = db.Close() })

	source := &countingQuotaSource{}
	connectors.RegisterQuotaSource("snapshot-test", source)
	now := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("account-%02d", i)
		require.NoError(t, db.Accounts().Create(ctx, store.Account{ID: id, TenantID: adminTenant, Provider: "snapshot-test", AuthKind: store.AuthOAuth, CreatedAt: now, UpdatedAt: now}))
	}
	fetched := now.Add(-time.Minute)
	require.NoError(t, db.QuotaSnapshots().RecordSuccess(ctx, store.QuotaSnapshot{
		AccountID: "account-49", Provider: "snapshot-test", Payload: `{"plan_name":"Pro","upstream_quotas":[{"resource_type":"credits","remaining":42}]}`,
		State: "reported", FetchedAt: &fetched, LastAttemptAt: fetched, NextRefreshAt: now.Add(time.Minute),
	}))

	// A nil vault proves this GET cannot decrypt credentials. A hanging upstream is
	// irrelevant because the handler may only read the database.
	s := &Server{db: db, accounts: db.Accounts(), usage: db.Usage()}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/quota?period=month", nil)
	done := make(chan struct{})
	go func() { s.adminQuotaUsage(rec, req); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("quota list blocked on live work")
	}

	require.Equal(t, http.StatusOK, rec.Code)
	var body struct {
		Accounts []map[string]any `json:"accounts"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	require.Len(t, body.Accounts, 50)
	last := body.Accounts[49]
	require.Equal(t, "account-49", last["id"])
	require.Equal(t, "reported", last["quota_state"])
	require.Equal(t, "Pro", last["plan_name"])
	require.NotEmpty(t, last["fetched_at"])
	require.Len(t, last["upstream_quotas"], 1)
	require.Zero(t, source.calls.Load(), "list handler must not call quota source")
}
