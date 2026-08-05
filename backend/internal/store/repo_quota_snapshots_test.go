package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQuotaSnapshots_PreserveLastGoodAcrossFailureAndRestart(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := db.QuotaSnapshots()
	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, db.Accounts().Create(ctx, Account{ID: "account-1", TenantID: DefaultTenantID, Provider: "grok-cli", AuthKind: AuthOAuth, CreatedAt: now, UpdatedAt: now}))

	require.NoError(t, repo.RecordSuccess(ctx, QuotaSnapshot{
		AccountID: "account-1", Provider: "grok-cli", Payload: `{"plan_name":"Pro"}`,
		State: "fresh", FetchedAt: &now, LastAttemptAt: now, NextRefreshAt: now.Add(10 * time.Minute),
	}))
	failedAt := now.Add(time.Minute)
	require.NoError(t, repo.RecordFailure(ctx, "account-1", "grok-cli", "upstream bearer secret=abc\nfailed", failedAt, failedAt.Add(2*time.Minute)))

	got, err := repo.Get(ctx, "account-1")
	require.NoError(t, err)
	require.Equal(t, `{"plan_name":"Pro"}`, got.Payload)
	require.Equal(t, "stale", got.State)
	require.Equal(t, &now, got.FetchedAt)
	require.Equal(t, failedAt, got.LastAttemptAt)
	require.Equal(t, 1, got.ConsecutiveFailures)
	require.NotContains(t, got.LastError, "secret")
	require.NotContains(t, got.LastError, "\n")

	// The row is persisted, not process-local scheduler state.
	restarted := db.QuotaSnapshots()
	again, err := restarted.Get(ctx, "account-1")
	require.NoError(t, err)
	require.Equal(t, got, again)
}

func TestQuotaSnapshots_TransientStateSurvivesForManualDedupeAndResetsOnRestart(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := db.QuotaSnapshots()
	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, db.Accounts().Create(ctx, Account{ID: "manual", TenantID: DefaultTenantID, Provider: "kiro", AuthKind: AuthOAuth, CreatedAt: now, UpdatedAt: now}))
	require.NoError(t, repo.UpsertState(ctx, "manual", "kiro", "queued", now, now))

	got, err := repo.Get(ctx, "manual")
	require.NoError(t, err)
	require.Equal(t, "queued", got.State)
	require.Equal(t, now, got.LastAttemptAt)
	require.NoError(t, repo.ResetTransient(ctx, now.Add(30*time.Second)))
	got, err = repo.Get(ctx, "manual")
	require.NoError(t, err)
	require.Equal(t, "pending", got.State)
	require.Equal(t, now.Add(30*time.Second), got.NextRefreshAt)
}

func TestQuotaSnapshots_FailureWithoutSuccessAndEligibility(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := db.QuotaSnapshots()
	now := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, db.Accounts().Create(ctx, Account{ID: "account-2", TenantID: DefaultTenantID, Provider: "kiro", AuthKind: AuthOAuth, CreatedAt: now, UpdatedAt: now}))

	require.NoError(t, repo.RecordFailure(ctx, "account-2", "kiro", "timeout", now, now.Add(time.Minute)))
	got, err := repo.Get(ctx, "account-2")
	require.NoError(t, err)
	require.Equal(t, "error", got.State)
	require.Nil(t, got.FetchedAt)

	eligible, err := repo.ListEligible(ctx, now.Add(2*time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, eligible, 1)
	require.Equal(t, "account-2", eligible[0].AccountID)

	_, err = repo.Get(ctx, "missing")
	require.True(t, errors.Is(err, ErrNotFound))
}

func TestQuotaSnapshots_EligibilityIsFairAcrossProvidersAtLimit(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	repo := db.QuotaSnapshots()
	now := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("a-%03d", i)
		require.NoError(t, db.Accounts().Create(ctx, Account{ID: id, TenantID: DefaultTenantID, Provider: "a", AuthKind: AuthOAuth, CreatedAt: now, UpdatedAt: now}))
		require.NoError(t, repo.UpsertState(ctx, id, "a", "pending", now, now.Add(-time.Minute)))
	}
	for _, provider := range []string{"b", "c"} {
		id := provider + "-000"
		require.NoError(t, db.Accounts().Create(ctx, Account{ID: id, TenantID: DefaultTenantID, Provider: provider, AuthKind: AuthOAuth, CreatedAt: now, UpdatedAt: now}))
		require.NoError(t, repo.UpsertState(ctx, id, provider, "pending", now, now))
	}

	eligible, err := repo.ListEligible(ctx, now, 48)
	require.NoError(t, err)
	providers := map[string]bool{}
	for _, snapshot := range eligible {
		providers[snapshot.Provider] = true
	}
	require.True(t, providers["b"] && providers["c"], "small providers must survive the bounded query")
}
