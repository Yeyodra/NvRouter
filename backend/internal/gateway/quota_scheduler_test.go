package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/config"
	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/store"
)

func newQuotaTestDB(t *testing.T) *store.DB {
	t.Helper()
	db, err := store.Open(context.Background(), config.DatabaseConfig{Driver: "sqlite", DSN: ":memory:"}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, db.Migrate(context.Background()))
	require.NoError(t, db.Tenants().EnsureDefault(context.Background()))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestQuotaSchedulePolicy(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	q := quotaPayload{RemainingRatio: 0.5}
	next := nextQuotaRefresh(now, q, nil, 0, 0)
	require.WithinDuration(t, now.Add(12*time.Minute+30*time.Second), next, 150*time.Second)

	q.RemainingRatio = 0.03
	q.HasRemainingRatio = true
	next = nextQuotaRefresh(now, q, nil, 0, 0)
	require.True(t, next.After(now.Add(2*time.Minute)) && next.Before(now.Add(5*time.Minute)))
	q.RemainingRatio = 0
	q.HasRemainingRatio = true
	next = nextQuotaRefresh(now, q, nil, 0, 0)
	require.True(t, next.After(now.Add(2*time.Minute)) && next.Before(now.Add(5*time.Minute)), "zero remaining refreshes quickly")

	next = nextQuotaRefresh(now, quotaPayload{}, context.DeadlineExceeded, 3, 0)
	require.Equal(t, now.Add(8*time.Minute), next)
	pe := &core.ProviderError{Kind: core.ErrRateLimit, RetryAfter: 17 * time.Minute}
	require.Equal(t, now.Add(17*time.Minute), nextQuotaRefresh(now, quotaPayload{}, pe, 1, 0))
	pe = &core.ProviderError{Kind: core.ErrAuth}
	require.Equal(t, now.Add(6*time.Hour), nextQuotaRefresh(now, quotaPayload{}, pe, 1, 0))
}

func TestQuotaErrorMessageDoesNotPersistRawUpstreamBody(t *testing.T) {
	raw := errors.New("upstream rejected api_key=super-sensitive-value")
	require.Equal(t, "upstream quota refresh failed", quotaErrorMessage(raw))
	require.Equal(t, "quota authentication failed", quotaErrorMessage(&core.ProviderError{Kind: core.ErrAuth, Message: "Bearer super-sensitive-value"}))
}

func TestFairQuotaOrder_RoundRobinsProvidersAndAllAccounts(t *testing.T) {
	items := make([]store.QuotaSnapshot, 0, 52)
	for i := 0; i < 50; i++ {
		items = append(items, store.QuotaSnapshot{AccountID: fmt.Sprintf("a%02d", i), Provider: "a"})
	}
	items = append(items, store.QuotaSnapshot{AccountID: "b00", Provider: "b"}, store.QuotaSnapshot{AccountID: "b01", Provider: "b"})

	got := fairQuotaOrder(items)
	require.Len(t, got, 52)
	require.Equal(t, []string{"a00", "b00", "a01", "b01"}, []string{got[0].AccountID, got[1].AccountID, got[2].AccountID, got[3].AccountID})
	require.Equal(t, "a49", got[51].AccountID)
}

func TestQuotaSchedulePolicy_UnsupportedHasNoNextAttempt(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	require.True(t, nextQuotaRefresh(now, quotaPayload{}, errQuotaUnsupported, 0, 0).IsZero())
}

func TestQuotaScheduler_ManualDedupeCooldownAndPersistence(t *testing.T) {
	ctx := context.Background()
	db := newQuotaTestDB(t)
	now := time.Now().UTC()
	account := store.Account{ID: "manual", TenantID: adminTenant, Provider: "manual-test", AuthKind: store.AuthOAuth, CreatedAt: now, UpdatedAt: now}
	require.NoError(t, db.Accounts().Create(ctx, account))
	s := newQuotaScheduler(db.QuotaSnapshots(), db.Accounts(), func(context.Context, store.Account) (quotaPayload, error) { return quotaPayload{}, nil })

	require.True(t, s.enqueue(account, true))
	require.False(t, s.enqueue(account, true), "in-memory dedupe")
	persisted, err := db.QuotaSnapshots().Get(ctx, account.ID)
	require.NoError(t, err)
	require.Equal(t, "queued", persisted.State)

	restarted := newQuotaScheduler(db.QuotaSnapshots(), db.Accounts(), s.fetch)
	require.False(t, restarted.enqueue(account, true), "persisted queued state dedupes after restart")
	require.NoError(t, db.QuotaSnapshots().UpsertState(ctx, account.ID, account.Provider, "reported", now, now))
	require.False(t, restarted.enqueue(account, true), "persisted last attempt enforces cooldown")
}

func TestQuotaScheduler_BoundsGlobalProviderAndAccountConcurrency(t *testing.T) {
	var global, globalMax atomic.Int32
	var mu sync.Mutex
	byProvider, providerMax, byAccount := map[string]int{}, map[string]int{}, map[string]int{}
	started := make(chan struct{}, 12)
	release := make(chan struct{})
	fetch := func(ctx context.Context, account store.Account) (quotaPayload, error) {
		g := global.Add(1)
		for old := globalMax.Load(); g > old && !globalMax.CompareAndSwap(old, g); old = globalMax.Load() {
		}
		mu.Lock()
		byProvider[account.Provider]++
		providerMax[account.Provider] = max(providerMax[account.Provider], byProvider[account.Provider])
		byAccount[account.ID]++
		require.Equal(t, 1, byAccount[account.ID])
		mu.Unlock()
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		mu.Lock()
		byProvider[account.Provider]--
		byAccount[account.ID]--
		mu.Unlock()
		global.Add(-1)
		return quotaPayload{}, nil
	}
	s := newQuotaScheduler(nil, nil, fetch)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startWorkers(ctx)
	for i := 0; i < 6; i++ {
		require.True(t, s.enqueue(store.Account{ID: string(rune('a' + i)), Provider: "p1"}, false))
		require.False(t, s.enqueue(store.Account{ID: string(rune('a' + i)), Provider: "p1"}, false), "account dedupe")
		require.True(t, s.enqueue(store.Account{ID: string(rune('k' + i)), Provider: "p2"}, false))
	}
	for i := 0; i < 4; i++ {
		<-started
	}
	time.Sleep(20 * time.Millisecond)
	mu.Lock()
	require.LessOrEqual(t, providerMax["p1"], 2)
	require.LessOrEqual(t, providerMax["p2"], 2)
	mu.Unlock()
	require.LessOrEqual(t, globalMax.Load(), int32(8))
	close(release)
}

func TestQuotaScheduler_RunLoopWaitsForWorkersOnShutdown(t *testing.T) {
	ctx := context.Background()
	db := newQuotaTestDB(t)
	now := time.Now().UTC()
	account := store.Account{ID: "shutdown", TenantID: adminTenant, Provider: "shutdown-test", AuthKind: store.AuthOAuth, CreatedAt: now, UpdatedAt: now}
	require.NoError(t, db.Accounts().Create(ctx, account))
	connectors.RegisterQuotaSource("shutdown-test", &countingQuotaSource{})
	started := make(chan struct{})
	release := make(chan struct{})
	s := newQuotaScheduler(db.QuotaSnapshots(), db.Accounts(), func(context.Context, store.Account) (quotaPayload, error) {
		close(started)
		<-release
		return quotaPayload{}, nil
	})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { s.runLoop(runCtx); close(done) }()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("scheduler returned while a worker still owned DB work")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not finish after worker exited")
	}
}
