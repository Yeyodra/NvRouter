package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUsageByKeyUsesPublicModelWithoutInternalFallback(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC()
	for _, rec := range []UsageRecord{
		{ID: "public-1", APIKeyID: "key-1", Provider: "internal-provider-a", Model: "internal-model-a", PublicModel: "public-route", PromptTokens: 2, CreatedAt: now},
		{ID: "public-2", APIKeyID: "key-1", Provider: "internal-provider-b", Model: "internal-model-b", PublicModel: "public-route", CompletionTokens: 3, CreatedAt: now},
		{ID: "legacy", APIKeyID: "key-1", Provider: "secret-provider", Model: "secret-model", CreatedAt: now},
	} {
		require.NoError(t, db.Usage().Record(ctx, rec))
	}

	models, err := db.Usage().ByModelByKey(ctx, "key-1", now.Add(-time.Minute))
	require.NoError(t, err)
	require.ElementsMatch(t, []ModelUsage{
		{Model: "public-route", TotalRequests: 2, PromptTokens: 2, CompletionTokens: 3},
		{Model: "legacy", TotalRequests: 1},
	}, models)

	recent, err := db.Usage().RecentByKey(ctx, "key-1", now.Add(-time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, recent, 3)
	require.ElementsMatch(t, []string{"public-route", "public-route", "legacy"}, []string{recent[0].Model, recent[1].Model, recent[2].Model})
	for _, rec := range recent {
		require.Empty(t, rec.Provider)
	}
}

func TestMigrateAddsNullableUsagePublicModel(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	var nullable int
	require.NoError(t, db.sql.QueryRowContext(ctx, `
		SELECT "notnull" FROM pragma_table_info('usage_records') WHERE name = 'public_model'`).Scan(&nullable))
	require.Zero(t, nullable)
}

func TestMigrateAddsNonNullUsageClientIP(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	var notNull int
	require.NoError(t, db.sql.QueryRowContext(ctx, `
		SELECT "notnull" FROM pragma_table_info('usage_records') WHERE name = 'client_ip'`).Scan(&notNull))
	require.Equal(t, 1, notNull)
}

func TestRecentByKeyReturnsClientIP(t *testing.T) {
	db := newTestDB(t)
	now := time.Now().UTC()
	require.NoError(t, db.Usage().Record(context.Background(), UsageRecord{
		ID: "with-ip", APIKeyID: "key-1", Provider: "internal", Model: "internal",
		PublicModel: "public-route", ClientIP: "203.0.113.10", CreatedAt: now,
	}))

	recent, err := db.Usage().RecentByKey(context.Background(), "key-1", now.Add(-time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	require.Equal(t, "203.0.113.10", recent[0].ClientIP)
}

func TestIPActivityByKeyAggregatesAndRanksTrackedIPs(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	for _, rec := range []UsageRecord{
		{ID: "a1", APIKeyID: "key-1", ClientIP: "203.0.113.10", Provider: "p", Model: "m", PromptTokens: 100, CompletionTokens: 50, CreatedAt: now.Add(-2 * time.Hour)},
		{ID: "a2", APIKeyID: "key-1", ClientIP: "203.0.113.10", Provider: "p", Model: "m", PromptTokens: 200, CompletionTokens: 50, CreatedAt: now.Add(-time.Hour)},
		{ID: "b1", APIKeyID: "key-1", ClientIP: "2001:db8::1", Provider: "p", Model: "m", PromptTokens: 25, CompletionTokens: 25, CreatedAt: now.Add(-30 * time.Minute)},
		{ID: "legacy", APIKeyID: "key-1", Provider: "p", Model: "m", PromptTokens: 999, CreatedAt: now},
		{ID: "other", APIKeyID: "key-2", ClientIP: "198.51.100.2", Provider: "p", Model: "m", PromptTokens: 999, CreatedAt: now},
	} {
		require.NoError(t, db.Usage().Record(ctx, rec))
	}

	activity, err := db.Usage().IPActivityByKey(ctx, "key-1", now.Add(-24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, int64(3), activity.TotalRequests)
	require.Equal(t, int64(450), activity.TotalTokens)
	require.Equal(t, []IPActivity{
		{IP: "203.0.113.10", Requests: 2, Tokens: 400, FirstSeen: now.Add(-2 * time.Hour), LastSeen: now.Add(-time.Hour)},
		{IP: "2001:db8::1", Requests: 1, Tokens: 50, FirstSeen: now.Add(-30 * time.Minute), LastSeen: now.Add(-30 * time.Minute)},
	}, activity.IPs)
}
