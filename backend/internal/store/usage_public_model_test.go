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
