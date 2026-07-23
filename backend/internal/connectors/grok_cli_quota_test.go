package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

var grokCLIActiveBilling = map[string]any{
	"config": map[string]any{
		"currentPeriod": map[string]any{
			"type":  "USAGE_PERIOD_TYPE_WEEKLY",
			"start": "2026-07-08T00:00:00+00:00",
			"end":   "2026-07-15T00:00:00+00:00",
		},
		"onDemandCap":          map[string]any{"val": 100},
		"onDemandUsed":         map[string]any{"val": 35},
		"isUnifiedBillingUser": true,
		"prepaidBalance":       map[string]any{"val": 12.5},
		"billingPeriodStart":   "2026-07-08T00:00:00+00:00",
		"billingPeriodEnd":     "2026-07-15T00:00:00+00:00",
	},
}

var grokCLIExhaustedBilling = map[string]any{
	"config": map[string]any{
		"currentPeriod": map[string]any{
			"type":  "USAGE_PERIOD_TYPE_WEEKLY",
			"start": "2026-07-08T00:00:00+00:00",
			"end":   "2026-07-15T00:00:00+00:00",
		},
		"onDemandCap":          map[string]any{"val": 0},
		"onDemandUsed":         map[string]any{"val": 0},
		"isUnifiedBillingUser": true,
		"prepaidBalance":       map[string]any{"val": 0},
		"billingPeriodStart":   "2026-07-08T00:00:00+00:00",
		"billingPeriodEnd":     "2026-07-15T00:00:00+00:00",
	},
}

var grokCLIUserProfile = map[string]any{
	"userId":            "d84768dd-224d-4052-ba49-0d336fa9160c",
	"email":             "user@example.com",
	"hasGrokCodeAccess": true,
	"subscriptionTier":  nil,
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func quotaByType(q *QuotaResult, resource string) (QuotaEntry, bool) {
	for _, e := range q.Quotas {
		if e.ResourceType == resource {
			return e, true
		}
	}
	return QuotaEntry{}, false
}

func remainingPercentage(e QuotaEntry) float64 {
	if e.Limit <= 0 {
		return 0
	}
	return float64(e.Remaining) / float64(e.Limit) * 100
}

func TestParseGrokCLIBilling_Active(t *testing.T) {
	parsed := parseGrokCLIBilling(mustJSON(t, grokCLIActiveBilling), mustJSON(t, grokCLIUserProfile))
	require.Equal(t, "Grok Code", parsed.PlanName)

	od, ok := quotaByType(parsed, "On-demand")
	require.True(t, ok)
	require.Equal(t, 35, od.Used)
	require.Equal(t, 100, od.Limit)
	require.Equal(t, 65, od.Remaining)
	require.InDelta(t, 65, remainingPercentage(od), 0.01)
	require.Equal(t, "2026-07-15T00:00:00+00:00", od.ResetAt)

	pre, ok := quotaByType(parsed, "Prepaid")
	require.True(t, ok)
	require.Equal(t, 0, pre.Used)
	require.Equal(t, 13, pre.Limit) // 12.5 rounded
	require.Equal(t, pre.Limit, pre.Remaining)
	require.InDelta(t, 100, remainingPercentage(pre), 0.01)
}

func TestParseGrokCLIBilling_ExhaustedCapZero(t *testing.T) {
	parsed := parseGrokCLIBilling(mustJSON(t, grokCLIExhaustedBilling), mustJSON(t, grokCLIUserProfile))

	od, ok := quotaByType(parsed, "On-demand")
	require.True(t, ok, "cap=0 must yield synthetic depleted On-demand row")
	require.Equal(t, 1, od.Used)
	require.Equal(t, 1, od.Limit)
	require.Equal(t, 0, od.Remaining)
	require.InDelta(t, 0, remainingPercentage(od), 0.01)
	require.NotEqual(t, 100, remainingPercentage(od), "must not look unlimited")
}

func TestParseGrokCLIBilling_SubscriptionTierPlan(t *testing.T) {
	user := map[string]any{
		"userId":            "x",
		"hasGrokCodeAccess": true,
		"subscriptionTier":  "super_grok",
	}
	parsed := parseGrokCLIBilling(mustJSON(t, grokCLIActiveBilling), mustJSON(t, user))
	require.Equal(t, "Super Grok", parsed.PlanName)
}

func TestParseGrokCLIBilling_PaidSubNoDepletedRow(t *testing.T) {
	user := map[string]any{
		"userId":            "x",
		"hasGrokCodeAccess": true,
		"subscriptionTier":  "XPremiumPlus",
	}
	parsed := parseGrokCLIBilling(mustJSON(t, grokCLIExhaustedBilling), mustJSON(t, user))
	require.Equal(t, "XPremiumPlus", parsed.PlanName)
	_, ok := quotaByType(parsed, "On-demand")
	require.False(t, ok, "paid subscription must not get synthetic depleted on-demand")
	require.NotEmpty(t, parsed.Message)
}

func TestParseGrokCLIBilling_MonthlyIncluded(t *testing.T) {
	billing := map[string]any{
		"monthlyLimit": map[string]any{"val": 1000},
		"includedUsed": map[string]any{"val": 275},
		"totalUsed":    map[string]any{"val": 300},
		"resetAt":      "2026-08-01T00:00:00Z",
	}
	user := map[string]any{"subscription_tier": "premium_plus"}
	parsed := parseGrokCLIBilling(mustJSON(t, billing), mustJSON(t, user))
	require.Equal(t, "Premium Plus", parsed.PlanName)

	m, ok := quotaByType(parsed, "Monthly included")
	require.True(t, ok)
	require.Equal(t, 275, m.Used)
	require.Equal(t, 1000, m.Limit)
	require.Equal(t, 725, m.Remaining)
	require.InDelta(t, 72.5, remainingPercentage(m), 0.01)
	require.Equal(t, "2026-08-01T00:00:00Z", m.ResetAt)
}

func TestUnwrapGrokCLIVal(t *testing.T) {
	f, ok := unwrapGrokCLIVal(map[string]any{"val": 12.5})
	require.True(t, ok)
	require.Equal(t, 12.5, f)

	f, ok = unwrapGrokCLIVal(float64(7))
	require.True(t, ok)
	require.Equal(t, 7.0, f)

	_, ok = unwrapGrokCLIVal(nil)
	require.False(t, ok)
}

func TestGrokCLI_FetchQuota_HTTPtest(t *testing.T) {
	var sawBillingAuth, sawUserAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		require.Equal(t, grokCLIUserAgent, r.Header.Get("User-Agent"))
		require.Equal(t, grokCLITokenAuth, r.Header.Get("x-xai-token-auth"))
		require.Equal(t, grokCLIVersion, r.Header.Get("x-grok-client-version"))
		require.Equal(t, grokCLIIdentifier, r.Header.Get("x-grok-client-identifier"))
		require.Equal(t, "headless", r.Header.Get("x-grok-client-mode"))

		switch {
		case r.URL.Path == "/v1/billing" && r.URL.Query().Get("format") == "credits":
			sawBillingAuth = true
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(grokCLIActiveBilling)
		case r.URL.Path == "/v1/user" && r.URL.Query().Get("include") == "subscription":
			sawUserAuth = true
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(grokCLIUserProfile)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewGrokCLI("grok-cli", srv.URL+"/v1")
	q, err := c.FetchQuota(context.Background(), core.Credentials{AccessToken: "test-token"})
	require.NoError(t, err)
	require.True(t, sawBillingAuth)
	require.True(t, sawUserAuth)
	require.Equal(t, "Grok Code", q.PlanName)

	od, ok := quotaByType(q, "On-demand")
	require.True(t, ok)
	require.Equal(t, 65, od.Remaining)
	require.InDelta(t, 65, remainingPercentage(od), 0.01)

	pre, ok := quotaByType(q, "Prepaid")
	require.True(t, ok)
	require.Equal(t, 0, pre.Used)
	require.Greater(t, pre.Limit, 0)
}

func TestGrokCLI_FetchQuota_Billing402Degrades(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/billing" {
			http.Error(w, `{"error":"spending-limit"}`, http.StatusPaymentRequired)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := NewGrokCLI("grok-cli", srv.URL+"/v1")
	q, err := c.FetchQuota(context.Background(), core.Credentials{AccessToken: "tok"})
	require.NoError(t, err)
	require.NotEmpty(t, q.Message)
	require.Empty(t, q.Quotas)
}

func TestGrokCLI_FetchQuota_NoCredential(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://example.invalid/v1")
	q, err := c.FetchQuota(context.Background(), core.Credentials{})
	require.NoError(t, err)
	require.Contains(t, q.Message, "No credential")
}

func TestRegisterQuotaSource_GrokCLI(t *testing.T) {
	// DefaultRegistry registers LiveModelSource + QuotaSource for grok-cli.
	_ = DefaultRegistry()
	src := GetQuotaSource("grok-cli")
	require.NotNil(t, src)
	_, ok := src.(*GrokCLI)
	require.True(t, ok)
}
