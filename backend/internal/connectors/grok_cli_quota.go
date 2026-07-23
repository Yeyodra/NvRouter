package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strings"
	"unicode"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// FetchQuota probes GET /billing?format=credits and GET /user?include=subscription
// with validate/probe headers. Degrades to QuotaResult.Message on 402/empty/auth
// errors instead of failing the call hard (dashboard keeps working).
func (c *GrokCLI) FetchQuota(ctx context.Context, creds core.Credentials) (*QuotaResult, error) {
	token := creds.AccessToken
	if token == "" {
		token = creds.APIKey
	}
	if token == "" {
		return &QuotaResult{Message: "No credential; cannot fetch quota."}, nil
	}

	base := strings.TrimRight(c.baseURL(creds), "/")
	base = strings.TrimSuffix(base, "/responses")
	billingURL := base + "/billing?format=credits"
	userURL := base + "/user?include=subscription"
	headers := c.validateHeaders(creds)

	billingBody, err := doJSONMethod(ctx, http.MethodGet, c.id, "quota", billingURL, nil, headers)
	if err != nil {
		return grokCLIQuotaFetchError(err), nil
	}

	// User profile is optional — billing alone is enough to render bars.
	userBody, _ := doJSONMethod(ctx, http.MethodGet, c.id, "quota", userURL, nil, headers)

	return parseGrokCLIBilling(billingBody, userBody), nil
}

func grokCLIQuotaFetchError(err error) *QuotaResult {
	if pe, ok := err.(*core.ProviderError); ok {
		switch pe.Kind {
		case core.ErrAuth:
			return &QuotaResult{Message: "Grok CLI authentication expired. Please re-authorize."}
		case core.ErrQuotaExhausted:
			// 402 on billing is rare; surface soft message (chat soft-402 is separate).
			return &QuotaResult{Message: "Grok CLI billing reported payment required / spending limit."}
		default:
			msg := pe.Message
			if msg == "" {
				msg = pe.Error()
			}
			return &QuotaResult{Message: fmt.Sprintf("Grok CLI billing API error: %s", msg)}
		}
	}
	return &QuotaResult{Message: "Grok CLI usage error: " + err.Error()}
}

// parseGrokCLIBilling maps billing (+ optional user) JSON to QuotaResult.
// Mirrors 9router open-sse/services/usage/grok-cli.js parseGrokCliBilling.
func parseGrokCLIBilling(billingJSON, userJSON []byte) *QuotaResult {
	var root map[string]any
	if err := json.Unmarshal(billingJSON, &root); err != nil || root == nil {
		return &QuotaResult{Message: "Grok CLI billing response was not JSON."}
	}

	config := root
	if cfg, ok := root["config"].(map[string]any); ok && cfg != nil {
		config = cfg
	}

	var user map[string]any
	if len(userJSON) > 0 {
		_ = json.Unmarshal(userJSON, &user)
	}

	periodEnd := firstGrokCLIPeriodEnd(config, root)
	tier := grokCLISubscriptionTier(user, config)
	subscriptionAccess := tier != "" && !grokCLIFreeTier.MatchString(tier)
	plan := resolveGrokCLIPlan(user, config, tier)

	result := &QuotaResult{PlanName: plan}

	// Monthly included (top-level or config).
	monthlyLimit, hasMonthly := unwrapGrokCLIVal(firstAny(
		config["monthlyLimit"], config["monthly_limit"],
		root["monthlyLimit"], root["monthly_limit"],
	))
	if hasMonthly && monthlyLimit > 0 {
		used := 0.0
		if u, ok := unwrapGrokCLIVal(firstAny(
			config["includedUsed"], config["included_used"],
			root["includedUsed"], root["included_used"],
		)); ok {
			used = u
		} else if u, ok := unwrapGrokCLIVal(firstAny(
			config["totalUsed"], config["total_used"],
			root["totalUsed"], root["total_used"],
		)); ok {
			used = u
		}
		result.Quotas = append(result.Quotas, grokCLIQuotaEntry("Monthly included", used, monthlyLimit, periodEnd, plan))
	}

	// On-demand spending window.
	onDemandCap, hasCap := unwrapGrokCLIVal(firstAny(config["onDemandCap"], root["onDemandCap"]))
	onDemandUsed, hasUsed := unwrapGrokCLIVal(firstAny(config["onDemandUsed"], root["onDemandUsed"]))
	if hasCap && onDemandCap > 0 {
		used := 0.0
		if hasUsed {
			used = math.Max(0, onDemandUsed)
		}
		result.Quotas = append(result.Quotas, grokCLIQuotaEntry("On-demand", used, onDemandCap, periodEnd, plan))
	} else if !subscriptionAccess && hasCap && onDemandCap == 0 && hasUsed {
		// Cap 0 = exhausted free/promo (chat 402 spending-limit).
		// Synthetic 1/1 depleted row so UI shows 0% remaining, not unlimited.
		result.Quotas = append(result.Quotas, QuotaEntry{
			ResourceType: "On-demand",
			Used:         1,
			Limit:        1,
			Remaining:    0,
			ResetAt:      periodEnd,
			PlanName:     plan,
		})
	}

	// Prepaid top-up balance (remaining pot; show full bar).
	if prepaid, ok := unwrapGrokCLIVal(firstAny(config["prepaidBalance"], root["prepaidBalance"])); ok && prepaid > 0 {
		result.Quotas = append(result.Quotas, QuotaEntry{
			ResourceType: "Prepaid",
			Used:         0,
			Limit:        grokCLIQuotaInt(prepaid),
			Remaining:    grokCLIQuotaInt(prepaid),
			PlanName:     plan,
		})
	}

	if len(result.Quotas) == 0 {
		if subscriptionAccess {
			result.Message = "Subscription access is active; Grok does not expose a numeric included quota."
		} else {
			result.Message = "Grok Build connected, but no credit allotment was returned. Free promo may be exhausted."
		}
	}
	return result
}

func grokCLIQuotaEntry(resource string, used, total float64, resetAt, plan string) QuotaEntry {
	limit := grokCLIQuotaInt(total)
	u := grokCLIQuotaInt(used)
	if u < 0 {
		u = 0
	}
	rem := limit - u
	if rem < 0 {
		rem = 0
	}
	return QuotaEntry{
		ResourceType: resource,
		Used:         u,
		Limit:        limit,
		Remaining:    rem,
		ResetAt:      resetAt,
		PlanName:     plan,
	}
}

func grokCLIQuotaInt(v float64) int {
	if v <= 0 {
		return 0
	}
	return int(math.Round(v))
}

// unwrapGrokCLIVal reads protobuf-json `{ "val": n }` or plain numbers/strings.
// ok is false when the value is missing or non-finite.
func unwrapGrokCLIVal(v any) (float64, bool) {
	if v == nil {
		return 0, false
	}
	if m, ok := v.(map[string]any); ok {
		if raw, exists := m["val"]; exists {
			return toFiniteFloat(raw)
		}
		return 0, false
	}
	return toFiniteFloat(v)
}

func toFiniteFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return 0, false
		}
		return n, true
	case float32:
		f := float64(n)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	case json.Number:
		f, err := n.Float64()
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case string:
		var f float64
		if _, err := fmt.Sscanf(strings.TrimSpace(n), "%f", &f); err != nil {
			return 0, false
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func firstAny(vals ...any) any {
	for _, v := range vals {
		if v != nil {
			return v
		}
	}
	return nil
}

func firstGrokCLIPeriodEnd(config, root map[string]any) string {
	candidates := []any{
		config["billingPeriodEnd"], config["billing_period_end"],
		config["resetAt"], config["resetsAt"], config["periodEnd"],
		root["billingPeriodEnd"], root["billing_period_end"],
		root["resetAt"], root["resetsAt"], root["periodEnd"],
	}
	if cp, ok := config["currentPeriod"].(map[string]any); ok {
		candidates = append([]any{cp["end"]}, candidates...)
	}
	for _, c := range candidates {
		if s, ok := c.(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

var grokCLIFreeTier = regexp.MustCompile(`(?i)^(free|none|null)$`)

func grokCLISubscriptionTier(user, config map[string]any) string {
	for _, src := range []map[string]any{user, config} {
		if src == nil {
			continue
		}
		for _, key := range []string{"subscriptionTier", "subscription_tier"} {
			if s, ok := src[key].(string); ok {
				if t := strings.TrimSpace(s); t != "" {
					return t
				}
			}
		}
		if sub, ok := src["subscription"].(map[string]any); ok {
			if s, ok := sub["tier"].(string); ok {
				if t := strings.TrimSpace(s); t != "" {
					return t
				}
			}
		}
	}
	return ""
}

func resolveGrokCLIPlan(user, config map[string]any, tier string) string {
	if tier != "" {
		return titleCaseGrokCLITier(tier)
	}
	if user != nil {
		if v, ok := user["hasGrokCodeAccess"].(bool); ok && v {
			return "Grok Code"
		}
	}
	return "Grok Build"
}

func titleCaseGrokCLITier(tier string) string {
	// No separators: keep as-is (XPremiumPlus). With _/-: Super Grok style.
	if !strings.ContainsAny(tier, "_- \t") {
		return tier
	}
	parts := strings.FieldsFunc(tier, func(r rune) bool {
		return r == '_' || r == '-' || unicode.IsSpace(r)
	})
	for i, p := range parts {
		if p == "" {
			continue
		}
		runes := []rune(strings.ToLower(p))
		runes[0] = unicode.ToUpper(runes[0])
		parts[i] = string(runes)
	}
	return strings.Join(parts, " ")
}
