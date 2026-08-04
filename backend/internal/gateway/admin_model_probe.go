package gateway

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/dispatch"
	"github.com/mydisha/keirouter/backend/internal/pipeline"
)

const modelTestTimeout = 30 * time.Second

type modelTestRequest struct {
	Provider  string `json:"provider"`
	Model     string `json:"model"`
	AccountID string `json:"account_id,omitempty"`
	Prompt    string `json:"prompt,omitempty"`
}

func (s *Server) adminTestModel(w http.ResponseWriter, r *http.Request) {
	var body modelTestRequest
	if !decodeJSON(w, r, &body) {
		return
	}
	body.Provider = strings.TrimSpace(body.Provider)
	body.Model = strings.TrimSpace(body.Model)
	body.AccountID = strings.TrimSpace(body.AccountID)
	if body.Provider == "" || body.Model == "" {
		writeModelTestError(w, http.StatusBadRequest, "provider and model are required", core.ErrBadRequest, 0, 0)
		return
	}
	if !validLLMModel(body.Provider, body.Model) {
		writeModelTestError(w, http.StatusBadRequest, "unknown provider/model or model is not an LLM", core.ErrBadRequest, 0, 0)
		return
	}

	planOpts := dispatch.PlanOptions{}
	if body.AccountID != "" {
		account, err := s.accounts.Get(r.Context(), body.AccountID)
		if err != nil || account.TenantID != adminTenant || account.Provider != body.Provider {
			writeModelTestError(w, http.StatusBadRequest, "account does not belong to provider", core.ErrBadRequest, 0, 0)
			return
		}
		planOpts.AllowedAccountIDs = map[string]struct{}{body.AccountID: {}}
	}

	prompt := strings.TrimSpace(body.Prompt)
	if prompt == "" {
		prompt = "Reply with exactly: OK"
	}
	maxTokens := 64
	req := &core.ChatRequest{
		Model: body.Model,
		Messages: []core.Message{{
			Role:    core.RoleUser,
			Content: []core.ContentPart{{Type: core.PartText, Text: prompt}},
		}},
		MaxTokens: &maxTokens,
		Stream:    false,
		Metadata: core.RequestMetadata{
			TenantID:      adminTenant,
			SourceDialect: core.DialectOpenAI,
			Provider:      body.Provider,
		},
	}

	ctx, cancel := context.WithTimeout(r.Context(), modelTestTimeout)
	defer cancel()
	started := time.Now()
	result, err := s.pipeline.Chat(ctx, req, pipeline.Options{
		Targets:  []dispatch.Target{{Provider: body.Provider, Model: body.Model}},
		PlanOpts: planOpts,
	})
	latencyMS := time.Since(started).Milliseconds()
	if err != nil {
		pe := core.AsProviderError(err)
		writeModelTestError(w, providerErrorHTTPStatus(pe), sanitizeUpstreamError(pe), pe.Kind, pe.StatusCode, latencyMS)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":              true,
		"requested_model": body.Model,
		"provider":        result.Provider,
		"actual_model":    result.Model,
		"account_id":      result.AccountID,
		"content":         result.Response.Message.TextContent(),
		"latency_ms":      latencyMS,
		"usage": map[string]int{
			"prompt_tokens":     result.Response.Usage.PromptTokens,
			"completion_tokens": result.Response.Usage.CompletionTokens,
			"total_tokens":      result.Response.Usage.TotalTokens,
		},
	})
}

func validLLMModel(provider, model string) bool {
	if _, ok := connectors.SpecByID(provider); !ok {
		return false
	}
	for _, candidate := range connectors.ModelsForProvider(provider) {
		if candidate.ID == model {
			return candidate.Kind == "" || candidate.Kind == core.ServiceLLM
		}
	}
	return false
}

func writeModelTestError(w http.ResponseWriter, status int, message string, kind core.ErrorKind, upstreamStatus int, latencyMS int64) {
	writeJSON(w, status, map[string]any{
		"ok":              false,
		"error":           message,
		"error_type":      kind,
		"kind":            kind,
		"upstream_status": upstreamStatus,
		"latency_ms":      latencyMS,
	})
}

func providerErrorHTTPStatus(pe *core.ProviderError) int {
	switch pe.Kind {
	case core.ErrBadRequest, core.ErrCapability:
		return http.StatusBadRequest
	case core.ErrModelUnavailable:
		return http.StatusNotFound
	case core.ErrAuth:
		return http.StatusUnauthorized
	case core.ErrRateLimit:
		return http.StatusTooManyRequests
	case core.ErrQuotaExhausted, core.ErrBudgetBlocked:
		return http.StatusPaymentRequired
	case core.ErrPolicyBlocked:
		return http.StatusForbidden
	case core.ErrTimeout:
		return http.StatusGatewayTimeout
	case core.ErrInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusBadGateway
	}
}
