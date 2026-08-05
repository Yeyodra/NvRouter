package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mydisha/keirouter/backend/internal/store"
	"github.com/stretchr/testify/require"
)

const publicModelCanary = "public-safe-route"

var internalIdentityCanaries = []string{
	"internal-provider-canary",
	"internal-model-canary",
	"internal-account-canary",
	"internal-workspace-canary",
	"internal-upstream-canary.invalid",
}

func TestE2E_ChatUnaryProjectsPublicModel(t *testing.T) {
	for _, tt := range []struct {
		name, path, body string
	}{
		{"openai-chat", "/v1/chat/completions", `{"model":"public-safe-route","messages":[{"role":"user","content":"hi"}]}`},
		{"openai-responses", "/v1/responses", `{"model":"public-safe-route","input":"hi"}`},
		{"anthropic", "/v1/messages", `{"model":"public-safe-route","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`},
		{"gemini", "/v1beta/models/public-safe-route:generateContent", `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newE2E(t, openAIUpstream())
			resp := h.post(t, tt.path, tt.body, h.apiKey)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assertPublicChatResponse(t, resp)
		})
	}
}

func TestE2E_ChatParsedStreamsProjectPublicModel(t *testing.T) {
	for _, tt := range []struct {
		name, path, body string
	}{
		{"openai-chat", "/v1/chat/completions", `{"model":"public-safe-route","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`},
		{"openai-responses", "/v1/responses", `{"model":"public-safe-route","stream":true,"input":"hi"}`},
		{"anthropic", "/v1/messages", `{"model":"public-safe-route","stream":true,"max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`},
		{"gemini", "/v1beta/models/public-safe-route:streamGenerateContent", `{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newE2E(t, modelCanaryStreamUpstream())
			resp := h.post(t, tt.path, tt.body, h.apiKey)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assertPublicChatResponse(t, resp)
		})
	}
}

func TestE2E_ChatDirectStreamProjectsPublicModel(t *testing.T) {
	h := newE2E(t, modelCanaryStreamUpstream())
	resp := h.post(t, "/v1/chat/completions", `{"model":"public-safe-route","stream":true,"messages":[{"role":"user","content":"hi"}]}`, h.apiKey)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assertPublicChatResponse(t, resp)
}

func TestKeyUsageProjectsPublicModels(t *testing.T) {
	h := newE2E(t, openAIUpstream())
	seedUsageProjectionCanaries(t, h)

	req, err := http.NewRequest(http.MethodGet, h.server.URL+"/v1/keys/me/usage", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+h.apiKey)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Models []map[string]any `json:"models"`
	}
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assertNoInternalUsageCanaries(t, raw)
	require.NoError(t, json.Unmarshal(raw, &body))
	assertUsageModelLabels(t, body.Models)
}

func TestPortalUsageProjectsPublicModelsAndRecent(t *testing.T) {
	h := newE2E(t, openAIUpstream())
	seedUsageProjectionCanaries(t, h)

	resp, err := http.Get(h.server.URL + "/v1/portal/keys/" + h.keyID + "/usage")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Models []map[string]any `json:"models"`
		Recent []map[string]any `json:"recent"`
	}
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assertNoInternalUsageCanaries(t, raw)
	require.NoError(t, json.Unmarshal(raw, &body))
	assertUsageModelLabels(t, body.Models)
	require.Len(t, body.Recent, 2)
	for _, entry := range body.Recent {
		_, hasProvider := entry["provider"]
		require.False(t, hasProvider)
	}
	require.ElementsMatch(t, []any{publicModelCanary, "legacy"}, []any{body.Recent[0]["model"], body.Recent[1]["model"]})
}

func seedUsageProjectionCanaries(t *testing.T, h *e2eHarness) {
	t.Helper()
	now := time.Now().UTC()
	for _, rec := range []store.UsageRecord{
		{ID: "usage-public", APIKeyID: h.keyID, Provider: "internal-provider-canary", Model: "internal-model-canary", PublicModel: publicModelCanary, AccountID: "internal-account-canary", CreatedAt: now},
		{ID: "usage-legacy", APIKeyID: h.keyID, Provider: "internal-provider-canary", Model: "internal-model-canary", AccountID: "internal-account-canary", CreatedAt: now},
	} {
		require.NoError(t, h.usage.Record(context.Background(), rec))
	}
}

func assertNoInternalUsageCanaries(t *testing.T, raw []byte) {
	t.Helper()
	for _, canary := range []string{"internal-provider-canary", "internal-model-canary", "internal-account-canary"} {
		require.NotContains(t, string(raw), canary)
	}
}

func assertUsageModelLabels(t *testing.T, models []map[string]any) {
	t.Helper()
	require.Len(t, models, 2)
	for _, model := range models {
		_, hasProvider := model["provider"]
		require.False(t, hasProvider)
	}
	require.ElementsMatch(t, []any{publicModelCanary, "legacy"}, []any{models[0]["model"], models[1]["model"]})
}

func modelCanaryStreamUpstream() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, line := range []string{
			`data: {"id":"chatcmpl-canary","model":"internal-model-canary","choices":[{"delta":{"role":"assistant","content":"hi"}}]}`,
			`data: {"id":"chatcmpl-canary","model":"internal-model-canary","choices":[{"delta":{},"finish_reason":"stop"}]}`,
		} {
			fmt.Fprintf(w, "%s\n\n", line)
		}
	}
}

func assertPublicChatResponse(t *testing.T, resp *http.Response) {
	t.Helper()
	for _, name := range []string{"X-KeiRouter-Provider", "X-KeiRouter-Model", "X-KeiRouter-Account"} {
		require.Empty(t, resp.Header.Get(name), "%s leaked routing identity", name)
	}
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	text := string(raw)
	for _, canary := range internalIdentityCanaries {
		require.NotContains(t, text, canary)
		for name, values := range resp.Header {
			require.NotContains(t, strings.Join(values, ","), canary, "header %s leaked identity", name)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" || !json.Valid([]byte(payload)) {
			continue
		}
		var value any
		require.NoError(t, json.Unmarshal([]byte(payload), &value))
		assertModelFields(t, value)
	}
	if json.Valid(raw) {
		var value any
		require.NoError(t, json.Unmarshal(raw, &value))
		assertModelFields(t, value)
	}
}

func assertModelFields(t *testing.T, value any) {
	t.Helper()
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if key == "model" {
				require.Equal(t, publicModelCanary, child)
			}
			assertModelFields(t, child)
		}
	case []any:
		for _, child := range value {
			assertModelFields(t, child)
		}
	}
}
