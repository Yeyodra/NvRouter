package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/budget"
	"github.com/mydisha/keirouter/backend/internal/config"
	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/consolelog"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/crypto"
	"github.com/mydisha/keirouter/backend/internal/dispatch"
	"github.com/mydisha/keirouter/backend/internal/identity"
	"github.com/mydisha/keirouter/backend/internal/meter"
	"github.com/mydisha/keirouter/backend/internal/pipeline"
	"github.com/mydisha/keirouter/backend/internal/store"
	"github.com/mydisha/keirouter/backend/internal/transform"
	"github.com/mydisha/keirouter/backend/internal/vault"
)

// e2eHarness wires a full gateway against an in-memory store and a fake upstream.
type e2eHarness struct {
	server     *httptest.Server
	apiKey     string
	upstream   *httptest.Server
	consoleLog *consolelog.Buffer
	usage      *store.UsageRepo
}

func newE2E(t *testing.T, upstreamHandler http.HandlerFunc, registries ...*transform.Registry) *e2eHarness {
	t.Helper()
	ctx := context.Background()

	// Fake upstream provider.
	upstream := httptest.NewServer(upstreamHandler)
	t.Cleanup(upstream.Close)

	// In-memory store.
	db, err := store.Open(ctx, config.DatabaseConfig{Driver: "sqlite", DSN: ":memory:"}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, db.Migrate(ctx))
	require.NoError(t, db.Tenants().EnsureDefault(ctx))
	t.Cleanup(func() { _ = db.Close() })

	// Crypto + vault.
	mk, err := crypto.GenerateMasterKey()
	require.NoError(t, err)
	sealer, err := crypto.NewSealer(mk)
	require.NoError(t, err)
	v := vault.New(sealer)

	// Seed an account for the "openai" provider pointing at the fake upstream.
	acc := store.Account{
		ID:        "acc-test",
		TenantID:  store.DefaultTenantID,
		Provider:  "openai",
		Label:     "test",
		AuthKind:  store.AuthAPIKey,
		Priority:  10,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	require.NoError(t, v.Seal(&acc, vault.NewSecret{
		APIKey:   "sk-upstream",
		Metadata: map[string]string{"base_url": upstream.URL},
	}))
	require.NoError(t, db.Accounts().Create(ctx, acc))

	// Issue an API key.
	idSvc := identity.New(db.APIKeys())
	issued, err := idSvc.Create(ctx, store.DefaultTenantID, "", "test-key")
	require.NoError(t, err)

	// Wire pipeline + gateway.
	connRegistry := connectors.DefaultRegistry()
	disp := dispatch.New(connRegistry, db.Accounts(), v)
	mtr := meter.New(db.Usage(), nil, nil)
	bud := budget.New(db.Budgets(), db.Usage())
	pipe := pipeline.New(pipeline.Deps{Dispatcher: disp, Meter: mtr, Budget: bud})

	codecs := transform.DefaultRegistry()
	if len(registries) > 0 {
		codecs = registries[0]
	}
	conLog := consolelog.New()
	gw := New(Deps{
		Config:     config.Default(),
		Identity:   idSvc,
		Pipeline:   pipe,
		Chains:     db.Chains(),
		Codecs:     codecs,
		ConsoleLog: conLog,
	})

	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	return &e2eHarness{server: srv, apiKey: issued.Plaintext, upstream: upstream, consoleLog: conLog, usage: db.Usage()}
}

func (h *e2eHarness) post(t *testing.T, path, body, auth string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if auth != "" {
		req.Header.Set("Authorization", "Bearer "+auth)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// openAIUpstream returns a handler that responds as an OpenAI chat endpoint.
func openAIUpstream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"up1","model":"gpt-4o","choices":[{"message":{"role":"assistant","content":"hello from upstream"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`)
	}
}

func TestE2E_OpenAIChat_DirectProviderModel(t *testing.T) {
	h := newE2E(t, openAIUpstream())

	body := `{"model":"openai/gpt-4o","messages":[{"role":"user","content":"hi"}]}`
	resp := h.post(t, "/v1/chat/completions", body, h.apiKey)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "openai", resp.Header.Get("X-KeiRouter-Provider"))

	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Choices, 1)
	require.Equal(t, "hello from upstream", out.Choices[0].Message.Content)
}

func TestE2E_RejectsMissingAuth(t *testing.T) {
	h := newE2E(t, openAIUpstream())
	resp := h.post(t, "/v1/chat/completions", `{"model":"openai/gpt-4o","messages":[]}`, "")
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestE2E_RejectsBadKey(t *testing.T) {
	h := newE2E(t, openAIUpstream())
	resp := h.post(t, "/v1/chat/completions", `{"model":"openai/gpt-4o","messages":[]}`, "kr_invalid")
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestE2E_UnknownModelIsBadRequest(t *testing.T) {
	h := newE2E(t, openAIUpstream())
	resp := h.post(t, "/v1/chat/completions", `{"model":"unknown-bare-name","messages":[{"role":"user","content":"hi"}]}`, h.apiKey)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// Cross-dialect: a client speaks Anthropic (/v1/messages) but routes to an
// OpenAI-dialect provider. The gateway must translate both ways.
func TestE2E_AnthropicClientToOpenAIProvider(t *testing.T) {
	h := newE2E(t, openAIUpstream())

	body := `{"model":"openai/gpt-4o","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`
	resp := h.post(t, "/v1/messages", body, h.apiKey)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Response must be in Anthropic shape (content blocks + stop_reason).
	var out struct {
		Type    string `json:"type"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StopReason string `json:"stop_reason"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, "message", out.Type)
	require.Len(t, out.Content, 1)
	require.Equal(t, "hello from upstream", out.Content[0].Text)
	require.Equal(t, "end_turn", out.StopReason)
}

func assertStreamFailed(t *testing.T, h *e2eHarness, out string) {
	t.Helper()
	require.Contains(t, out, `"type":"upstream_error"`)
	require.NotContains(t, out, "invalid arguments")

	var failed, completed bool
	for _, entry := range h.consoleLog.Entries() {
		failed = failed || strings.HasPrefix(entry.Message, "Request failed")
		completed = completed || strings.HasPrefix(entry.Message, "Request completed")
	}
	require.True(t, failed)
	require.False(t, completed)
}

func TestE2E_StreamingToolSanitizerErrorFailsStream(t *testing.T) {
	for _, finish := range []bool{true, false} {
		t.Run(fmt.Sprintf("finish=%v", finish), func(t *testing.T) {
			h := newE2E(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintln(w, `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"Read","arguments":"["}}]}}]}`)
				if finish {
					fmt.Fprintln(w, `data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`)
				}
				fmt.Fprintln(w, "data: [DONE]")
			})

			resp := h.post(t, "/v1/chat/completions", `{"model":"openai/gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"Read","parameters":{"type":"object"}}}]}`, h.apiKey)
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assertStreamFailed(t, h, string(raw))
			recent, err := h.usage.RecentAccurate(context.Background(), store.DefaultTenantID, time.Now().Add(-time.Minute), 10)
			require.NoError(t, err)
			require.Len(t, recent, 1)
			require.Equal(t, "failed", recent[0].Status)
			require.Equal(t, string(core.ErrResponseIntegrity), recent[0].ErrorKind)
		})
	}
}

type failingRenderCodec struct{ transform.OpenAICodec }

func (failingRenderCodec) RenderStreamChunk(chunk core.StreamChunk, state *transform.StreamState) ([][]byte, error) {
	if chunk.Type == core.ChunkText {
		return nil, fmt.Errorf("render failed")
	}
	return transform.OpenAICodec{}.RenderStreamChunk(chunk, state)
}

func TestE2E_DirectMalformedStreamIsNotAccountedAsSuccess(t *testing.T) {
	h := newE2E(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"ok"}}]}`)
		fmt.Fprintln(w, `data: {bad`)
	})

	resp := h.post(t, "/v1/chat/completions", `{"model":"openai/gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`, h.apiKey)
	defer resp.Body.Close()
	_, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	recent, err := h.usage.RecentAccurate(context.Background(), store.DefaultTenantID, time.Now().Add(-time.Minute), 10)
	require.NoError(t, err)
	require.Len(t, recent, 1)
	require.Equal(t, "failed", recent[0].Status)
	require.Equal(t, string(core.ErrResponseIntegrity), recent[0].ErrorKind)
	assertStreamFailed(t, h, string(streamErrorEvent(core.DialectOpenAI, "upstream provider request failed")))
}

func TestE2E_StreamingRendererErrorFailsStream(t *testing.T) {
	for _, content := range []string{"text", "<"} {
		t.Run(content, func(t *testing.T) {
			codec := failingRenderCodec{}
			h := newE2E(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", content)
				fmt.Fprintln(w, "data: [DONE]")
			}, transform.NewRegistry(codec))

			resp := h.post(t, "/v1/chat/completions", `{"model":"openai/gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"Read","parameters":{"type":"object"}}}]}`, h.apiKey)
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assertStreamFailed(t, h, string(raw))
		})
	}
}

func TestE2E_AnthropicTranslatedFailureHasNoSuccessTerminal(t *testing.T) {
	h := newE2E(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintln(w, `data: {"choices":[{"delta":{"content":"partial"}}]}`)
		fmt.Fprintln(w, `data: {bad`)
	})

	resp := h.post(t, "/v1/messages", `{"model":"openai/gpt-4o","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, h.apiKey)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := string(raw)
	require.Contains(t, out, "event: error\n")
	require.NotContains(t, out, `"stop_reason":"end_turn"`)
	require.NotContains(t, out, "event: message_stop\n")
}

func TestE2E_StreamingChat(t *testing.T) {
	streamUpstream := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flush, _ := w.(http.Flusher)
		for _, l := range []string{
			`data: {"choices":[{"delta":{"role":"assistant","content":"par"}}]}`,
			`data: {"choices":[{"delta":{"content":"tial"}}]}`,
			`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`data: [DONE]`,
		} {
			fmt.Fprintf(w, "%s\n\n", l)
			if flush != nil {
				flush.Flush()
			}
		}
	}
	h := newE2E(t, streamUpstream)

	body := `{"model":"openai/gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	resp := h.post(t, "/v1/chat/completions", body, h.apiKey)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := string(raw)

	require.Contains(t, out, `"content":"par"`)
	require.Contains(t, out, `"content":"tial"`)
	require.Contains(t, out, "[DONE]")
}

// streamNoUsageUpstream streams content + a finish chunk but NEVER reports
// usage — the common case for many OpenAI-compatible providers that reject
// stream_options.include_usage. Used to prove the pipeline synthesizes a usage
// event for clients that opted in.
func streamNoUsageUpstream() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flush, _ := w.(http.Flusher)
		for _, l := range []string{
			`data: {"choices":[{"delta":{"role":"assistant","content":"par"}}]}`,
			`data: {"choices":[{"delta":{"content":"tial"}}]}`,
			`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`data: [DONE]`,
		} {
			fmt.Fprintf(w, "%s\n\n", l)
			if flush != nil {
				flush.Flush()
			}
		}
	}
}

// lastStreamUsage scans an OpenAI SSE body and returns the last usage block
// found across all chunks (nil if none carried usage).
func lastStreamUsage(t *testing.T, sse string) *core.Usage {
	t.Helper()
	var found *core.Usage
	for _, line := range strings.Split(sse, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var chunk struct {
			Usage *core.Usage `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			continue
		}
		if chunk.Usage != nil {
			u := *chunk.Usage
			found = &u
		}
	}
	return found
}

// TestE2E_StreamingChat_IncludeUsageInjected proves the improvement: when the
// client opts in via stream_options.include_usage and the provider never
// reports usage, the gateway synthesizes and delivers a usage event.
func TestE2E_StreamingChat_IncludeUsageInjected(t *testing.T) {
	h := newE2E(t, streamNoUsageUpstream())

	body := `{"model":"openai/gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`
	resp := h.post(t, "/v1/chat/completions", body, h.apiKey)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := string(raw)

	// Content still streamed through.
	require.Contains(t, out, `"content":"par"`)
	require.Contains(t, out, "[DONE]")

	// A usage event was synthesized and delivered to the client.
	usage := lastStreamUsage(t, out)
	require.NotNil(t, usage, "expected an injected usage event, got none")
	require.Greater(t, usage.CompletionTokens, 0, "completion tokens should be estimated from streamed output")
	require.Greater(t, usage.TotalTokens, 0, "total tokens should be non-zero")
}

// TestE2E_StreamingChat_NoUsageWithoutOptIn is the regression guard: without
// stream_options.include_usage, a provider that omits usage must NOT get a
// synthesized usage event (respect the OpenAI contract + preserve prior
// behavior).
func TestE2E_StreamingChat_NoUsageWithoutOptIn(t *testing.T) {
	h := newE2E(t, streamNoUsageUpstream())

	body := `{"model":"openai/gpt-4o","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	resp := h.post(t, "/v1/chat/completions", body, h.apiKey)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := string(raw)

	require.Contains(t, out, `"content":"par"`)
	require.Contains(t, out, "[DONE]")
	require.Nil(t, lastStreamUsage(t, out), "no usage event should be injected without include_usage")
}

// TestE2E_StreamingChat_ProviderUsageNotDoubled verifies that when the provider
// DOES report usage, we pass it through and do not overwrite it with an
// estimate (sawUsage short-circuits injection).
func TestE2E_StreamingChat_ProviderUsageNotDoubled(t *testing.T) {
	streamWithUsage := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flush, _ := w.(http.Flusher)
		for _, l := range []string{
			`data: {"choices":[{"delta":{"role":"assistant","content":"hi"}}]}`,
			`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":22,"total_tokens":33}}`,
			`data: [DONE]`,
		} {
			fmt.Fprintf(w, "%s\n\n", l)
			if flush != nil {
				flush.Flush()
			}
		}
	}
	h := newE2E(t, streamWithUsage)

	body := `{"model":"openai/gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`
	resp := h.post(t, "/v1/chat/completions", body, h.apiKey)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	usage := lastStreamUsage(t, string(raw))
	require.NotNil(t, usage)
	// The real provider numbers survive — not replaced by the ~4 char/token estimate.
	require.Equal(t, 11, usage.PromptTokens)
	require.Equal(t, 22, usage.CompletionTokens)
	require.Equal(t, 33, usage.TotalTokens)
}
