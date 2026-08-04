package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestGrokCLI_Dialect(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	require.Equal(t, "grok-cli", c.ID())
	require.Equal(t, core.DialectOpenAIResponses, c.Dialect())
}

func TestRegistry_GetGrokCLI(t *testing.T) {
	r := DefaultRegistry()
	c, err := r.Get("grok-cli")
	require.NoError(t, err)
	require.Equal(t, "grok-cli", c.ID())
	require.Equal(t, core.DialectOpenAIResponses, c.Dialect())
	_, ok := c.(*GrokCLI)
	require.True(t, ok, "registry must return *GrokCLI, not generic OpenAIResponses")
}

// TestGrokCLI_RegistryCatalogConsistency locks catalog ID/aliases with NewGrokCLI registration.
func TestGrokCLI_RegistryCatalogConsistency(t *testing.T) {
	spec, ok := SpecByID("grok-cli")
	require.True(t, ok, "catalog missing grok-cli")
	require.Equal(t, "gcli", spec.Alias)
	require.Equal(t, []string{"gb", "grok-build"}, spec.Aliases)
	require.Equal(t, core.DialectOpenAIResponses, spec.Dialect)
	require.Equal(t, "oauth", spec.AuthKind)

	r := DefaultRegistry()
	c, err := r.Get(spec.ID)
	require.NoError(t, err)
	_, ok = c.(*GrokCLI)
	require.True(t, ok, "registry must wire NewGrokCLI for %s", spec.ID)

	for _, alias := range append([]string{spec.Alias}, spec.Aliases...) {
		by, ok := SpecByAlias(alias)
		require.True(t, ok, "alias %q missing", alias)
		require.Equal(t, "grok-cli", by.ID)
	}
}

func TestGrokCLI_ChatHeaders_NoTokenAuth(t *testing.T) {
	resetGrokCLITurnStore()
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	h := c.chatHeaders(core.Credentials{
		AccessToken: "tok",
		Extra: map[string]string{
			"email":  "u@example.com",
			"userId": "user-1",
		},
	}, "grok-4.5", nil)

	require.Equal(t, "Bearer tok", h["Authorization"])
	require.Equal(t, grokCLIUserAgent, h["User-Agent"])
	require.Equal(t, grokCLIIdentifier, h["x-grok-client-identifier"])
	require.Equal(t, grokCLIVersion, h["x-grok-client-version"])
	require.Equal(t, "grok-4.5", h["x-grok-model-override"])
	require.Equal(t, "u@example.com", h["x-email"])
	require.Equal(t, "user-1", h["x-userid"])
	require.NotEmpty(t, h["x-grok-session-id"])
	require.Equal(t, h["x-grok-session-id"], h["x-grok-conv-id"])
	require.NotEmpty(t, h["x-grok-req-id"])
	require.Equal(t, "1", h["x-grok-turn-idx"])
	require.NotEmpty(t, h["x-grok-agent-id"])
	_, has := h["x-xai-token-auth"]
	require.False(t, has, "chat headers must never set x-xai-token-auth")
}

func TestGrokCLI_TurnIdx_IncreasesWithUserMessages(t *testing.T) {
	resetGrokCLITurnStore()
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	creds := core.Credentials{AccessToken: "tok", AccountID: "acct-turn-msg"}

	req1 := &core.ChatRequest{
		Model: "grok-4.5",
		Messages: []core.Message{
			{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}},
		},
	}
	req2 := &core.ChatRequest{
		Model: "grok-4.5",
		Messages: []core.Message{
			{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}},
			{Role: core.RoleAssistant, Content: []core.ContentPart{{Type: core.PartText, Text: "yo"}}},
			{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "again"}}},
		},
	}

	h1 := c.chatHeaders(creds, "grok-4.5", req1)
	h2 := c.chatHeaders(creds, "grok-4.5", req2)

	require.Equal(t, "acct-turn-msg", h1["x-grok-session-id"])
	require.Equal(t, h1["x-grok-session-id"], h2["x-grok-session-id"])
	require.Equal(t, "1", h1["x-grok-turn-idx"])
	require.Equal(t, "2", h2["x-grok-turn-idx"])
}

func TestGrokCLI_TurnIdx_NeverDecreasesSameSession(t *testing.T) {
	resetGrokCLITurnStore()
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	creds := core.Credentials{AccessToken: "tok"}
	// Full history clients may send growing message lists; delta clients may
	// re-send only the latest user message (count=1). Store must not go back.
	reqFull := &core.ChatRequest{
		Model:    "grok-4.5",
		Metadata: core.RequestMetadata{ContextAffinityKey: "sess-mono"},
		Messages: []core.Message{
			{Role: core.RoleUser},
			{Role: core.RoleAssistant},
			{Role: core.RoleUser},
			{Role: core.RoleAssistant},
			{Role: core.RoleUser},
		},
	}
	reqDelta := &core.ChatRequest{
		Model:    "grok-4.5",
		Metadata: core.RequestMetadata{ContextAffinityKey: "sess-mono"},
		Messages: []core.Message{
			{Role: core.RoleUser},
		},
	}

	h1 := c.chatHeaders(creds, "grok-4.5", reqFull)
	h2 := c.chatHeaders(creds, "grok-4.5", reqDelta)
	h3 := c.chatHeaders(creds, "grok-4.5", reqDelta)

	require.Equal(t, "sess-mono", h1["x-grok-session-id"])
	require.Equal(t, "3", h1["x-grok-turn-idx"])
	// prev=3, fromInput=1 → max(1, 3+1)=4
	require.Equal(t, "4", h2["x-grok-turn-idx"])
	require.Equal(t, "5", h3["x-grok-turn-idx"])
}

func TestGrokCLI_SessionID_FromPromptCacheKey(t *testing.T) {
	resetGrokCLITurnStore()
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	req := &core.ChatRequest{
		Model: "grok-4.5",
		Extra: map[string]json.RawMessage{
			"prompt_cache_key": json.RawMessage(`"cache-key-abc"`),
		},
		Messages: []core.Message{{Role: core.RoleUser}},
	}
	h := c.chatHeaders(core.Credentials{AccessToken: "tok"}, "grok-4.5", req)
	require.Equal(t, "cache-key-abc", h["x-grok-session-id"])
	require.Equal(t, h["x-grok-session-id"], h["x-grok-conv-id"])
}

func TestGrokCLI_AgentID_FromDeviceId(t *testing.T) {
	resetGrokCLITurnStore()
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	h := c.chatHeaders(core.Credentials{
		AccessToken: "tok",
		Extra:       map[string]string{"deviceId": "dev-123"},
	}, "grok-4.5", &core.ChatRequest{Messages: []core.Message{{Role: core.RoleUser}}})
	require.Equal(t, "dev-123", h["x-grok-agent-id"])
}

func TestGrokCLI_ResolveModel_Effort(t *testing.T) {
	cases := []struct {
		in, model, effort string
	}{
		{"grok-4.5-high", "grok-4.5", "high"},
		{"grok-4.5-xhigh", "grok-4.5", "xhigh"},
		{"grok-4.5-medium", "grok-4.5", "medium"},
		{"grok-4.5-low", "grok-4.5", "low"},
		{"grok-4.5", "grok-4.5", ""},
		{"grok-build", "grok-build", ""},
		{"grok-build-high", "grok-build", ""}, // effort only for grok-4.5*
	}
	for _, tc := range cases {
		m, e := resolveGrokCLIModel(tc.in)
		if m != tc.model || e != tc.effort {
			t.Errorf("resolveGrokCLIModel(%q) = (%q, %q), want (%q, %q)", tc.in, m, e, tc.model, tc.effort)
		}
	}
}

func TestGrokCLI_PatchBody(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	raw := []byte(`{"model":"grok-4.5-high","input":[],"stream":false,"store":true,"messages":[{"role":"user"}],"max_tokens":10}`)
	out, err := c.patchBody(raw, "grok-4.5", "high")
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	require.Equal(t, "grok-4.5", m["model"])
	require.Equal(t, true, m["stream"])
	require.Equal(t, false, m["store"])

	reasoning, ok := m["reasoning"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "concise", reasoning["summary"])
	require.Equal(t, "high", reasoning["effort"])

	include := asStringSlice(m["include"])
	require.Contains(t, include, "reasoning.encrypted_content")

	_, hasMessages := m["messages"]
	require.False(t, hasMessages, "messages must be stripped")
	_, hasMax := m["max_tokens"]
	require.False(t, hasMax, "max_tokens must be stripped")
}

func TestGrokCLI_Validate_Soft402(t *testing.T) {
	var sawTokenAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-xai-token-auth") == grokCLITokenAuth {
			sawTokenAuth = true
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"code":"personal-team-blocked:spending-limit","error":"limit"}`))
	}))
	defer srv.Close()

	c := NewGrokCLI("grok-cli", srv.URL)
	err := c.Validate(context.Background(), core.Credentials{AccessToken: "tok", BaseURL: srv.URL})
	require.NoError(t, err, "HTTP 402 must be soft-success for validate")
	require.True(t, sawTokenAuth, "validate must send x-xai-token-auth")
}

func TestGrokCLI_Validate_Rejected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer srv.Close()

	c := NewGrokCLI("grok-cli", srv.URL)
	err := c.Validate(context.Background(), core.Credentials{AccessToken: "bad", BaseURL: srv.URL})
	require.Error(t, err)
}

func TestGrokCLI_Validate_OK(t *testing.T) {
	srv := modelsServer(t, http.StatusOK)
	defer srv.Close()
	c := NewGrokCLI("grok-cli", srv.URL)
	require.NoError(t, c.Validate(context.Background(), core.Credentials{AccessToken: "tok", BaseURL: srv.URL}))
}

func TestGrokCLI_RenderBody_EffortVirtual(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	req := &core.ChatRequest{
		Model: "grok-4.5-high",
		Messages: []core.Message{{
			Role:    core.RoleUser,
			Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}},
		}},
	}
	body, model, err := c.renderBody(req)
	require.NoError(t, err)
	require.Equal(t, "grok-4.5", model)
	require.Equal(t, "grok-4.5-high", req.Model, "caller model id must stay virtual")

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	require.Equal(t, "grok-4.5", m["model"])
	require.Equal(t, true, m["stream"])
	require.Equal(t, false, m["store"])
	reasoning := m["reasoning"].(map[string]any)
	require.Equal(t, "high", reasoning["effort"])
	require.Contains(t, asStringSlice(m["include"]), "reasoning.encrypted_content")
}

// Golden from 9router: system stays on instructions path — never role=developer.
// (Codex converts system→developer; Grok CLI does not.)
func TestGrokCLI_RenderBody_SystemNotDeveloper(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	req := &core.ChatRequest{
		Model:  "grok-4.5",
		System: "You are Grok",
		Messages: []core.Message{{
			Role:    core.RoleUser,
			Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}},
		}},
	}
	body, _, err := c.renderBody(req)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	require.Equal(t, "You are Grok", m["instructions"])

	// No developer role anywhere in the outbound body.
	raw := string(body)
	require.NotContains(t, raw, `"developer"`)
	require.NotContains(t, raw, `"role":"developer"`)

	input, _ := m["input"].([]any)
	for _, item := range input {
		im, ok := item.(map[string]any)
		if !ok {
			continue
		}
		require.NotEqual(t, "developer", im["role"], "system must not be rewritten to developer")
	}
}

// Golden from 9router: tools are flat Responses shape (no nested function).
func TestGrokCLI_RenderBody_ToolsFlatShape(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	req := &core.ChatRequest{
		Model: "grok-4.5",
		Messages: []core.Message{{
			Role:    core.RoleUser,
			Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}},
		}},
		Tools: []core.Tool{{
			Name:        "run_terminal_command",
			Description: "Run bash",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}}}`),
		}},
	}
	body, _, err := c.renderBody(req)
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	tools, ok := m["tools"].([]any)
	require.True(t, ok)
	require.Len(t, tools, 1)

	tool := tools[0].(map[string]any)
	require.Equal(t, "function", tool["type"])
	require.Equal(t, "run_terminal_command", tool["name"])
	require.NotNil(t, tool["parameters"])
	_, hasNested := tool["function"]
	require.False(t, hasNested, "tools must be flat Responses shape, not nested function")
}

// Golden from 9router: nested Chat Completions tool shape flattens in patchBody.
func TestGrokCLI_PatchBody_FlattenNestedTools(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	raw := []byte(`{
		"model":"grok-4.5",
		"input":[],
		"tools":[
			{"type":"function","function":{"name":"run_terminal_command","description":"Run bash","parameters":{"type":"object","properties":{"command":{"type":"string"}}}}},
			{"type":"web_search"},
			{"type":"x_search"}
		]
	}`)
	out, err := c.patchBody(raw, "grok-4.5", "")
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	tools := m["tools"].([]any)
	require.Len(t, tools, 3)

	t0 := tools[0].(map[string]any)
	require.Equal(t, "function", t0["type"])
	require.Equal(t, "run_terminal_command", t0["name"])
	require.NotNil(t, t0["parameters"])
	_, hasFn := t0["function"]
	require.False(t, hasFn)

	require.Equal(t, map[string]any{"type": "web_search"}, tools[1])
	require.Equal(t, map[string]any{"type": "x_search"}, tools[2])
}

// Hosted tools allowlist + nested function flatten + name cap 128.
func TestGrokCLI_PatchBody_HostedToolsPassthrough(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	longName := strings.Repeat("a", 140)
	raw, err := json.Marshal(map[string]any{
		"model": "grok-4.5",
		"input": []any{},
		"tools": []any{
			map[string]any{
				"type": "function",
				"function": map[string]any{
					"name":        longName,
					"description": "tool",
					"parameters":  map[string]any{"type": "object"},
				},
			},
			map[string]any{"type": "web_search"},
			map[string]any{"type": "x_search"},
			map[string]any{"type": "web_search_preview"},
			map[string]any{"type": "file_search"},
			map[string]any{"type": "image_generation"},
			map[string]any{"type": "code_interpreter"},
			map[string]any{"type": "mcp", "server_label": "exa"},
			map[string]any{"type": "local_shell"},
			map[string]any{"type": "unknown_hosted"}, // dropped
		},
	})
	require.NoError(t, err)

	out, err := c.patchBody(raw, "grok-4.5", "")
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	tools := m["tools"].([]any)
	require.Len(t, tools, 9) // function + 8 hosted; unknown dropped

	t0 := tools[0].(map[string]any)
	require.Equal(t, "function", t0["type"])
	require.Equal(t, strings.Repeat("a", 128), t0["name"])
	require.Len(t, t0["name"].(string), 128)
	_, hasFn := t0["function"]
	require.False(t, hasFn)

	hosted := []string{
		"web_search", "x_search", "web_search_preview", "file_search",
		"image_generation", "code_interpreter", "mcp", "local_shell",
	}
	for i, want := range hosted {
		got := tools[i+1].(map[string]any)
		require.Equal(t, want, got["type"], "tool index %d", i+1)
	}
	// mcp extras preserved
	require.Equal(t, "exa", tools[7].(map[string]any)["server_label"])
}

// Golden from 9router: item_reference + bare server-id strings stripped when store=false.
func TestGrokCLI_PatchBody_StripItemReference(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	raw := []byte(`{
		"model":"grok-4.5",
		"input":[
			{"type":"message","role":"system","content":"You are Grok"},
			{"type":"message","role":"user","content":"hi","id":"msg_server_id"},
			{"type":"item_reference","id":"rs_abc"},
			"rs_should_drop"
		]
	}`)
	out, err := c.patchBody(raw, "grok-4.5", "medium")
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	input := m["input"].([]any)
	require.Len(t, input, 2)

	// system role preserved (not rewritten to developer)
	require.Equal(t, "system", input[0].(map[string]any)["role"])
	require.Equal(t, "user", input[1].(map[string]any)["role"])

	for _, item := range input {
		im, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("unexpected bare item after strip: %#v", item)
		}
		require.NotEqual(t, "item_reference", im["type"])
	}
	require.Equal(t, "medium", m["reasoning"].(map[string]any)["effort"])
}

// Golden from 9router: grok-build never gets reasoning.effort (even with -high suffix).
func TestGrokCLI_RenderBody_GrokBuildNoEffort(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	req := &core.ChatRequest{
		Model: "grok-build-high",
		Messages: []core.Message{{
			Role:    core.RoleUser,
			Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}},
		}},
	}
	body, model, err := c.renderBody(req)
	require.NoError(t, err)
	require.Equal(t, "grok-build", model)

	var m map[string]any
	require.NoError(t, json.Unmarshal(body, &m))
	require.Equal(t, "grok-build", m["model"])

	reasoning, ok := m["reasoning"].(map[string]any)
	require.True(t, ok)
	_, hasEffort := reasoning["effort"]
	require.False(t, hasEffort, "grok-build must not carry reasoning.effort")
	require.Equal(t, "concise", reasoning["summary"])
}

func TestGrokCLI_PatchBody_GrokBuildNoEffort(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	// Even if a pre-rendered body had effort, empty effort arg must delete it.
	raw := []byte(`{"model":"grok-build","input":[],"reasoning":{"effort":"high","summary":"concise"}}`)
	out, err := c.patchBody(raw, "grok-build", "")
	require.NoError(t, err)

	var m map[string]any
	require.NoError(t, json.Unmarshal(out, &m))
	reasoning := m["reasoning"].(map[string]any)
	_, hasEffort := reasoning["effort"]
	require.False(t, hasEffort)
}

func TestGrokCLI_RenderBody_CrossProviderToolHistory(t *testing.T) {
	c := NewGrokCLI("grok-cli", "https://cli-chat-proxy.grok.com/v1")
	req := &core.ChatRequest{
		Model: "grok-4.5",
		Messages: []core.Message{
			{Role: core.RoleAssistant, Content: []core.ContentPart{
				{Type: core.PartThinking, Text: "foreign summary", Signature: "openai-ciphertext"},
				{Type: core.PartToolCall, ToolCall: &core.ToolCall{ID: "call-custom", Name: "exec", Arguments: json.RawMessage(`{"input":"run this"}`), Kind: core.ToolCallCustom}},
			}},
			{Role: core.RoleTool, Content: []core.ContentPart{{Type: core.PartToolResult, ToolResult: &core.ToolResult{CallID: "call-custom", Content: `[{"type":"input_text","text":"done"}]`}}}},
			{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "continue"}}},
		},
		Tools: []core.Tool{{Name: "exec", Description: "Run command", Kind: core.ToolCustom}},
	}
	body, _, err := c.renderBody(req)
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	input := out["input"].([]any)
	for _, item := range input {
		m, _ := item.(map[string]any)
		require.NotEqual(t, "reasoning", m["type"], "foreign encrypted reasoning must not be replayed to Grok")
	}
	require.Equal(t, "function_call", input[0].(map[string]any)["type"])
	require.Equal(t, "call-custom", input[0].(map[string]any)["call_id"])
	require.Equal(t, "function_call_output", input[1].(map[string]any)["type"])
	require.Equal(t, "call-custom", input[1].(map[string]any)["call_id"])

	tools := out["tools"].([]any)
	tool := tools[0].(map[string]any)
	require.Equal(t, "function", tool["type"])
	require.Equal(t, map[string]any{
		"type": "object", "properties": map[string]any{"input": map[string]any{"type": "string"}}, "required": []any{"input"},
	}, tool["parameters"])
}

func TestGrokCLI_StreamFailsClosedWithoutTerminalEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n")
	}))
	defer srv.Close()

	c := NewGrokCLI("grok-cli", srv.URL)
	stream, err := c.Stream(context.Background(), &core.ChatRequest{Model: "grok-build", Messages: []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}}}}, core.Credentials{AccessToken: "tok"}, core.StreamConfig{})
	require.NoError(t, err)
	var gotIntegrity bool
	for chunk := range stream {
		if pe := core.AsProviderError(chunk.Err); chunk.Type == core.ChunkError && pe.Kind == core.ErrResponseIntegrity {
			gotIntegrity = true
		}
	}
	require.True(t, gotIntegrity)
}
