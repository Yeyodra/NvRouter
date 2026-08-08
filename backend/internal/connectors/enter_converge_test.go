package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func enterTestRequest(model string) *core.ChatRequest {
	max := 42
	temp, topP := .7, .8
	return &core.ChatRequest{
		Model: model, MaxTokens: &max, Temperature: &temp, TopP: &topP,
		Messages: []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hello"}}}},
		Tools:    []core.Tool{{Name: "lookup", Parameters: json.RawMessage(`{"type":"object"}`)}}, ToolChoice: "auto",
		Reasoning: &core.ReasoningConfig{Effort: "high"},
	}
}

func TestEnterCanonicalModelUsesNativeUpstreamID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["model"] != "openai/gpt-5.6-sol" {
			t.Fatalf("upstream model = %v, want openai/gpt-5.6-sol", body["model"])
		}
		_, _ = w.Write([]byte(`{"model":"openai/gpt-5.6-sol","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()

	c := NewEnterConverge(server.URL)
	resp, err := c.Chat(context.Background(), enterTestRequest("gpt-5.6-sol"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Model != "gpt-5.6-sol" {
		t.Fatalf("response model = %q, want gpt-5.6-sol", resp.Model)
	}
}

func TestEnterClaudeUsesNativeMessagesTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Fatalf("path = %q, want /messages", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer ek_test" || r.Header.Get("X-Workspace-ID") != "ws" || r.Header.Get("anthropic-version") != "2023-06-01" {
			t.Fatalf("unexpected headers: %v", r.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["model"] != "anthropic/claude-opus-4.8" || body["max_tokens"] != float64(42) || body["top_p"] != 0.8 {
			t.Fatalf("body = %v", body)
		}
		if _, ok := body["temperature"]; ok {
			t.Fatalf("Opus request retained unsupported temperature: %v", body)
		}
		thinking, _ := body["thinking"].(map[string]any)
		if thinking["type"] != "enabled" {
			t.Fatalf("thinking = %v", body["thinking"])
		}
		tools, _ := body["tools"].([]any)
		if len(tools) != 1 {
			t.Fatalf("tools = %v", body["tools"])
		}
		_, _ = w.Write([]byte(`{"type":"message","role":"assistant","model":"claude-opus-4-8","content":[{"type":"tool_use","id":"tool-1","name":"lookup","input":{"q":1}}],"stop_reason":"tool_use","usage":{"input_tokens":5,"output_tokens":2}}`))
	}))
	defer server.Close()

	c := NewEnterConverge(server.URL)
	resp, err := c.Chat(context.Background(), enterTestRequest("claude-opus-4.8"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}})
	if err != nil {
		t.Fatal(err)
	}
	var tool *core.ToolCall
	for _, part := range resp.Message.Content {
		if part.ToolCall != nil {
			tool = part.ToolCall
			break
		}
	}
	if resp.Model != "claude-opus-4.8" || tool == nil || tool.Name != "lookup" {
		t.Fatalf("response = %+v", resp)
	}
}

func TestEnterClaudeStreamsNativeMessages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"internal-route\",\"usage\":{\"input_tokens\":1}}}\n\n"))
		_, _ = w.Write([]byte("event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n"))
		_, _ = w.Write([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n"))
		_, _ = w.Write([]byte("event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	stream, err := NewEnterConverge(server.URL).Stream(context.Background(), enterTestRequest("claude-opus-5"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var text string
	var finished bool
	for chunk := range stream {
		if chunk.Type == core.ChunkText {
			text += chunk.Delta
		}
		if chunk.Type == core.ChunkFinish {
			finished = true
		}
	}
	if text != "hi" || !finished {
		t.Fatalf("text = %q, finished = %v", text, finished)
	}
}

func TestEnterClaudeRejectsStreamWithoutMessageStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"))
	}))
	defer server.Close()

	stream, err := NewEnterConverge(server.URL).Stream(context.Background(), enterTestRequest("claude-opus-5"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	assertEnterStreamIntegrityError(t, stream)
}

func TestEnterClaudeAllowsNexusUsageAfterMessageStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"message_stop\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"nexus_usage\",\"credits_used\":1}\n\n"))
	}))
	defer server.Close()

	stream, err := NewEnterConverge(server.URL).Stream(context.Background(), enterTestRequest("claude-opus-5"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var finished bool
	for chunk := range stream {
		if chunk.Type == core.ChunkError {
			t.Fatalf("unexpected error: %v", chunk.Err)
		}
		if chunk.Type == core.ChunkFinish {
			finished = true
		}
	}
	if !finished {
		t.Fatal("missing finish")
	}
}

func TestEnterClaudeRejectsContentAfterMessageStop(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"message_stop\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"late\"}}\n\n"))
	}))
	defer server.Close()

	stream, err := NewEnterConverge(server.URL).Stream(context.Background(), enterTestRequest("claude-opus-5"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	assertEnterStreamIntegrityError(t, stream)
}

func TestEnterClaudeRejectsContentAfterStopReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"late\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	stream, err := NewEnterConverge(server.URL).Stream(context.Background(), enterTestRequest("claude-opus-5"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	assertEnterStreamIntegrityError(t, stream)
}

func TestEnterClaudeRejectsMessageStopWithoutStopReason(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"message_stop\"}\n\n"))
	}))
	defer server.Close()

	stream, err := NewEnterConverge(server.URL).Stream(context.Background(), enterTestRequest("claude-opus-5"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	assertEnterStreamIntegrityError(t, stream)
}

func assertEnterStreamIntegrityError(t *testing.T, stream <-chan core.StreamChunk) {
	t.Helper()
	var gotErr error
	var finished bool
	for chunk := range stream {
		if chunk.Type == core.ChunkError {
			gotErr = chunk.Err
		}
		if chunk.Type == core.ChunkFinish {
			finished = true
		}
	}
	if gotErr == nil || core.AsProviderError(gotErr).Kind != core.ErrResponseIntegrity || finished {
		t.Fatalf("error = %v, finished = %v", gotErr, finished)
	}
}

func TestEnterWorkspaceResolutionHeadersCacheAndValidation(t *testing.T) {
	var workspaces int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer ek_test" || r.Header.Get("Origin") != "https://enter.converge.ai" || r.Header.Get("Referer") != "https://enter.converge.ai/" || r.Header.Get("User-Agent") != enterUA {
			t.Errorf("unexpected headers: %v", r.Header)
		}
		switch r.URL.Path {
		case "/workspaces":
			atomic.AddInt32(&workspaces, 1)
			_, _ = w.Write([]byte(`{"data":{"workspaces":[{"id":10000373107}]}}`))
		case "/ai-capability/models":
			if r.Header.Get("X-Workspace-ID") != "10000373107" {
				t.Errorf("workspace header = %q", r.Header.Get("X-Workspace-ID"))
			}
			_, _ = w.Write([]byte(`{"data":{"models":[{"id":"openai/gpt-5.6-sol"},{"id":"google/gemini-3.5-flash"}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c := NewEnterConverge(server.URL)
	creds := core.Credentials{APIKey: "ek_test", Extra: map[string]string{}}
	if err := c.Validate(context.Background(), creds); err != nil {
		t.Fatal(err)
	}
	models, err := c.ListModels(context.Background(), creds)
	if err != nil {
		t.Fatal(err)
	}
	if atomic.LoadInt32(&workspaces) != 1 {
		t.Fatalf("workspace calls = %d, want 1", workspaces)
	}
	if len(models) != 1 || models[0].ID != "gpt-5.6-sol" {
		t.Fatalf("models = %+v", models)
	}
	if got := enterStoredWorkspace(core.Credentials{Extra: map[string]string{"workspaceId": "camel", "workspace_id": "snake"}}); got != "camel" {
		t.Fatalf("camel alias = %q", got)
	}
	if got := enterStoredWorkspace(core.Credentials{Extra: map[string]string{"workspace_id": "snake"}}); got != "snake" {
		t.Fatalf("snake alias = %q", got)
	}
}

func TestEnterChatTransformsNonstreamAndRetry(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			http.NotFound(w, r)
			return
		}
		if atomic.AddInt32(&attempts, 1) < 3 {
			http.Error(w, `{"error":"bad gateway"}`, http.StatusBadGateway)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		for _, key := range []string{"max_tokens", "temperature", "top_p", "reasoning_effort", "thinking", "reasoning"} {
			if _, ok := body[key]; ok {
				t.Errorf("body retained %s: %v", key, body)
			}
		}
		if body["max_completion_tokens"] != float64(42) || body["tool_choice"] != "auto" {
			t.Errorf("body = %v", body)
		}
		_, _ = w.Write([]byte(`{"model":"openai/gpt-5.6-sol","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	c := NewEnterConverge(server.URL)
	resp, err := c.Chat(context.Background(), enterTestRequest("openai/gpt-5.6-sol"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Message.TextContent() != "ok" || atomic.LoadInt32(&attempts) != 3 {
		t.Fatalf("resp=%+v attempts=%d", resp, attempts)
	}
}

func TestEnterStreamFragmentedToolsAndNoPostStartRetry(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\"}}]}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"1}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n"))
	}))
	defer server.Close()
	c := NewEnterConverge(server.URL)
	stream, err := c.Stream(context.Background(), enterTestRequest("minimax/minimax-m3"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var text, args string
	for chunk := range stream {
		if chunk.Type == core.ChunkText {
			text += chunk.Delta
		}
		if chunk.ToolCall != nil {
			args += string(chunk.ToolCall.Arguments)
		}
	}
	if text != "hi" || args != `{"q":1}` || atomic.LoadInt32(&attempts) != 1 {
		t.Fatalf("text=%q args=%q attempts=%d", text, args, attempts)
	}
}

func TestEnter402IsPermanentExactModelScope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"credits"}`, http.StatusPaymentRequired)
	}))
	defer server.Close()
	c := NewEnterConverge(server.URL)
	_, err := c.Chat(context.Background(), enterTestRequest("openai/gpt-5.6-sol"), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}})
	pe := core.AsProviderError(err)
	if pe.EffectiveScope() != core.FailureScopeModel || pe.Model != "openai/gpt-5.6-sol" || pe.CreditsExhausted || pe.RetryAfter < 9*365*24*time.Hour {
		t.Fatalf("error = %+v", pe)
	}
}

func TestEnterQuotaPreservesZeroAndSubscriptionEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/workspaces/ws/credits/dashboard":
			_, _ = w.Write([]byte(`{"code":0,"data":{"credits_balance":{"total":0}}}`))
		case "/workspaces/ws/subscription/status":
			_, _ = w.Write([]byte(`{"code":0,"data":{"plan_type":"free","entitlement":{"daily_credits":100}}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	quota, err := NewEnterConverge(server.URL).FetchQuota(context.Background(), core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}})
	if err != nil {
		t.Fatal(err)
	}
	if quota.PlanName != "free" || len(quota.Quotas) != 1 || quota.Quotas[0].Remaining != 0 || quota.Quotas[0].Limit != 100 || quota.Quotas[0].Used != 100 {
		t.Fatalf("quota = %+v", quota)
	}
}

func TestEnterRemoteImageUsesProxyAndRejectsInvalidMIME(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("not an image"))
	}))
	defer proxy.Close()
	c := NewEnterConverge("https://example.invalid")
	req := enterTestRequest("anthropic/claude-opus-4.6")
	req.Messages[0].Content = []core.ContentPart{{Type: core.PartImage, Media: &core.MediaPayload{URL: "http://image.example.invalid/image.png"}}}
	_, _, err := c.prepare(context.Background(), req, core.Credentials{APIKey: "ek_test", ProxyURL: proxy.URL, Extra: map[string]string{"workspace_id": "ws"}}, false)
	if err == nil || !strings.Contains(err.Error(), "not an image") {
		t.Fatalf("expected MIME failure through proxy, got %v", err)
	}
}

func TestEnterRemoteImageSSRFAndDataURI(t *testing.T) {
	c := NewEnterConverge("https://example.invalid")
	req := enterTestRequest("anthropic/claude-opus-4.6")
	req.Messages[0].Content = []core.ContentPart{{Type: core.PartImage, Media: &core.MediaPayload{URL: "http://127.0.0.1/image.png"}}}
	if _, _, err := c.prepare(context.Background(), req, core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, false); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf("expected SSRF failure, got %v", err)
	}
	req.Messages[0].Content[0].Media = &core.MediaPayload{MIMEType: "image/png", Data: "aGVsbG8="}
	body, _, err := c.prepare(context.Background(), req, core.Credentials{APIKey: "ek_test", Extra: map[string]string{"workspace_id": "ws"}}, false)
	if err != nil || !strings.Contains(string(body), "data:image/png;base64,aGVsbG8=") {
		t.Fatalf("body=%s err=%v", body, err)
	}
}
