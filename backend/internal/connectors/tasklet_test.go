package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestTaskletCatalogAndRegistry(t *testing.T) {
	spec, ok := SpecByAlias("tl")
	require.True(t, ok)
	require.Equal(t, "tasklet", spec.ID)
	require.Equal(t, "https://api.tasklet.ai", spec.BaseURL)
	require.Equal(t, "api_key", spec.AuthKind)
	require.Equal(t, core.DialectOpenAI, spec.Dialect)

	models := ModelsForProvider("tasklet")
	require.Len(t, models, 23)
	for _, id := range []string{"claude-haiku-4.5", "gpt-5.6-sol", "gemini-pro-3.1-preview", "muse-spark-1.1"} {
		model, found := FindModel("tasklet", id)
		require.True(t, found, id)
		require.Equal(t, core.ServiceLLM, model.Kind)
	}

	conn, err := DefaultRegistry().Get("tasklet")
	require.NoError(t, err)
	require.IsType(t, &Tasklet{}, conn)
	require.Same(t, conn, GetQuotaSource("tasklet"))
	require.Nil(t, GetLiveModelSource("tasklet"))
}

func TestTaskletProxyURLRoutesHTTPSubmitAndWebSocketDial(t *testing.T) {
	httpHit := make(chan struct{}, 1)
	websocketHit := make(chan struct{}, 1)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "tasklet.invalid", r.Host)
		switch r.URL.Path {
		case "/api/sendChatMessage":
			httpHit <- struct{}{}
			writeJSONResponse(t, w, map[string]any{"agentId": "agent-1"})
		case "/api/sync":
			websocketHit <- struct{}{}
			serveTaskletWS(t, w, r, []map[string]any{{"type": "syncUpdate", "state": map[string]any{"runState": map[string]any{"type": "idle"}}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer proxy.Close()

	resp, err := NewTasklet("http://tasklet.invalid").Chat(context.Background(), taskletRequest(), core.Credentials{APIKey: "token", ProxyURL: proxy.URL})
	require.NoError(t, err)
	require.Empty(t, resp.Message.TextContent())
	require.Len(t, httpHit, 1)
	require.Len(t, websocketHit, 1)
}

func TestTaskletRelayUsesCanonicalHTTPOriginForWebSocket(t *testing.T) {
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "http://tasklet.invalid", r.Header.Get("x-relay-target"))
		switch r.Header.Get("x-relay-path") {
		case "/api/sendChatMessage":
			writeJSONResponse(t, w, map[string]any{"agentId": "agent-1"})
		case "/api/sync":
			serveTaskletWS(t, w, r, []map[string]any{{"type": "syncUpdate", "state": map[string]any{"runState": map[string]any{"type": "idle"}}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer relay.Close()

	_, err := NewTasklet("http://tasklet.invalid").Chat(context.Background(), taskletRequest(), core.Credentials{APIKey: "token", RelayURL: relay.URL})
	require.NoError(t, err)
}

func TestTaskletUnaryRequestAndWebSocket(t *testing.T) {
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/sendChatMessage":
			require.Equal(t, "Bearer session-token", r.Header.Get("Authorization"))
			require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
			writeJSONResponse(t, w, map[string]any{"agentId": "agent-1"})
		case "/api/sync":
			serveTaskletWS(t, w, r, []map[string]any{
				{"type": "blocksUpdate", "updates": map[string]any{"a": map[string]any{"type": "thinking", "content": "plan"}, "b": map[string]any{"type": "agent_content", "content": "hello"}}},
				{"type": "blocksUpdate", "updates": map[string]any{"b": map[string]any{"type": "agent_content", "content": "hello world"}}},
				{"type": "syncUpdate", "state": map[string]any{"runState": map[string]any{"type": "idle"}}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	c := NewTasklet(server.URL)
	resp, err := c.Chat(context.Background(), taskletRequest(), core.Credentials{APIKey: "session-token", Extra: map[string]string{"workspace_id": "ws-1"}})
	require.NoError(t, err)
	require.Equal(t, "hello world", resp.Message.TextContent())
	require.Equal(t, "gpt-5.6-sol", resp.Model)
	require.Equal(t, "gpt_5_6_sol", payload["modelConfig"].(map[string]any)["model"])
	require.Equal(t, "ws-1", payload["workspaceId"])
	require.Equal(t, "[System]: rules\n\n[Assistant]: prior\n\n[Tool Call: lookup]\n\n[Tool Result (call-1)]: result\n\nquestion", payload["message"])
}

func TestTaskletStreamEmitsIncrementalCanonicalChunks(t *testing.T) {
	server := newTaskletServer(t, []map[string]any{
		{"type": "blocksUpdate", "updates": map[string]any{"t": map[string]any{"type": "thinking", "content": "A"}}},
		{"type": "blocksUpdate", "updates": map[string]any{"c": map[string]any{"type": "agent_content", "content": "Hi"}}},
		{"type": "blocksUpdate", "updates": map[string]any{"t": map[string]any{"type": "thinking", "content": "AB"}}},
		{"type": "blocksUpdate", "updates": map[string]any{"c": map[string]any{"type": "agent_content", "content": "Hi!"}}},
		{"type": "syncUpdate", "state": map[string]any{"runState": map[string]any{"type": "idle"}}},
	})
	defer server.Close()

	chunks, err := NewTasklet(server.URL).Stream(context.Background(), taskletRequest(), core.Credentials{APIKey: "token"}, core.StreamConfig{})
	require.NoError(t, err)
	var got []core.StreamChunk
	for chunk := range chunks {
		got = append(got, chunk)
	}
	require.Equal(t, []core.ChunkType{core.ChunkThinking, core.ChunkText, core.ChunkThinking, core.ChunkText, core.ChunkFinish}, chunkTypes(got))
	require.Equal(t, []string{"A", "Hi", "B", "!", ""}, chunkDeltas(got))
}

func TestTaskletStreamFailuresAreCanonicalAndPreciselyClassified(t *testing.T) {
	tests := []struct {
		name, signal string
		wantKind     core.ErrorKind
		wantScope    core.FailureScope
	}{
		{"quota", "insufficient credits", core.ErrQuotaExhausted, core.FailureScopeAccount},
		{"model", "model not found", core.ErrModelUnavailable, core.FailureScopeModel},
		{"provider", "agent crashed", core.ErrUpstream, core.FailureScopeProvider},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newTaskletServer(t, []map[string]any{{"type": "syncUpdate", "state": map[string]any{"runState": map[string]any{"type": "error", "error": tt.signal}}}})
			defer server.Close()
			chunks, err := NewTasklet(server.URL).Stream(context.Background(), taskletRequest(), core.Credentials{APIKey: "token"}, core.StreamConfig{})
			require.NoError(t, err)
			got := collectTaskletChunks(chunks)
			require.Len(t, got, 1)
			require.Equal(t, core.ChunkError, got[0].Type)
			pe := core.AsProviderError(got[0].Err)
			require.Equal(t, tt.wantKind, pe.Kind)
			require.Equal(t, tt.wantScope, pe.EffectiveScope())
			require.Empty(t, got[0].Delta)
		})
	}
}

func TestTaskletEmptyIdleAndClosedStreamAreNotQuota(t *testing.T) {
	t.Run("empty idle is valid completion", func(t *testing.T) {
		server := newTaskletServer(t, []map[string]any{{"type": "syncUpdate", "state": map[string]any{"runState": map[string]any{"type": "idle"}}}})
		defer server.Close()
		chunks, err := NewTasklet(server.URL).Stream(context.Background(), taskletRequest(), core.Credentials{APIKey: "token"}, core.StreamConfig{})
		require.NoError(t, err)
		require.Equal(t, []core.ChunkType{core.ChunkFinish}, chunkTypes(collectTaskletChunks(chunks)))
	})
	t.Run("early close is integrity", func(t *testing.T) {
		server := newTaskletServer(t, nil)
		defer server.Close()
		chunks, err := NewTasklet(server.URL).Stream(context.Background(), taskletRequest(), core.Credentials{APIKey: "token"}, core.StreamConfig{})
		require.NoError(t, err)
		got := collectTaskletChunks(chunks)
		require.Len(t, got, 1)
		require.Equal(t, core.ErrResponseIntegrity, core.AsProviderError(got[0].Err).Kind)
	})
}

func TestTaskletErrorDeliveryDoesNotBlockAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan core.StreamChunk, 1)
	out <- core.StreamChunk{Type: core.ChunkText, Delta: "buffered"}
	cancel()

	done := make(chan struct{})
	go func() {
		emitTaskletError(ctx, out, taskletError(core.ErrUpstream, core.FailureScopeProvider, "model", 0, "failed", nil))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal error delivery blocked on a full channel after cancellation")
	}
}

func TestTaskletCancellationProducesCanonicalClientCanceled(t *testing.T) {
	server := newTaskletServer(t, []map[string]any{{"type": "hold"}})
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	chunks, err := NewTasklet(server.URL).Stream(ctx, taskletRequest(), core.Credentials{APIKey: "token"}, core.StreamConfig{})
	require.NoError(t, err)
	cancel()
	got := collectTaskletChunks(chunks)
	require.Len(t, got, 1)
	require.Equal(t, core.ChunkError, got[0].Type)
	require.Equal(t, core.ErrClientCanceled, core.AsProviderError(got[0].Err).Kind)
}

func TestTaskletHTTPStatusClassification(t *testing.T) {
	tests := []struct {
		status int
		body   string
		kind   core.ErrorKind
		scope  core.FailureScope
	}{
		{400, `{"error":"bad payload"}`, core.ErrBadRequest, core.FailureScopeRequest},
		{401, `{"error":"invalid token"}`, core.ErrAuth, core.FailureScopeAccount},
		{404, `{"error":"model not found"}`, core.ErrModelUnavailable, core.FailureScopeModel},
		{429, `{"error":"rate limited"}`, core.ErrRateLimit, core.FailureScopeAccount},
		{429, `{"error":"credits exhausted"}`, core.ErrQuotaExhausted, core.FailureScopeAccount},
		{503, `{"error":"down"}`, core.ErrUpstream, core.FailureScopeProvider},
	}
	for _, tt := range tests {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tt.status)
			_, _ = w.Write([]byte(tt.body))
		}))
		_, err := NewTasklet(server.URL).Stream(context.Background(), taskletRequest(), core.Credentials{APIKey: "token"}, core.StreamConfig{})
		server.Close()
		pe := core.AsProviderError(err)
		require.Equal(t, tt.kind, pe.Kind, tt.body)
		require.Equal(t, tt.scope, pe.EffectiveScope(), tt.body)
	}
}

func TestTaskletValidateAndQuota(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		if r.URL.Path == "/api/profile" {
			writeJSONResponse(t, w, map[string]any{"id": "user"})
			return
		}
		var body map[string]string
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "org-1", body["organizationId"])
		writeJSONResponse(t, w, map[string]any{"grants": []map[string]any{{"amount": 0, "consumed": 0, "type": "daily_bonus_credits", "expiration": "2030-01-01T00:00:00Z"}}})
	}))
	defer server.Close()
	c := NewTasklet(server.URL)
	require.NoError(t, c.Validate(context.Background(), core.Credentials{APIKey: "token"}))
	quota, err := c.FetchQuota(context.Background(), core.Credentials{APIKey: "token", Extra: map[string]string{"organization_id": "org-1"}})
	require.NoError(t, err)
	require.Equal(t, []string{"/api/profile", "/api/billing/creditGrants"}, paths)
	require.Len(t, quota.Quotas, 1)
	require.Zero(t, quota.Quotas[0].Limit)
	require.Zero(t, quota.Quotas[0].Remaining)
}

func TestTaskletQuotaAcceptsNumericExpiration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{"grants": []map[string]any{{"amount": 10, "consumed": 2, "type": "credits", "expiration": 1893456000000}}})
	}))
	defer server.Close()
	creds := core.Credentials{Extra: map[string]string{"organization_id": "org"}}
	creds.APIKey = strings.Repeat("t", 5)
	quota, err := NewTasklet(server.URL).FetchQuota(context.Background(), creds)
	require.NoError(t, err)
	require.Len(t, quota.Quotas, 1)
	require.NotEmpty(t, quota.Quotas[0].ResetAt)
}

func TestTaskletQuotaMissingBalanceIsNotExplicitZero(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{"grants": []map[string]any{{"type": "credits"}}})
	}))
	defer server.Close()
	_, err := NewTasklet(server.URL).FetchQuota(context.Background(), core.Credentials{APIKey: "token", Extra: map[string]string{"organization_id": "org"}})
	require.ErrorContains(t, err, "missing amount or consumed")
}

func collectTaskletChunks(ch <-chan core.StreamChunk) []core.StreamChunk {
	var out []core.StreamChunk
	for chunk := range ch {
		out = append(out, chunk)
	}
	return out
}

func taskletRequest() *core.ChatRequest {
	return &core.ChatRequest{Model: "gpt-5.6-sol", Messages: []core.Message{
		{Role: core.RoleSystem, Content: []core.ContentPart{{Type: core.PartText, Text: "rules"}}},
		{Role: core.RoleAssistant, Content: []core.ContentPart{{Type: core.PartText, Text: "prior"}, {Type: core.PartToolCall, ToolCall: &core.ToolCall{ID: "call-1", Name: "lookup"}}}},
		{Role: core.RoleTool, Content: []core.ContentPart{{Type: core.PartToolResult, ToolResult: &core.ToolResult{CallID: "call-1", Content: "result"}}}},
		{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "question"}, {Type: core.PartImage}}},
	}}
}

func newTaskletServer(t *testing.T, events []map[string]any) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sendChatMessage" {
			writeJSONResponse(t, w, map[string]any{"agentId": "agent-1"})
			return
		}
		if r.URL.Path == "/api/sync" {
			serveTaskletWS(t, w, r, events)
			return
		}
		http.NotFound(w, r)
	}))
}

func serveTaskletWS(t *testing.T, w http.ResponseWriter, r *http.Request, events []map[string]any) {
	c, err := websocket.Accept(w, r, nil)
	require.NoError(t, err)
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	require.NoError(t, err)
	var connect map[string]any
	require.NoError(t, json.Unmarshal(data, &connect))
	require.Equal(t, "connect", connect["type"])
	require.NoError(t, c.Write(ctx, websocket.MessageText, []byte(`{"type":"connected"}`)))
	for _, typ := range []string{"startSync", "subscribeBlocks"} {
		_, data, err = c.Read(ctx)
		if err != nil {
			return
		}
		var msg map[string]any
		require.NoError(t, json.Unmarshal(data, &msg))
		require.Equal(t, typ, msg["type"])
	}
	for _, event := range events {
		b, err := json.Marshal(event)
		require.NoError(t, err)
		if err := c.Write(ctx, websocket.MessageText, b); err != nil {
			return
		}
	}
	_ = c.Close(websocket.StatusNormalClosure, "done")
}

func writeJSONResponse(t *testing.T, w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(v))
}
func chunkTypes(chunks []core.StreamChunk) []core.ChunkType {
	out := make([]core.ChunkType, len(chunks))
	for i, c := range chunks {
		out[i] = c.Type
	}
	return out
}
func chunkDeltas(chunks []core.StreamChunk) []string {
	out := make([]string, len(chunks))
	for i, c := range chunks {
		out[i] = c.Delta
	}
	return out
}

func TestTaskletStreamAcceptsLargeCumulativeSnapshot(t *testing.T) {
	content := strings.Repeat("x", 40*1024)
	server := newTaskletServer(t, []map[string]any{
		{"type": "blocksUpdate", "updates": map[string]any{"c": map[string]any{"type": "agent_content", "content": content}}},
		{"type": "syncUpdate", "state": map[string]any{"runState": map[string]any{"type": "idle"}}},
	})
	defer server.Close()

	creds := core.Credentials{}
	creds.APIKey = strings.Repeat("t", 5)
	chunks, err := NewTasklet(server.URL).Stream(
		context.Background(), taskletRequest(), creds, core.StreamConfig{},
	)
	require.NoError(t, err)
	got := collectTaskletChunks(chunks)
	require.Equal(t, []core.ChunkType{core.ChunkText, core.ChunkFinish}, chunkTypes(got))
	require.Len(t, got[0].Delta, len(content))
}

func TestTaskletPostCreateDialFailureIsNotFallbackable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/sendChatMessage" {
			writeJSONResponse(t, w, map[string]any{"agentId": "created-run"})
			return
		}
		http.Error(w, "websocket unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	creds := core.Credentials{}
	creds.APIKey = strings.Repeat("t", 5)
	_, err := NewTasklet(server.URL).Stream(
		context.Background(), taskletRequest(), creds, core.StreamConfig{},
	)
	require.Error(t, err)
	pe := core.AsProviderError(err)
	require.False(t, pe.Fallbackable(), "a created Tasklet run must not be replayed on another account")
	require.Equal(t, core.FailureScopeRequest, pe.EffectiveScope())
}
