package connectors

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// encodeEventStreamFrame builds a minimal AWS EventStream frame with a single
// ":event-type" string header and a JSON payload. CRC fields are zeroed (the
// parser does not validate them). This mirrors the frame layout the Kiro
// connector decodes.
func encodeEventStreamFrame(eventType string, payload []byte) []byte {
	// Header: 1-byte name len, name, 1-byte type(7), 2-byte value len, value.
	name := ":event-type"
	var hdr bytes.Buffer
	hdr.WriteByte(byte(len(name)))
	hdr.WriteString(name)
	hdr.WriteByte(7) // string type
	var vl [2]byte
	binary.BigEndian.PutUint16(vl[:], uint16(len(eventType)))
	hdr.Write(vl[:])
	hdr.WriteString(eventType)

	headers := hdr.Bytes()
	headersLen := len(headers)
	totalLen := 12 + headersLen + len(payload) + 4 // prelude(8)+preludeCRC(4)+headers+payload+msgCRC(4)

	var out bytes.Buffer
	var u32 [4]byte
	binary.BigEndian.PutUint32(u32[:], uint32(totalLen))
	out.Write(u32[:])
	binary.BigEndian.PutUint32(u32[:], uint32(headersLen))
	out.Write(u32[:])
	out.Write([]byte{0, 0, 0, 0}) // prelude CRC (ignored)
	out.Write(headers)
	out.Write(payload)
	out.Write([]byte{0, 0, 0, 0}) // message CRC (ignored)
	return out.Bytes()
}

func TestEventStreamParser_DecodesFrames(t *testing.T) {
	var stream bytes.Buffer
	stream.Write(encodeEventStreamFrame("assistantResponseEvent", []byte(`{"content":"Hello"}`)))
	stream.Write(encodeEventStreamFrame("assistantResponseEvent", []byte(`{"content":" world"}`)))
	stream.Write(encodeEventStreamFrame("messageStopEvent", []byte(`{}`)))

	parser := newEventStreamParser(&stream)
	var events []string
	for {
		frame, err := parser.next()
		if err == errEventStreamEOF {
			break
		}
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if frame == nil {
			continue
		}
		events = append(events, frame.headers[":event-type"])
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 frames, got %d: %v", len(events), events)
	}
	if events[0] != "assistantResponseEvent" || events[2] != "messageStopEvent" {
		t.Errorf("unexpected event order: %v", events)
	}
}

type eofWithDataReader struct {
	data []byte
	read bool
}

func (r *eofWithDataReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, io.EOF
	}
	r.read = true
	n := copy(p, r.data)
	return n, io.EOF
}

func TestEventStreamParserProcessesDataReturnedWithEOF(t *testing.T) {
	parser := newEventStreamParser(&eofWithDataReader{
		data: encodeEventStreamFrame("assistantResponseEvent", []byte(`{"content":"hello"}`)),
	})

	frame, err := parser.next()
	require.NoError(t, err)
	require.Equal(t, "assistantResponseEvent", frame.headers[":event-type"])

	_, err = parser.next()
	require.Equal(t, errEventStreamEOF, err)
}

func TestEventStreamParserRejectsTruncatedFrame(t *testing.T) {
	frame := encodeEventStreamFrame("assistantResponseEvent", []byte(`{"content":"hello"}`))
	parser := newEventStreamParser(bytes.NewReader(frame[:len(frame)-1]))

	_, err := parser.next()
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestKiroParserErrorClassification(t *testing.T) {
	request := &core.ChatRequest{Model: "model"}
	for _, tt := range []struct {
		name string
		body io.ReadCloser
		kind core.ErrorKind
	}{
		{"truncated", io.NopCloser(bytes.NewReader(encodeEventStreamFrame("messageStopEvent", []byte(`{}`))[:8])), core.ErrResponseIntegrity},
		{"invalid length", io.NopCloser(bytes.NewReader(make([]byte, 12))), core.ErrResponseIntegrity},
		{"transport", io.NopCloser(failingReader{errors.New("connection reset")}), core.ErrUpstream},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stream := NewKiro("kiro", "").pipeKiroResponse(context.Background(), &http.Response{Body: tt.body}, request, nil)
			chunk := <-stream
			require.Equal(t, core.ChunkError, chunk.Type)
			require.Equal(t, tt.kind, core.AsProviderError(chunk.Err).Kind)
		})
	}
}

func TestKiroFrameToChunks_TextAndStop(t *testing.T) {
	seen := map[string]bool{}
	hasTool := false

	frame := mustDecode(t, encodeEventStreamFrame("assistantResponseEvent", []byte(`{"content":"hi"}`)))
	chunks := kiroFrameToChunks(frame, seen, &hasTool)
	if len(chunks) != 1 || chunks[0].Type != core.ChunkText || chunks[0].Delta != "hi" {
		t.Fatalf("expected text chunk 'hi', got %+v", chunks)
	}

	stop := mustDecode(t, encodeEventStreamFrame("messageStopEvent", []byte(`{}`)))
	chunks = kiroFrameToChunks(stop, seen, &hasTool)
	if len(chunks) != 1 || chunks[0].Type != core.ChunkFinish || chunks[0].FinishReason != core.FinishStop {
		t.Fatalf("expected finish=stop, got %+v", chunks)
	}
}

func TestKiroFrameToChunks_ToolUse(t *testing.T) {
	seen := map[string]bool{}
	hasTool := false

	frame := mustDecode(t, encodeEventStreamFrame("toolUseEvent",
		[]byte(`{"toolUseId":"t1","name":"get_weather","input":{"city":"SF"}}`)))
	chunks := kiroFrameToChunks(frame, seen, &hasTool)
	if !hasTool {
		t.Error("hasTool should be set")
	}
	// First chunk announces the tool, second carries its arguments.
	if len(chunks) < 2 {
		t.Fatalf("expected >=2 chunks, got %d", len(chunks))
	}
	if chunks[0].ToolCall.Name != "get_weather" {
		t.Errorf("tool name wrong: %v", chunks[0].ToolCall.Name)
	}
	if !bytes.Contains(chunks[1].ToolCall.Arguments, []byte("SF")) {
		t.Errorf("tool args missing input: %s", chunks[1].ToolCall.Arguments)
	}
}

func TestKiroFrameToChunks_UsageEvent(t *testing.T) {
	seen := map[string]bool{}
	hasTool := false

	// Top-level inputTokens/outputTokens under usageEvent name.
	frame := mustDecode(t, encodeEventStreamFrame("usageEvent",
		[]byte(`{"inputTokens":120,"outputTokens":34}`)))
	chunks := kiroFrameToChunks(frame, seen, &hasTool)
	if len(chunks) != 1 || chunks[0].Type != core.ChunkUsage || chunks[0].Usage == nil {
		t.Fatalf("expected usage chunk, got %+v", chunks)
	}
	if chunks[0].Usage.PromptTokens != 120 || chunks[0].Usage.CompletionTokens != 34 {
		t.Errorf("usage tokens wrong: %+v", chunks[0].Usage)
	}
	if chunks[0].Usage.TotalTokens != 154 {
		t.Errorf("total tokens wrong: %d", chunks[0].Usage.TotalTokens)
	}
}

func TestKiroFrameToChunks_MetricsEventNested(t *testing.T) {
	seen := map[string]bool{}
	hasTool := false

	// Counts nested under a key matching the event type.
	frame := mustDecode(t, encodeEventStreamFrame("metricsEvent",
		[]byte(`{"metricsEvent":{"inputTokens":10,"outputTokens":5}}`)))
	chunks := kiroFrameToChunks(frame, seen, &hasTool)
	if len(chunks) != 1 || chunks[0].Type != core.ChunkUsage {
		t.Fatalf("expected usage chunk, got %+v", chunks)
	}
	if chunks[0].Usage.PromptTokens != 10 || chunks[0].Usage.CompletionTokens != 5 {
		t.Errorf("nested usage wrong: %+v", chunks[0].Usage)
	}
}

func TestParseKiroUsage_ZeroReturnsNil(t *testing.T) {
	if u := parseKiroUsage("usageEvent", []byte(`{"inputTokens":0,"outputTokens":0}`)); u != nil {
		t.Errorf("expected nil for zero usage, got %+v", u)
	}
	if u := parseKiroUsage("usageEvent", []byte(`not json`)); u != nil {
		t.Errorf("expected nil for bad json, got %+v", u)
	}
}

func TestEstimateKiroUsage(t *testing.T) {
	req := &core.ChatRequest{
		System: "you are helpful", // 15 chars
		Messages: []core.Message{
			{Role: core.RoleUser, Content: []core.ContentPart{
				{Type: core.PartText, Text: "hello there"}, // 11 chars
			}},
		},
	}
	// input chars = 15 + 11 = 26 → ceil(26/4) = 7 prompt tokens
	// output chars = 40 → ceil(40/4) = 10 completion tokens
	u := estimateKiroUsage(req, 40)
	if u == nil {
		t.Fatal("expected non-nil estimate")
	}
	if u.PromptTokens != 7 {
		t.Errorf("prompt tokens = %d, want 7", u.PromptTokens)
	}
	if u.CompletionTokens != 10 {
		t.Errorf("completion tokens = %d, want 10", u.CompletionTokens)
	}
	if u.TotalTokens != 17 {
		t.Errorf("total tokens = %d, want 17", u.TotalTokens)
	}

	// Empty request + zero output → nil (nothing to record).
	if got := estimateKiroUsage(&core.ChatRequest{}, 0); got != nil {
		t.Errorf("expected nil for empty request, got %+v", got)
	}
}

func TestCharsToTokens(t *testing.T) {
	cases := []struct{ in, want int }{
		{0, 0}, {-5, 0}, {1, 1}, {4, 1}, {5, 2}, {8, 2}, {9, 3},
	}
	for _, c := range cases {
		if got := charsToTokens(c.in); got != c.want {
			t.Errorf("charsToTokens(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

func mustDecode(t *testing.T, frame []byte) *eventStreamFrame {
	t.Helper()
	f, err := decodeEventStreamFrame(frame)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return f
}

func TestKiroEndpoints_OAuthDefaultOrder(t *testing.T) {
	c := NewKiro("kiro", kiroEndpoints[0])
	got := c.endpoints(core.Credentials{})
	if len(got) != len(kiroEndpoints) {
		t.Fatalf("expected %d endpoints, got %d", len(kiroEndpoints), len(got))
	}
	// OAuth keeps the default order: kiro.dev leads.
	if !strings.Contains(got[0], "kiro.dev") {
		t.Errorf("oauth should lead with kiro.dev, got %s", got[0])
	}
}

func TestKiroEndpoints_APIKeyPrefersAmazon(t *testing.T) {
	c := NewKiro("kiro", kiroEndpoints[0])
	got := c.endpoints(core.Credentials{Extra: map[string]string{"kiro_auth_method": "api_key"}})
	if len(got) != len(kiroEndpoints) {
		t.Fatalf("expected %d endpoints, got %d", len(kiroEndpoints), len(got))
	}
	// API-key auth must hit the amazonaws.com surface first; kiro.dev rejects it.
	if !strings.Contains(got[0], "amazonaws.com") {
		t.Errorf("api_key should lead with amazonaws.com, got %s", got[0])
	}
}

func TestKiroEndpoints_ExternalIDPUsesRegionalAmazonSurface(t *testing.T) {
	c := NewKiro("kiro", kiroEndpoints[0])
	got := c.endpoints(core.Credentials{Extra: map[string]string{
		"kiro_auth_method": "external_idp",
		"kiro_region":      "eu-west-1",
	}})
	if !strings.Contains(got[0], "amazonaws.com") || !strings.Contains(got[0], ".eu-west-1.") {
		t.Fatalf("external_idp should lead with the regional amazon endpoint, got %v", got)
	}
}

func TestKiroEndpoints_CredentialBaseURLLeads(t *testing.T) {
	c := NewKiro("kiro", kiroEndpoints[0])
	custom := "https://relay.example.com/generateAssistantResponse"
	got := c.endpoints(core.Credentials{BaseURL: custom})
	if got[0] != custom {
		t.Fatalf("explicit base url should lead, got %s", got[0])
	}
	// The custom URL must not be duplicated among the known endpoints.
	for _, e := range got[1:] {
		if e == custom {
			t.Errorf("custom base url duplicated in fallback list")
		}
	}
}

func TestOrderAmazonFirst(t *testing.T) {
	in := []string{
		"https://runtime.us-east-1.kiro.dev/x",
		"https://codewhisperer.us-east-1.amazonaws.com/x",
		"https://q.us-east-1.amazonaws.com/x",
	}
	got := orderAmazonFirst(in)
	if !strings.Contains(got[0], "amazonaws.com") || !strings.Contains(got[1], "amazonaws.com") {
		t.Errorf("amazon hosts should come first: %v", got)
	}
	if !strings.Contains(got[2], "kiro.dev") {
		t.Errorf("non-amazon host should come last: %v", got)
	}
	// No amazon hosts: returned unchanged.
	only := []string{"https://runtime.us-east-1.kiro.dev/x"}
	if out := orderAmazonFirst(only); out[0] != only[0] {
		t.Errorf("single non-amazon host should be unchanged, got %v", out)
	}
}

func TestKiroHeaders_APIKeyMarker(t *testing.T) {
	c := NewKiro("kiro", kiroEndpoints[0])
	h := c.headers(core.Credentials{
		APIKey: "secret-key",
		Extra:  map[string]string{"kiro_auth_method": "api_key"},
	})
	if h["Authorization"] != "Bearer secret-key" {
		t.Errorf("api key should be sent as bearer, got %q", h["Authorization"])
	}
	if h["tokentype"] != "API_KEY" {
		t.Errorf("api key requests must carry tokentype=API_KEY, got %q", h["tokentype"])
	}
}

func TestKiroHeaders_OAuthHasNoTokenType(t *testing.T) {
	c := NewKiro("kiro", kiroEndpoints[0])
	h := c.headers(core.Credentials{AccessToken: "oauth-token"})
	if h["Authorization"] != "Bearer oauth-token" {
		t.Errorf("oauth should be sent as bearer, got %q", h["Authorization"])
	}
	if _, ok := h["tokentype"]; ok {
		t.Errorf("oauth requests must not carry tokentype")
	}
}

func TestKiroHeaders_ExternalIDPMarker(t *testing.T) {
	c := NewKiro("kiro", kiroEndpoints[0])
	h := c.headers(core.Credentials{
		AccessToken: "microsoft-token",
		Extra:       map[string]string{"kiro_auth_method": "external_idp"},
	})
	if h["Authorization"] != "Bearer microsoft-token" {
		t.Errorf("external identity token should be sent as bearer, got %q", h["Authorization"])
	}
	if h["TokenType"] != "EXTERNAL_IDP" {
		t.Errorf("external identity requests must carry TokenType=EXTERNAL_IDP, got %q", h["TokenType"])
	}
}

func TestKiroResolveProfileArn(t *testing.T) {
	cases := []struct {
		name  string
		creds core.Credentials
		want  string
	}{
		{
			name:  "oauth builder-id falls back to builder-id default",
			creds: core.Credentials{Extra: map[string]string{"kiro_auth_method": "builder-id"}},
			want:  kiroDefaultProfileArnBuilderID,
		},
		{
			name:  "idc without resolved arn injects no default",
			creds: core.Credentials{Extra: map[string]string{"kiro_auth_method": "idc"}},
			want:  "",
		},
		{
			name:  "imported social falls back to social default",
			creds: core.Credentials{Extra: map[string]string{"kiro_auth_method": "imported"}},
			want:  kiroDefaultProfileArnSocial,
		},
		{
			name:  "google social falls back to social default",
			creds: core.Credentials{Extra: map[string]string{"kiro_auth_method": "google"}},
			want:  kiroDefaultProfileArnSocial,
		},
		{
			name:  "no auth method falls back to builder-id default",
			creds: core.Credentials{Extra: map[string]string{}},
			want:  kiroDefaultProfileArnBuilderID,
		},
		{
			name: "resolved kiro_profile_arn wins over default",
			creds: core.Credentials{Extra: map[string]string{
				"kiro_auth_method": "builder-id",
				"kiro_profile_arn": "arn:aws:codewhisperer:us-east-1:111:profile/RESOLVED",
			}},
			want: "arn:aws:codewhisperer:us-east-1:111:profile/RESOLVED",
		},
		{
			name: "resolved profile_arn used when kiro_profile_arn empty",
			creds: core.Credentials{Extra: map[string]string{
				"kiro_auth_method": "google",
				"profile_arn":      "arn:aws:codewhisperer:us-east-1:222:profile/ALT",
			}},
			want: "arn:aws:codewhisperer:us-east-1:222:profile/ALT",
		},
		{
			name: "api_key uses only resolved arn",
			creds: core.Credentials{Extra: map[string]string{
				"kiro_auth_method": "api_key",
				"kiro_profile_arn": "arn:aws:codewhisperer:us-east-1:333:profile/KEY",
			}},
			want: "arn:aws:codewhisperer:us-east-1:333:profile/KEY",
		},
		{
			name:  "api_key without resolved arn injects no default",
			creds: core.Credentials{Extra: map[string]string{"kiro_auth_method": "api_key"}},
			want:  "",
		},
		{
			name:  "external_idp without resolved arn injects no default",
			creds: core.Credentials{Extra: map[string]string{"kiro_auth_method": "external_idp"}},
			want:  "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kiroResolveProfileArn(tc.creds); got != tc.want {
				t.Errorf("kiroResolveProfileArn() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestKiroEndpointRetryable(t *testing.T) {

	cases := []struct {
		kind core.ErrorKind
		want bool
	}{
		{core.ErrRateLimit, false},
		{core.ErrUpstream, true},
		{core.ErrTimeout, true},
		{core.ErrAuth, false},
		{core.ErrBadRequest, false},
		{core.ErrQuotaExhausted, false},
	}
	for _, tc := range cases {
		err := &core.ProviderError{Kind: tc.kind}
		if got := kiroEndpointRetryable(err); got != tc.want {
			t.Errorf("kiroEndpointRetryable(%s) = %v, want %v", tc.kind, got, tc.want)
		}
	}
}

func TestKiroRateLimitDoesNotFailOverToAnotherHost(t *testing.T) {
	var secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		secondCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer second.Close()

	c := NewKiro("kiro", "")
	_, err := c.openStreamWithFailover(context.Background(), "model", []byte("{}"), nil,
		[]string{first.URL, second.URL})
	require.Error(t, err)
	require.Equal(t, core.ErrRateLimit, core.AsProviderError(err).Kind)
	require.Equal(t, int32(0), secondCalls.Load())
}

func TestKiroAccountSlotRejectsConcurrentRequest(t *testing.T) {
	c := NewKiro("kiro", "")
	secondConnector := NewKiro("kiro", "")
	creds := core.Credentials{AccountID: "account-1"}

	release, err := c.acquireAccountSlot(context.Background(), creds, "model")
	require.NoError(t, err)

	_, err = secondConnector.acquireAccountSlot(context.Background(), creds, "model")
	require.Error(t, err)
	pe := core.AsProviderError(err)
	require.Equal(t, core.ErrRateLimit, pe.Kind)
	require.Equal(t, 2*time.Second, pe.RetryAfter)

	release()
	releaseAgain, err := c.acquireAccountSlot(context.Background(), creds, "model")
	require.NoError(t, err)
	releaseAgain()
}

func TestKiroQuotaEndpointRetryableStopsOnRateLimit(t *testing.T) {
	require.False(t, kiroQuotaEndpointRetryable(&core.ProviderError{Kind: core.ErrRateLimit}))
	require.False(t, kiroQuotaEndpointRetryable(&core.ProviderError{Kind: core.ErrQuotaExhausted}))
	require.True(t, kiroQuotaEndpointRetryable(&core.ProviderError{Kind: core.ErrAuth}))
	require.True(t, kiroQuotaEndpointRetryable(&core.ProviderError{Kind: core.ErrUpstream}))
}

func TestKiroIntegrityKind(t *testing.T) {
	cases := []struct {
		content string
		hasTool bool
		want    string
	}{
		{"hello world", false, ""},
		{"...", false, "ellipsis"},
		{"\u2026", false, "ellipsis"},
		{"Let me check the status", false, "short_final"},
		{"I will verify the deployment", false, "short_final"},
		{"Done. The deployment is complete.", false, ""},
		{"Let me check the status", true, ""},
		{"現在我確認一下", false, "short_final"},
		{"Let me verify: status is 200 OK", false, ""},
	}
	for _, tc := range cases {
		got := kiroIntegrityKind(tc.content, tc.hasTool)
		if got != tc.want {
			t.Errorf("kiroIntegrityKind(%q, hasTool=%v) = %q, want %q",
				tc.content, tc.hasTool, got, tc.want)
		}
	}
}

func TestKiroFrameToChunksMalformedToolRequestsRepair(t *testing.T) {
	frame := mustDecode(t, encodeEventStreamFrame("toolUseEvent",
		[]byte(`{"toolUseId":"t1","input":{"city":"SF"}}`)))
	hasTool := false
	chunks := kiroFrameToChunks(frame, map[string]bool{}, &hasTool)

	require.Len(t, chunks, 1)
	require.Equal(t, core.ChunkError, chunks[0].Type)
	pe := core.AsProviderError(chunks[0].Err)
	require.Equal(t, core.ErrResponseIntegrity, pe.Kind)
	require.Equal(t, core.FailureScopeRequest, pe.Scope)
	require.NotEmpty(t, pe.RetrySystemInstruction)
	require.False(t, hasTool)
}

func TestKiroToolUseChunksValidatesArgumentsAtomically(t *testing.T) {
	t.Run("empty object is valid", func(t *testing.T) {
		seen := map[string]bool{}
		chunks, err := kiroToolUseChunks(
			[]byte(`{"toolUseId":"t1","name":"lookup","input":{}}`),
			seen,
		)
		require.NoError(t, err)
		require.Len(t, chunks, 2)
		require.JSONEq(t, `{}`, string(chunks[1].ToolCall.Arguments))
		require.True(t, seen["t1"])
	})

	t.Run("invalid JSON string is rejected", func(t *testing.T) {
		seen := map[string]bool{}
		_, err := kiroToolUseChunks(
			[]byte(`{"toolUseId":"t1","name":"lookup","input":"not-json"}`),
			seen,
		)
		require.Error(t, err)
		require.Empty(t, seen)
	})

	t.Run("invalid array item does not commit earlier tools", func(t *testing.T) {
		seen := map[string]bool{}
		_, err := kiroToolUseChunks([]byte(`[
			{"toolUseId":"t1","name":"lookup","input":{}},
			{"toolUseId":"t2","input":{}}
		]`), seen)
		require.Error(t, err)
		require.Empty(t, seen)
	})
}

func TestKiroStreamAlignedEOFMissingStopPreservesUsageInIntegrityError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encodeEventStreamFrame("usageEvent", []byte(`{"inputTokens":3,"outputTokens":1}`)))
	}))
	defer srv.Close()

	stream, err := NewKiro("kiro", srv.URL).Stream(context.Background(), &core.ChatRequest{
		Model: "model", Messages: []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hello"}}}},
	}, core.Credentials{BaseURL: srv.URL}, core.StreamConfig{})
	require.NoError(t, err)

	var chunks []core.StreamChunk
	for chunk := range stream {
		chunks = append(chunks, chunk)
	}
	require.Len(t, chunks, 2)
	require.Equal(t, core.ChunkUsage, chunks[0].Type)
	require.Equal(t, 4, chunks[0].Usage.TotalTokens)
	require.Equal(t, core.ChunkError, chunks[1].Type)
	pe := core.AsProviderError(chunks[1].Err)
	require.Equal(t, core.ErrResponseIntegrity, pe.Kind)
	require.NotNil(t, pe.AttemptUsage)
	require.Equal(t, 4, pe.AttemptUsage.TotalTokens)
}

func TestKiroStreamForwardsBeforeUpstreamEOF(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encodeEventStreamFrame("assistantResponseEvent", []byte(`{"content":"hello"}`)))
		w.(http.Flusher).Flush()
		<-release
		_, _ = w.Write(encodeEventStreamFrame("messageStopEvent", []byte(`{}`)))
	}))
	defer srv.Close()
	defer close(release)

	conn := NewKiro("kiro", srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := conn.Stream(ctx, &core.ChatRequest{
		Model: "model",
		Messages: []core.Message{{
			Role:    core.RoleUser,
			Content: []core.ContentPart{{Type: core.PartText, Text: "hello"}},
		}},
	}, core.Credentials{BaseURL: srv.URL}, core.StreamConfig{})
	require.NoError(t, err)

	select {
	case chunk := <-stream:
		require.Equal(t, core.ChunkText, chunk.Type)
		require.Equal(t, "hello", chunk.Delta)
	case <-time.After(time.Second):
		t.Fatal("first chunk was not forwarded while the upstream stream remained open")
	}
}

func TestKiroChatCollectionErrorPreservesSeenUsage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encodeEventStreamFrame("usageEvent", []byte(`{"inputTokens":3,"outputTokens":1}`)))
		_, _ = w.Write(encodeEventStreamFrame("toolUseEvent", []byte(`{"toolUseId":"t1","input":{}}`)))
	}))
	defer srv.Close()

	_, err := NewKiro("kiro", srv.URL).Chat(context.Background(), &core.ChatRequest{
		Model: "model", Messages: []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hello"}}}},
	}, core.Credentials{BaseURL: srv.URL})
	pe := core.AsProviderError(err)
	require.Equal(t, core.ErrResponseIntegrity, pe.Kind)
	require.NotNil(t, pe.AttemptUsage)
	require.Equal(t, 4, pe.AttemptUsage.TotalTokens)
}

func TestKiroChatRepeatedCompleteToolUpdatesReplaceArguments(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encodeEventStreamFrame("toolUseEvent", []byte(`{"toolUseId":"t1","name":"lookup","input":{"city":"SF"}}`)))
		_, _ = w.Write(encodeEventStreamFrame("toolUseEvent", []byte(`{"toolUseId":"t1","name":"lookup","input":{"city":"Paris"}}`)))
		_, _ = w.Write(encodeEventStreamFrame("messageStopEvent", []byte(`{}`)))
	}))
	defer srv.Close()

	resp, err := NewKiro("kiro", srv.URL).Chat(context.Background(), &core.ChatRequest{
		Model:    "model",
		Messages: []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hello"}}}},
	}, core.Credentials{BaseURL: srv.URL})
	require.NoError(t, err)
	require.Len(t, resp.Message.Content, 1)
	require.JSONEq(t, `{"city":"Paris"}`, string(resp.Message.Content[0].ToolCall.Arguments))
}

func TestKiroChatIncompleteResponseReturnsRoutedRepair(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(encodeEventStreamFrame("assistantResponseEvent", []byte(`{"content":"..."}`)))
		_, _ = w.Write(encodeEventStreamFrame("usageEvent", []byte(`{"inputTokens":3,"outputTokens":1}`)))
		_, _ = w.Write(encodeEventStreamFrame("messageStopEvent", []byte(`{}`)))
	}))
	defer srv.Close()

	conn := NewKiro("kiro", srv.URL)
	resp, err := conn.Chat(context.Background(), &core.ChatRequest{
		Model: "model",
		Messages: []core.Message{{
			Role:    core.RoleUser,
			Content: []core.ContentPart{{Type: core.PartText, Text: "hello"}},
		}},
	}, core.Credentials{BaseURL: srv.URL})
	if err == nil {
		t.Fatalf("expected repair error, got response %+v", resp)
	}

	pe := core.AsProviderError(err)
	require.Equal(t, core.ErrResponseIntegrity, pe.Kind)
	require.Equal(t, core.FailureScopeRequest, pe.Scope)
	require.NotEmpty(t, pe.RetrySystemInstruction)
	require.NotNil(t, pe.AttemptUsage)
	require.Equal(t, 4, pe.AttemptUsage.TotalTokens)
	require.Equal(t, int32(1), calls.Load())
}

func TestKiroMetadataCachesReturnCopies(t *testing.T) {
	accountID := "cache-account"
	kiroModelCache.Delete(accountID)
	kiroQuotaCache.Delete(accountID)
	t.Cleanup(func() {
		kiroModelCache.Delete(accountID)
		kiroQuotaCache.Delete(accountID)
	})

	storeKiroModels(accountID, []ModelSpec{{ID: "model-1"}})
	models, ok := loadKiroModels(accountID)
	require.True(t, ok)
	models[0].ID = "mutated"
	modelsAgain, ok := loadKiroModels(accountID)
	require.True(t, ok)
	require.Equal(t, "model-1", modelsAgain[0].ID)

	kiroQuotaCache.Store(accountID, kiroQuotaCacheEntry{
		expiresAt: time.Now().Add(time.Minute),
		quota:     &QuotaResult{PlanName: "plan", Quotas: []QuotaEntry{{ResourceType: "credit", Limit: 100}}},
	})
	quota, ok := loadKiroQuota(accountID)
	require.True(t, ok)
	quota.Quotas[0].Limit = 0
	quotaAgain, ok := loadKiroQuota(accountID)
	require.True(t, ok)
	require.Equal(t, 100, quotaAgain.Quotas[0].Limit)
}
