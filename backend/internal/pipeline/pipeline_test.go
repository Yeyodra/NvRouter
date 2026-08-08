package pipeline

import (
	"testing"
	"time"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestProviderAllowsDirectStreamVisibleReasoning(t *testing.T) {
	require.False(t, providerAllowsDirectStream("grok-cli"), "Grok output must pass through the channel path to project tagged summaries as thinking")
	require.False(t, providerAllowsDirectStream("enter-converge"), "Enter switches wire dialect by model, so raw SSE cannot be assumed to match the connector dialect")
	require.True(t, providerAllowsDirectStream("openai"))
}

func TestShouldRetryStreamRateLimit(t *testing.T) {
	tests := []struct {
		name       string
		error      *core.ProviderError
		retries    int
		waitBudget time.Duration
		want       bool
		wantWait   time.Duration
	}{
		{
			name:       "transient other provider",
			error:      &core.ProviderError{Kind: core.ErrRateLimit, Provider: "openai", RetryAfter: 2 * time.Second},
			waitBudget: 10 * time.Second,
			want:       true,
			wantWait:   2050 * time.Millisecond,
		},
		{
			name:       "kiro account limit",
			error:      &core.ProviderError{Kind: core.ErrRateLimit, Provider: "kiro", RetryAfter: 2 * time.Second},
			waitBudget: 10 * time.Second,
			want:       false,
		},
		{
			name:       "explicit reset exceeds budget",
			error:      &core.ProviderError{Kind: core.ErrRateLimit, Provider: "openai", RetryAfter: time.Minute},
			waitBudget: 10 * time.Second,
			want:       false,
		},
		{
			name:       "retry budget exhausted",
			error:      &core.ProviderError{Kind: core.ErrRateLimit, Provider: "openai", RetryAfter: time.Second},
			retries:    maxRateLimitRetries,
			waitBudget: 10 * time.Second,
			want:       false,
		},
		{
			name:       "not a rate limit",
			error:      &core.ProviderError{Kind: core.ErrUpstream, Provider: "openai"},
			waitBudget: 10 * time.Second,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotWait, got := streamRateLimitWait(tt.error, tt.retries, tt.waitBudget)
			if got != tt.want {
				t.Fatalf("streamRateLimitWait() ok = %v, want %v", got, tt.want)
			}
			if gotWait != tt.wantWait {
				t.Fatalf("streamRateLimitWait() wait = %v, want %v", gotWait, tt.wantWait)
			}
		})
	}
}

func TestExtractUsageFromStream_OpenAI(t *testing.T) {
	// OpenAI format: usage in the last chunk before [DONE].
	raw := []byte(`data: {"id":"1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hi"}}]}

data: {"id":"2","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":20,"total_tokens":30}}

data: [DONE]
`)
	usage := extractUsageFromStream(raw)
	if usage.PromptTokens != 10 {
		t.Errorf("PromptTokens = %d, want 10", usage.PromptTokens)
	}
	if usage.CompletionTokens != 20 {
		t.Errorf("CompletionTokens = %d, want 20", usage.CompletionTokens)
	}
}

func TestExtractUsageFromStream_Anthropic(t *testing.T) {
	// Anthropic format: input_tokens in message_start, output_tokens in message_delta.
	raw := []byte(`event: message_start
data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","content":[],"model":"claude-sonnet-4-20250514","stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":50,"output_tokens":0,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":15}}

event: message_stop
data: {"type":"message_stop"}
`)
	usage := extractUsageFromStream(raw)
	if usage.PromptTokens != 50 {
		t.Errorf("PromptTokens = %d, want 50", usage.PromptTokens)
	}
	if usage.CompletionTokens != 15 {
		t.Errorf("CompletionTokens = %d, want 15", usage.CompletionTokens)
	}
}

func TestExtractUsageFromStream_AnthropicIncludesCachedInput(t *testing.T) {
	raw := []byte(`event: message_start
data: {"type":"message_start","message":{"usage":{"input_tokens":100,"output_tokens":0,"cache_creation_input_tokens":25,"cache_read_input_tokens":900}}}

event: message_delta
data: {"type":"message_delta","usage":{"output_tokens":15}}
`)

	usage := extractUsageFromStream(raw)
	if usage.PromptTokens != 1025 {
		t.Errorf("PromptTokens = %d, want 1025", usage.PromptTokens)
	}
	if usage.CachedTokens != 900 {
		t.Errorf("CachedTokens = %d, want 900", usage.CachedTokens)
	}
	if usage.CacheWriteTokens != 25 {
		t.Errorf("CacheWriteTokens = %d, want 25", usage.CacheWriteTokens)
	}
	if usage.TotalTokens != 1040 {
		t.Errorf("TotalTokens = %d, want 1040", usage.TotalTokens)
	}
}

func TestExtractUsageFromStream_Empty(t *testing.T) {
	raw := []byte(`data: {"choices":[{"delta":{"content":"no usage here"}}]}

data: [DONE]
`)
	usage := extractUsageFromStream(raw)
	if usage.PromptTokens != 0 || usage.CompletionTokens != 0 {
		t.Errorf("expected zero usage, got prompt=%d completion=%d", usage.PromptTokens, usage.CompletionTokens)
	}
}

func TestExtractUsageFromSSEData_TopLevelUsage(t *testing.T) {
	data := []byte(`{"usage":{"prompt_tokens":100,"completion_tokens":200}}`)
	u := extractUsageFromSSEData(data)
	if u == nil {
		t.Fatal("expected non-nil usage")
	}
	if u.PromptTokens != 100 {
		t.Errorf("PromptTokens = %d, want 100", u.PromptTokens)
	}
	if u.CompletionTokens != 200 {
		t.Errorf("CompletionTokens = %d, want 200", u.CompletionTokens)
	}
}

func TestExtractUsageFromSSEData_AnthropicMessageStart(t *testing.T) {
	data := []byte(`{"type":"message_start","message":{"usage":{"input_tokens":50,"output_tokens":0}}}`)
	u := extractUsageFromSSEData(data)
	if u == nil {
		t.Fatal("expected non-nil usage")
	}
	if u.PromptTokens != 50 {
		t.Errorf("PromptTokens = %d, want 50", u.PromptTokens)
	}
}

func TestExtractUsageFromSSEData_NoUsage(t *testing.T) {
	data := []byte(`{"choices":[{"delta":{"content":"hello"}}]}`)
	u := extractUsageFromSSEData(data)
	if u != nil {
		t.Errorf("expected nil, got %+v", u)
	}
}

func TestMergeUsage(t *testing.T) {
	old := core.Usage{PromptTokens: 10}
	new := core.Usage{CompletionTokens: 20}
	merged := mergeUsage(old, new)
	if merged.PromptTokens != 10 {
		t.Errorf("PromptTokens = %d, want 10", merged.PromptTokens)
	}
	if merged.CompletionTokens != 20 {
		t.Errorf("CompletionTokens = %d, want 20", merged.CompletionTokens)
	}
}

func TestAddAttemptUsageSumsCompletedAttempts(t *testing.T) {
	first := core.Usage{
		PromptTokens: 10, CompletionTokens: 4, TotalTokens: 14,
		CachedTokens: 3, Source: core.UsageSourceProvider,
	}
	second := core.Usage{
		PromptTokens: 12, CompletionTokens: 8, TotalTokens: 20,
		ReasoningTokens: 2, Source: core.UsageSourceProvider,
	}
	total := addAttemptUsage(first, second)

	if total.PromptTokens != 22 || total.CompletionTokens != 12 || total.TotalTokens != 34 {
		t.Fatalf("unexpected summed usage: %+v", total)
	}
	if total.CachedTokens != 3 || total.ReasoningTokens != 2 {
		t.Fatalf("usage details were not summed: %+v", total)
	}
	if total.Source != core.UsageSourceProvider {
		t.Fatalf("usage source = %q, want provider", total.Source)
	}
}

func TestCloneWithSystemInstructionDoesNotMutateOriginal(t *testing.T) {
	req := &core.ChatRequest{System: "original"}
	clone := cloneWithSystemInstruction(req, "repair")

	if req.System != "original" {
		t.Fatalf("original request was mutated: %q", req.System)
	}
	if clone.System != "original\n\nrepair" {
		t.Fatalf("clone system = %q", clone.System)
	}
}

func TestSafeBuffer_SmallStream(t *testing.T) {
	var buf safeBuffer
	data := []byte("hello world")
	buf.Write(data)
	got := buf.Bytes()
	if string(got) != "hello world" {
		t.Errorf("Bytes() = %q, want %q", got, "hello world")
	}
}
