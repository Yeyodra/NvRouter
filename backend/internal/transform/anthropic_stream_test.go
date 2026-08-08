package transform

import (
	"encoding/json"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestAnthropicParseResponsePreservesThinkingSignature(t *testing.T) {
	codec := AnthropicCodec{}
	resp, err := codec.ParseResponse([]byte(`{"content":[{"type":"thinking","thinking":"reasoning","signature":"sig_123"}],"stop_reason":"end_turn"}`), "claude")
	require.NoError(t, err)
	require.Len(t, resp.Message.Content, 1)
	require.Equal(t, core.PartThinking, resp.Message.Content[0].Type)
	require.Equal(t, "reasoning", resp.Message.Content[0].Text)
	require.Equal(t, "sig_123", resp.Message.Content[0].Signature)

	rendered, err := codec.RenderResponse(resp)
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rendered, &body))
	block := body["content"].([]any)[0].(map[string]any)
	require.Equal(t, "reasoning", block["thinking"])
	require.Equal(t, "sig_123", block["signature"])
}

func TestAnthropicParseStreamSignatureDelta(t *testing.T) {
	chunks, err := (AnthropicCodec{}).ParseStreamLine([]byte(`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig_stream"}}`), "claude")
	require.NoError(t, err)
	require.Len(t, chunks, 1)
	require.Equal(t, core.ChunkThinking, chunks[0].Type)
	require.Empty(t, chunks[0].Delta)
	require.Equal(t, "sig_stream", chunks[0].Signature)
}

func TestAnthropicRenderStreamThinkingSignatureDelta(t *testing.T) {
	state := &StreamState{}
	var rendered string
	for _, chunk := range []core.StreamChunk{
		{Type: core.ChunkThinking, Delta: "reasoning"},
		{Type: core.ChunkThinking, Signature: "sig_stream"},
	} {
		events, err := (AnthropicCodec{}).RenderStreamChunk(chunk, state)
		require.NoError(t, err)
		for _, event := range events {
			rendered += string(event)
		}
	}
	require.Contains(t, rendered, `"type":"thinking_delta"`)
	require.Contains(t, rendered, `"type":"signature_delta"`)
	require.Contains(t, rendered, `"signature":"sig_stream"`)
}

func TestAnthropic_ThinkingBudgetReconciliation(t *testing.T) {
	// When thinking budget >= max_tokens, max_tokens must be raised to budget+1024.
	budget := 8000
	maxTok := 4096
	req := &core.ChatRequest{
		Model:     "claude-sonnet-4.5",
		MaxTokens: &maxTok,
		Reasoning: &core.ReasoningConfig{
			Effort:    "high",
			MaxTokens: budget,
		},
		Messages: []core.Message{
			{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}},
		},
	}

	body, err := AnthropicCodec{}.RenderRequest(req)
	require.NoError(t, err)

	// max_tokens should be raised to budget + 1024 = 9024
	var antReq map[string]any
	require.NoError(t, json.Unmarshal(body, &antReq))
	require.Equal(t, float64(9024), antReq["max_tokens"])
	require.Equal(t, float64(8000), antReq["thinking"].(map[string]any)["budget_tokens"])
}

func TestAnthropic_ParseThinking(t *testing.T) {
	body := []byte(`{
		"model": "claude-sonnet-4.5",
		"max_tokens": 1024,
		"thinking": {"type": "enabled", "budget_tokens": 8000},
		"messages": [{"role": "user", "content": "hi"}]
	}`)

	req, err := AnthropicCodec{}.ParseRequest(body)
	require.NoError(t, err)
	require.NotNil(t, req.Reasoning)
	require.Equal(t, "high", req.Reasoning.Effort)
	require.Equal(t, 8000, req.Reasoning.MaxTokens)
}

func TestAnthropic_AdaptiveThinkingRoundTrip(t *testing.T) {
	body := []byte(`{
		"model": "claude-sonnet-4.5",
		"max_tokens": 1024,
		"thinking": {"type": "adaptive"},
		"messages": [{"role": "user", "content": "hi"}]
	}`)

	req, err := AnthropicCodec{}.ParseRequest(body)
	require.NoError(t, err)
	require.Equal(t, "adaptive", req.Reasoning.Effort)

	rendered, err := AnthropicCodec{}.RenderRequest(req)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rendered, &out))
	require.Equal(t, "adaptive", out["thinking"].(map[string]any)["type"])
}

func TestAnthropic_SignatureFieldInBlock(t *testing.T) {
	body := []byte(`{
		"model": "claude-sonnet-4.5",
		"max_tokens": 1024,
		"messages": [
			{"role": "assistant", "content": [
				{"type": "thinking", "thinking": "reasoning...", "signature": "sig_abc123"},
				{"type": "text", "text": "answer"}
			]},
			{"role": "user", "content": "thanks"}
		]
	}`)

	req, err := AnthropicCodec{}.ParseRequest(body)
	require.NoError(t, err)
	require.Len(t, req.Messages, 2)
	// Thinking block signature should be parsed
	assistantMsg := req.Messages[0]
	found := false
	for _, p := range assistantMsg.Content {
		if p.Type == core.PartThinking {
			found = true
			require.Equal(t, "reasoning...", p.Text)
			require.Equal(t, "sig_abc123", p.Signature, "signature must be preserved")
		}
	}
	require.True(t, found, "thinking block should be parsed")
}

// TestAnthropic_ThinkingRoundTrip verifies that a thinking block with signature
// survives ParseRequest → RenderRequest intact. This is critical for
// Anthropic-compatible providers (GLM, Zhipu, etc.) that validate the signature
// on follow-up turns.
func TestAnthropic_ThinkingRoundTrip(t *testing.T) {
	original := []byte(`{
		"model": "glm-5.2",
		"max_tokens": 4096,
		"messages": [
			{"role": "assistant", "content": [
				{"type": "thinking", "thinking": "step 1: analyze...", "signature": "sig_xyz789"},
				{"type": "text", "text": "Here is the answer."}
			]},
			{"role": "user", "content": "what about edge cases?"}
		]
	}`)

	req, err := AnthropicCodec{}.ParseRequest(original)
	require.NoError(t, err)

	rendered, err := AnthropicCodec{}.RenderRequest(req)
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rendered, &out))

	messages := out["messages"].([]any)
	assistant := messages[0].(map[string]any)
	content := assistant["content"].([]any)

	// First block should be thinking with signature
	thinkBlock := content[0].(map[string]any)
	require.Equal(t, "thinking", thinkBlock["type"])
	require.Equal(t, "step 1: analyze...", thinkBlock["thinking"])
	require.Equal(t, "sig_xyz789", thinkBlock["signature"])

	// Second block should be text
	textBlock := content[1].(map[string]any)
	require.Equal(t, "text", textBlock["type"])
	require.Equal(t, "Here is the answer.", textBlock["text"])
}

// TestAnthropic_RenderRequestForwardsThinking verifies that the thinking
// configuration from the client is forwarded to the upstream. Without this,
// Anthropic-compatible providers (GLM, Zhipu) skip reasoning blocks and
// clients like Claude Code see an unexpected response shape.
func TestAnthropic_RenderRequestForwardsThinking(t *testing.T) {
	maxTok := 4096
	req := &core.ChatRequest{
		Model:     "glm-5.2",
		MaxTokens: &maxTok,
		Reasoning: &core.ReasoningConfig{
			Effort:    "high",
			MaxTokens: 8000,
		},
		Messages: []core.Message{
			{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}},
		},
	}

	body, err := AnthropicCodec{}.RenderRequest(req)
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))

	thinking, ok := out["thinking"].(map[string]any)
	require.True(t, ok, "thinking field must be present in rendered request")
	require.Equal(t, "enabled", thinking["type"])
	require.Equal(t, float64(8000), thinking["budget_tokens"])
}

// TestAnthropic_RenderRequestOmitsThinkingWhenNotRequested verifies that
// the thinking field is omitted when the client did not request it.
func TestAnthropic_RenderRequestOmitsThinkingWhenNotRequested(t *testing.T) {
	maxTok := 4096
	req := &core.ChatRequest{
		Model:     "glm-5.2",
		MaxTokens: &maxTok,
		Messages: []core.Message{
			{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}},
		},
	}

	body, err := AnthropicCodec{}.RenderRequest(req)
	require.NoError(t, err)

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))

	_, ok := out["thinking"]
	require.False(t, ok, "thinking field must be omitted when not requested")
}

func TestAnthropic_RenderStreamChunkZeroStateStartsAtIndexZero(t *testing.T) {
	state := &StreamState{Model: "claude-x"}
	events, err := AnthropicCodec{}.RenderStreamChunk(core.StreamChunk{
		Type:  core.ChunkText,
		Delta: "hello",
	}, state)
	require.NoError(t, err)
	require.Len(t, events, 3)
	require.Contains(t, string(events[1]), `"index":0`)
	require.Contains(t, string(events[2]), `"index":0`)
}

func TestAnthropic_RenderStreamChunkUsesDistinctContentIndices(t *testing.T) {
	state := &StreamState{Model: "claude-x"}
	codec := AnthropicCodec{}

	thinking, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkThinking, Delta: "reason"}, state)
	require.NoError(t, err)
	require.Contains(t, string(thinking[1]), `"index":0`)

	text, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkText, Delta: "answer"}, state)
	require.NoError(t, err)
	require.Contains(t, string(text[1]), `"index":1`)

	tool, err := codec.RenderStreamChunk(core.StreamChunk{
		Type: core.ChunkToolCall,
		ToolCall: &core.ToolCall{
			ID:   "toolu_1",
			Name: "Read",
		},
	}, state)
	require.NoError(t, err)
	require.Contains(t, string(tool[len(tool)-1]), `"index":2`)
}

func TestAnthropic_RenderStreamChunkDuplicateFinishIsIdempotent(t *testing.T) {
	state := &StreamState{Model: "claude-x"}
	codec := AnthropicCodec{}
	_, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkText, Delta: "answer"}, state)
	require.NoError(t, err)

	first, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}, state)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	second, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}, state)
	require.NoError(t, err)
	require.Empty(t, second)
}
