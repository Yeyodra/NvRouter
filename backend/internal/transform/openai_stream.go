package transform

import (
	"bytes"
	"fmt"

	json "github.com/mydisha/keirouter/backend/internal/fastjson"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// oaiStreamChunk is one SSE "data:" payload from an OpenAI streaming response.
type oaiStreamChunk struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Delta struct {
			Role    string `json:"role"`
			Content string `json:"content"`
			// ReasoningContent carries thinking/reasoning text from models
			// that expose it as a structured field. The name varies by
			// provider: reasoning_content (DeepSeek/MiMo), reasoning
			// (OpenRouter), reasoning_text (a few others). Precedence:
			// reasoning_content > reasoning > reasoning_text.
			ReasoningContent string              `json:"reasoning_content"`
			Reasoning        string              `json:"reasoning"`
			ReasoningText    string              `json:"reasoning_text"`
			ToolCalls        []oaiStreamToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *oaiUsage `json:"usage"`
}

type oaiStreamToolCall struct {
	Index     int             `json:"index"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Input     json.RawMessage `json:"input"`
	Params    json.RawMessage `json:"parameters"`
	Args      json.RawMessage `json:"args"`
	Function  struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
		Input     json.RawMessage `json:"input"`
		Params    json.RawMessage `json:"parameters"`
		Args      json.RawMessage `json:"args"`
	} `json:"function"`
}

// ParseStreamLine decodes one upstream SSE data payload into canonical chunks.
// The caller strips the "data: " prefix before calling; the special "[DONE]"
// sentinel is handled here and yields no chunks.
func (OpenAICodec) ParseStreamLine(line []byte, model string) ([]core.StreamChunk, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || bytes.Equal(line, []byte("[DONE]")) {
		return nil, nil
	}

	var raw oaiStreamChunk
	if err := json.Unmarshal(line, &raw); err != nil {
		return nil, fmt.Errorf("openai: parse stream chunk: %w", err)
	}

	var chunks []core.StreamChunk
	if len(raw.Choices) > 0 {
		c := raw.Choices[0]
		// Structured reasoning field under any of its provider-specific names.
		if rc := firstNonEmpty(c.Delta.ReasoningContent, c.Delta.Reasoning, c.Delta.ReasoningText); rc != "" {
			chunks = append(chunks, core.StreamChunk{Type: core.ChunkThinking, Delta: rc})
		}
		if c.Delta.Content != "" {
			chunks = append(chunks, core.StreamChunk{Type: core.ChunkText, Delta: c.Delta.Content})
		}
		for _, tc := range c.Delta.ToolCalls {
			chunks = append(chunks, core.StreamChunk{
				Type:             core.ChunkToolCall,
				Index:            tc.Index,
				ToolArgumentMode: core.ToolArgumentDelta,
				ToolCall: &core.ToolCall{
					ID:        tc.ID,
					Name:      firstNonEmpty(tc.Function.Name, tc.Name),
					Arguments: extractOpenAIStreamToolArguments(tc),
				},
			})
		}
		if c.FinishReason != nil {
			chunks = append(chunks, core.StreamChunk{
				Type:         core.ChunkFinish,
				FinishReason: mapOAIFinish(*c.FinishReason),
			})
		}
	}

	if raw.Usage != nil {
		var cached, reasoning int
		if raw.Usage.PromptTokensDetails != nil {
			cached = raw.Usage.PromptTokensDetails.CachedTokens
		}
		if raw.Usage.CompletionTokensDetails != nil {
			reasoning = raw.Usage.CompletionTokensDetails.ReasoningTokens
		}
		chunks = append(chunks, core.StreamChunk{
			Type: core.ChunkUsage,
			Usage: &core.Usage{
				PromptTokens:     raw.Usage.PromptTokens,
				CompletionTokens: raw.Usage.CompletionTokens,
				TotalTokens:      raw.Usage.TotalTokens,
				CachedTokens:     cached,
				ReasoningTokens:  reasoning,
				Source:           core.UsageSourceProvider,
			},
		})
	}
	return chunks, nil
}

func extractOpenAIStreamToolArguments(tc oaiStreamToolCall) json.RawMessage {
	for _, raw := range []json.RawMessage{
		tc.Function.Arguments,
		tc.Function.Input,
		tc.Function.Params,
		tc.Function.Args,
		tc.Arguments,
		tc.Input,
		tc.Params,
		tc.Args,
	} {
		if normalized := normalizeOpenAIToolArguments(raw); len(normalized) > 0 {
			return normalized
		}
	}
	return nil
}

func normalizeOpenAIToolArguments(raw json.RawMessage) json.RawMessage {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}

	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		raw = json.RawMessage(s)
	}
	return raw
}

// RenderStreamChunk encodes a canonical chunk as an OpenAI SSE event. The first
// emitted event carries the assistant role per the OpenAI contract.
func (OpenAICodec) RenderStreamChunk(chunk core.StreamChunk, state *StreamState) ([][]byte, error) {
	delta := map[string]any{}
	switch chunk.Type {
	case core.ChunkThinking:
		// Echo structured reasoning back to the client as reasoning_content so
		// clients that replay it on follow-up turns (Cursor, Cline, etc.) keep
		// the real reasoning. DeepSeek/MiniMax thinking mode requires this
		// field on subsequent turns or it returns a 400.
		if !state.SentRole {
			delta["role"] = "assistant"
			state.SentRole = true
		}
		delta["reasoning_content"] = chunk.Delta
	case core.ChunkText:
		if !state.SentRole {
			delta["role"] = "assistant"
			state.SentRole = true
		}
		delta["content"] = chunk.Delta

	case core.ChunkToolCall:
		if chunk.ToolCall == nil {
			return nil, fmt.Errorf("openai: missing tool call")
		}
		args := chunk.ToolCall.Arguments
		if len(args) == 0 {
			args = json.RawMessage("{}")
		}
		if !validCompleteToolArguments(args) {
			return nil, fmt.Errorf("openai: tool arguments must be a JSON object")
		}
		if !state.SentRole {
			delta["role"] = "assistant"
			state.SentRole = true
		}
		tc := map[string]any{
			"index": chunk.Index,
			"type":  "function",
			"function": map[string]string{
				"name":      chunk.ToolCall.Name,
				"arguments": string(args),
			},
		}
		if chunk.ToolCall.ID != "" {
			tc["id"] = chunk.ToolCall.ID
		}
		delta["tool_calls"] = []any{tc}
	case core.ChunkFinish:
		return [][]byte{encodeOAIEvent(state, map[string]any{}, ptr(string(chunk.FinishReason)), nil)}, nil
	case core.ChunkUsage:
		// Emit a usage-only chunk (OpenAI sends this as a final empty-choices event).
		return [][]byte{encodeOAIUsageEvent(state, chunk.Usage)}, nil
	default:
		return nil, nil
	}
	return [][]byte{encodeOAIEvent(state, delta, nil, nil)}, nil
}

// RenderStreamDone returns the OpenAI terminal sentinel.
func (OpenAICodec) RenderStreamDone(_ *StreamState) [][]byte {
	return [][]byte{[]byte("data: [DONE]\n\n")}
}

func encodeOAIEvent(state *StreamState, delta map[string]any, finish *string, _ any) []byte {
	choice := map[string]any{"index": 0, "delta": delta}
	if finish != nil {
		choice["finish_reason"] = *finish
	} else {
		choice["finish_reason"] = nil
	}
	payload := map[string]any{
		"id":      firstNonEmpty(state.MessageID, "chatcmpl-stream"),
		"object":  "chat.completion.chunk",
		"model":   state.Model,
		"choices": []any{choice},
	}
	b, _ := json.Marshal(payload)
	return append([]byte("data: "), append(b, '\n', '\n')...)
}

func encodeOAIUsageEvent(state *StreamState, usage *core.Usage) []byte {
	payload := map[string]any{
		"id":      firstNonEmpty(state.MessageID, "chatcmpl-stream"),
		"object":  "chat.completion.chunk",
		"model":   state.Model,
		"choices": []any{},
		"usage": map[string]int{
			"prompt_tokens":     usage.PromptTokens,
			"completion_tokens": usage.CompletionTokens,
			"total_tokens":      usage.TotalTokens,
		},
	}
	b, _ := json.Marshal(payload)
	return append([]byte("data: "), append(b, '\n', '\n')...)
}

func ptr[T any](v T) *T { return &v }
