package gateway

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// streamUpstreamPing mimics an OpenAI-compatible provider (e.g. MiMo) replying
// to a short ping with content + finish + an optional trailing usage event.
// trailingUsage simulates providers that emit a usage-only chunk AFTER the
// finish chunk (common with reasoning/OpenAI-compatible models).
func streamUpstreamPing(trailingUsage bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flush, _ := w.(http.Flusher)
		lines := []string{
			`data: {"choices":[{"delta":{"role":"assistant","content":"Hi"}}]}`,
			`data: {"choices":[{"delta":{"content":"!"}}]}`,
			`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		}
		if trailingUsage {
			lines = append(lines, `data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`)
		}
		lines = append(lines, `data: [DONE]`)
		for _, l := range lines {
			fmt.Fprintf(w, "%s\n\n", l)
			if flush != nil {
				flush.Flush()
			}
		}
	}
}

// streamUpstreamPingReasoning mimics a reasoning OpenAI-compatible model
// (e.g. MiMo) that emits reasoning_content BEFORE content, then a finish
// chunk. This is the cross-dialect case that exercises the Anthropic codec's
// thinking-block handling.
func streamUpstreamPingReasoning() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flush, _ := w.(http.Flusher)
		for _, l := range []string{
			`data: {"choices":[{"delta":{"role":"assistant","reasoning_content":"let me think"}}]}`,
			`data: {"choices":[{"delta":{"reasoning_content":"briefly"}}]}`,
			`data: {"choices":[{"delta":{"content":"Hi!"}}]}`,
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

// streamUpstreamPingThinkTags mimics models (MiMo, QwQ) that embed reasoning
// as <think>...</think> XML tags inside the content field instead of using
// reasoning_content. The gateway's ThinkTagState strips these.
func streamUpstreamPingThinkTags() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flush, _ := w.(http.Flusher)
		for _, l := range []string{
			`data: {"choices":[{"delta":{"role":"assistant","content":""}}]}`,
			`data: {"choices":[{"delta":{"content":"Hi<thi"}}]}`,
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

// TestE2E_AnthropicStreamPing_SingleMessage reproduces the reported bug: a
// Claude Code ping ("hi bro"/"test") to an OpenAI-compatible provider must
// produce exactly ONE Anthropic message lifecycle (1 message_start, 1
// message_stop), not two. Claude Code shows two conversation turns when it
// receives two message_start events in one stream.
func TestE2E_AnthropicStreamPing_SingleMessage(t *testing.T) {
	cases := []struct {
		name     string
		upstream http.HandlerFunc
		pingBody string
	}{
		{"no_trailing_usage", streamUpstreamPing(false), `{"model":"openai/gpt-4o","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi bro"}]}`},
		{"with_trailing_usage", streamUpstreamPing(true), `{"model":"openai/gpt-4o","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi bro"}]}`},
		{"ping_test", streamUpstreamPing(false), `{"model":"openai/gpt-4o","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"test"}]}`},
		{"reasoning_content", streamUpstreamPingReasoning(), `{"model":"openai/gpt-4o","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi bro"}]}`},
		{"think_tags", streamUpstreamPingThinkTags(), `{"model":"openai/gpt-4o","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi bro"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newE2E(t, tc.upstream)

			resp := h.post(t, "/v1/messages", tc.pingBody, h.apiKey)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			out := string(raw)
			t.Logf("body:\n%s", out)

			starts := strings.Count(out, "event: message_start")
			stops := strings.Count(out, "event: message_stop")
			deltas := strings.Count(out, "event: message_delta")
			require.Equal(t, 1, starts, "expected exactly 1 message_start, got %d — a second message_start is the 2-turn bug", starts)
			require.Equal(t, 1, stops, "expected exactly 1 message_stop, got %d", stops)
			require.LessOrEqual(t, deltas, 1, "expected at most 1 message_delta, got %d — extra message_delta can signal a 2nd turn to clients", deltas)
			if tc.name == "with_trailing_usage" {
				require.Contains(t, out, `"output_tokens":2`)
			}
			if tc.name == "think_tags" {
				require.Less(t, strings.Index(out, `"text":"<thi"`), strings.Index(out, `"stop_reason":"end_turn"`), "buffered text must precede terminal finish")
			}
		})
	}
}
