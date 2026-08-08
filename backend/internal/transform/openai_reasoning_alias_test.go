package transform

import (
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

// Some OpenAI-compatible providers expose reasoning under a name other than
// reasoning_content: OpenRouter uses "reasoning", a few others use
// "reasoning_text". These must be parsed as canonical thinking so it is not
// silently dropped. Precedence when more than one is present:
// reasoning_content > reasoning > reasoning_text (no duplication).

func TestOpenAI_ParseResponse_ReasoningAlias(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"reasoning_content", `{"choices":[{"message":{"role":"assistant","reasoning_content":"rc","content":"a"}}]}`, "rc"},
		{"reasoning", `{"choices":[{"message":{"role":"assistant","reasoning":"r","content":"a"}}]}`, "r"},
		{"reasoning_text", `{"choices":[{"message":{"role":"assistant","reasoning_text":"rt","content":"a"}}]}`, "rt"},
		{"precedence_content_wins", `{"choices":[{"message":{"role":"assistant","reasoning_content":"rc","reasoning":"r","reasoning_text":"rt","content":"a"}}]}`, "rc"},
		{"precedence_reasoning_over_text", `{"choices":[{"message":{"role":"assistant","reasoning":"r","reasoning_text":"rt","content":"a"}}]}`, "r"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := OpenAICodec{}.ParseResponse([]byte(tc.body), "model")
			require.NoError(t, err)
			var thinking []string
			var text string
			for _, p := range resp.Message.Content {
				switch p.Type {
				case core.PartThinking:
					thinking = append(thinking, p.Text)
				case core.PartText:
					text = p.Text
				}
			}
			require.Equal(t, []string{tc.want}, thinking, "exactly one thinking part with expected text")
			require.Equal(t, "a", text)
		})
	}
}

func TestOpenAI_ParseStreamLine_ReasoningAlias(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"reasoning", `{"choices":[{"delta":{"reasoning":"r"}}]}`, "r"},
		{"reasoning_text", `{"choices":[{"delta":{"reasoning_text":"rt"}}]}`, "rt"},
		{"precedence_content_wins", `{"choices":[{"delta":{"reasoning_content":"rc","reasoning":"r"}}]}`, "rc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chunks, err := OpenAICodec{}.ParseStreamLine([]byte(tc.body), "model")
			require.NoError(t, err)
			require.Len(t, chunks, 1)
			require.Equal(t, core.ChunkThinking, chunks[0].Type)
			require.Equal(t, tc.want, chunks[0].Delta)
		})
	}
}
