package transform

import (
	"encoding/json"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

// Generic OpenAI-compatible reasoning models that are not in a hardcoded
// reasoning scope (deepseek/kimi/glm/minimax) must still forward a
// client-provided reasoning_effort. Absence stays absent; explicit off is
// omitted. The non-standard `thinking` object is NOT injected for generic
// providers, to avoid 400s on providers that don't understand it.

func TestOpenAI_RenderRequestForProvider_GenericEffortPassthrough(t *testing.T) {
	cases := []struct {
		name       string
		effort     string
		wantEffort string
	}{
		{"low", "low", "low"},
		{"medium", "medium", "medium"},
		{"high", "high", "high"},
		{"max", "max", "max"},
		{"off_omitted", "none", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &core.ChatRequest{
				Model:     "some-reasoning-model",
				Reasoning: &core.ReasoningConfig{Effort: tc.effort},
				Messages:  []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}}},
			}
			body, err := OpenAICodec{}.RenderRequestForProvider(req, "custom-openai")
			require.NoError(t, err)
			var got oaiRequest
			require.NoError(t, json.Unmarshal(body, &got))
			require.Equal(t, tc.wantEffort, got.ReasoningEffort)
			require.Nil(t, got.Thinking, "generic provider must not get a thinking object")
		})
	}
}

func TestOpenAI_RenderRequestForProvider_NoReasoningStaysAbsent(t *testing.T) {
	req := &core.ChatRequest{
		Model:    "some-reasoning-model",
		Messages: []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}}},
	}
	body, err := OpenAICodec{}.RenderRequestForProvider(req, "custom-openai")
	require.NoError(t, err)
	var got oaiRequest
	require.NoError(t, json.Unmarshal(body, &got))
	require.Empty(t, got.ReasoningEffort)
	require.Nil(t, got.Thinking)
}
