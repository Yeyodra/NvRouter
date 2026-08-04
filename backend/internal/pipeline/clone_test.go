package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestCloneForAttemptDeepClonesMutableRequestData(t *testing.T) {
	temperature := 0.5
	maxTokens := 100
	req := &core.ChatRequest{
		Messages: []core.Message{{Content: []core.ContentPart{
			{Type: core.PartImage, Media: &core.MediaPayload{MIMEType: "image/png", Data: "original-image"}},
			{Type: core.PartToolCall, ToolCall: &core.ToolCall{Arguments: json.RawMessage(`{"original":true}`)}},
			{Type: core.PartToolResult, ToolResult: &core.ToolResult{Content: "original-result"}},
		}}},
		Tools:          []core.Tool{{Parameters: json.RawMessage(`{"type":"object"}`), Format: json.RawMessage(`{"format":"original"}`)}},
		ToolChoice:     map[string]any{"function": map[string]any{"name": "original"}, "allowed": []any{"one"}},
		Stop:           []string{"original-stop"},
		Temperature:    &temperature,
		MaxTokens:      &maxTokens,
		Reasoning:      &core.ReasoningConfig{Effort: "high"},
		ResponseFormat: json.RawMessage(`{"type":"json_schema"}`),
		Extra:          map[string]json.RawMessage{"nested": json.RawMessage(`{"original":true}`)},
	}

	first := cloneForAttempt(req, "first")
	first.Messages[0].Content[0].Media.Data = "stripped"
	first.Messages[0].Content[1].ToolCall.Arguments[2] = 'X'
	first.Messages[0].Content[2].ToolResult.Content = "changed"
	first.Tools[0].Parameters[2] = 'X'
	first.Tools[0].Format[2] = 'X'
	first.ToolChoice.(map[string]any)["function"].(map[string]any)["name"] = "changed"
	first.ToolChoice.(map[string]any)["allowed"].([]any)[0] = "changed"
	first.Stop[0] = "changed"
	*first.Temperature = 1
	*first.MaxTokens = 1
	first.Reasoning.Effort = "low"
	first.ResponseFormat[2] = 'X'
	first.Extra["nested"][2] = 'X'

	second := cloneForAttempt(req, "second")
	if got := second.Messages[0].Content[0].Media.Data; got != "original-image" {
		t.Fatalf("fallback image = %q, want original image", got)
	}
	if string(second.Messages[0].Content[1].ToolCall.Arguments) != `{"original":true}` || second.Messages[0].Content[2].ToolResult.Content != "original-result" {
		t.Fatalf("fallback message was mutated: %+v", second.Messages[0])
	}
	if string(second.Tools[0].Parameters) != `{"type":"object"}` || string(second.Tools[0].Format) != `{"format":"original"}` {
		t.Fatalf("fallback tools were mutated: %+v", second.Tools[0])
	}
	if second.ToolChoice.(map[string]any)["function"].(map[string]any)["name"] != "original" || second.ToolChoice.(map[string]any)["allowed"].([]any)[0] != "one" {
		t.Fatalf("fallback tool choice was mutated: %#v", second.ToolChoice)
	}
	if second.Stop[0] != "original-stop" || *second.Temperature != 0.5 || *second.MaxTokens != 100 || second.Reasoning.Effort != "high" {
		t.Fatalf("fallback scalar pointers/slices were mutated: %+v", second)
	}
	if string(second.ResponseFormat) != `{"type":"json_schema"}` || string(second.Extra["nested"]) != `{"original":true}` {
		t.Fatalf("fallback raw JSON was mutated: %+v", second)
	}
}
