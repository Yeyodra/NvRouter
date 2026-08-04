package transform

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestResponsesCustomToolInputRemainsFreeform(t *testing.T) {
	parser := OpenAIResponsesCodec{}.NewStreamParser()
	var chunks []core.StreamChunk
	for _, line := range []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","id":"ct_1","call_id":"call_1","name":"shell"}}`,
		`{"type":"response.custom_tool_call_input.delta","output_index":0,"item_id":"ct_1","delta":"echo hi"}`,
	} {
		got, err := parser.ParseStreamLine([]byte(line), "model")
		if err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, got...)
	}

	state := &StreamState{}
	var rendered []byte
	for _, chunk := range chunks {
		events, err := (OpenAIResponsesCodec{}).RenderStreamChunk(chunk, state)
		if err != nil {
			t.Fatalf("render custom tool input: %v", err)
		}
		for _, event := range events {
			rendered = append(rendered, event...)
		}
	}
	got := string(rendered)
	if !strings.Contains(got, `"type":"custom_tool_call"`) || !strings.Contains(got, `response.custom_tool_call_input.delta`) || !strings.Contains(got, `"delta":"echo hi"`) {
		t.Fatalf("custom tool kind/input not preserved: %s", got)
	}

	_, err := (OpenAIResponsesCodec{}).RenderStreamChunk(core.StreamChunk{
		Type:     core.ChunkToolCall,
		ToolCall: &core.ToolCall{ID: "call_2", Name: "fn", Arguments: json.RawMessage(`not-json`)},
	}, &StreamState{})
	if err == nil {
		t.Fatal("function call with non-object arguments was accepted")
	}
}

func TestFunctionRenderersNormalizeEmptyArguments(t *testing.T) {
	chunk := core.StreamChunk{Type: core.ChunkToolCall, ToolCall: &core.ToolCall{ID: "call_1", Name: "fn"}}
	for _, tt := range []struct {
		name   string
		render func(core.StreamChunk, *StreamState) ([][]byte, error)
	}{
		{"openai", OpenAICodec{}.RenderStreamChunk},
		{"responses", OpenAIResponsesCodec{}.RenderStreamChunk},
		{"anthropic", AnthropicCodec{}.RenderStreamChunk},
		{"gemini", GeminiCodec{}.RenderStreamChunk},
		{"ollama", OllamaCodec{}.RenderStreamChunk},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out, err := tt.render(chunk, &StreamState{})
			if err != nil {
				t.Fatalf("empty arguments rejected: %v", err)
			}
			if len(out) == 0 {
				t.Fatal("empty arguments produced no tool event")
			}
		})
	}
}

func TestOpenAIStreamArgumentNormalizationPreservesQuotedFragments(t *testing.T) {
	chunks, err := (OpenAICodec{}).ParseStreamLine([]byte(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"hello\""}}]}}]}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 || chunks[0].ToolCall == nil || string(chunks[0].ToolCall.Arguments) != `"hello"` {
		t.Fatalf("arguments = %#v, want quoted fragment bytes %q", chunks, `"hello"`)
	}

	for _, want := range []string{
		`{"payload":{"x":1}}`,
		`{"arguments":{"x":1}}`,
		`{"arguments":"{ \"x\" : 1 }"}`,
	} {
		line := `{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":` + want + `}}]}}]}`
		chunks, err = (OpenAICodec{}).ParseStreamLine([]byte(line), "model")
		if err != nil {
			t.Fatal(err)
		}
		if got := string(chunks[0].ToolCall.Arguments); got != want {
			t.Fatalf("ordinary object %s was normalized to %s", want, got)
		}
	}
}

func TestAnthropicUsageSurvivesParseAndRenderWithoutDoubleCountingCache(t *testing.T) {
	state := &StreamState{Model: "model"}
	var rendered string
	for _, line := range []string{
		`{"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":2,"cache_creation_input_tokens":3}}}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`,
		`{"type":"message_delta","usage":{"output_tokens":4}}`,
	} {
		chunks, err := (AnthropicCodec{}).ParseStreamLine([]byte(line), "model")
		if err != nil {
			t.Fatal(err)
		}
		for _, chunk := range chunks {
			if chunk.Type == core.ChunkUsage && chunk.Usage.PromptTokens != 0 && chunk.Usage.PromptTokens != 15 {
				t.Fatalf("canonical prompt tokens = %d, want inclusive total 15", chunk.Usage.PromptTokens)
			}
			events, err := (AnthropicCodec{}).RenderStreamChunk(chunk, state)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				rendered += string(event)
			}
		}
	}
	for _, event := range (AnthropicCodec{}).RenderStreamDone(state) {
		rendered += string(event)
	}
	if !strings.Contains(rendered, `"input_tokens":10`) || !strings.Contains(rendered, `"cache_read_input_tokens":2`) || !strings.Contains(rendered, `"cache_creation_input_tokens":3`) {
		t.Fatalf("cache-exclusive Anthropic input usage not rendered: %s", rendered)
	}
	if strings.Count(rendered, "event: message_start\n") != 1 || strings.Count(rendered, "event: message_delta\n") != 1 || strings.Count(rendered, "event: message_stop\n") != 1 {
		t.Fatalf("stream emitted more than one message lifecycle: %s", rendered)
	}
	if strings.Count(rendered, `"stop_reason":"end_turn"`) != 1 || !strings.Contains(rendered, `"output_tokens":4`) {
		t.Fatalf("late usage duplicated finish or was lost: %s", rendered)
	}
}

func TestAnthropicUnaryUsageRoundTripDoesNotDoubleCountCache(t *testing.T) {
	codec := AnthropicCodec{}
	resp, err := codec.ParseResponse([]byte(`{"usage":{"input_tokens":10,"output_tokens":4,"cache_read_input_tokens":2,"cache_creation_input_tokens":3}}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.PromptTokens != 15 || resp.Usage.TotalTokens != 19 {
		t.Fatalf("canonical usage = %+v, want inclusive prompt=15 total=19", resp.Usage)
	}
	rendered, err := codec.RenderResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"input_tokens":10`, `"cache_read_input_tokens":2`, `"cache_creation_input_tokens":3`} {
		if !strings.Contains(string(rendered), want) {
			t.Fatalf("rendered usage %s missing %s", rendered, want)
		}
	}
}

func TestGeminiUsagePrecedesFinish(t *testing.T) {
	chunks, err := (GeminiCodec{}).ParseStreamLine([]byte(`{"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3,"totalTokenCount":5}}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || chunks[0].Type != core.ChunkUsage || chunks[1].Type != core.ChunkFinish {
		t.Fatalf("chunks = %#v, want usage before finish", chunks)
	}
}

func TestOllamaTerminalDoneReasonPreservesFinish(t *testing.T) {
	state := &StreamState{Model: "model"}
	if _, err := (OllamaCodec{}).RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishLength}, state); err != nil {
		t.Fatal(err)
	}
	got := string((OllamaCodec{}).RenderStreamDone(state)[0])
	if !strings.Contains(got, `"done_reason":"length"`) {
		t.Fatalf("terminal line lost finish reason: %s", got)
	}
}
