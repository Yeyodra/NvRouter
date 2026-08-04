package transform

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestToolLifecycle_DeltaContractDoesNotGuessSnapshot(t *testing.T) {
	s := NewToolArgSanitizer()
	var got []core.StreamChunk
	emit := func(c core.StreamChunk) { got = append(got, c) }

	s.Process(core.StreamChunk{Type: core.ChunkToolCall, Index: 0, ToolCall: &core.ToolCall{
		ID: "call_1", Name: "echo", Arguments: json.RawMessage(` `),
	}, ToolArgumentMode: core.ToolArgumentDelta}, emit)
	// This declared delta starts with the preceding bytes. Prefix heuristics
	// misclassify it as a full snapshot even though concatenation remains valid.
	s.Process(core.StreamChunk{Type: core.ChunkToolCall, Index: 0, ToolCall: &core.ToolCall{
		Arguments: json.RawMessage(` {"text":"a"}`),
	}, ToolArgumentMode: core.ToolArgumentDelta}, emit)
	s.Process(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishToolCalls}, emit)

	if len(got) == 0 || got[0].ToolCall == nil {
		t.Fatalf("missing finalized tool call: %#v", got)
	}
	want := `  {"text":"a"}`
	if actual := string(got[0].ToolCall.Arguments); actual != want {
		t.Fatalf("arguments = %q, want lossless delta concatenation %q", actual, want)
	}
}

func TestToolLifecycle_ParallelCallsPreserveLateIdentityAndOrder(t *testing.T) {
	s := NewToolArgSanitizer()
	var got []core.StreamChunk
	emit := func(c core.StreamChunk) { got = append(got, c) }

	for _, c := range []core.StreamChunk{
		{Type: core.ChunkToolCall, Index: 1, ToolCall: &core.ToolCall{ID: "call_b"}},
		{Type: core.ChunkToolCall, Index: 0, ToolCall: &core.ToolCall{ID: "call_a"}},
		{Type: core.ChunkToolCall, Index: 1, ToolCall: &core.ToolCall{Name: "second", Arguments: json.RawMessage(`{"b":2}`)}},
		{Type: core.ChunkToolCall, Index: 0, ToolCall: &core.ToolCall{Name: "first", Arguments: json.RawMessage(`{"a":1}`)}},
		{Type: core.ChunkFinish, FinishReason: core.FinishToolCalls},
	} {
		s.Process(c, emit)
	}

	if len(got) != 3 {
		t.Fatalf("got %d chunks, want two calls then finish: %#v", len(got), got)
	}
	for i, want := range []struct{ id, name, args string }{
		{"call_a", "first", `{"a":1}`},
		{"call_b", "second", `{"b":2}`},
	} {
		if got[i].Index != i || got[i].ToolCall == nil || got[i].ToolCall.ID != want.id || got[i].ToolCall.Name != want.name || string(got[i].ToolCall.Arguments) != want.args {
			t.Fatalf("call[%d] = %#v, want index/id/name/args preserved", i, got[i])
		}
	}
	if got[2].Type != core.ChunkFinish {
		t.Fatalf("last chunk = %#v, want finish", got[2])
	}
}

func TestResponsesStream_ParallelCallsCorrelateIndicesAndLateIdentity(t *testing.T) {
	codec := OpenAIResponsesCodec{}.NewStreamParser()
	lines := []string{
		`{"type":"response.output_item.added","output_index":4,"item":{"type":"function_call","id":"fc_a","call_id":"call_a","name":"first"}}`,
		`{"type":"response.function_call_arguments.delta","output_index":7,"item_id":"fc_b","delta":"{\"b\":"}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_a","delta":"{\"a\":1}"}`,
		`{"type":"response.output_item.added","item":{"type":"function_call","id":"fc_b","call_id":"call_b","name":"second"}}`,
		`{"type":"response.function_call_arguments.delta","item_id":"fc_b","delta":"2}"}`,
	}
	var got []core.StreamChunk
	for _, line := range lines {
		chunks, err := codec.ParseStreamLine([]byte(line), "gpt-5")
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, chunks...)
	}
	want := []struct {
		index int
		id    string
		args  string
	}{{4, "call_a", ""}, {7, "", `{"b":`}, {4, "", `{"a":1}`}, {7, "call_b", ""}, {7, "", `2}`}}
	if len(got) != len(want) {
		t.Fatalf("got %d chunks, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Index != want[i].index || got[i].ToolCall == nil || got[i].ToolCall.ID != want[i].id || string(got[i].ToolCall.Arguments) != want[i].args {
			t.Fatalf("chunk[%d] = %#v, want index=%d id=%q args=%q", i, got[i], want[i].index, want[i].id, want[i].args)
		}
	}
}

func TestCommandCodeStream_ParallelCallsRetainIdentity(t *testing.T) {
	codec := CommandCodeCodec{}.NewStreamParser()
	lines := []string{
		`{"type":"tool-input-start","id":"call_a","toolName":"same"}`,
		`{"type":"tool-input-start","id":"call_b","toolName":"same"}`,
		`{"type":"tool-input-delta","id":"call_b","delta":"{\"b\":2}"}`,
		`{"type":"tool-input-delta","id":"call_a","delta":"{\"a\":1}"}`,
		`{"type":"tool-call","toolCallId":"call_b","toolName":"same","input":{"b":2}}`,
		`{"type":"tool-call","toolCallId":"call_a","toolName":"same","input":{"a":1}}`,
	}
	var got []core.StreamChunk
	for _, line := range lines {
		chunks, err := codec.ParseStreamLine([]byte(line), "model")
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, chunks...)
	}
	want := []struct {
		index int
		id    string
	}{{0, "call_a"}, {1, "call_b"}, {1, "call_b"}, {0, "call_a"}, {1, "call_b"}, {0, "call_a"}}
	if len(got) != len(want) {
		t.Fatalf("got %d chunks, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].Index != want[i].index || got[i].ToolCall == nil || got[i].ToolCall.ID != want[i].id {
			t.Fatalf("chunk[%d] = %#v, want index=%d id=%q", i, got[i], want[i].index, want[i].id)
		}
	}
}

func TestOllamaStream_OmittedFunctionIndicesUseArrayOrdinal(t *testing.T) {
	chunks, err := OllamaCodec{}.ParseStreamLine([]byte(`{"message":{"tool_calls":[{"function":{"name":"same","arguments":{"a":1}}},{"function":{"name":"same","arguments":{"b":2}}}]}}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || chunks[0].Index != 0 || chunks[1].Index != 1 {
		t.Fatalf("chunks = %#v, want array ordinal indices 0,1", chunks)
	}
}

func TestGeminiStream_SameFunctionCallsHaveDistinctStableIDs(t *testing.T) {
	chunks, err := GeminiCodec{}.ParseStreamLine([]byte(`{"candidates":[{"content":{"parts":[{"text":"thinking"},{"functionCall":{"name":"same","args":{"a":1}}},{"functionCall":{"name":"same","args":{"b":2}}}]}}]}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || chunks[1].Index != 0 || chunks[2].Index != 1 || chunks[1].ToolCall.ID != "call_same_0" || chunks[2].ToolCall.ID != "call_same_1" {
		t.Fatalf("chunks = %#v, want tool ordinals and distinct stable IDs", chunks)
	}
}

func TestResponsesRender_DeterministicDistinctOutputIndices(t *testing.T) {
	codec := OpenAIResponsesCodec{}
	state := &StreamState{MessageID: "parallel"}
	var out [][]byte
	for _, chunk := range []core.StreamChunk{
		{Type: core.ChunkText, Delta: "hi"},
		{Type: core.ChunkThinking, Delta: "why"},
		{Type: core.ChunkToolCall, Index: 1, ToolCall: &core.ToolCall{ID: "call_b", Name: "second", Arguments: json.RawMessage(`{"b":2}`)}},
		{Type: core.ChunkToolCall, Index: 0, ToolCall: &core.ToolCall{ID: "call_a", Name: "first", Arguments: json.RawMessage(`{"a":1}`)}},
	} {
		events, err := codec.RenderStreamChunk(chunk, state)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, events...)
	}
	events, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish}, state)
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, events...)
	joined := strings.Join(toStrings(out), "")
	first := strings.LastIndex(joined, `"call_id":"call_a"`)
	second := strings.LastIndex(joined, `"call_id":"call_b"`)
	if first < 0 || second < 0 || first >= second {
		t.Fatalf("completion order is not deterministic: %s", joined)
	}
	for _, index := range []string{`"output_index":0`, `"output_index":1`, `"output_index":2`, `"output_index":3`} {
		if !strings.Contains(joined, index) {
			t.Fatalf("missing distinct %s: %s", index, joined)
		}
	}
}

func TestStreamFinishDeduplicationPreservesUsage(t *testing.T) {
	tests := []struct {
		name  string
		parse func([]byte, string) ([]core.StreamChunk, error)
		lines []string
	}{
		{"openai chat", OpenAICodec{}.ParseStreamLine, []string{
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"choices":[],"usage":{"total_tokens":3}}`,
		}},
		{"openai responses", OpenAIResponsesCodec{}.ParseStreamLine, []string{
			`{"type":"response.completed"}`,
			`{"type":"response.completed"}`,
			`{"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":2}}}`,
		}},
		{"gemini", GeminiCodec{}.ParseStreamLine, []string{
			`{"candidates":[{"finishReason":"STOP"}]}`,
			`{"candidates":[{"finishReason":"STOP"}]}`,
			`{"usageMetadata":{"totalTokenCount":3}}`,
		}},
		{"ollama", OllamaCodec{}.ParseStreamLine, []string{
			`{"done":true}`,
			`{"done":true}`,
			`{"done":true,"prompt_eval_count":1,"eval_count":2}`,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewToolArgSanitizer()
			var got []core.StreamChunk
			for _, line := range tt.lines {
				chunks, err := tt.parse([]byte(line), "model")
				if err != nil {
					t.Fatal(err)
				}
				for _, chunk := range chunks {
					s.Process(chunk, func(c core.StreamChunk) { got = append(got, c) })
				}
			}
			var finishes, usages int
			for _, chunk := range got {
				if chunk.Type == core.ChunkFinish {
					finishes++
				}
				if chunk.Type == core.ChunkUsage {
					usages++
				}
			}
			if finishes != 1 || usages == 0 {
				t.Fatalf("finish/usage chunks = %d/%d, want one finish and later usage: %#v", finishes, usages, got)
			}
		})
	}
}

func TestToolLifecycle_AllStreamProducersDeclareArgumentMode(t *testing.T) {
	tests := []struct {
		name  string
		parse func([]byte, string) ([]core.StreamChunk, error)
		line  string
		want  core.ToolArgumentMode
	}{
		{"openai", OpenAICodec{}.ParseStreamLine, `{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\\\"x\\\":1}"}}]}}]}`, core.ToolArgumentDelta},
		{"anthropic", AnthropicCodec{}.ParseStreamLine, `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\\\"x\\\":1}"}}`, core.ToolArgumentDelta},
		{"responses", OpenAIResponsesCodec{}.ParseStreamLine, `{"type":"response.function_call_arguments.delta","delta":"{\\\"x\\\":1}"}`, core.ToolArgumentDelta},
		{"commandcode delta", CommandCodeCodec{}.ParseStreamLine, `{"type":"tool-input-delta","delta":"{\\\"x\\\":1}"}`, core.ToolArgumentDelta},
		{"commandcode complete", CommandCodeCodec{}.ParseStreamLine, `{"type":"tool-call","toolCallId":"c","toolName":"f","input":{"x":1}}`, core.ToolArgumentComplete},
		{"gemini", GeminiCodec{}.ParseStreamLine, `{"candidates":[{"content":{"parts":[{"functionCall":{"name":"f","args":{"x":1}}}]}}]}`, core.ToolArgumentComplete},
		{"ollama", OllamaCodec{}.ParseStreamLine, `{"message":{"tool_calls":[{"function":{"name":"f","arguments":{"x":1}}}]}}`, core.ToolArgumentComplete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, err := tt.parse([]byte(tt.line), "model")
			if err != nil || len(chunks) != 1 || chunks[0].Type != core.ChunkToolCall || chunks[0].ToolArgumentMode != tt.want {
				t.Fatalf("chunks = %#v, err = %v, want one %s tool update", chunks, err, tt.want)
			}
		})
	}
}

func TestToolLifecycle_RenderersRejectInvalidArguments(t *testing.T) {
	chunk := core.StreamChunk{Type: core.ChunkToolCall, ToolArgumentMode: core.ToolArgumentComplete, ToolCall: &core.ToolCall{ID: "c", Name: "f", Arguments: json.RawMessage(`{"x":`)}}
	for _, tt := range []struct {
		name   string
		render func(core.StreamChunk, *StreamState) ([][]byte, error)
	}{
		{"openai", OpenAICodec{}.RenderStreamChunk},
		{"anthropic", AnthropicCodec{}.RenderStreamChunk},
		{"responses", OpenAIResponsesCodec{}.RenderStreamChunk},
		{"gemini", GeminiCodec{}.RenderStreamChunk},
		{"ollama", OllamaCodec{}.RenderStreamChunk},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if out, err := tt.render(chunk, &StreamState{}); err == nil || len(out) != 0 {
				t.Fatalf("out = %q, err = %v, want rejection", out, err)
			}
		})
	}
}

func TestToolLifecycle_InvalidFinalArgumentsDoNotEmitSuccessfulFinish(t *testing.T) {
	s := NewToolArgSanitizer()
	var got []core.StreamChunk
	emit := func(c core.StreamChunk) { got = append(got, c) }

	s.Process(core.StreamChunk{Type: core.ChunkToolCall, Index: 0, ToolCall: &core.ToolCall{
		ID: "call_1", Name: "echo", Arguments: json.RawMessage(`{"text:hello"}`),
	}}, emit)
	s.Process(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishToolCalls}, emit)

	for _, c := range got {
		if c.Type == core.ChunkFinish && c.FinishReason == core.FinishToolCalls {
			t.Fatalf("malformed arguments were reported as a successful tool call: %#v", got)
		}
	}
	if len(got) != 1 || got[0].Type != core.ChunkError {
		t.Fatalf("got %#v, want one protocol error", got)
	}
}
