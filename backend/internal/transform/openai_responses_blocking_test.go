package transform

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestResponsesParseDoneToolUpdatesAreComplete(t *testing.T) {
	parser := OpenAIResponsesCodec{}.NewStreamParser()
	for _, tt := range []struct {
		name string
		line string
		kind core.ToolCallKind
		want string
	}{
		{"function", `{"type":"response.function_call_arguments.done","output_index":2,"item_id":"fc_1","arguments":"{\"x\":1}"}`, "", `{"x":1}`},
		{"custom", `{"type":"response.custom_tool_call_input.done","output_index":3,"item_id":"ct_1","input":"echo hi"}`, core.ToolCallCustom, "echo hi"},
		{"empty custom", `{"type":"response.custom_tool_call_input.done","output_index":4,"item_id":"ct_2","input":""}`, core.ToolCallCustom, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			chunks, err := parser.ParseStreamLine([]byte(tt.line), "model")
			if err != nil {
				t.Fatal(err)
			}
			if len(chunks) != 1 || chunks[0].Type != core.ChunkToolCall || chunks[0].ToolArgumentMode != core.ToolArgumentComplete || chunks[0].ToolCall == nil || chunks[0].ToolCall.Kind != tt.kind || string(chunks[0].ToolCall.Arguments) != tt.want {
				t.Fatalf("chunks = %#v, want one complete %s update %q", chunks, tt.kind, tt.want)
			}
		})
	}
}

func TestResponsesRenderClosesItemsBeforeOneCompletedWithCachedUsage(t *testing.T) {
	codec := OpenAIResponsesCodec{}
	state := &StreamState{MessageID: "ordering"}
	var out string
	for _, chunk := range []core.StreamChunk{
		{Type: core.ChunkToolCall, Index: 0, ToolArgumentMode: core.ToolArgumentComplete, ToolCall: &core.ToolCall{ID: "call_1", Name: "fn", Arguments: json.RawMessage(`{"x":1}`)}},
		{Type: core.ChunkUsage, Usage: &core.Usage{PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6, CachedTokens: 3}},
	} {
		events, err := codec.RenderStreamChunk(chunk, state)
		if err != nil {
			t.Fatal(err)
		}
		out += strings.Join(toStrings(events), "")
	}
	if strings.Contains(out, "response.completed") {
		t.Fatalf("completed before output item was closed: %s", out)
	}
	events, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishToolCalls}, state)
	if err != nil {
		t.Fatal(err)
	}
	out += strings.Join(toStrings(events), "")
	out += strings.Join(toStrings(codec.RenderStreamDone(state)), "")
	if strings.Count(out, `event: response.completed`) != 1 {
		t.Fatalf("completed count != 1: %s", out)
	}
	if strings.Index(out, `response.output_item.done`) > strings.Index(out, `event: response.completed`) {
		t.Fatalf("completion preceded item done: %s", out)
	}
	if !strings.Contains(out, `"cached_tokens":3`) {
		t.Fatalf("cached usage missing from completion: %s", out)
	}
}

func TestResponsesErrorRendersNativeFailedWithoutCompleted(t *testing.T) {
	codec := OpenAIResponsesCodec{}
	state := &StreamState{MessageID: "failed"}
	if _, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkText, Delta: "before"}, state); err != nil {
		t.Fatal(err)
	}
	events, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkError, Err: errors.New("upstream broke")}, state)
	if err != nil {
		t.Fatal(err)
	}
	out := strings.Join(toStrings(events), "") + strings.Join(toStrings(codec.RenderStreamDone(state)), "")
	if !strings.Contains(out, `event: response.failed`) || !strings.Contains(out, `"status":"failed"`) || !strings.Contains(out, `"message":"upstream broke"`) {
		t.Fatalf("native failed envelope missing: %s", out)
	}
	if strings.Contains(out, "response.completed") {
		t.Fatalf("failed response also completed successfully: %s", out)
	}
}

func TestResponsesCompletedAfterToolEventsFinishesWithToolCalls(t *testing.T) {
	parser := OpenAIResponsesCodec{}.NewStreamParser()
	for _, line := range []string{
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"fn"}}`,
		`{"type":"response.function_call_arguments.done","output_index":0,"item_id":"fc_1","arguments":"{}"}`,
	} {
		if _, err := parser.ParseStreamLine([]byte(line), "model"); err != nil {
			t.Fatal(err)
		}
	}
	chunks, err := parser.ParseStreamLine([]byte(`{"type":"response.completed","response":{"status":"completed"}}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 || chunks[0].Type != core.ChunkFinish || chunks[0].FinishReason != core.FinishToolCalls {
		t.Fatalf("chunks = %#v, want tool_calls finish", chunks)
	}

	plain := OpenAIResponsesCodec{}.NewStreamParser()
	chunks, err = plain.ParseStreamLine([]byte(`{"type":"response.completed","response":{"status":"completed"}}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 || chunks[0].FinishReason != core.FinishStop {
		t.Fatalf("plain completion chunks = %#v, want stop finish", chunks)
	}
}

func TestResponsesIncompletePreservesReasonAndUsage(t *testing.T) {
	parser := OpenAIResponsesCodec{}.NewStreamParser()
	chunks, err := parser.ParseStreamLine([]byte(`{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":2,"output_tokens":3}}}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 2 || chunks[0].Type != core.ChunkFinish || chunks[0].FinishReason != core.FinishLength || chunks[0].Delta != "max_output_tokens" || chunks[1].Type != core.ChunkUsage {
		t.Fatalf("chunks = %#v, want length finish preserving reason plus usage", chunks)
	}

	malformed, err := parser.ParseStreamLine([]byte(`{"type":"response.incomplete","response":{"status":"incomplete"}}`), "model")
	if err == nil || len(malformed) != 0 {
		t.Fatalf("malformed incomplete = %#v, %v; want error", malformed, err)
	}
}

func TestResponsesRenderClosesReasoningBeforeCompletion(t *testing.T) {
	codec := OpenAIResponsesCodec{}
	state := &StreamState{MessageID: "reasoning"}
	events, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkThinking, Delta: "thinking"}, state)
	if err != nil {
		t.Fatal(err)
	}
	out := strings.Join(toStrings(events), "")
	events, err = codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}, state)
	if err != nil {
		t.Fatal(err)
	}
	out += strings.Join(toStrings(events), "") + strings.Join(toStrings(codec.RenderStreamDone(state)), "")
	itemDone := strings.Index(out, `event: response.output_item.done`)
	completed := strings.Index(out, `event: response.completed`)
	if itemDone < 0 || completed < 0 || itemDone > completed || !strings.Contains(out, `"type":"reasoning"`) {
		t.Fatalf("reasoning item not closed before completion: %s", out)
	}
}

func TestResponsesParseUnaryIncomplete(t *testing.T) {
	resp, err := (OpenAIResponsesCodec{}).ParseResponse([]byte(`{"id":"resp_1","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","content":[{"type":"output_text","text":"partial"}]}]}`), "model")
	if err != nil {
		t.Fatal(err)
	}
	if resp.FinishReason != core.FinishLength {
		t.Fatalf("finish reason = %q, want length", resp.FinishReason)
	}

	if _, err := (OpenAIResponsesCodec{}).ParseResponse([]byte(`{"id":"resp_1","status":"incomplete","output":[]}`), "model"); err == nil {
		t.Fatal("incomplete unary response without reason was accepted")
	}
}

func TestResponsesRenderIncompleteDoesNotComplete(t *testing.T) {
	codec := OpenAIResponsesCodec{}
	state := &StreamState{MessageID: "incomplete"}
	if _, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkText, Delta: "partial"}, state); err != nil {
		t.Fatal(err)
	}
	if _, err := codec.RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishLength, Delta: "max_output_tokens"}, state); err != nil {
		t.Fatal(err)
	}
	out := strings.Join(toStrings(codec.RenderStreamDone(state)), "")
	if !strings.Contains(out, `event: response.incomplete`) || !strings.Contains(out, `"reason":"max_output_tokens"`) || strings.Contains(out, "response.completed") {
		t.Fatalf("incomplete outcome not preserved: %s", out)
	}
}

func TestResponsesCustomToolDefinitionRoundTrip(t *testing.T) {
	body := []byte(`{"model":"gpt-5","input":[],"tools":[{"type":"custom","name":"shell","description":"commands","format":{"type":"text"}},{"type":"freeform","name":"code","format":{"type":"grammar","syntax":"lark","definition":"start: WORD"}}]}`)
	req, err := OpenAIResponsesCodec{}.ParseRequest(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Tools) != 2 || req.Tools[0].Kind != core.ToolCustom || string(req.Tools[0].Format) != `{"type":"text"}` || req.Tools[1].Kind != core.ToolFreeform {
		t.Fatalf("custom/freeform tool lost canonical kind/format: %#v", req.Tools)
	}
	rendered, err := (OpenAIResponsesCodec{}).RenderRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Tools []struct {
			Type       string          `json:"type"`
			Format     json.RawMessage `json:"format"`
			Parameters json.RawMessage `json:"parameters"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(rendered, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Tools) != 2 || got.Tools[0].Type != "custom" || string(got.Tools[0].Format) != `{"type":"text"}` || len(got.Tools[0].Parameters) != 0 || got.Tools[1].Type != "freeform" || len(got.Tools[1].Parameters) != 0 {
		t.Fatalf("rendered custom/freeform tool became function schema: %s", rendered)
	}
}

func TestResponsesEmptyToolPayloadDependsOnKind(t *testing.T) {
	for _, tt := range []struct {
		name string
		item string
		kind core.ToolCallKind
		want string
	}{
		{"function", `{"type":"function_call","call_id":"f","name":"fn"}`, "", `{}`},
		{"custom", `{"type":"custom_tool_call","call_id":"c","name":"shell","input":""}`, core.ToolCallCustom, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := (OpenAIResponsesCodec{}).ParseResponse([]byte(`{"output":[`+tt.item+`]}`), "model")
			if err != nil {
				t.Fatal(err)
			}
			call := resp.Message.Content[0].ToolCall
			if call.Kind != tt.kind || string(call.Arguments) != tt.want {
				t.Fatalf("call = %#v, want kind %q payload %q", call, tt.kind, tt.want)
			}
			rendered, err := (OpenAIResponsesCodec{}).RenderResponse(resp)
			if err != nil {
				t.Fatal(err)
			}
			if tt.kind == core.ToolCallCustom && !strings.Contains(string(rendered), `"input":""`) {
				t.Fatalf("empty custom input changed: %s", rendered)
			}
		})
	}
}
