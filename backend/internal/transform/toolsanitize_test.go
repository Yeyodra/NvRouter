package transform

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestToolArgSanitizer_PassesThroughNonToolChunks(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk

	s.Process(core.StreamChunk{Type: core.ChunkText, Delta: "hello"}, func(c core.StreamChunk) {
		emitted = append(emitted, c)
	})

	if len(emitted) != 1 || emitted[0].Delta != "hello" {
		t.Errorf("non-tool chunk not passed through: %+v", emitted)
	}
}

func TestToolArgSanitizer_BuffersAndFlushes(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk

	// First chunk: tool call start with ID.
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			ID:        "tc1",
			Name:      "Read",
			Arguments: nil,
		},
	}, func(c core.StreamChunk) { emitted = append(emitted, c) })

	// Second chunk: argument delta.
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			Arguments: json.RawMessage(`{"file_path":"/tmp/a.txt"}`),
		},
	}, func(c core.StreamChunk) { emitted = append(emitted, c) })

	// Nothing emitted yet (buffering).
	if len(emitted) != 0 {
		t.Errorf("expected 0 emitted during buffering, got %d", len(emitted))
	}

	// Flush.
	s.Flush(func(c core.StreamChunk) { emitted = append(emitted, c) })

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted on flush, got %d", len(emitted))
	}
	if emitted[0].ToolCall.ID != "tc1" {
		t.Errorf("expected ID 'tc1', got %q", emitted[0].ToolCall.ID)
	}
	if emitted[0].ToolCall.Name != "Read" {
		t.Errorf("expected Name 'Read', got %q", emitted[0].ToolCall.Name)
	}
}

func TestToolArgSanitizer_SanitizesReadLimit(t *testing.T) {
	sanitized := sanitizeToolArgs("Read", `{"file_path":"/tmp/a.txt","limit":"100"}`)
	var args map[string]any
	json.Unmarshal([]byte(sanitized), &args)

	if args["limit"] != float64(100) {
		t.Errorf("expected limit 100 (float64), got %v (%T)", args["limit"], args["limit"])
	}
}

func TestToolArgSanitizer_ClampsReadLimit(t *testing.T) {
	sanitized := sanitizeToolArgs("Read", `{"file_path":"/tmp/a.txt","limit":5000}`)
	var args map[string]any
	json.Unmarshal([]byte(sanitized), &args)

	if args["limit"] != float64(2000) {
		t.Errorf("expected limit clamped to 2000, got %v", args["limit"])
	}
}

func TestToolArgSanitizer_ClampsReadLimitMin(t *testing.T) {
	sanitized := sanitizeToolArgs("Read", `{"file_path":"/tmp/a.txt","limit":0}`)
	var args map[string]any
	json.Unmarshal([]byte(sanitized), &args)

	if args["limit"] != float64(1) {
		t.Errorf("expected limit clamped to 1, got %v", args["limit"])
	}
}

func TestToolArgSanitizer_SanitizesReadOffset(t *testing.T) {
	sanitized := sanitizeToolArgs("Read", `{"file_path":"/tmp/a.txt","offset":"-5"}`)
	var args map[string]any
	json.Unmarshal([]byte(sanitized), &args)

	if args["offset"] != float64(0) {
		t.Errorf("expected offset clamped to 0, got %v", args["offset"])
	}
}

func TestToolArgSanitizer_RemovesPagesForNonPdf(t *testing.T) {
	sanitized := sanitizeToolArgs("Read", `{"file_path":"/tmp/a.txt","pages":"1-5"}`)
	var args map[string]any
	json.Unmarshal([]byte(sanitized), &args)

	if _, ok := args["pages"]; ok {
		t.Error("expected 'pages' to be removed for non-PDF file")
	}
}

func TestToolArgSanitizer_KeepsPagesForPdf(t *testing.T) {
	sanitized := sanitizeToolArgs("Read", `{"file_path":"/tmp/doc.pdf","pages":"1-5"}`)
	var args map[string]any
	json.Unmarshal([]byte(sanitized), &args)

	if args["pages"] != "1-5" {
		t.Errorf("expected pages '1-5' for PDF, got %v", args["pages"])
	}
}

func TestToolArgSanitizer_FlushOnNewToolAtSameIndex(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk
	emit := func(c core.StreamChunk) { emitted = append(emitted, c) }

	// First tool call at index 0.
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			ID: "tc1", Name: "Read",
			Arguments: json.RawMessage(`{"file_path":"/a"}`),
		},
	}, emit)

	// Second tool call at same index 0 — should flush first.
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			ID: "tc2", Name: "Write",
			Arguments: json.RawMessage(`{"file_path":"/b","content":"hi"}`),
		},
	}, emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 flushed tool call, got %d", len(emitted))
	}
	if emitted[0].ToolCall.ID != "tc1" {
		t.Errorf("expected first tool 'tc1', got %q", emitted[0].ToolCall.ID)
	}

	// Flush remaining.
	s.Flush(emit)
	if len(emitted) != 2 {
		t.Fatalf("expected 2 total, got %d", len(emitted))
	}
	if emitted[1].ToolCall.ID != "tc2" {
		t.Errorf("expected second tool 'tc2', got %q", emitted[1].ToolCall.ID)
	}
}

// TestToolArgSanitizer_KiroRepeatedIDDelta reproduces the Kiro streaming shape:
// the connector emits an opening chunk (ID + Name + empty args) followed by an
// argument chunk that REPEATS the same tool-use ID. The sanitizer must treat
// the second chunk as a continuation of the same call, not a brand-new one.
// Before the fix, the repeated ID prematurely flushed the empty buffer (a tool
// call with "{}" args) and accumulated the real arguments into a second,
// name-less buffer — which made Cline see "read_file" with no parameters.
func TestToolArgSanitizer_KiroRepeatedIDDelta(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk
	emit := func(c core.StreamChunk) { emitted = append(emitted, c) }

	// Opening chunk: ID + Name + empty args (Kiro toolUseEvent announce).
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			ID:        "tooluse_1",
			Name:      "read_file",
			Arguments: json.RawMessage(""),
		},
	}, emit)

	// Argument chunk: SAME ID repeated, carries the real input.
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			ID:        "tooluse_1",
			Arguments: json.RawMessage(`{"path":"src/main.go"}`),
		},
	}, emit)

	// Nothing should be emitted while buffering — no premature flush.
	if len(emitted) != 0 {
		t.Fatalf("expected 0 emitted during buffering, got %d: %+v", len(emitted), emitted)
	}

	// Finish flushes exactly one consolidated tool call.
	s.Process(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishToolCalls}, emit)

	var tools []core.StreamChunk
	for _, c := range emitted {
		if c.Type == core.ChunkToolCall {
			tools = append(tools, c)
		}
	}
	if len(tools) != 1 {
		t.Fatalf("expected exactly 1 tool call, got %d: %+v", len(tools), tools)
	}
	tc := tools[0].ToolCall
	if tc.Name != "read_file" {
		t.Errorf("expected Name 'read_file', got %q", tc.Name)
	}
	if tc.ID != "tooluse_1" {
		t.Errorf("expected ID 'tooluse_1', got %q", tc.ID)
	}
	var args map[string]any
	if err := json.Unmarshal(tc.Arguments, &args); err != nil {
		t.Fatalf("arguments not valid JSON: %v (%s)", err, tc.Arguments)
	}
	if args["path"] != "src/main.go" {
		t.Errorf("expected path 'src/main.go', got %v (full args: %s)", args["path"], tc.Arguments)
	}
}

// TestToolArgSanitizer_KiroFragmentedArgs covers Kiro emitting the arguments as
// multiple fragments after the announce chunk, all repeating the same ID. They
// must accumulate into one complete JSON object.
func TestToolArgSanitizer_KiroFragmentedArgs(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk
	emit := func(c core.StreamChunk) { emitted = append(emitted, c) }

	s.Process(core.StreamChunk{
		Type: core.ChunkToolCall, Index: 0,
		ToolCall: &core.ToolCall{ID: "t1", Name: "read_file", Arguments: json.RawMessage("")},
	}, emit)
	for _, frag := range []string{`{"path":`, `"a/b/`, `c.go"}`} {
		s.Process(core.StreamChunk{
			Type: core.ChunkToolCall, Index: 0,
			ToolCall: &core.ToolCall{ID: "t1", Arguments: json.RawMessage(frag)},
		}, emit)
	}
	s.Flush(emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 tool call, got %d: %+v", len(emitted), emitted)
	}
	var args map[string]any
	if err := json.Unmarshal(emitted[0].ToolCall.Arguments, &args); err != nil {
		t.Fatalf("reassembled args invalid: %v (%s)", err, emitted[0].ToolCall.Arguments)
	}
	if args["path"] != "a/b/c.go" {
		t.Errorf("expected path 'a/b/c.go', got %v", args["path"])
	}
}

func TestSanitizeToolArgs_InvalidJSON(t *testing.T) {

	// Should return original if unparseable.
	got := sanitizeToolArgs("Read", "not json")
	if got != "not json" {
		t.Errorf("expected passthrough for invalid JSON, got %q", got)
	}
}

func TestSanitizeToolArgs_PassthroughValidArgs(t *testing.T) {
	input := `{"file_path":"/tmp/a.txt","limit":50}`
	got := sanitizeToolArgs("Read", input)
	var args map[string]any
	json.Unmarshal([]byte(got), &args)

	if args["limit"] != float64(50) {
		t.Errorf("valid limit should pass through, got %v", args["limit"])
	}
}

// TestToolArgSanitizer_SnapshotVsDelta verifies that full-snapshot argument
// retransmissions (where each chunk re-sends the ENTIRE accumulated arguments)
// are replaced rather than concatenated, while true incremental deltas are
// appended verbatim. Some providers (GLM, certain OpenAI-compatible gateways)
// send snapshots; others (Kiro, Cursor) send true deltas.
func TestToolArgSanitizer_SnapshotVsDelta(t *testing.T) {
	tests := []struct {
		name     string
		mode     core.ToolArgumentMode
		existing string
		fragment string
		want     string
	}{
		{"empty buffer, first fragment", core.ToolArgumentDelta, "", `{"file_path":"/a"}`, `{"file_path":"/a"}`},
		{"delta append", core.ToolArgumentDelta, `{"file_path":`, `"/a"}`, `{"file_path":"/a"}`},
		{"snapshot repeat", core.ToolArgumentSnapshot, `{"file_path":"/a"}`, `{"file_path":"/a"}`, `{"file_path":"/a"}`},
		{"snapshot growth", core.ToolArgumentSnapshot, `{"file_path":"/a"`, `{"file_path":"/a"}`, `{"file_path":"/a"}`},
		{"complete replaces", core.ToolArgumentComplete, `{"file_path":"/a","limit":1`, `{"file_path":"/a","limit":100}`, `{"file_path":"/a","limit":100}`},
		{"nested object delta", core.ToolArgumentDelta, `{"config":`, `{"enabled":true}`, `{"config":{"enabled":true}`},
		{"delta with repeated chars", core.ToolArgumentDelta, `{"command":"ls -l`, `l"}`, `{"command":"ls -ll"}`},
		{"delta with repeated prefix", core.ToolArgumentDelta, `{"path":"/home/user`, `/home/user2"}`, `{"path":"/home/user/home/user2"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &toolBuffer{}
			buf.args.WriteString(tt.existing)
			applyToolArgs(buf, tt.mode, tt.fragment)
			if got := buf.args.String(); got != tt.want {
				t.Errorf("applyToolArgs(%q, %q) = %q, want %q", tt.existing, tt.fragment, got, tt.want)
			}
		})
	}
}

// TestToolArgSanitizer_SnapshotStream simulates a provider that sends full
// argument snapshots on every chunk (GLM-style). The sanitizer must replace
// the buffer each time, not concatenate.
func TestToolArgSanitizer_SnapshotStream(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk
	emit := func(c core.StreamChunk) { emitted = append(emitted, c) }

	// Provider sends snapshot growth: each chunk re-sends the full args.
	s.Process(core.StreamChunk{
		Type: core.ChunkToolCall, Index: 0, ToolArgumentMode: core.ToolArgumentSnapshot,
		ToolCall: &core.ToolCall{ID: "tc1", Name: "Read", Arguments: json.RawMessage(`{"file_path":"/a"}`)},
	}, emit)
	s.Process(core.StreamChunk{
		Type: core.ChunkToolCall, Index: 0, ToolArgumentMode: core.ToolArgumentSnapshot,
		ToolCall: &core.ToolCall{ID: "tc1", Arguments: json.RawMessage(`{"file_path":"/a","limit":100}`)},
	}, emit)
	s.Process(core.StreamChunk{
		Type: core.ChunkToolCall, Index: 0, ToolArgumentMode: core.ToolArgumentSnapshot,
		ToolCall: &core.ToolCall{ID: "tc1", Arguments: json.RawMessage(`{"file_path":"/a","limit":100,"offset":0}`)},
	}, emit)

	s.Flush(emit)

	if len(emitted) != 1 {
		t.Fatalf("expected 1 emitted tool call, got %d", len(emitted))
	}
	args := string(emitted[0].ToolCall.Arguments)
	// Must NOT contain concatenated snapshots.
	if strings.Count(args, "file_path") != 1 {
		t.Errorf("snapshot duplication detected: %s", args)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(args), &parsed); err != nil {
		t.Fatalf("args not valid JSON: %v (%s)", err, args)
	}
	if parsed["limit"] != float64(100) {
		t.Errorf("expected limit 100, got %v", parsed["limit"])
	}
}

// multiple times does not re-emit already-flushed tool calls. This prevents
// duplicate tool calls when upstream providers send duplicate ChunkFinish
// events (e.g. some OpenAI-compatible gateways).
func TestToolArgSanitizer_DoubleFlushIsIdempotent(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk
	emit := func(c core.StreamChunk) { emitted = append(emitted, c) }

	// Buffer a tool call.
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			ID: "tc1", Name: "Read",
			Arguments: json.RawMessage(`{"file_path":"/a"}`),
		},
	}, emit)

	// First flush — emits the tool call.
	s.Flush(emit)
	if len(emitted) != 1 {
		t.Fatalf("first flush: expected 1 emitted, got %d", len(emitted))
	}

	// Second flush — must be a no-op.
	s.Flush(emit)
	if len(emitted) != 1 {
		t.Fatalf("second flush: expected still 1 emitted (idempotent), got %d", len(emitted))
	}

	// Third flush — still a no-op.
	s.Flush(emit)
	if len(emitted) != 1 {
		t.Fatalf("third flush: expected still 1 emitted (idempotent), got %d", len(emitted))
	}
}

// TestToolArgSanitizer_DuplicateChunkFinishDoesNotDoubleEmit verifies that
// a duplicate ChunkFinish from upstream does not cause double-emission of
// buffered tool calls.
func TestToolArgSanitizer_RejectsEveryPostTerminalNonUsageChunk(t *testing.T) {
	for _, chunk := range []core.StreamChunk{
		{Type: core.ChunkText, Delta: "late"},
		{Type: core.ChunkThinking, Delta: "late"},
		{Type: core.ChunkToolCall, ToolCall: &core.ToolCall{ID: "late", Name: "Read", Arguments: json.RawMessage(`{"file_path":"/late"}`)}},
		{Type: core.ChunkPing},
		{Type: core.ChunkError, Err: errors.New("late")},
	} {
		t.Run(string(chunk.Type), func(t *testing.T) {
			s := NewToolArgSanitizer()
			var emitted []core.StreamChunk
			emit := func(c core.StreamChunk) { emitted = append(emitted, c) }
			s.Process(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}, emit)
			s.Process(chunk, emit)

			if len(emitted) != 2 || emitted[0].Type != core.ChunkFinish || emitted[1].Type != core.ChunkError {
				t.Fatalf("emitted = %+v, want finish then integrity error", emitted)
			}
			if pe := core.AsProviderError(emitted[1].Err); pe.Kind != core.ErrResponseIntegrity {
				t.Fatalf("error kind = %q, want response_integrity", pe.Kind)
			}
		})
	}
}

func TestToolArgSanitizer_AllowsTrailingUsageOnly(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk
	emit := func(c core.StreamChunk) { emitted = append(emitted, c) }
	s.Process(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}, emit)
	s.Process(core.StreamChunk{Type: core.ChunkUsage, Usage: &core.Usage{PromptTokens: 1}}, emit)

	if len(emitted) != 2 || emitted[1].Type != core.ChunkUsage {
		t.Fatalf("emitted = %+v, want finish then usage", emitted)
	}
}

func TestToolArgSanitizer_FailsClosedOnAmbiguousIdentityTransition(t *testing.T) {
	for _, tt := range []struct {
		name   string
		first  *core.ToolCall
		second *core.ToolCall
	}{
		{"new name lacks ID", &core.ToolCall{ID: "old", Name: "Read", Arguments: json.RawMessage(`{"file_path":"/old"}`)}, &core.ToolCall{Name: "Write", Arguments: json.RawMessage(`{"file_path":"/new","content":"x"}`)}},
		{"old identity lacks ID", &core.ToolCall{Name: "Read", Arguments: json.RawMessage(`{"file_path":"/old"}`)}, &core.ToolCall{ID: "new", Name: "Write", Arguments: json.RawMessage(`{"file_path":"/new","content":"x"}`)}},
		{"same ID conflicts by name", &core.ToolCall{ID: "same", Name: "Read", Arguments: json.RawMessage(`{"file_path":"/old"}`)}, &core.ToolCall{ID: "same", Name: "Write", Arguments: json.RawMessage(`{"file_path":"/new","content":"x"}`)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewToolArgSanitizer()
			var emitted []core.StreamChunk
			emit := func(c core.StreamChunk) { emitted = append(emitted, c) }
			s.Process(core.StreamChunk{Type: core.ChunkToolCall, Index: 0, ToolCall: tt.first}, emit)
			s.Process(core.StreamChunk{Type: core.ChunkToolCall, Index: 0, ToolCall: tt.second}, emit)
			s.Flush(emit)

			if len(emitted) != 1 || emitted[0].Type != core.ChunkError {
				t.Fatalf("emitted = %+v, want one integrity error and no combined tool call", emitted)
			}
			if pe := core.AsProviderError(emitted[0].Err); pe.Kind != core.ErrResponseIntegrity {
				t.Fatalf("error kind = %q, want response_integrity", pe.Kind)
			}
		})
	}
}

func TestToolArgSanitizer_DuplicateChunkFinishDoesNotDoubleEmit(t *testing.T) {
	s := NewToolArgSanitizer()
	var emitted []core.StreamChunk
	emit := func(c core.StreamChunk) { emitted = append(emitted, c) }

	// Buffer a tool call.
	s.Process(core.StreamChunk{
		Type:  core.ChunkToolCall,
		Index: 0,
		ToolCall: &core.ToolCall{
			ID: "tc1", Name: "Read",
			Arguments: json.RawMessage(`{"file_path":"/a"}`),
		},
	}, emit)

	// First ChunkFinish — flushes and emits.
	s.Process(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishToolCalls}, emit)
	if len(emitted) != 2 { // tool call + finish
		t.Fatalf("after first finish: expected 2 emitted (tool+finish), got %d", len(emitted))
	}

	// Duplicate ChunkFinish is suppressed, but later usage is preserved.
	s.Process(core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishToolCalls}, emit)
	s.Process(core.StreamChunk{Type: core.ChunkUsage, Usage: &core.Usage{TotalTokens: 3}}, emit)

	var toolCalls, finishes, usages int
	for _, c := range emitted {
		switch c.Type {
		case core.ChunkToolCall:
			toolCalls++
		case core.ChunkFinish:
			finishes++
		case core.ChunkUsage:
			usages++
		}
	}
	if toolCalls != 1 || finishes != 1 || usages != 1 {
		t.Fatalf("emitted tool/finish/usage = %d/%d/%d, want 1/1/1", toolCalls, finishes, usages)
	}
}
