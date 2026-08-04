package transform

import (
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestStripThinkTags(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantThink []string
		wantClean string
	}{
		{
			name:      "no think tags",
			input:     "Hello world",
			wantClean: "Hello world",
		},
		{
			name:      "simple think tag",
			input:     "<think>\nreasoning\n</think>\nanswer",
			wantThink: []string{"\nreasoning\n"},
			wantClean: "\nanswer",
		},
		{
			name:      "multiple think tags",
			input:     "<think>\nfirst\n</think>\nmid\n<think>\nsecond\n</think>\nend",
			wantThink: []string{"\nfirst\n", "\nsecond\n"},
			wantClean: "\nmid\n\nend",
		},
		{
			name:      "unclosed think tag",
			input:     "<think>\nnever closed",
			wantThink: []string{"\nnever closed"},
			wantClean: "",
		},
		{
			name:      "empty think tags",
			input:     "<think></think>answer",
			wantThink: nil,
			wantClean: "answer",
		},
		{
			name:      "think tag with surrounding text",
			input:     "before<think>\ninner\n</think>after",
			wantThink: []string{"\ninner\n"},
			wantClean: "beforeafter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks, clean := StripThinkTags(tt.input)
			if clean != tt.wantClean {
				t.Errorf("StripThinkTags clean = %q, want %q", clean, tt.wantClean)
			}
			if len(chunks) != len(tt.wantThink) {
				t.Fatalf("StripThinkTags got %d thinking chunks, want %d", len(chunks), len(tt.wantThink))
			}
			for i, ch := range chunks {
				if ch.Type != core.ChunkThinking {
					t.Errorf("chunk[%d].Type = %q, want %q", i, ch.Type, core.ChunkThinking)
				}
				if ch.Delta != tt.wantThink[i] {
					t.Errorf("chunk[%d].Delta = %q, want %q", i, ch.Delta, tt.wantThink[i])
				}
			}
		})
	}
}

func TestStripThinkTagsNoTags(t *testing.T) {
	input := "Just a normal response with no thinking tags."
	chunks, clean := StripThinkTags(input)
	if len(chunks) != 0 {
		t.Fatalf("expected 0 thinking chunks, got %d", len(chunks))
	}
	if clean != input {
		t.Errorf("clean = %q, want %q", clean, input)
	}
}

func TestThinkTagStateStreaming(t *testing.T) {
	// Simulate a stream where <think> arrives in parts.
	ts := &ThinkTagState{}

	// Feed: "<think>" split across chunks
	chunks := ts.ProcessFeed("<thi")
	assertNoChunks(t, chunks, "partial think open tag")

	chunks = ts.ProcessFeed("nk>")
	assertNoChunks(t, chunks, "rest of think open tag")

	// Feed thinking content
	chunks = ts.ProcessFeed("reasoning")
	assertThinkingOnly(t, chunks, "reasoning")

	// Feed: "</think>" split across chunks.
	// "</" is a prefix of "</think>", so it's held as potential partial tag.
	chunks = ts.ProcessFeed("</")
	assertNoChunks(t, chunks, "</ partial close tag (held)")

	chunks = ts.ProcessFeed("think>\nanswer")
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk after close tag + text, got %d", len(chunks))
	}
	if chunks[0].Type != core.ChunkText {
		t.Errorf("expected ChunkText, got %q", chunks[0].Type)
	}
	if chunks[0].Delta != "\nanswer" {
		t.Errorf("Delta = %q, want %q", chunks[0].Delta, "\nanswer")
	}

	// Flush should be empty.
	flush := ts.Flush()
	if len(flush) != 0 {
		t.Errorf("Flush returned %d chunks, want 0", len(flush))
	}
}

func TestThinkTagStateFlushesBufferedTextBeforeTerminal(t *testing.T) {
	ts := &ThinkTagState{}
	if chunks := ts.ProcessFeed("answer<thi"); len(chunks) != 1 || chunks[0].Delta != "answer" {
		t.Fatalf("ProcessFeed() = %+v, want safe text only", chunks)
	}
	chunks := ts.Flush()
	if len(chunks) != 1 || chunks[0].Type != core.ChunkText || chunks[0].Delta != "<thi" {
		t.Fatalf("Flush() = %+v, want buffered legitimate text", chunks)
	}
}

func TestThinkTagStateNoTags(t *testing.T) {
	ts := &ThinkTagState{}

	chunks := ts.ProcessFeed("Hello ")
	chunks = append(chunks, ts.ProcessFeed("world")...)

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}
	for _, ch := range chunks {
		if ch.Type != core.ChunkText {
			t.Errorf("expected ChunkText, got %q", ch.Type)
		}
	}

	flush := ts.Flush()
	if len(flush) != 0 {
		t.Errorf("Flush returned %d chunks, want 0", len(flush))
	}
}

func TestThinkTagStateOneChunk(t *testing.T) {
	ts := &ThinkTagState{}
	whole := "<think>\nreason\n</think>\nanswer"
	chunks := ts.ProcessFeed(whole)
	flush := ts.Flush()
	chunks = append(chunks, flush...)

	var thinkText, contentText strings.Builder
	for _, ch := range chunks {
		switch ch.Type {
		case core.ChunkThinking:
			thinkText.WriteString(ch.Delta)
		case core.ChunkText:
			contentText.WriteString(ch.Delta)
		}
	}

	if thinkText.String() != "\nreason\n" {
		t.Errorf("thinking = %q, want %q", thinkText.String(), "\nreason\n")
	}
	if contentText.String() != "\nanswer" {
		t.Errorf("content = %q, want %q", contentText.String(), "\nanswer")
	}
}

// TestThinkTagStateFastPath verifies that plain text deltas (no '<') are
// forwarded verbatim, one chunk per feed, with nothing held back waiting for a
// subsequent frame. This is the hot path for models that never emit inline
// think tags and must not buffer or stall.
func TestThinkTagStateFastPath(t *testing.T) {
	ts := &ThinkTagState{}

	deltas := []string{"Hello", " there", ", how", " are you?"}
	for _, d := range deltas {
		chunks := ts.ProcessFeed(d)
		if len(chunks) != 1 {
			t.Fatalf("ProcessFeed(%q) = %d chunks, want 1 (no holdback)", d, len(chunks))
		}
		if chunks[0].Type != core.ChunkText {
			t.Errorf("ProcessFeed(%q) type = %q, want ChunkText", d, chunks[0].Type)
		}
		if chunks[0].Delta != d {
			t.Errorf("ProcessFeed(%q) delta = %q, want verbatim", d, chunks[0].Delta)
		}
	}

	// Nothing buffered — Flush must be empty.
	if flush := ts.Flush(); len(flush) != 0 {
		t.Errorf("Flush returned %d chunks, want 0", len(flush))
	}
}

// TestThinkTagStateAngleBracketNotThink verifies that a '<' that does not begin
// a <think> tag still streams correctly without being permanently held back.
func TestThinkTagStateAngleBracketNotThink(t *testing.T) {
	ts := &ThinkTagState{}

	var got strings.Builder
	for _, d := range []string{"a < b", " and c > d"} {
		for _, ch := range ts.ProcessFeed(d) {
			if ch.Type == core.ChunkText {
				got.WriteString(ch.Delta)
			}
		}
	}
	for _, ch := range ts.Flush() {
		if ch.Type == core.ChunkText {
			got.WriteString(ch.Delta)
		}
	}
	if got.String() != "a < b and c > d" {
		t.Errorf("reassembled text = %q, want %q", got.String(), "a < b and c > d")
	}
}

func assertThinkingOnly(t *testing.T, chunks []core.StreamChunk, expected string) {

	t.Helper()
	for _, ch := range chunks {
		if ch.Type != core.ChunkThinking {
			t.Errorf("expected ChunkThinking, got %q: %q", ch.Type, ch.Delta)
		}
	}
}

func assertNoChunks(t *testing.T, chunks []core.StreamChunk, context string) {
	t.Helper()
	for _, ch := range chunks {
		t.Errorf("%s: unexpected chunk type=%q delta=%q", context, ch.Type, ch.Delta)
	}
}

// TestThinkTagState_DoubleFlushIsIdempotent verifies that calling Flush
// multiple times does not re-emit already-flushed buffered content. This
// prevents duplicate text when stream cleanup logic calls Flush twice.
func TestThinkTagState_DoubleFlushIsIdempotent(t *testing.T) {
	ts := &ThinkTagState{}

	// Feed content that gets buffered (a '<' that might start a tag).
	// The '<' is held as potential partial tag, so "hello " is emitted
	// immediately and "<" stays in the buffer.
	chunks := ts.ProcessFeed("hello <")
	if len(chunks) != 1 || chunks[0].Delta != "hello " {
		t.Fatalf("expected 'hello ' emitted immediately, got %+v", chunks)
	}

	// First flush — emits the buffered "<".
	flush1 := ts.Flush()
	if len(flush1) != 1 {
		t.Fatalf("first flush: expected 1 chunk, got %d", len(flush1))
	}
	if flush1[0].Delta != "<" {
		t.Errorf("first flush delta = %q, want %q", flush1[0].Delta, "<")
	}

	// Second flush — must return nil.
	flush2 := ts.Flush()
	if len(flush2) != 0 {
		t.Fatalf("second flush: expected 0 chunks (idempotent), got %d", len(flush2))
	}

	// Third flush — still nil.
	flush3 := ts.Flush()
	if len(flush3) != 0 {
		t.Fatalf("third flush: expected 0 chunks (idempotent), got %d", len(flush3))
	}
}

// TestThinkTagState_DoubleFlushEmptyBuffer verifies that double Flush on an
// empty buffer is also safe (returns nil both times).
func TestThinkTagState_DoubleFlushEmptyBuffer(t *testing.T) {
	ts := &ThinkTagState{}

	flush1 := ts.Flush()
	if len(flush1) != 0 {
		t.Fatalf("first flush on empty: expected 0 chunks, got %d", len(flush1))
	}

	flush2 := ts.Flush()
	if len(flush2) != 0 {
		t.Fatalf("second flush on empty: expected 0 chunks, got %d", len(flush2))
	}
}
