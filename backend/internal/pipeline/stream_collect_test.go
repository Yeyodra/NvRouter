package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestDrainStreamAssemblesToolLifecycle(t *testing.T) {
	tests := []struct {
		name   string
		chunks []core.StreamChunk
		want   string
	}{
		{
			name: "delta with empty ID continuation and late identity",
			chunks: []core.StreamChunk{
				toolChunk(0, "", "", core.ToolArgumentDelta, `{"city":`),
				toolChunk(0, "call_0", "weather", core.ToolArgumentDelta, `"Paris"}`),
			},
			want: `[{"id":"call_0","name":"weather","arguments":{"city":"Paris"}}]`,
		},
		{
			name: "snapshot replaces prior snapshot",
			chunks: []core.StreamChunk{
				toolChunk(0, "call_0", "weather", core.ToolArgumentSnapshot, `{"city":"Pa"}`),
				toolChunk(0, "", "", core.ToolArgumentSnapshot, `{"city":"Paris"}`),
			},
			want: `[{"id":"call_0","name":"weather","arguments":{"city":"Paris"}}]`,
		},
		{
			name: "complete replaces deltas",
			chunks: []core.StreamChunk{
				toolChunk(0, "call_0", "weather", core.ToolArgumentDelta, `{"city":"wrong"}`),
				toolChunk(0, "", "", core.ToolArgumentComplete, `{"city":"Paris"}`),
			},
			want: `[{"id":"call_0","name":"weather","arguments":{"city":"Paris"}}]`,
		},
		{
			name: "parallel indices use deterministic index order",
			chunks: []core.StreamChunk{
				toolChunk(2, "call_2", "second", core.ToolArgumentDelta, `{"n":2}`),
				toolChunk(0, "", "", core.ToolArgumentDelta, `{"n":`),
				toolChunk(0, "call_0", "first", core.ToolArgumentDelta, `0}`),
			},
			want: `[{"id":"call_0","name":"first","arguments":{"n":0}},{"id":"call_2","name":"second","arguments":{"n":2}}]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := append(append([]core.StreamChunk(nil), tt.chunks...), core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop})
			resp, err := drainStream(chunkStream(chunks...), "model")
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(toolCalls(resp.Message))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Fatalf("tool calls = %s, want %s", got, tt.want)
			}
			if resp.FinishReason != core.FinishToolCalls {
				t.Fatalf("finish reason = %q, want %q", resp.FinishReason, core.FinishToolCalls)
			}
		})
	}
}

func TestDrainStreamRejectsMalformedFinalToolArguments(t *testing.T) {
	_, err := drainStream(chunkStream(
		toolChunk(0, "call_0", "weather", core.ToolArgumentDelta, `{"city":`),
	), "model")
	if err == nil {
		t.Fatal("drainStream() error = nil, want malformed tool arguments error")
	}
}

func toolChunk(index int, id, name string, mode core.ToolArgumentMode, args string) core.StreamChunk {
	return core.StreamChunk{
		Type:             core.ChunkToolCall,
		Index:            index,
		ToolArgumentMode: mode,
		ToolCall:         &core.ToolCall{ID: id, Name: name, Arguments: json.RawMessage(args)},
	}
}

func chunkStream(chunks ...core.StreamChunk) <-chan core.StreamChunk {
	stream := make(chan core.StreamChunk, len(chunks))
	for _, chunk := range chunks {
		stream <- chunk
	}
	close(stream)
	return stream
}

func toolCalls(message core.Message) []*core.ToolCall {
	var calls []*core.ToolCall
	for _, part := range message.Content {
		if part.Type == core.PartToolCall {
			calls = append(calls, part.ToolCall)
		}
	}
	return calls
}
