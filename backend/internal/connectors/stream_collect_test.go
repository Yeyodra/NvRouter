package connectors

import (
	"encoding/json"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestDrainStreamToResponseAssemblesByIndexAndMode(t *testing.T) {
	stream := make(chan core.StreamChunk, 5)
	stream <- connectorToolChunk(1, "call_1", "second", core.ToolArgumentSnapshot, `{"n":1}`)
	stream <- connectorToolChunk(0, "", "", core.ToolArgumentDelta, `{"n":`)
	stream <- connectorToolChunk(0, "call_0", "first", core.ToolArgumentDelta, `0}`)
	stream <- connectorToolChunk(1, "", "", core.ToolArgumentComplete, `{"n":2}`)
	stream <- core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}
	close(stream)

	resp, err := drainStreamToResponse(stream, "model")
	if err != nil {
		t.Fatal(err)
	}
	if resp.FinishReason != core.FinishToolCalls {
		t.Fatalf("finish reason = %q, want tool_calls", resp.FinishReason)
	}
	calls := connectorToolCalls(resp.Message)
	if len(calls) != 2 {
		t.Fatalf("tool calls = %d, want 2", len(calls))
	}
	if calls[0].ID != "call_0" || calls[0].Name != "first" || string(calls[0].Arguments) != `{"n":0}` {
		t.Fatalf("first tool call = %+v, want index 0 assembled call", calls[0])
	}
	if calls[1].ID != "call_1" || calls[1].Name != "second" || string(calls[1].Arguments) != `{"n":2}` {
		t.Fatalf("second tool call = %+v, want completed index 1 call", calls[1])
	}
}

func TestDrainStreamToResponseRejectsMalformedFinalToolArguments(t *testing.T) {
	stream := make(chan core.StreamChunk, 1)
	stream <- connectorToolChunk(0, "call_0", "broken", core.ToolArgumentDelta, `{"x":`)
	close(stream)

	if _, err := drainStreamToResponse(stream, "model"); err == nil {
		t.Fatal("drainStreamToResponse() error = nil, want malformed tool arguments error")
	}
}

func TestDrainStreamToResponsePreservesContentOrder(t *testing.T) {
	stream := make(chan core.StreamChunk, 4)
	stream <- core.StreamChunk{Type: core.ChunkText, Delta: "before"}
	stream <- connectorToolChunk(0, "call_0", "middle", core.ToolArgumentComplete, `{}`)
	stream <- core.StreamChunk{Type: core.ChunkText, Delta: "after"}
	stream <- core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}
	close(stream)

	resp, err := drainStreamToResponse(stream, "model")
	require.NoError(t, err)
	require.Len(t, resp.Message.Content, 3)
	require.Equal(t, core.PartText, resp.Message.Content[0].Type)
	require.Equal(t, "before", resp.Message.Content[0].Text)
	require.Equal(t, core.PartToolCall, resp.Message.Content[1].Type)
	require.Equal(t, core.PartText, resp.Message.Content[2].Type)
	require.Equal(t, "after", resp.Message.Content[2].Text)
}

func TestDrainStreamToResponseMergesSplitUsageFields(t *testing.T) {
	stream := make(chan core.StreamChunk, 3)
	stream <- core.StreamChunk{Type: core.ChunkUsage, Usage: &core.Usage{PromptTokens: 7, CachedTokens: 2, Source: core.UsageSourceProvider}}
	stream <- core.StreamChunk{Type: core.ChunkUsage, Usage: &core.Usage{CompletionTokens: 3, ReasoningTokens: 1, TotalTokens: 10}}
	stream <- core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}
	close(stream)

	resp, err := drainStreamToResponse(stream, "model")
	require.NoError(t, err)
	require.Equal(t, core.Usage{PromptTokens: 7, CompletionTokens: 3, TotalTokens: 10, CachedTokens: 2, ReasoningTokens: 1, Source: core.UsageSourceProvider}, resp.Usage)
}

func TestDrainStreamToResponseErrorPreservesSeenUsage(t *testing.T) {
	stream := make(chan core.StreamChunk, 2)
	stream <- core.StreamChunk{Type: core.ChunkUsage, Usage: &core.Usage{PromptTokens: 3, CompletionTokens: 1, TotalTokens: 4}}
	stream <- core.StreamChunk{Type: core.ChunkError, Err: &core.ProviderError{Kind: core.ErrResponseIntegrity, Message: "bad frame"}}
	close(stream)

	_, err := drainStreamToResponse(stream, "model")
	pe := core.AsProviderError(err)
	require.NotNil(t, pe.AttemptUsage)
	require.Equal(t, 4, pe.AttemptUsage.TotalTokens)
}

func TestDrainStreamToResponseRejectsAmbiguousEmptyIdentityReuse(t *testing.T) {
	stream := make(chan core.StreamChunk, 2)
	stream <- connectorToolChunk(0, "call_0", "first", core.ToolArgumentComplete, `{"a":1}`)
	stream <- connectorToolChunk(0, "", "", core.ToolArgumentComplete, `{"b":2}`)
	close(stream)

	_, err := drainStreamToResponse(stream, "model")
	require.Error(t, err)
	require.Equal(t, core.ErrResponseIntegrity, core.AsProviderError(err).Kind)
}

func TestDrainStreamToResponseRejectsUnknownToolArgumentMode(t *testing.T) {
	stream := make(chan core.StreamChunk, 1)
	stream <- connectorToolChunk(0, "call_0", "bad", core.ToolArgumentMode("replace-ish"), `{}`)
	close(stream)

	_, err := drainStreamToResponse(stream, "model")
	require.Error(t, err)
	require.Equal(t, core.ErrResponseIntegrity, core.AsProviderError(err).Kind)
}

func TestDrainStreamToResponseNormalizesNilChunkError(t *testing.T) {
	stream := make(chan core.StreamChunk, 1)
	stream <- core.StreamChunk{Type: core.ChunkError}
	close(stream)

	_, err := drainStreamToResponse(stream, "model")
	require.Error(t, err)
	require.Equal(t, core.ErrResponseIntegrity, core.AsProviderError(err).Kind)
}

func TestDrainStreamToResponseRejectsContentAfterTerminalFinish(t *testing.T) {
	stream := make(chan core.StreamChunk, 2)
	stream <- core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}
	stream <- core.StreamChunk{Type: core.ChunkText, Delta: "lost"}
	close(stream)

	_, err := drainStreamToResponse(stream, "model")
	require.Error(t, err)
	require.Equal(t, core.ErrResponseIntegrity, core.AsProviderError(err).Kind)
}

func connectorToolChunk(index int, id, name string, mode core.ToolArgumentMode, args string) core.StreamChunk {
	return core.StreamChunk{
		Type:             core.ChunkToolCall,
		Index:            index,
		ToolArgumentMode: mode,
		ToolCall:         &core.ToolCall{ID: id, Name: name, Arguments: json.RawMessage(args)},
	}
}

func connectorToolCalls(message core.Message) []*core.ToolCall {
	var calls []*core.ToolCall
	for _, part := range message.Content {
		if part.Type == core.PartToolCall {
			calls = append(calls, part.ToolCall)
		}
	}
	return calls
}
