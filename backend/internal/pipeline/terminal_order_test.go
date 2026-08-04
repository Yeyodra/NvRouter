package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/mydisha/keirouter/backend/internal/budget"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/dispatch"
	"github.com/mydisha/keirouter/backend/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPumpStreamDefersFinishAndSuccessUntilValidatedEOF(t *testing.T) {
	p := &Pipeline{dispatcher: newPlannerDispatcherWithConnector(t, plannerConnector{}, "account")}
	in := make(chan core.StreamChunk)
	out := make(chan core.StreamChunk, 8)
	req := &core.ChatRequest{Messages: []core.Message{{Role: core.RoleUser, Content: []core.ContentPart{{Type: core.PartText, Text: "hi"}}}}}
	attempt := dispatch.Attempt{Target: dispatch.Target{Provider: "openai", Model: "model"}, Account: store.Account{ID: "account"}}
	started := time.Now()
	var ttft time.Duration
	go p.pumpStream(context.Background(), req, in, out, core.RequestMetadata{}, attempt, started, started,
		&ttft, nil, budget.Scope{}, func(int64) {}, false, func() {})

	in <- core.StreamChunk{Type: core.ChunkText, Delta: "early"}
	require.Equal(t, core.ChunkText, (<-out).Type)
	in <- core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop}
	select {
	case chunk := <-out:
		t.Fatalf("finish exposed before EOF: %#v", chunk)
	case <-time.After(50 * time.Millisecond):
	}
	in <- core.StreamChunk{Type: core.ChunkUsage, Usage: &core.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}}
	in <- core.StreamChunk{Type: core.ChunkText, Delta: "late"}
	close(in)

	var chunks []core.StreamChunk
	for chunk := range out {
		chunks = append(chunks, chunk)
	}
	require.Len(t, chunks, 2)
	require.Equal(t, core.ChunkUsage, chunks[0].Type)
	require.Equal(t, 5, chunks[0].Usage.TotalTokens)
	require.Equal(t, core.ChunkError, chunks[1].Type)
	pe := core.AsProviderError(chunks[1].Err)
	require.Equal(t, core.ErrResponseIntegrity, pe.Kind)
	require.NotNil(t, pe.AttemptUsage)
	require.Equal(t, 5, pe.AttemptUsage.TotalTokens)
}
