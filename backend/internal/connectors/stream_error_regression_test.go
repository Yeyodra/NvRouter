package connectors

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestStreamMalformedFrameEmitsOneErrorAndStops(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		stream func(context.Context, string) (<-chan core.StreamChunk, error)
	}{
		{
			name: "openai chat",
			body: "data: {\"choices\":[{\"delta\":{\"content\":\"before\"}}]}\n\ndata: {bad\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"after\"}}]}\n\n",
			stream: func(ctx context.Context, url string) (<-chan core.StreamChunk, error) {
				return NewOpenAICompatible("openai", url).Stream(ctx, textReq("model", true), core.Credentials{}, core.StreamConfig{})
			},
		},
		{
			name: "openai responses",
			body: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\ndata: {bad\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"after\"}\n\n",
			stream: func(ctx context.Context, url string) (<-chan core.StreamChunk, error) {
				return NewOpenAIResponses("openai-responses", url).Stream(ctx, textReq("model", true), core.Credentials{}, core.StreamConfig{})
			},
		},
		{
			name: "anthropic",
			body: "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"before\"}}\n\ndata: {bad\n\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"after\"}}\n\n",
			stream: func(ctx context.Context, url string) (<-chan core.StreamChunk, error) {
				return NewAnthropic("anthropic", url).Stream(ctx, textReq("model", true), core.Credentials{}, core.StreamConfig{})
			},
		},
		{
			name: "gemini",
			body: "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"before\"}]}}]}\n\ndata: {bad\n\ndata: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"after\"}]}}]}\n\n",
			stream: func(ctx context.Context, url string) (<-chan core.StreamChunk, error) {
				return NewGemini("gemini", url).Stream(ctx, textReq("model", true), core.Credentials{APIKey: "test"}, core.StreamConfig{})
			},
		},
		{
			name: "ollama",
			body: "{\"message\":{\"content\":\"before\"}}\n{bad\n{\"message\":{\"content\":\"after\"}}\n",
			stream: func(ctx context.Context, url string) (<-chan core.StreamChunk, error) {
				return NewOllama("ollama", url).Stream(ctx, textReq("model", true), core.Credentials{}, core.StreamConfig{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, tt.body)
			}))
			defer srv.Close()

			stream, err := tt.stream(context.Background(), srv.URL)
			require.NoError(t, err)
			var text string
			var streamErrors []error
			for chunk := range stream {
				switch chunk.Type {
				case core.ChunkText:
					text += chunk.Delta
				case core.ChunkError:
					streamErrors = append(streamErrors, chunk.Err)
				}
			}
			require.Equal(t, "before", text)
			require.Len(t, streamErrors, 1)
			require.Equal(t, core.ErrResponseIntegrity, core.AsProviderError(streamErrors[0]).Kind)
		})
	}
}

func TestAnthropicErrorEventEmitsTypedRequestError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad tool schema\"}}\n\n")
	}))
	defer srv.Close()

	stream, err := NewAnthropic("anthropic", srv.URL).Stream(context.Background(), textReq("model", true), core.Credentials{}, core.StreamConfig{})
	require.NoError(t, err)
	var streamErr error
	for chunk := range stream {
		if chunk.Type == core.ChunkError {
			streamErr = chunk.Err
		}
	}
	require.Error(t, streamErr)
	pe := core.AsProviderError(streamErr)
	require.Equal(t, core.ErrBadRequest, pe.Kind)
	require.Equal(t, core.FailureScopeRequest, pe.EffectiveScope())
	require.Contains(t, pe.Message, "bad tool schema")
}

func TestResponsesAbruptEOFEmitsTypedErrorWithoutAssistantProtocolText(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n")
	}))
	defer srv.Close()

	stream, err := NewOpenAIResponses("openai-responses", srv.URL).Stream(context.Background(), textReq("model", true), core.Credentials{}, core.StreamConfig{})
	require.NoError(t, err)
	var text string
	var streamErr error
	for chunk := range stream {
		switch chunk.Type {
		case core.ChunkText:
			text += chunk.Delta
		case core.ChunkError:
			streamErr = chunk.Err
		}
	}
	require.Equal(t, "before", text)
	require.Error(t, streamErr)
	require.Equal(t, core.ErrResponseIntegrity, core.AsProviderError(streamErr).Kind)
	require.NotContains(t, text, "response.failed")
}

func TestResponsesIncompleteEndsStreamWithoutError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n")
	}))
	defer srv.Close()

	stream, err := NewOpenAIResponses("openai-responses", srv.URL).Stream(context.Background(), textReq("model", true), core.Credentials{}, core.StreamConfig{})
	require.NoError(t, err)
	var finish core.FinishReason
	var streamErr error
	for chunk := range stream {
		if chunk.Type == core.ChunkFinish {
			finish = chunk.FinishReason
		}
		if chunk.Type == core.ChunkError {
			streamErr = chunk.Err
		}
	}
	require.NoError(t, streamErr)
	require.Equal(t, core.FinishLength, finish)
}

func TestSendStreamErrorDoesNotBlockOnFullUnreadChannelAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan core.StreamChunk, 1)
	out <- core.StreamChunk{Type: core.ChunkText}
	cancel()

	done := make(chan struct{})
	go func() {
		sendStreamError(ctx, out, core.ErrUpstream, "openai", "model", errors.New("malformed frame"))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal error send blocked on a full unread channel after cancellation")
	}
}
