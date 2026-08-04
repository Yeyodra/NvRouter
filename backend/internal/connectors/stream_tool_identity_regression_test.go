package connectors

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestGeminiStream_SeparateSameNameCallsKeepIdentityAndToolFinish(t *testing.T) {
	body := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"same\",\"args\":{\"n\":1}}}]}}]}\n\n" +
		"data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"same\",\"args\":{\"n\":2}}}]},\"finishReason\":\"STOP\"}]}\n\n"
	stream := geminiTestStream(t, body)

	var calls []core.StreamChunk
	var finish core.FinishReason
	for chunk := range stream {
		switch chunk.Type {
		case core.ChunkToolCall:
			calls = append(calls, chunk)
		case core.ChunkFinish:
			finish = chunk.FinishReason
		case core.ChunkError:
			t.Fatal(chunk.Err)
		}
	}
	if len(calls) != 2 || calls[0].Index != 0 || calls[1].Index != 1 || calls[0].ToolCall.ID != "call_same_0" || calls[1].ToolCall.ID != "call_same_1" {
		t.Fatalf("calls = %#v, want distinct stream-local identity", calls)
	}
	if finish != core.FinishToolCalls {
		t.Fatalf("finish = %q, want tool_calls", finish)
	}
}

func TestGeminiStream_IdentityRestartsPerRequest(t *testing.T) {
	body := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"same\",\"args\":{}}}]}}]}\n\n"
	for i := 0; i < 2; i++ {
		stream := geminiTestStream(t, body)
		chunk := <-stream
		if chunk.Index != 0 || chunk.ToolCall == nil || chunk.ToolCall.ID != "call_same_0" {
			t.Fatalf("request %d first call = %#v, want fresh stream identity", i, chunk)
		}
		for range stream {
		}
	}
}

func TestVertexAndCloudCodeStreams_ReuseGeminiParser(t *testing.T) {
	plain := "data: {\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"same\",\"args\":{\"n\":1}}}]}}]}\n\n" +
		"data: {\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}\n\n"
	wrapped := "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"name\":\"same\",\"args\":{\"n\":1}}}]}}]}}\n\n" +
		"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[]},\"finishReason\":\"STOP\"}]}}\n\n"

	for _, tc := range []struct {
		name string
		body string
		open func(string) (<-chan core.StreamChunk, error)
	}{
		{name: "vertex", body: plain, open: func(url string) (<-chan core.StreamChunk, error) {
			return NewVertex("vertex", url).Stream(context.Background(), textReq("model", true), core.Credentials{APIKey: "test"}, core.StreamConfig{})
		}},
		{name: "cloudcode", body: wrapped, open: func(url string) (<-chan core.StreamChunk, error) {
			return NewGeminiCLI("cloudcode", url+"/v1internal").Stream(context.Background(), textReq("model", true), core.Credentials{AccessToken: "test"}, core.StreamConfig{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, tc.body)
			}))
			defer srv.Close()

			stream, err := tc.open(srv.URL)
			if err != nil {
				t.Fatal(err)
			}
			var finish core.FinishReason
			for chunk := range stream {
				if chunk.Type == core.ChunkFinish {
					finish = chunk.FinishReason
				} else if chunk.Type == core.ChunkError {
					t.Fatal(chunk.Err)
				}
			}
			if finish != core.FinishToolCalls {
				t.Fatalf("finish = %q, want tool_calls", finish)
			}
		})
	}
}

func TestOllamaStream_IDLessCallsAcrossFramesKeepIdentity(t *testing.T) {
	body := "{\"message\":{\"tool_calls\":[{\"function\":{\"name\":\"same\",\"arguments\":{\"n\":1}}}]}}\n" +
		"{\"message\":{\"tool_calls\":[{\"function\":{\"name\":\"same\",\"arguments\":{\"n\":2}}}]}}\n" +
		"{\"done\":true}\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
	defer srv.Close()

	stream, err := NewOllama("ollama", srv.URL).Stream(context.Background(), textReq("model", true), core.Credentials{}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var calls []core.StreamChunk
	var finish core.FinishReason
	for chunk := range stream {
		switch chunk.Type {
		case core.ChunkToolCall:
			calls = append(calls, chunk)
		case core.ChunkFinish:
			finish = chunk.FinishReason
		case core.ChunkError:
			t.Fatal(chunk.Err)
		}
	}
	if len(calls) != 2 || calls[0].Index != 0 || calls[1].Index != 1 || calls[0].ToolCall.ID != "call_0" || calls[1].ToolCall.ID != "call_1" {
		t.Fatalf("calls = %#v, want distinct stream-local identity", calls)
	}
	if finish != core.FinishToolCalls {
		t.Fatalf("finish = %q, want tool_calls", finish)
	}
}

func TestCursorStream_InterleavedFragmentsKeepCallIndices(t *testing.T) {
	frames := [][]byte{
		cursorToolFrame("call_a", "same", `{"a":`),
		cursorToolFrame("call_b", "same", `{"b":`),
		cursorToolFrame("call_a", "same", `1}`),
		cursorToolFrame("call_b", "same", `2}`),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for _, frame := range frames {
			_, _ = w.Write(frame)
		}
	}))
	defer srv.Close()

	stream, err := NewCursor("cursor", srv.URL).Stream(context.Background(), textReq("model", true), core.Credentials{AccessToken: "test"}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	var calls []core.StreamChunk
	for chunk := range stream {
		if chunk.Type == core.ChunkToolCall {
			calls = append(calls, chunk)
		} else if chunk.Type == core.ChunkError {
			t.Fatal(chunk.Err)
		}
	}
	if len(calls) != 6 {
		t.Fatalf("got %d tool fragments, want 6: %#v", len(calls), calls)
	}
	want := []int{0, 0, 1, 1, 0, 1}
	for i, index := range want {
		if calls[i].Index != index {
			t.Fatalf("fragment %d index = %d, want %d: %#v", i, calls[i].Index, index, calls)
		}
	}
}

func geminiTestStream(t *testing.T, body string) <-chan core.StreamChunk {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	stream, err := NewGemini("gemini", srv.URL).Stream(context.Background(), textReq("model", true), core.Credentials{APIKey: "test"}, core.StreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func cursorToolFrame(id, name, args string) []byte {
	var tool bytes.Buffer
	tool.Write(pbStr(fTOOL_ID, id))
	tool.Write(pbStr(fTOOL_NAME, name))
	tool.Write(pbStr(fTOOL_RAW_ARGS, args))
	return wrapConnectRPCFrame(pbLen(fTOOL_CALL, tool.Bytes()))
}
