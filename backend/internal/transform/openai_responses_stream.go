package transform

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	json "github.com/mydisha/keirouter/backend/internal/fastjson"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// The Responses API streams a rich, typed event sequence rather than uniform
// chunks:
//
//	response.created → response.in_progress →
//	response.output_item.added (message|reasoning|function_call) →
//	response.output_text.delta* / response.function_call_arguments.delta* /
//	response.reasoning_summary_text.delta* →
//	response.output_item.done → response.completed
//
// ParseStreamLine maps each event payload to canonical chunks (for routing a
// Responses provider like Codex back to a client). RenderStreamChunk produces
// the corresponding event sequence for a client that speaks the Responses
// dialect.

// respStreamEvent is one Responses SSE data payload.
type respStreamEvent struct {
	Type        string          `json:"type"`
	Delta       string          `json:"delta"`
	Arguments   string          `json:"arguments"`
	Input       string          `json:"input"`
	ItemID      string          `json:"item_id"`
	OutputIndex *int            `json:"output_index"`
	Item        *respStreamItem `json:"item"`
	Response    *struct {
		IncompleteDetails *struct {
			Reason string `json:"reason"`
		} `json:"incomplete_details"`
		Usage *struct {
			InputTokens        int `json:"input_tokens"`
			OutputTokens       int `json:"output_tokens"`
			InputTokensDetails struct {
				CachedTokens int `json:"cached_tokens"`
			} `json:"input_tokens_details"`
			OutputTokensDetails struct {
				ReasoningTokens int `json:"reasoning_tokens"`
			} `json:"output_tokens_details"`
		} `json:"usage"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	} `json:"response"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

type respStreamItem struct {
	Type             string `json:"type"`
	ID               string `json:"id"`
	CallID           string `json:"call_id"`
	Name             string `json:"name"`
	EncryptedContent string `json:"encrypted_content"`
}

type respParseState struct {
	byID        map[string]int
	sawToolCall bool
}

func (s *respParseState) responseIndex(ev respStreamEvent) int {
	ids := []string{ev.ItemID}
	if ev.Item != nil {
		ids = append(ids, ev.Item.ID, ev.Item.CallID)
	}
	if ev.OutputIndex != nil {
		for _, id := range ids {
			if id != "" {
				s.byID[id] = *ev.OutputIndex
			}
		}
		return *ev.OutputIndex
	}
	for _, id := range ids {
		if idx, ok := s.byID[id]; ok {
			for _, alias := range ids {
				if alias != "" {
					s.byID[alias] = idx
				}
			}
			return idx
		}
	}
	return 0
}

type ResponsesStreamParser struct {
	state respParseState
}

func (OpenAIResponsesCodec) NewStreamParser() *ResponsesStreamParser {
	return &ResponsesStreamParser{state: respParseState{byID: map[string]int{}}}
}

// ParseStreamLine converts one Responses SSE event payload into canonical chunks.
func (c OpenAIResponsesCodec) ParseStreamLine(line []byte, model string) ([]core.StreamChunk, error) {
	return c.NewStreamParser().ParseStreamLine(line, model)
}

func (p *ResponsesStreamParser) ParseStreamLine(line []byte, model string) ([]core.StreamChunk, error) {
	return p.state.parse(line, model)
}

func (s *respParseState) parse(line []byte, _ string) ([]core.StreamChunk, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || bytes.Equal(line, []byte("[DONE]")) {
		return nil, nil
	}

	var ev respStreamEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return nil, fmt.Errorf("openai-responses: parse stream event: %w", err)
	}

	switch ev.Type {
	case "response.output_text.delta":
		if ev.Delta == "" {
			return nil, nil
		}
		return []core.StreamChunk{{Type: core.ChunkText, Delta: ev.Delta}}, nil

	case "response.reasoning_summary_text.delta":
		if ev.Delta == "" {
			return nil, nil
		}
		return []core.StreamChunk{{Type: core.ChunkThinking, Delta: ev.Delta}}, nil

	case "response.output_item.added":
		if ev.Item != nil && (ev.Item.Type == "function_call" || ev.Item.Type == "custom_tool_call") {
			s.sawToolCall = true
			kind := core.ToolCallKind("")
			if ev.Item.Type == "custom_tool_call" {
				kind = core.ToolCallCustom
			}
			return []core.StreamChunk{{
				Type:             core.ChunkToolCall,
				Index:            s.responseIndex(ev),
				ToolArgumentMode: core.ToolArgumentDelta,
				ToolCall: &core.ToolCall{
					ID:        ev.Item.CallID,
					Name:      ev.Item.Name,
					Arguments: nil,
					Kind:      kind,
				},
			}}, nil
		}
		return nil, nil

	case "response.output_item.done":
		if ev.Item != nil && ev.Item.Type == "reasoning" && ev.Item.EncryptedContent != "" {
			return []core.StreamChunk{{Type: core.ChunkThinking, Signature: ev.Item.EncryptedContent, SignatureID: ev.Item.ID}}, nil
		}
		return nil, nil

	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		s.sawToolCall = true
		if ev.Delta == "" {
			return nil, nil
		}
		kind := core.ToolCallKind("")
		if ev.Type == "response.custom_tool_call_input.delta" {
			kind = core.ToolCallCustom
		}
		return []core.StreamChunk{{
			Type:             core.ChunkToolCall,
			Index:            s.responseIndex(ev),
			ToolArgumentMode: core.ToolArgumentDelta,
			ToolCall:         &core.ToolCall{Arguments: json.RawMessage(ev.Delta), Kind: kind},
		}}, nil

	case "response.function_call_arguments.done", "response.custom_tool_call_input.done":
		s.sawToolCall = true
		args := ev.Arguments
		kind := core.ToolCallKind("")
		if ev.Type == "response.custom_tool_call_input.done" {
			args = ev.Input
			kind = core.ToolCallCustom
		}
		return []core.StreamChunk{{
			Type:             core.ChunkToolCall,
			Index:            s.responseIndex(ev),
			ToolArgumentMode: core.ToolArgumentComplete,
			ToolCall:         &core.ToolCall{Arguments: json.RawMessage(args), Kind: kind},
		}}, nil

	case "response.completed", "response.incomplete":
		finish := core.FinishStop
		reason := ""
		if s.sawToolCall {
			finish = core.FinishToolCalls
		}
		if ev.Type == "response.incomplete" {
			if ev.Response == nil || ev.Response.IncompleteDetails == nil || ev.Response.IncompleteDetails.Reason == "" {
				return nil, fmt.Errorf("openai-responses: incomplete response missing reason")
			}
			finish = core.FinishLength
			reason = ev.Response.IncompleteDetails.Reason
		}
		chunks := []core.StreamChunk{{Type: core.ChunkFinish, FinishReason: finish, Delta: reason}}
		if ev.Response != nil && ev.Response.Usage != nil {
			u := ev.Response.Usage
			chunks = append(chunks, core.StreamChunk{
				Type: core.ChunkUsage,
				Usage: &core.Usage{
					PromptTokens:     u.InputTokens,
					CompletionTokens: u.OutputTokens,
					TotalTokens:      u.InputTokens + u.OutputTokens,
					CachedTokens:     u.InputTokensDetails.CachedTokens,
					ReasoningTokens:  u.OutputTokensDetails.ReasoningTokens,
					Source:           core.UsageSourceProvider,
				},
			})
		}
		return chunks, nil

	case "error", "response.failed":
		msg := ""
		if ev.Error != nil {
			msg = ev.Error.Message
		} else if ev.Response != nil && ev.Response.Error != nil {
			msg = ev.Response.Error.Message
		}
		return []core.StreamChunk{{
			Type: core.ChunkError,
			Err:  classifyRespStreamError(msg),
		}}, nil

	default:
		// response.created, in_progress, output_item.done, content_part.*,
		// output_text.done, reasoning_summary_*: nothing canonical to emit.
		return nil, nil
	}
}

// classifyRespStreamError maps an in-stream error event (HTTP 200 with an
// error in the SSE body, so no status code) onto the right error kind/scope.
// Codex reports request-level problems this way — e.g. "Your input exceeds
// the context window of this model." — and the previous blanket ErrUpstream
// classification (provider scope) put the account on cooldown and fed the
// provider circuit breaker on every client retry, eventually locking out all
// accounts with "all matching candidates are temporarily unavailable".
func classifyRespStreamError(msg string) *core.ProviderError {
	m := strings.ToLower(msg)
	switch {
	// Rate limits mention token counts too ("Rate limit reached … too many
	// tokens per min"); match the rate-limit vocabulary first so a TPM limit
	// is never mistaken for context overflow below and left without cooldown.
	case strings.Contains(m, "rate limit") || strings.Contains(m, "rate_limit") ||
		strings.Contains(m, "tokens per min"):
		return &core.ProviderError{Kind: core.ErrRateLimit, Message: msg}
	// Context overflow: the request itself is too large. Only the client can
	// fix it (compact/trim), so scope it to the request — no cooldown, no
	// fallback, surface the error straight back.
	case strings.Contains(m, "exceeds the context window"),
		strings.Contains(m, "context length"),
		strings.Contains(m, "maximum context"),
		strings.Contains(m, "input is too long"),
		strings.Contains(m, "prompt is too long"),
		strings.Contains(m, "too many tokens"):
		return &core.ProviderError{Kind: core.ErrBadRequest, Scope: core.FailureScopeRequest, Message: msg}
	// Unknown/inaccessible model reported in-stream: model-scoped so chains
	// can fall back, mirroring the HTTP-status classification in connectors.
	case strings.Contains(m, "model") &&
		(strings.Contains(m, "not supported") || strings.Contains(m, "not available") ||
			strings.Contains(m, "not found") || strings.Contains(m, "does not exist")):
		return &core.ProviderError{Kind: core.ErrModelUnavailable, Scope: core.FailureScopeModel, Message: msg}
	}
	return &core.ProviderError{Kind: core.ErrUpstream, Message: msg}
}

// respStreamState tracks per-stream rendering bookkeeping for the Responses
// event sequence, stashed in StreamState.Custom.
type respStreamState struct {
	seq             int
	started         bool
	responseID      string
	msgAdded        bool
	msgPartAdded    bool
	msgText         string
	msgIdx          int
	reasoningIdx    int
	reasoningText   string
	reasoningID     string
	reasoningSig    string
	reasoningWireID string
	nextOutput      int
	toolOutput      map[int]int
	toolAdded       map[int]bool
	toolCallID      map[int]string
	toolName        map[int]string
	toolKind        map[int]core.ToolCallKind
	toolArgs        map[int]string
	usage           *core.Usage
	finished        bool
	completed       bool
	failed          bool
	finishReason    core.FinishReason
	finishDetail    string
}

func respState(state *StreamState) *respStreamState {
	if state.Custom == nil {
		state.Custom = map[string]any{}
	}
	if s, ok := state.Custom["resp"].(*respStreamState); ok {
		return s
	}
	s := &respStreamState{
		responseID: "resp_" + firstNonEmpty(state.MessageID, "stream"),
		toolOutput: map[int]int{},
		toolAdded:  map[int]bool{},
		toolCallID: map[int]string{},
		toolName:   map[int]string{},
		toolKind:   map[int]core.ToolCallKind{},
		toolArgs:   map[int]string{},
	}
	state.Custom["resp"] = s
	return s
}

// RenderStreamChunk emits Responses API event(s) for a canonical chunk.
func (OpenAIResponsesCodec) RenderStreamChunk(chunk core.StreamChunk, state *StreamState) ([][]byte, error) {
	s := respState(state)
	var events [][]byte
	emit := func(eventType string, data map[string]any) {
		s.seq++
		data["sequence_number"] = s.seq
		data["type"] = eventType
		events = append(events, respEvent(eventType, data))
	}

	outputIndex := func(kind string, idx int) int {
		switch kind {
		case "message":
			if !s.msgAdded {
				s.msgIdx = s.nextOutput
				s.nextOutput++
			}
			return s.msgIdx
		case "reasoning":
			if _, ok := state.Custom["resp_reasoning_added"]; !ok {
				s.reasoningIdx = s.nextOutput
				s.nextOutput++
			}
			return s.reasoningIdx
		default:
			if output, ok := s.toolOutput[idx]; ok {
				return output
			}
			output := s.nextOutput
			s.nextOutput++
			s.toolOutput[idx] = output
			return output
		}
	}

	ensureStarted := func() {
		if s.started {
			return
		}
		s.started = true
		emit("response.created", map[string]any{
			"response": map[string]any{
				"id": s.responseID, "object": "response", "status": "in_progress", "output": []any{},
			},
		})
		emit("response.in_progress", map[string]any{
			"response": map[string]any{"id": s.responseID, "object": "response", "status": "in_progress"},
		})
	}

	switch chunk.Type {
	case core.ChunkText:
		ensureStarted()
		msgID := "msg_" + s.responseID + "_0"
		output := outputIndex("message", 0)
		if !s.msgAdded {
			s.msgAdded = true
			emit("response.output_item.added", map[string]any{
				"output_index": output,
				"item":         map[string]any{"id": msgID, "type": "message", "role": "assistant", "content": []any{}},
			})
		}
		if !s.msgPartAdded {
			s.msgPartAdded = true
			emit("response.content_part.added", map[string]any{
				"item_id": msgID, "output_index": output, "content_index": 0,
				"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
			})
		}
		s.msgText += chunk.Delta
		emit("response.output_text.delta", map[string]any{
			"item_id": msgID, "output_index": output, "content_index": 0, "delta": chunk.Delta,
		})

	case core.ChunkThinking:
		ensureStarted()
		if chunk.SignatureID != "" {
			s.reasoningID = chunk.SignatureID
		}
		if chunk.Signature != "" {
			s.reasoningSig = chunk.Signature
		}
		if s.reasoningWireID == "" {
			s.reasoningWireID = firstNonEmpty(s.reasoningID, "rs_"+s.responseID+"_0")
		}
		rsID := s.reasoningWireID
		output := outputIndex("reasoning", 0)
		if _, ok := state.Custom["resp_reasoning_added"]; !ok {
			state.Custom["resp_reasoning_added"] = true
			emit("response.output_item.added", map[string]any{
				"output_index": output,
				"item":         map[string]any{"id": rsID, "type": "reasoning", "summary": []any{}},
			})
		}
		if chunk.Delta != "" {
			s.reasoningText += chunk.Delta
			emit("response.reasoning_summary_text.delta", map[string]any{
				"item_id": rsID, "output_index": output, "summary_index": 0, "delta": chunk.Delta,
			})
		}

	case core.ChunkToolCall:
		if chunk.ToolCall == nil {
			return nil, nil
		}
		if chunk.ToolCall.Kind != core.ToolCallCustom && len(chunk.ToolCall.Arguments) > 0 && !validCompleteToolArguments(chunk.ToolCall.Arguments) {
			return nil, fmt.Errorf("openai responses: tool arguments must be a JSON object")
		}
		ensureStarted()
		idx := chunk.Index
		if chunk.ToolCall.Kind != "" {
			s.toolKind[idx] = chunk.ToolCall.Kind
		}
		output := outputIndex("tool", idx)
		if chunk.ToolCall.Name != "" {
			s.toolName[idx] = chunk.ToolCall.Name
		}
		if chunk.ToolCall.ID != "" && !s.toolAdded[idx] {
			s.toolAdded[idx] = true
			s.toolCallID[idx] = chunk.ToolCall.ID
			item := map[string]any{
				"id": "fc_" + chunk.ToolCall.ID, "type": "function_call",
				"call_id": chunk.ToolCall.ID, "name": s.toolName[idx], "arguments": "",
			}
			if s.toolKind[idx] == core.ToolCallCustom {
				item["type"] = "custom_tool_call"
				delete(item, "arguments")
				item["input"] = ""
			}
			emit("response.output_item.added", map[string]any{"output_index": output, "item": item})
		}
		if args := string(chunk.ToolCall.Arguments); args != "" && (args != "{}" || s.toolKind[idx] == core.ToolCallCustom) {
			callID := s.toolCallID[idx]
			s.toolArgs[idx] += args
			eventType := "response.function_call_arguments.delta"
			if s.toolKind[idx] == core.ToolCallCustom {
				eventType = "response.custom_tool_call_input.delta"
			}
			emit(eventType, map[string]any{
				"item_id": "fc_" + callID, "output_index": output, "delta": args,
			})
		}

	case core.ChunkFinish:
		if s.finished {
			return nil, nil
		}
		s.finished = true
		s.finishReason = chunk.FinishReason
		s.finishDetail = chunk.Delta
		// Close any open output items before completing the response.
		if s.msgAdded {
			msgID := "msg_" + s.responseID + "_0"
			emit("response.output_text.done", map[string]any{
				"item_id": msgID, "output_index": s.msgIdx, "content_index": 0, "text": s.msgText,
			})
			emit("response.output_item.done", map[string]any{
				"output_index": s.msgIdx,
				"item": map[string]any{
					"id": msgID, "type": "message", "role": "assistant",
					"content": []map[string]any{{"type": "output_text", "text": s.msgText, "annotations": []any{}}},
				},
			})
		}
		if s.reasoningText != "" || s.reasoningSig != "" {
			rsID := firstNonEmpty(s.reasoningWireID, s.reasoningID, "rs_"+s.responseID+"_0")
			if s.reasoningText != "" {
				emit("response.reasoning_summary_text.done", map[string]any{
					"item_id": rsID, "output_index": s.reasoningIdx, "summary_index": 0, "text": s.reasoningText,
				})
			}
			item := map[string]any{"id": rsID, "type": "reasoning", "summary": []any{}}
			if s.reasoningText != "" {
				item["summary"] = []map[string]any{{"type": "summary_text", "text": s.reasoningText}}
			}
			if s.reasoningSig != "" {
				item["encrypted_content"] = s.reasoningSig
			}
			emit("response.output_item.done", map[string]any{
				"output_index": s.reasoningIdx,
				"item":         item,
			})
		}
		indices := make([]int, 0, len(s.toolCallID))
		for idx := range s.toolCallID {
			indices = append(indices, idx)
		}
		sort.Ints(indices)
		for _, idx := range indices {
			callID := s.toolCallID[idx]
			output := s.toolOutput[idx]
			args := s.toolArgs[idx]
			if s.toolKind[idx] == core.ToolCallCustom {
				emit("response.custom_tool_call_input.done", map[string]any{
					"item_id": "fc_" + callID, "output_index": output, "input": args,
				})
				emit("response.output_item.done", map[string]any{
					"output_index": output,
					"item": map[string]any{
						"id": "fc_" + callID, "type": "custom_tool_call",
						"call_id": callID, "name": s.toolName[idx], "input": args,
					},
				})
				continue
			}
			if args == "" {
				args = "{}"
			}
			emit("response.function_call_arguments.done", map[string]any{
				"item_id": "fc_" + callID, "output_index": output, "arguments": args,
			})
			emit("response.output_item.done", map[string]any{
				"output_index": output,
				"item": map[string]any{
					"id": "fc_" + callID, "type": "function_call",
					"call_id": callID, "name": s.toolName[idx], "arguments": args,
				},
			})
		}

	case core.ChunkUsage:
		if chunk.Usage != nil {
			usage := *chunk.Usage
			s.usage = &usage
		}

	case core.ChunkError:
		if s.failed || s.completed {
			return nil, nil
		}
		ensureStarted()
		s.failed = true
		message := "provider stream failed"
		if chunk.Err != nil {
			message = chunk.Err.Error()
		}
		emit("response.failed", map[string]any{"response": map[string]any{
			"id": s.responseID, "object": "response", "status": "failed",
			"error": map[string]any{"type": "upstream_error", "code": "upstream_error", "message": message},
		}})

	default:
		return nil, nil
	}
	return events, nil
}

// RenderStreamDone emits response.completed after all output items are closed.
func (c OpenAIResponsesCodec) RenderStreamDone(state *StreamState) [][]byte {
	s := respState(state)
	if s.completed || s.failed || !s.started {
		return nil
	}
	events, _ := c.RenderStreamChunk(core.StreamChunk{Type: core.ChunkFinish}, state)
	s.completed = true
	s.seq++
	status := "completed"
	eventType := "response.completed"
	if s.finishReason == core.FinishLength {
		status = "incomplete"
		eventType = "response.incomplete"
	}
	response := map[string]any{"id": s.responseID, "object": "response", "status": status}
	if status == "incomplete" {
		reason := firstNonEmpty(s.finishDetail, "max_output_tokens")
		response["incomplete_details"] = map[string]any{"reason": reason}
	}
	if s.usage != nil {
		response["usage"] = map[string]any{
			"input_tokens":  s.usage.PromptTokens,
			"output_tokens": s.usage.CompletionTokens,
			"total_tokens":  s.usage.TotalTokens,
			"input_tokens_details": map[string]int{
				"cached_tokens": s.usage.CachedTokens,
			},
			"output_tokens_details": map[string]int{
				"reasoning_tokens": s.usage.ReasoningTokens,
			},
		}
	}
	return append(events, respEvent(eventType, map[string]any{
		"type":            eventType,
		"sequence_number": s.seq,
		"response":        response,
	}))
}

// respEvent formats a Responses API SSE event: "event: <name>\ndata: <json>\n\n".
func respEvent(name string, payload map[string]any) []byte {
	b, _ := json.Marshal(payload)
	out := make([]byte, 0, len(name)+len(b)+20)
	out = append(out, "event: "...)
	out = append(out, name...)
	out = append(out, '\n')
	out = append(out, "data: "...)
	out = append(out, b...)
	out = append(out, '\n', '\n')
	return out
}
