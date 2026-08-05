package transform

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// ToolArgSanitizer assembles streaming tool calls and sanitizes valid arguments.
type ToolArgSanitizer struct {
	buffers    map[int]*toolBuffer
	flushed    bool
	failed     bool
	finishSeen bool
}

type toolBuffer struct {
	id       string
	name     string
	kind     core.ToolCallKind
	complete bool
	args     strings.Builder
}

func NewToolArgSanitizer() *ToolArgSanitizer {
	return &ToolArgSanitizer{buffers: make(map[int]*toolBuffer)}
}

// StreamResponseAssembler folds canonical stream chunks into a unary response.
type StreamResponseAssembler struct {
	model     string
	sanitizer *ToolArgSanitizer
	content   []core.ContentPart
	toolSlots map[int][]int
	toolIDs   map[int]string
	finish    core.FinishReason
	usage     core.Usage
	err       error
	terminal  bool
}

func NewStreamResponseAssembler(model string) *StreamResponseAssembler {
	return &StreamResponseAssembler{
		model: model, sanitizer: NewToolArgSanitizer(), finish: core.FinishStop,
		toolSlots: make(map[int][]int), toolIDs: make(map[int]string),
	}
}

func (a *StreamResponseAssembler) Process(chunk core.StreamChunk) {
	if a.terminal {
		switch chunk.Type {
		case core.ChunkFinish, core.ChunkPing:
			return
		case core.ChunkUsage:
			a.consume(chunk)
			return
		default:
			a.setError(protocolError("received %s chunk after terminal finish", chunk.Type))
			return
		}
	}
	if chunk.Type == core.ChunkToolCall && chunk.ToolCall != nil {
		a.reserveToolSlot(chunk)
	}
	a.sanitizer.Process(chunk, a.consume)
}

func (a *StreamResponseAssembler) reserveToolSlot(chunk core.StreamChunk) {
	id := chunk.ToolCall.ID
	if _, ok := a.toolSlots[chunk.Index]; !ok || id != "" && a.toolIDs[chunk.Index] != "" && id != a.toolIDs[chunk.Index] {
		pos := len(a.content)
		for idx, slots := range a.toolSlots {
			if idx > chunk.Index && len(slots) > 0 && slots[0] < pos {
				pos = slots[0]
			}
		}
		a.content = append(a.content, core.ContentPart{})
		copy(a.content[pos+1:], a.content[pos:])
		a.content[pos] = core.ContentPart{Type: core.PartToolCall}
		for idx, slots := range a.toolSlots {
			for i := range slots {
				if slots[i] >= pos {
					a.toolSlots[idx][i]++
				}
			}
		}
		a.toolSlots[chunk.Index] = append(a.toolSlots[chunk.Index], pos)
	}
	if id != "" {
		a.toolIDs[chunk.Index] = id
	}
}

func (a *StreamResponseAssembler) consume(chunk core.StreamChunk) {
	switch chunk.Type {
	case core.ChunkText:
		a.appendText(core.PartText, chunk.Delta, "")
	case core.ChunkThinking:
		if chunk.Delta != "" {
			a.appendText(core.PartThinking, chunk.Delta, chunk.Signature)
		} else if chunk.Signature != "" {
			if n := len(a.content); n > 0 && a.content[n-1].Type == core.PartThinking {
				a.content[n-1].Signature = chunk.Signature
				a.content[n-1].SignatureID = chunk.SignatureID
				a.content[n-1].SignatureProvider = chunk.SignatureProvider
			} else {
				a.content = append(a.content, core.ContentPart{Type: core.PartThinking, Signature: chunk.Signature, SignatureID: chunk.SignatureID, SignatureProvider: chunk.SignatureProvider})
			}
		}
	case core.ChunkToolCall:
		if chunk.ToolCall != nil {
			slots := a.toolSlots[chunk.Index]
			if len(slots) == 0 {
				a.setError(protocolError("tool call %d completed without an ordering slot", chunk.Index))
				return
			}
			a.content[slots[0]].ToolCall = chunk.ToolCall
			a.toolSlots[chunk.Index] = slots[1:]
			a.finish = core.FinishToolCalls
		}
	case core.ChunkFinish:
		a.terminal = true
		if chunk.FinishReason != "" && (a.finish != core.FinishToolCalls || chunk.FinishReason != core.FinishStop) {
			a.finish = chunk.FinishReason
		}
	case core.ChunkUsage:
		if chunk.Usage != nil {
			mergeUsage(&a.usage, *chunk.Usage)
		}
	case core.ChunkError:
		if chunk.Err == nil {
			a.setError(protocolError("upstream emitted an error chunk without an error"))
		} else {
			a.setError(chunk.Err)
		}
	}
}

func (a *StreamResponseAssembler) appendText(kind core.PartType, text, signature string) {
	if text == "" {
		return
	}
	if n := len(a.content); n > 0 && a.content[n-1].Type == kind && a.content[n-1].Signature == signature {
		a.content[n-1].Text += text
		return
	}
	a.content = append(a.content, core.ContentPart{Type: kind, Text: text, Signature: signature})
}

func (a *StreamResponseAssembler) setError(err error) {
	if a.err == nil {
		a.err = err
	}
}

func (a *StreamResponseAssembler) Response() (*core.ChatResponse, error) {
	a.sanitizer.Flush(a.consume)
	if a.err != nil {
		pe := core.AsProviderError(a.err)
		copy := *pe
		if copy.AttemptUsage == nil && a.usage != (core.Usage{}) {
			usage := a.usage
			copy.AttemptUsage = &usage
		}
		return nil, &copy
	}
	if !a.terminal {
		return nil, protocolError("provider stream ended without a terminal finish")
	}
	return &core.ChatResponse{Model: a.model, Message: core.Message{Role: core.RoleAssistant, Content: a.content}, FinishReason: a.finish, Usage: a.usage}, nil
}

func CollectStreamResponse(stream <-chan core.StreamChunk, model string) (*core.ChatResponse, error) {
	assembler := NewStreamResponseAssembler(model)
	for chunk := range stream {
		assembler.Process(chunk)
	}
	return assembler.Response()
}

func mergeUsage(dst *core.Usage, src core.Usage) {
	if src.PromptTokens != 0 {
		dst.PromptTokens = src.PromptTokens
	}
	if src.CompletionTokens != 0 {
		dst.CompletionTokens = src.CompletionTokens
	}
	if src.TotalTokens != 0 {
		dst.TotalTokens = src.TotalTokens
	}
	if src.CachedTokens != 0 {
		dst.CachedTokens = src.CachedTokens
	}
	if src.CacheWriteTokens != 0 {
		dst.CacheWriteTokens = src.CacheWriteTokens
	}
	if src.ReasoningTokens != 0 {
		dst.ReasoningTokens = src.ReasoningTokens
	}
	if src.Source != "" && src.Source != core.UsageSourceNone {
		dst.Source = src.Source
	}
}

func (s *ToolArgSanitizer) Process(chunk core.StreamChunk, emit func(core.StreamChunk)) {
	if s.failed {
		return
	}
	if s.finishSeen {
		switch chunk.Type {
		case core.ChunkUsage:
			emit(chunk)
		case core.ChunkFinish:
			return
		default:
			s.fail(emit, "received %s chunk after terminal finish", chunk.Type)
		}
		return
	}
	if chunk.Type != core.ChunkToolCall || chunk.ToolCall == nil {
		if chunk.Type == core.ChunkFinish {
			s.finishSeen = true
			s.Flush(emit)
			if s.failed {
				return
			}
		}
		emit(chunk)
		return
	}
	if chunk.ToolArgumentMode != "" && chunk.ToolArgumentMode != core.ToolArgumentDelta && chunk.ToolArgumentMode != core.ToolArgumentSnapshot && chunk.ToolArgumentMode != core.ToolArgumentComplete {
		s.fail(emit, "tool call %d: unknown argument mode %q", chunk.Index, chunk.ToolArgumentMode)
		return
	}

	idx := chunk.Index
	buf, ok := s.buffers[idx]
	if !ok {
		buf = &toolBuffer{}
		s.buffers[idx] = buf
	} else {
		idChanged := chunk.ToolCall.ID != "" && buf.id != "" && buf.id != chunk.ToolCall.ID
		nameChanged := chunk.ToolCall.Name != "" && buf.name != "" && buf.name != chunk.ToolCall.Name
		if nameChanged && !idChanged {
			s.fail(emit, "tool call %d: conflicting identity", idx)
			return
		}
		if idChanged {
			s.flushIndex(idx, emit)
			if s.failed {
				return
			}
			buf = &toolBuffer{}
			s.buffers[idx] = buf
		}
	}
	if chunk.ToolCall.ID != "" {
		buf.id = chunk.ToolCall.ID
	}
	if chunk.ToolCall.Name != "" {
		buf.name = chunk.ToolCall.Name
	}
	if chunk.ToolCall.Kind != "" {
		buf.kind = chunk.ToolCall.Kind
	}
	if buf.complete && chunk.ToolCall.ID == "" && chunk.ToolCall.Name == "" {
		s.fail(emit, "tool call %d: ambiguous identity-less update after complete call", idx)
		return
	}
	applyToolArgs(buf, chunk.ToolArgumentMode, string(chunk.ToolCall.Arguments))
	buf.complete = chunk.ToolArgumentMode == core.ToolArgumentComplete
}

func (s *ToolArgSanitizer) fail(emit func(core.StreamChunk), format string, args ...any) {
	s.failed = true
	emit(core.StreamChunk{Type: core.ChunkError, Err: protocolError(format, args...)})
}

func protocolError(format string, args ...any) error {
	message := fmt.Sprintf(format, args...)
	return &core.ProviderError{Kind: core.ErrResponseIntegrity, Message: message, Cause: fmt.Errorf("%s", message)}
}

func ValidateResponseIntegrity(resp *core.ChatResponse) error {
	if resp == nil {
		return protocolError("provider returned an empty response")
	}
	for i, part := range resp.Message.Content {
		if part.Type != core.PartToolCall {
			continue
		}
		if part.ToolCall == nil {
			return protocolError("tool call %d is missing", i)
		}
		if part.ToolCall.Kind != core.ToolCallCustom && !validCompleteToolArguments(part.ToolCall.Arguments) {
			return protocolError("tool call %d: invalid arguments", i)
		}
	}
	return nil
}

func validCompleteToolArguments(args json.RawMessage) bool {
	var object map[string]any
	return json.Unmarshal(args, &object) == nil && object != nil
}

func applyToolArgs(buf *toolBuffer, mode core.ToolArgumentMode, update string) {
	if update == "" {
		return
	}
	switch mode {
	case core.ToolArgumentSnapshot, core.ToolArgumentComplete:
		buf.args.Reset()
		buf.args.WriteString(update)
	case core.ToolArgumentDelta, "":
		buf.args.WriteString(update)
	}
}

func (s *ToolArgSanitizer) Flush(emit func(core.StreamChunk)) {
	if s.failed || s.flushed {
		return
	}
	s.flushed = true
	indices := make([]int, 0, len(s.buffers))
	for idx := range s.buffers {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	for _, idx := range indices {
		s.flushIndex(idx, emit)
	}
}

func (s *ToolArgSanitizer) flushIndex(idx int, emit func(core.StreamChunk)) {
	buf, ok := s.buffers[idx]
	if !ok {
		return
	}
	delete(s.buffers, idx)

	args := buf.args.String()
	if buf.kind != core.ToolCallCustom {
		if args == "" {
			args = "{}"
		}
		var object map[string]any
		if err := json.Unmarshal([]byte(args), &object); err != nil || object == nil {
			if err == nil {
				err = fmt.Errorf("arguments must be a JSON object")
			}
			s.fail(emit, "tool call %d: invalid arguments: %v", idx, err)
			return
		}
		args = sanitizeToolArgs(buf.name, args)
	}

	emit(core.StreamChunk{
		Type:             core.ChunkToolCall,
		Index:            idx,
		ToolArgumentMode: core.ToolArgumentComplete,
		ToolCall: &core.ToolCall{
			ID:        buf.id,
			Name:      buf.name,
			Arguments: json.RawMessage(args),
			Kind:      buf.kind,
		},
	})
}

// sanitizeToolArgs applies argument cleanup rules. It fixes common issues from
// non-Anthropic models after assembly has produced a valid JSON object.
func sanitizeToolArgs(toolName, argsJSON string) string {
	var sanitize func(map[string]any)
	switch toolName {
	case "Read":
		sanitize = sanitizeReadArgs
	case "Write":
		sanitize = sanitizeWriteArgs
	case "Edit":
		sanitize = sanitizeEditArgs
	case "Bash":
		sanitize = sanitizeBashArgs
	case "Glob":
		sanitize = sanitizeGlobArgs
	case "Grep":
		sanitize = sanitizeGrepArgs
	default:
		return argsJSON
	}

	var args map[string]any
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return argsJSON
	}
	sanitize(args)
	out, err := json.Marshal(args)
	if err != nil {
		return argsJSON
	}
	return string(out)
}

func sanitizeReadArgs(args map[string]any) {
	if v, ok := args["limit"]; ok {
		switch val := v.(type) {
		case string:
			if n, err := strconv.Atoi(val); err == nil {
				args["limit"] = clampInt(n, 1, 2000)
			} else {
				delete(args, "limit")
			}
		case float64:
			args["limit"] = clampInt(int(val), 1, 2000)
		}
	}
	if v, ok := args["offset"]; ok {
		switch val := v.(type) {
		case string:
			if n, err := strconv.Atoi(val); err == nil {
				args["offset"] = maxInt(n, 0)
			} else {
				delete(args, "offset")
			}
		case float64:
			args["offset"] = maxInt(int(val), 0)
		}
	}
	if pages, ok := args["pages"]; ok {
		fp, _ := args["file_path"].(string)
		if !strings.HasSuffix(strings.ToLower(fp), ".pdf") {
			delete(args, "pages")
		} else if _, ok := pages.(string); !ok {
			delete(args, "pages")
		}
	}
}

func sanitizeWriteArgs(args map[string]any) {
	if content, ok := args["content"]; ok {
		if s, ok := content.(string); !ok || s == "" {
			args["content"] = ""
		}
	}
}

func sanitizeEditArgs(args map[string]any) {
	for _, key := range []string{"old_string", "new_string"} {
		if v, ok := args[key]; ok {
			if _, ok := v.(string); !ok {
				args[key] = ""
			}
		}
	}
}

func sanitizeBashArgs(args map[string]any) {
	if cmd, ok := args["command"]; ok {
		if _, ok := cmd.(string); !ok {
			args["command"] = ""
		}
	}
}

func sanitizeGlobArgs(args map[string]any) {
	if pattern, ok := args["pattern"]; ok {
		if _, ok := pattern.(string); !ok {
			args["pattern"] = ""
		}
	}
}

func sanitizeGrepArgs(args map[string]any) {
	if query, ok := args["query"]; ok {
		if _, ok := query.(string); !ok {
			args["query"] = ""
		}
	}
}

func clampInt(val, min, max int) int {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
