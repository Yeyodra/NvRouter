package transform

import (
	"bytes"
	"fmt"
	"strings"

	json "github.com/mydisha/keirouter/backend/internal/fastjson"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// AnthropicCodec handles the Anthropic Messages wire format (/v1/messages).
type AnthropicCodec struct{}

func (AnthropicCodec) Dialect() core.Dialect { return core.DialectAnthropic }

// ---- wire types -------------------------------------------------------------

type antRequest struct {
	Model      string          `json:"model"`
	System     json.RawMessage `json:"system,omitempty"`
	Messages   []antMessage    `json:"messages"`
	Tools      []antTool       `json:"tools,omitempty"`
	ToolChoice json.RawMessage `json:"tool_choice,omitempty"`
	MaxTokens  int             `json:"max_tokens"`
	Stream     bool            `json:"stream,omitempty"`
	Temp       *float64        `json:"temperature,omitempty"`
	TopP       *float64        `json:"top_p,omitempty"`
	Stop       []string        `json:"stop_sequences,omitempty"`
	// Thinking carries the extended-thinking configuration that clients like
	// Claude Code send. It must be forwarded to Anthropic-compatible upstreams
	// (e.g. GLM, Zhipu) or the model will not emit reasoning blocks, confusing
	// clients that expect them.
	Thinking json.RawMessage `json:"thinking,omitempty"`
}

type antMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type antBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	// Thinking holds the reasoning content for thinking blocks. Anthropic uses
	// "thinking" as the JSON key (not "text") for this block type.
	Thinking  string          `json:"thinking,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
	Source    *antImageSource `json:"source,omitempty"`
	// Signature is the cryptographic proof tag for thinking blocks that must be
	// echoed back to the upstream on the next turn. Only the originating provider's
	// signatures are valid; foreign ones (from combo-mixed models) are rejected.
	Signature string `json:"signature,omitempty"`
}

type antImageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

type antTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
}

// ---- request parsing --------------------------------------------------------

func (AnthropicCodec) ParseRequest(body []byte) (*core.ChatRequest, error) {
	var raw antRequest
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("anthropic: parse request: %w", err)
	}

	maxTokens := raw.MaxTokens
	req := &core.ChatRequest{
		Model:       raw.Model,
		System:      decodeAntSystem(raw.System),
		Temperature: raw.Temp,
		TopP:        raw.TopP,
		Stop:        raw.Stop,
		Stream:      raw.Stream,
	}
	if maxTokens > 0 {
		req.MaxTokens = &maxTokens
	}

	for _, t := range raw.Tools {
		req.Tools = append(req.Tools, core.Tool{
			Name:        t.Name,
			Description: t.Description,
			Parameters:  t.InputSchema,
		})
	}
	req.ToolChoice = claudeToolChoiceToOpenAI(raw.ToolChoice)

	for _, m := range raw.Messages {
		req.Messages = append(req.Messages, parseAntMessage(m))
	}

	// Parse thinking configuration from the raw body (before unmarshaling)
	req.Reasoning = parseAntThinkingFromBytes(body)

	return req, nil
}

// parseAntThinkingFromBytes extracts thinking configuration from raw JSON bytes.
// This is called before the antRequest struct is unmarshaled to capture the
// thinking field which is not part of the antRequest struct.
func parseAntThinkingFromBytes(body []byte) *core.ReasoningConfig {
	var thinkingWrapper struct {
		Thinking *struct {
			Type         string `json:"type"`
			BudgetTokens int    `json:"budget_tokens,omitempty"`
		} `json:"thinking"`
	}
	if err := json.Unmarshal(body, &thinkingWrapper); err == nil && thinkingWrapper.Thinking != nil {
		cfg := &core.ReasoningConfig{}
		switch thinkingWrapper.Thinking.Type {
		case "enabled":
			cfg.Effort = "high"
			if thinkingWrapper.Thinking.BudgetTokens > 0 {
				cfg.MaxTokens = thinkingWrapper.Thinking.BudgetTokens
			}
		case "adaptive":
			cfg.Effort = "adaptive"
		}
		return cfg
	}
	return nil
}

func decodeAntSystem(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []antBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return ""
	}
	var out string
	for _, b := range blocks {
		if b.Type == "text" {
			out += b.Text
		}
	}
	return out
}

func parseAntMessage(m antMessage) core.Message {
	msg := core.Message{Role: mapAntRole(m.Role)}

	// Content may be a plain string or an array of blocks.
	var s string
	if err := json.Unmarshal(m.Content, &s); err == nil {
		msg.Content = append(msg.Content, core.ContentPart{Type: core.PartText, Text: s})
		return msg
	}

	var blocks []antBlock
	if err := json.Unmarshal(m.Content, &blocks); err != nil {
		return msg
	}
	for _, b := range blocks {
		switch b.Type {
		case "text":
			msg.Content = append(msg.Content, core.ContentPart{Type: core.PartText, Text: b.Text})
		case "thinking":
			// Anthropic thinking blocks carry content in the "thinking" field
			// (not "text"). Signature must be preserved for echoing back on
			// follow-up turns — the upstream validates it.
			msg.Content = append(msg.Content, core.ContentPart{
				Type:      core.PartThinking,
				Text:      b.Thinking,
				Signature: b.Signature,
			})
		case "tool_use":
			msg.Content = append(msg.Content, core.ContentPart{
				Type:     core.PartToolCall,
				ToolCall: &core.ToolCall{ID: b.ID, Name: b.Name, Arguments: b.Input},
			})
		case "tool_result":
			msg.Content = append(msg.Content, core.ContentPart{
				Type: core.PartToolResult,
				ToolResult: &core.ToolResult{
					CallID:  b.ToolUseID,
					Content: decodeAntToolResultContent(b.Content),
					IsError: b.IsError,
				},
			})
		case "image":
			if b.Source != nil {
				if b.Source.Type == "url" && b.Source.URL != "" {
					msg.Content = append(msg.Content, core.ContentPart{
						Type:  core.PartImage,
						Media: &core.MediaPayload{URL: b.Source.URL},
					})
				} else {
					msg.Content = append(msg.Content, core.ContentPart{
						Type:  core.PartImage,
						Media: &core.MediaPayload{MIMEType: b.Source.MediaType, Data: b.Source.Data},
					})
				}
			}
		}
	}
	return msg
}

func decodeAntToolResultContent(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var blocks []antBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return string(raw)
	}
	var out string
	for _, b := range blocks {
		if b.Type == "text" {
			out += b.Text
		}
	}
	return out
}

func mapAntRole(role string) core.Role {
	switch role {
	case "assistant":
		return core.RoleAssistant
	default:
		return core.RoleUser
	}
}

// ---- request rendering ------------------------------------------------------

// antDefaultMaxOutput is the conservative output ceiling for Claude models.
const antDefaultMaxOutput = 64000

func (AnthropicCodec) RenderRequest(req *core.ChatRequest) ([]byte, error) {
	maxTokens := 4096
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		maxTokens = *req.MaxTokens
	}
	thinkingBudget := 0

	// Reconcile max_tokens against thinking budget. Anthropic requires
	// max_tokens strictly greater than budget_tokens (else 400). Prefer raising
	// max_tokens to preserve the requested thinking depth; if the budget alone
	// meets/exceeds the ceiling, cap output and shrink the budget so some tokens
	// remain for the answer.
	ceiling := antDefaultMaxOutput
	if req.Reasoning != nil && req.Reasoning.MaxTokens > 0 {
		thinkingBudget = req.Reasoning.MaxTokens
		if thinkingBudget >= maxTokens {
			// Raise max_tokens to preserve thinking depth (up to ceiling)
			maxTokens = min(thinkingBudget+1024, ceiling)
			if thinkingBudget >= maxTokens {
				// Budget exceeds ceiling; shrink budget so 1024 tokens remain for answer
				// Note: We don't mutate req.Reasoning, the reconciliation happens at render time
				thinkingBudget = max(1024, maxTokens-1024)
			}
		}
	}

	out := antRequest{
		Model:     req.Model,
		MaxTokens: maxTokens,
		Stream:    req.Stream,
		Temp:      req.Temperature,
		TopP:      req.TopP,
		Stop:      req.Stop,
	}
	// claude-opus-4 deprecated temperature and returns a 400 when it is present.
	if modelRejectsTemperature(req.Model) {
		out.Temp = nil
	}

	// Forward extended-thinking configuration to Anthropic-compatible upstreams.
	// Claude Code sends thinking: {type: "enabled", budget_tokens: N} and expects
	// reasoning blocks in the response. Dropping this field causes the upstream
	// (GLM, Zhipu, etc.) to skip reasoning, which confuses clients and may
	// trigger retries.
	if req.Reasoning != nil {
		effort := strings.ToLower(strings.TrimSpace(req.Reasoning.Effort))
		var thinking map[string]any
		switch effort {
		case "adaptive", "auto":
			thinking = map[string]any{"type": "adaptive"}
		case "", "none", "off", "disabled":
			if thinkingBudget > 0 {
				thinking = map[string]any{"type": "enabled"}
			}
		default:
			thinking = map[string]any{"type": "enabled"}
		}
		if thinking != nil && thinking["type"] == "enabled" && thinkingBudget > 0 {
			thinking["budget_tokens"] = thinkingBudget
		}
		if thinking != nil {
			if raw, err := json.Marshal(thinking); err == nil {
				out.Thinking = raw
			}
		}
	}

	if req.System != "" {
		sys, _ := json.Marshal(req.System)
		out.System = sys
	}

	for _, t := range req.Tools {
		out.Tools = append(out.Tools, antTool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.Parameters,
		})
	}

	// Render tool_choice only when tools are declared; Anthropic rejects a
	// tool_choice with an empty tools array.
	if len(out.Tools) > 0 {
		if tc := openAIToolChoiceToClaude(req.ToolChoice); tc != nil {
			if raw, err := json.Marshal(tc); err == nil {
				out.ToolChoice = raw
			}
		}
	}

	// Anthropic requires alternating user/assistant roles and groups tool
	// results into user messages. We render each canonical message to a block
	// array; consecutive same-role messages are merged.
	for _, m := range req.Messages {
		blocks := renderAntBlocks(m)
		raw, _ := json.Marshal(blocks)
		role := "user"
		if m.Role == core.RoleAssistant {
			role = "assistant"
		}
		out.Messages = appendAntMessage(out.Messages, role, raw, blocks)
	}

	return json.Marshal(out)
}

func renderAntBlocks(m core.Message) []antBlock {
	var blocks []antBlock
	for _, p := range m.Content {
		switch p.Type {
		case core.PartText:
			if p.Text == "" {
				continue
			}
			blocks = append(blocks, antBlock{Type: "text", Text: p.Text})
		case core.PartThinking:
			// Render thinking with the correct "thinking" JSON key and echo back
			// the signature when present (required by upstream for validation).
			// When no signature exists (e.g. thinking synthesized by a non-Anthropic
			// upstream), omit it — some providers reject a null/empty signature.
			block := antBlock{Type: "thinking", Thinking: p.Text}
			if p.Signature != "" {
				block.Signature = p.Signature
			}
			blocks = append(blocks, block)
		case core.PartToolCall:
			blocks = append(blocks, antBlock{
				Type:  "tool_use",
				ID:    p.ToolCall.ID,
				Name:  p.ToolCall.Name,
				Input: normalizeAntToolInputRaw(p.ToolCall.Arguments),
			})
		case core.PartToolResult:
			content, _ := json.Marshal(p.ToolResult.Content)
			blocks = append(blocks, antBlock{
				Type:      "tool_result",
				ToolUseID: p.ToolResult.CallID,
				Content:   content,
				IsError:   p.ToolResult.IsError,
			})
		case core.PartImage:
			if p.Media != nil {
				if p.Media.Data != "" {
					blocks = append(blocks, antBlock{
						Type:   "image",
						Source: &antImageSource{Type: "base64", MediaType: p.Media.MIMEType, Data: p.Media.Data},
					})
				} else if p.Media.URL != "" {
					blocks = append(blocks, antBlock{
						Type:   "image",
						Source: &antImageSource{Type: "url", URL: p.Media.URL},
					})
				}
			}
		}
	}
	if len(blocks) == 0 {
		blocks = append(blocks, antBlock{Type: "text", Text: " "})
	}
	return blocks
}

// appendAntMessage merges blocks into the previous message when roles match, as
// Anthropic forbids consecutive messages with the same role.
func appendAntMessage(msgs []antMessage, role string, raw json.RawMessage, blocks []antBlock) []antMessage {
	if n := len(msgs); n > 0 && msgs[n-1].Role == role {
		var prev []antBlock
		_ = json.Unmarshal(msgs[n-1].Content, &prev)
		prev = append(prev, blocks...)
		merged, _ := json.Marshal(prev)
		msgs[n-1].Content = merged
		return msgs
	}
	return append(msgs, antMessage{Role: role, Content: raw})
}

func normalizeAntToolInputRaw(raw json.RawMessage) json.RawMessage {
	if antToolInputIsObject(raw) {
		return raw
	}
	return json.RawMessage(`{}`)
}

func normalizeAntToolInputValue(raw json.RawMessage) any {
	raw = normalizeAntToolInputRaw(raw)
	var input map[string]any
	if err := json.Unmarshal(raw, &input); err != nil {
		return map[string]any{}
	}
	return input
}

func antToolInputIsObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || !json.Valid(raw) {
		return false
	}
	return raw[0] == '{'
}

// ---- response parsing -------------------------------------------------------

type antResponse struct {
	ID         string     `json:"id"`
	Model      string     `json:"model"`
	Content    []antBlock `json:"content"`
	StopReason string     `json:"stop_reason"`
	Usage      struct {
		InputTokens              int `json:"input_tokens"`
		OutputTokens             int `json:"output_tokens"`
		CacheReadInputTokens     int `json:"cache_read_input_tokens"`
		CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	} `json:"usage"`
}

func (AnthropicCodec) ParseResponse(body []byte, model string) (*core.ChatResponse, error) {
	var raw antResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("anthropic: parse response: %w", err)
	}

	msg := core.Message{Role: core.RoleAssistant}
	for _, b := range raw.Content {
		switch b.Type {
		case "text":
			msg.Content = append(msg.Content, core.ContentPart{Type: core.PartText, Text: b.Text})
		case "thinking":
			msg.Content = append(msg.Content, core.ContentPart{Type: core.PartThinking, Text: b.Thinking, Signature: b.Signature})
		case "tool_use":
			msg.Content = append(msg.Content, core.ContentPart{
				Type:     core.PartToolCall,
				ToolCall: &core.ToolCall{ID: b.ID, Name: b.Name, Arguments: b.Input},
			})
		}
	}

	return &core.ChatResponse{
		ID:           raw.ID,
		Model:        firstNonEmpty(raw.Model, model),
		Message:      msg,
		FinishReason: mapAntStop(raw.StopReason),
		Usage: core.Usage{
			PromptTokens:     raw.Usage.InputTokens + raw.Usage.CacheReadInputTokens + raw.Usage.CacheCreationInputTokens,
			CompletionTokens: raw.Usage.OutputTokens,
			TotalTokens: raw.Usage.InputTokens + raw.Usage.CacheReadInputTokens +
				raw.Usage.CacheCreationInputTokens + raw.Usage.OutputTokens,
			CachedTokens:     raw.Usage.CacheReadInputTokens,
			CacheWriteTokens: raw.Usage.CacheCreationInputTokens,
			Source:           core.UsageSourceProvider,
		},
	}, nil
}

func (AnthropicCodec) RenderResponse(resp *core.ChatResponse) ([]byte, error) {
	var content []map[string]any
	for _, p := range resp.Message.Content {
		switch p.Type {
		case core.PartText:
			content = append(content, map[string]any{"type": "text", "text": p.Text})
		case core.PartThinking:
			block := map[string]any{"type": "thinking", "thinking": p.Text}
			if p.Signature != "" {
				block["signature"] = p.Signature
			}
			content = append(content, block)
		case core.PartToolCall:
			content = append(content, map[string]any{
				"type": "tool_use", "id": p.ToolCall.ID, "name": p.ToolCall.Name, "input": normalizeAntToolInputValue(p.ToolCall.Arguments),
			})
		}
	}
	out := map[string]any{
		"id":          firstNonEmpty(resp.ID, "msg_"+resp.Model),
		"type":        "message",
		"role":        "assistant",
		"model":       resp.Model,
		"content":     content,
		"stop_reason": renderAntStop(resp.FinishReason),
		"usage": map[string]int{
			"input_tokens":                anthropicInputTokens(resp.Usage),
			"output_tokens":               resp.Usage.CompletionTokens,
			"cache_read_input_tokens":     resp.Usage.CachedTokens,
			"cache_creation_input_tokens": resp.Usage.CacheWriteTokens,
		},
	}
	return json.Marshal(out)
}

// Canonical PromptTokens includes cache reads and writes; Anthropic's
// input_tokens field excludes the separately reported cache token classes.
func anthropicInputTokens(u core.Usage) int {
	input := u.PromptTokens - u.CachedTokens - u.CacheWriteTokens
	if input < 0 {
		return 0
	}
	return input
}

func mapAntStop(r string) core.FinishReason {
	switch r {
	case "end_turn", "stop_sequence":
		return core.FinishStop
	case "max_tokens":
		return core.FinishLength
	case "tool_use":
		return core.FinishToolCalls
	default:
		return core.FinishStop
	}
}

func renderAntStop(r core.FinishReason) string {
	switch r {
	case core.FinishLength:
		return "max_tokens"
	case core.FinishToolCalls:
		return "tool_use"
	default:
		return "end_turn"
	}
}
