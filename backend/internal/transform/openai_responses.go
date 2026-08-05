package transform

import (
	"fmt"
	"strings"

	json "github.com/mydisha/keirouter/backend/internal/fastjson"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// OpenAIResponsesCodec handles OpenAI's Responses API wire format (/v1/responses),
// the dialect spoken by Codex and Responses-native clients. The Responses API
// differs from Chat Completions: turns live under "input" as typed items
// (message / function_call / function_call_output / reasoning), the system
// prompt is carried as "instructions", tools are flat
// ({type,name,description,parameters}), and the streaming surface is a rich
// event sequence (response.created → output_item.added → output_text.delta →
// ... → response.completed) rather than uniform chat.completion.chunk deltas.
type OpenAIResponsesCodec struct {
	// Codex tailors the rendered request to the ChatGPT Codex backend
	// (reasoning summary, encrypted-content include, default instructions).
	// It is set by the codex connector; the zero value renders the plain
	// OpenAI Responses API shape.
	Codex bool
	// ReasoningProvider permits opaque reasoning replay only for matching
	// router-authenticated provenance.
	ReasoningProvider string
	// InternalReasoningProvenance carries the trusted provider marker only to a
	// provider-specific post-render hook; direct wire codecs must leave it false.
	InternalReasoningProvenance bool
}

func (OpenAIResponsesCodec) Dialect() core.Dialect { return core.DialectOpenAIResponses }

// maxCallIDLen is the maximum length of a call_id accepted by the Responses API.
// Call IDs longer than 64 characters cause 400 errors.
const maxCallIDLen = 64

func clampCallID(id string) string {
	if len(id) > maxCallIDLen {
		return id[:maxCallIDLen]
	}
	return id
}

// ---- wire types -------------------------------------------------------------

type respRequest struct {
	Model        string          `json:"model"`
	Input        json.RawMessage `json:"input"`
	Instructions string          `json:"instructions,omitempty"`
	Tools        []respTool      `json:"tools,omitempty"`
	Stream       bool            `json:"stream"`
	Store        bool            `json:"store"`
	// Chat Completions parameters accepted on inbound parse for graceful
	// passthrough through the canonical model, but never rendered outbound —
	// the Responses API rejects these with 400 "Unsupported parameter".
	Temperature *float64 `json:"temperature,omitempty"`
	MaxTokens   *int     `json:"max_tokens,omitempty"`
	TopP        *float64 `json:"top_p,omitempty"`
	// Reasoning carries the Responses-native effort knob so codex CLI style
	// clients keep their requested effort through the canonical model.
	Reasoning *struct {
		Effort string `json:"effort,omitempty"`
	} `json:"reasoning,omitempty"`
	ToolChoice any `json:"tool_choice,omitempty"`
}

// responsesAPIAllowlist enumerates the fields that the Responses API (/v1/responses)
// accepts. Any field not in this set is stripped from the outbound request to
// prevent 400 "Unsupported parameter" errors from the upstream provider. This
// guards against Chat Completions fields (max_tokens, temperature, top_p,
// frequency_penalty, stream_options, user, metadata, etc.) leaking through.
var responsesAPIAllowlist = map[string]bool{
	"model":             true,
	"input":             true,
	"instructions":      true,
	"tools":             true,
	"tool_choice":       true,
	"stream":            true,
	"store":             true,
	"reasoning":         true,
	"service_tier":      true,
	"include":           true,
	"prompt_cache_key":  true,
	"max_output_tokens": true,
	"client_metadata":   true,
}

type respTool struct {
	Type        string          `json:"type"`
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Format      json.RawMessage `json:"format,omitempty"`
	Function    *struct {
		Name        string          `json:"name"`
		Description string          `json:"description"`
		Parameters  json.RawMessage `json:"parameters"`
	} `json:"function,omitempty"`
}

type respInputItem struct {
	Type             string            `json:"type"`
	Role             string            `json:"role,omitempty"`
	Content          json.RawMessage   `json:"content,omitempty"`
	CallID           string            `json:"call_id,omitempty"`
	Name             string            `json:"name,omitempty"`
	Arguments        string            `json:"arguments,omitempty"`
	Input            string            `json:"input,omitempty"`
	Output           json.RawMessage   `json:"output,omitempty"`
	Summary          []respSummaryPart `json:"summary,omitempty"`
	EncryptedContent string            `json:"encrypted_content,omitempty"`
	ID               string            `json:"id,omitempty"`
}

type respSummaryPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type respContentPart struct {
	Type     string          `json:"type"`
	Text     string          `json:"text,omitempty"`
	ImageURL json.RawMessage `json:"image_url,omitempty"`
}

// ---- request parsing (inbound Responses client) -----------------------------

func (OpenAIResponsesCodec) ParseRequest(body []byte) (*core.ChatRequest, error) {
	var raw respRequest
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("openai-responses: parse request: %w", err)
	}

	req := &core.ChatRequest{Model: raw.Model, Stream: raw.Stream, System: raw.Instructions, ToolChoice: raw.ToolChoice}
	req.Temperature = raw.Temperature
	req.MaxTokens = raw.MaxTokens
	req.TopP = raw.TopP
	if raw.Reasoning != nil && raw.Reasoning.Effort != "" {
		req.Reasoning = &core.ReasoningConfig{Effort: raw.Reasoning.Effort}
	}

	for _, t := range raw.Tools {
		name := t.Name
		desc := t.Description
		params := t.Parameters
		if t.Function != nil {
			name = t.Function.Name
			desc = t.Function.Description
			params = t.Function.Parameters
		}
		if strings.TrimSpace(name) == "" {
			continue // hosted tools without a name can't be functions
		}
		tool := core.Tool{Name: name, Description: desc, Parameters: params}
		switch t.Type {
		case "custom":
			tool.Kind = core.ToolCustom
			tool.Format = t.Format
		case "freeform":
			tool.Kind = core.ToolFreeform
			tool.Format = t.Format
		}
		req.Tools = append(req.Tools, tool)
	}

	var items []respInputItem
	if len(raw.Input) > 0 {
		// input may be a string (plain prompt) or an array of typed items.
		if raw.Input[0] == '"' {
			var s string
			if err := json.Unmarshal(raw.Input, &s); err == nil && s != "" {
				req.Messages = append(req.Messages, core.Message{
					Role:    core.RoleUser,
					Content: []core.ContentPart{{Type: core.PartText, Text: s}},
				})
			}
			return req, nil
		}
		if err := json.Unmarshal(raw.Input, &items); err != nil {
			return nil, fmt.Errorf("openai-responses: parse input: %w", err)
		}
	}

	var pendingReasoning string
	var pendingEncrypted string
	var pendingReasoningID string
	for _, item := range items {
		itemType := item.Type
		if itemType == "" && item.Role != "" {
			itemType = "message" // Droid CLI omits type on role items
		}

		switch itemType {
		case "message":
			msg := core.Message{Role: mapRespRole(item.Role)}
			if pendingReasoning != "" || pendingEncrypted != "" {
				if item.Role == "assistant" {
					msg.Content = append(msg.Content, core.ContentPart{
						Type:              core.PartThinking,
						Text:              pendingReasoning,
						Signature:         pendingEncrypted,
						SignatureID:       pendingReasoningID,
						SignatureProvider: "responses-native",
					})
				}
				pendingReasoning = ""
				pendingEncrypted = ""
				pendingReasoningID = ""
			}
			msg.Content = append(msg.Content, parseRespContent(item.Content)...)
			req.Messages = append(req.Messages, msg)

		case "function_call", "custom_tool_call":
			if strings.TrimSpace(item.Name) == "" {
				continue
			}
			args := json.RawMessage(item.Arguments)
			kind := core.ToolCallKind("")
			if itemType == "custom_tool_call" {
				args = json.RawMessage(item.Input)
				kind = core.ToolCallCustom
			} else if len(args) == 0 {
				args = json.RawMessage("{}")
			}
			msg := core.Message{Role: core.RoleAssistant}
			if pendingReasoning != "" || pendingEncrypted != "" {
				msg.Content = append(msg.Content, core.ContentPart{
					Type:              core.PartThinking,
					Text:              pendingReasoning,
					Signature:         pendingEncrypted,
					SignatureID:       pendingReasoningID,
					SignatureProvider: "responses-native",
				})
				pendingReasoning = ""
				pendingEncrypted = ""
				pendingReasoningID = ""
			}
			msg.Content = append(msg.Content, core.ContentPart{
				Type:     core.PartToolCall,
				ToolCall: &core.ToolCall{ID: item.CallID, Name: item.Name, Arguments: args, Kind: kind},
			})
			req.Messages = append(req.Messages, msg)

		case "function_call_output":
			req.Messages = append(req.Messages, core.Message{
				Role: core.RoleTool,
				Content: []core.ContentPart{{
					Type:       core.PartToolResult,
					ToolResult: &core.ToolResult{CallID: item.CallID, Content: rawToString(item.Output)},
				}},
			})

		case "reasoning":
			txt, encrypted := extractRespReasoning(item)
			if txt != "" || encrypted != "" {
				if pendingReasoning != "" {
					pendingReasoning += "\n"
				}
				pendingReasoning += txt
				pendingEncrypted = encrypted
				pendingReasoningID = item.ID
			}
		}
	}
	return req, nil
}

func parseRespContent(raw json.RawMessage) []core.ContentPart {
	if len(raw) == 0 {
		return nil
	}
	// content may be a plain string.
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			return []core.ContentPart{{Type: core.PartText, Text: s}}
		}
		return nil
	}
	var parts []respContentPart
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil
	}
	var out []core.ContentPart
	for _, p := range parts {
		switch p.Type {
		case "input_text", "output_text", "text":
			if p.Text != "" {
				out = append(out, core.ContentPart{Type: core.PartText, Text: p.Text})
			}
		case "input_image":
			if url := respImageURL(p.ImageURL); url != "" {
				out = append(out, core.ContentPart{Type: core.PartImage, Media: &core.MediaPayload{URL: url}})
			}
		}
	}
	return out
}

func respImageURL(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '"' {
		var s string
		_ = json.Unmarshal(raw, &s)
		return s
	}
	var obj struct {
		URL string `json:"url"`
	}
	_ = json.Unmarshal(raw, &obj)
	return obj.URL
}

// extractRespReasoning returns the summary text and encrypted_content from a
// reasoning item. The encrypted_content must be echoed back on follow-up turns
// when the client requested include: ["reasoning.encrypted_content"].
func extractRespReasoning(item respInputItem) (text string, encrypted string) {
	var parts []string
	for _, s := range item.Summary {
		if s.Text != "" {
			parts = append(parts, s.Text)
		}
	}
	return strings.Join(parts, "\n"), item.EncryptedContent
}

func mapRespRole(role string) core.Role {
	switch role {
	case "assistant":
		return core.RoleAssistant
	case "system":
		return core.RoleSystem
	case "tool":
		return core.RoleTool
	case "developer":
		// OpenAI Responses API uses "developer" role for GPT-5/Codex system prompts.
		// Treat as developer role which is semantically equivalent to system.
		return core.RoleDeveloper
	default:
		return core.RoleUser
	}
}

func rawToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
	}
	return string(raw)
}

// ---- request rendering (outbound to Codex/Responses provider) ---------------

func (c OpenAIResponsesCodec) RenderRequest(req *core.ChatRequest) ([]byte, error) {
	// Catalog aliases like grok-4.5-high / gpt-5.3-codex-high are not real
	// upstream model IDs. Strip the effort suffix and emit reasoning.effort
	// instead. Explicit req.Reasoning.Effort wins over the suffix value.
	model, suffixEffort := splitResponsesEffortModel(req.Model)
	effort := suffixEffort
	if req.Reasoning != nil && strings.TrimSpace(req.Reasoning.Effort) != "" {
		effort = strings.TrimSpace(req.Reasoning.Effort)
	}

	out := map[string]any{
		"model":  model,
		"stream": req.Stream,
		"store":  false,
	}
	switch {
	case req.System != "":
		out["instructions"] = req.System
	case c.Codex:
		// The Codex backend expects non-empty instructions; a neutral default
		// keeps bare chat requests working (mirrors the codex CLI).
		out["instructions"] = "You are a ChatGPT agent."
	default:
		out["instructions"] = ""
	}
	// Reasoning effort where the client, the catalog suffix, or the codex
	// connector requested it. "Model decides" values (auto/adaptive — e.g.
	// Claude Code's adaptive thinking) have no Responses API wire value and
	// cause 400 invalid_value upstream, so they are omitted rather than
	// forwarded. The codex connector normalizes efforts before rendering, so
	// that guard only fires for non-codex Responses providers.
	effort = strings.ToLower(effort)
	if effort == "auto" || effort == "adaptive" || effort == "default" {
		effort = ""
	}
	switch {
	case c.Codex && effort != "":
		// Codex accepts "none" as a wire value; the other efforts also stream
		// reasoning summaries and need the encrypted reasoning content echoed
		// back on follow-up turns (store=false).
		reasoning := map[string]any{"effort": effort}
		if effort != "none" {
			reasoning["summary"] = "auto"
			out["include"] = []string{"reasoning.encrypted_content"}
		}
		out["reasoning"] = reasoning
	case isResponsesReasoningEffortEnabled(effort):
		out["reasoning"] = map[string]any{"effort": effort}
	}
	// NOTE: temperature, max_tokens, top_p are intentionally NOT included.
	// These are Chat Completions parameters that the Responses API does not
	// accept. Sending them causes 400 "Unsupported parameter" errors from
	// Codex and other Responses-native backends.

	var input []map[string]any
	for _, m := range req.Messages {
		switch m.Role {
		case core.RoleUser, core.RoleAssistant:
			// Emit reasoning items with encrypted_content BEFORE the
			// message/function_call items. The Codex API requires
			// reasoning items to precede the assistant turn they belong to.
			for _, p := range m.Content {
				if p.Type == core.PartThinking && (p.Text != "" || p.Signature != "") {
					reasoningItem := map[string]any{"type": "reasoning"}
					trusted := c.ReasoningProvider != "" && p.SignatureProvider == c.ReasoningProvider
					if trusted && p.SignatureID != "" {
						reasoningItem["id"] = p.SignatureID
					}
					if trusted && c.InternalReasoningProvenance {
						reasoningItem["signature_provider"] = p.SignatureProvider
					}
					if trusted && p.Signature != "" {
						reasoningItem["encrypted_content"] = p.Signature
					}
					if p.Text != "" {
						reasoningItem["summary"] = []map[string]any{
							{"type": "summary_text", "text": p.Text},
						}
					}
					input = append(input, reasoningItem)
					break // only one reasoning item per assistant turn
				}
			}

			contentType := "input_text"
			if m.Role == core.RoleAssistant {
				contentType = "output_text"
			}
			var content []map[string]any
			for _, p := range m.Content {
				switch p.Type {
				case core.PartText:
					content = append(content, map[string]any{"type": contentType, "text": p.Text})
				case core.PartImage:
					if p.Media != nil {
						url := p.Media.URL
						if url == "" && p.Media.Data != "" {
							url = "data:" + p.Media.MIMEType + ";base64," + p.Media.Data
						}
						content = append(content, map[string]any{"type": "input_image", "image_url": url, "detail": "auto"})
					}
				}
			}
			if len(content) > 0 {
				input = append(input, map[string]any{"type": "message", "role": string(m.Role), "content": content})
			}
			// Assistant tool calls become function_call items.
			for _, p := range m.Content {
				if p.Type == core.PartToolCall && p.ToolCall != nil {
					args := string(p.ToolCall.Arguments)
					if args == "" && p.ToolCall.Kind != core.ToolCallCustom {
						args = "{}"
					}
					name := p.ToolCall.Name
					if name == "" {
						name = "_unknown"
					}
					item := map[string]any{"type": "function_call", "call_id": clampCallID(p.ToolCall.ID), "name": name, "arguments": args}
					if p.ToolCall.Kind == core.ToolCallCustom {
						item["type"] = "custom_tool_call"
						delete(item, "arguments")
						item["input"] = args
					}
					input = append(input, item)
				}
			}
			// Anthropic clients (Claude Code) send tool_result blocks inside a
			// user message, not as a separate role=tool message. Responses API
			// still requires these as top-level function_call_output items.
			for _, p := range m.Content {
				if p.Type == core.PartToolResult && p.ToolResult != nil {
					input = append(input, map[string]any{
						"type":    "function_call_output",
						"call_id": clampCallID(p.ToolResult.CallID),
						"output":  p.ToolResult.Content,
					})
				}
			}

		case core.RoleTool:
			for _, p := range m.Content {
				if p.Type == core.PartToolResult && p.ToolResult != nil {
					input = append(input, map[string]any{
						"type":    "function_call_output",
						"call_id": clampCallID(p.ToolResult.CallID),
						"output":  p.ToolResult.Content,
					})
				}
			}
		}
	}
	if input == nil {
		input = []map[string]any{}
	}
	out["input"] = input

	if len(req.Tools) > 0 {
		var tools []map[string]any
		for _, t := range req.Tools {
			if t.Kind == core.ToolCustom || t.Kind == core.ToolFreeform {
				tool := map[string]any{"type": string(t.Kind), "name": t.Name, "description": t.Description}
				if len(t.Format) > 0 {
					tool["format"] = t.Format
				}
				tools = append(tools, tool)
				continue
			}
			params := t.Parameters
			if len(params) == 0 {
				params = json.RawMessage(`{"type":"object","properties":{}}`)
			}
			tools = append(tools, map[string]any{
				"type":        "function",
				"name":        t.Name,
				"description": t.Description,
				"parameters":  params,
			})
		}
		out["tools"] = tools
	}
	if len(req.Tools) > 0 && req.ToolChoice != nil {
		out["tool_choice"] = responsesToolChoice(req.ToolChoice)
	}

	// Allowlist filter: strip any field not recognized by the Responses API.
	// This prevents Chat Completions parameters or client-specific fields from
	// leaking through and causing 400 "Unsupported parameter" errors.
	for k := range out {
		if !responsesAPIAllowlist[k] {
			delete(out, k)
		}
	}

	return json.Marshal(out)
}

func responsesToolChoice(choice any) any {
	m, ok := choice.(map[string]any)
	if !ok || m["type"] != "function" {
		return choice
	}
	fn, ok := m["function"].(map[string]any)
	if !ok {
		return choice
	}
	name, _ := fn["name"].(string)
	if name == "" {
		return choice
	}
	return map[string]any{"type": "function", "name": name}
}

// splitResponsesEffortModel peels a trailing effort suffix off a synthetic
// catalog model id (e.g. "grok-4.5-high" → "grok-4.5", "high"). Longest
// suffixes are checked first so "-xhigh" wins over "-high".
func splitResponsesEffortModel(model string) (base, effort string) {
	for _, suf := range []string{"-xhigh", "-high", "-medium", "-low", "-none"} {
		if strings.HasSuffix(model, suf) && len(model) > len(suf) {
			return strings.TrimSuffix(model, suf), strings.TrimPrefix(suf, "-")
		}
	}
	return model, ""
}

func isResponsesReasoningEffortEnabled(effort string) bool {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "", "none", "off", "disable", "disabled":
		return false
	default:
		return true
	}
}

// ---- unary response parsing (from Codex/Responses provider) -----------------

type respUnary struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	IncompleteDetails *struct {
		Reason string `json:"reason"`
	} `json:"incomplete_details"`
	Output []struct {
		ID               string            `json:"id"`
		Type             string            `json:"type"`
		Role             string            `json:"role"`
		Content          []respContentPart `json:"content"`
		CallID           string            `json:"call_id"`
		Name             string            `json:"name"`
		Arguments        string            `json:"arguments"`
		Input            string            `json:"input"`
		Summary          []respSummaryPart `json:"summary"`
		EncryptedContent string            `json:"encrypted_content"`
	} `json:"output"`
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
}

func (OpenAIResponsesCodec) ParseResponse(body []byte, model string) (*core.ChatResponse, error) {
	var raw respUnary
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("openai-responses: parse response: %w", err)
	}

	msg := core.Message{Role: core.RoleAssistant}
	finish := core.FinishStop
	if raw.Status == "incomplete" {
		if raw.IncompleteDetails == nil || raw.IncompleteDetails.Reason == "" {
			return nil, fmt.Errorf("openai-responses: incomplete response missing reason")
		}
		finish = core.FinishLength
	}
	for _, item := range raw.Output {
		switch item.Type {
		case "reasoning":
			var b strings.Builder
			for _, s := range item.Summary {
				b.WriteString(s.Text)
			}
			if b.Len() > 0 || item.EncryptedContent != "" {
				msg.Content = append(msg.Content, core.ContentPart{
					Type:              core.PartThinking,
					Text:              b.String(),
					Signature:         item.EncryptedContent,
					SignatureID:       item.ID,
					SignatureProvider: "responses-native",
				})
			}
		case "message":
			for _, p := range item.Content {
				if (p.Type == "output_text" || p.Type == "text") && p.Text != "" {
					msg.Content = append(msg.Content, core.ContentPart{Type: core.PartText, Text: p.Text})
				}
			}
		case "function_call", "custom_tool_call":
			args := json.RawMessage(item.Arguments)
			kind := core.ToolCallKind("")
			if item.Type == "custom_tool_call" {
				args = json.RawMessage(item.Input)
				kind = core.ToolCallCustom
			} else if len(args) == 0 {
				args = json.RawMessage("{}")
			}
			msg.Content = append(msg.Content, core.ContentPart{
				Type:     core.PartToolCall,
				ToolCall: &core.ToolCall{ID: item.CallID, Name: item.Name, Arguments: args, Kind: kind},
			})
			finish = core.FinishToolCalls
		}
	}

	resp := &core.ChatResponse{ID: raw.ID, Model: model, Message: msg, FinishReason: finish}
	if raw.Usage != nil {
		cached := raw.Usage.InputTokensDetails.CachedTokens
		resp.Usage = core.Usage{
			PromptTokens:     raw.Usage.InputTokens,
			CompletionTokens: raw.Usage.OutputTokens,
			TotalTokens:      raw.Usage.InputTokens + raw.Usage.OutputTokens,
			CachedTokens:     cached,
			ReasoningTokens:  raw.Usage.OutputTokensDetails.ReasoningTokens,
			Source:           core.UsageSourceProvider,
		}
	}
	return resp, nil
}

// RenderResponse encodes a canonical response as a Responses API result for a
// client that speaks the Responses dialect.
func (OpenAIResponsesCodec) RenderResponse(resp *core.ChatResponse) ([]byte, error) {
	var output []map[string]any
	idx := 0
	for _, p := range resp.Message.Content {
		switch p.Type {
		case core.PartThinking:
			reasoningItem := map[string]any{
				"id":   fmt.Sprintf("rs_%s_%d", firstNonEmpty(resp.ID, "resp"), idx),
				"type": "reasoning",
			}
			if p.Signature != "" {
				reasoningItem["encrypted_content"] = p.Signature
			}
			if p.Text != "" {
				reasoningItem["summary"] = []map[string]any{{"type": "summary_text", "text": p.Text}}
			}
			output = append(output, reasoningItem)
			idx++
		case core.PartText:
			output = append(output, map[string]any{
				"id":   fmt.Sprintf("msg_%s_%d", firstNonEmpty(resp.ID, "resp"), idx),
				"type": "message", "role": "assistant",
				"content": []map[string]any{{"type": "output_text", "text": p.Text, "annotations": []any{}}},
			})
			idx++
		case core.PartToolCall:
			if p.ToolCall != nil {
				args := string(p.ToolCall.Arguments)
				if args == "" && p.ToolCall.Kind != core.ToolCallCustom {
					args = "{}"
				}
				item := map[string]any{"id": "fc_" + p.ToolCall.ID, "type": "function_call", "call_id": p.ToolCall.ID, "name": p.ToolCall.Name, "arguments": args}
				if p.ToolCall.Kind == core.ToolCallCustom {
					item["type"] = "custom_tool_call"
					delete(item, "arguments")
					item["input"] = args
				}
				output = append(output, item)
				idx++
			}
		}
	}
	out := map[string]any{
		"id":     firstNonEmpty(resp.ID, "resp_stream"),
		"object": "response",
		"status": "completed",
		"output": output,
		"usage": map[string]int{
			"input_tokens":  resp.Usage.PromptTokens,
			"output_tokens": resp.Usage.CompletionTokens,
			"total_tokens":  resp.Usage.TotalTokens,
		},
	}
	return json.Marshal(out)
}
