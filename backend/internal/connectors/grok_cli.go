package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/transform"
)

// Grok CLI (Grok Build) client fingerprint.
// Keep in sync with xai-org/grok-build xai-grok-version.
const (
	grokCLIVersion    = "0.2.120"
	grokCLIIdentifier = "grok-shell"
	grokCLIUserAgent  = "grok-shell/0.2.120 (linux; x86_64)"
	grokCLITokenAuth  = "xai-grok-cli"
)

// grokCLIEffortSuffix matches virtual effort models: grok-4.5-high → base + effort.
var grokCLIEffortSuffix = regexp.MustCompile(`-(low|medium|high|xhigh)$`)

// grokCLISupportsEffort is true for upstream models that accept reasoning.effort.
var grokCLISupportsEffort = regexp.MustCompile(`^grok-4\.5(?:$|-)`)

// GrokCLI drives Grok Build's OpenAI Responses API on cli-chat-proxy.grok.com.
// Auth is OAuth device-code bearer; chat never sends x-xai-token-auth (validate does).
type GrokCLI struct {
	id          string
	defaultBase string
	codec       transform.OpenAIResponsesCodec
}

// NewGrokCLI builds a Grok CLI / Grok Build connector.
func NewGrokCLI(id, defaultBaseURL string) *GrokCLI {
	return &GrokCLI{id: id, defaultBase: defaultBaseURL, codec: transform.OpenAIResponsesCodec{ReasoningProvider: "grok-cli", InternalReasoningProvenance: true}}
}

func (c *GrokCLI) ID() string            { return c.id }
func (c *GrokCLI) Dialect() core.Dialect { return core.DialectOpenAIResponses }

func (c *GrokCLI) baseURL(creds core.Credentials) string {
	if creds.BaseURL != "" {
		return creds.BaseURL
	}
	return c.defaultBase
}

func (c *GrokCLI) endpoint(creds core.Credentials) string {
	base := strings.TrimRight(c.baseURL(creds), "/")
	if strings.HasSuffix(base, "/responses") {
		return base
	}
	return joinURL(base, "responses")
}

// resolveGrokCLIModel strips virtual effort suffixes and returns the upstream
// model id plus optional reasoning effort (only for grok-4.5*).
func resolveGrokCLIModel(model string) (upstream, effort string) {
	upstream = model
	if m := grokCLIEffortSuffix.FindStringSubmatch(model); len(m) == 2 {
		effort = m[1]
		upstream = strings.TrimSuffix(model, "-"+effort)
	}
	if !grokCLISupportsEffort.MatchString(upstream) {
		effort = ""
	}
	return upstream, effort
}

// chatHeaders mirrors the official Grok Build chat fingerprint.
// req may be nil (falls back to uuid session + turn 1).
func (c *GrokCLI) chatHeaders(creds core.Credentials, model string, req *core.ChatRequest) map[string]string {
	token := creds.AccessToken
	if token == "" {
		token = creds.APIKey
	}
	sessionID := resolveGrokCLISessionID(req, creds)
	turnIdx := resolveGrokCLITurnIdx(sessionID, countGrokCLIUserTurns(req))
	reqID := uuid.NewString()
	h := map[string]string{
		"Authorization":            bearer(token),
		"User-Agent":               grokCLIUserAgent,
		"x-grok-client-identifier": grokCLIIdentifier,
		"x-grok-client-version":    grokCLIVersion,
		"x-grok-session-id":        sessionID,
		"x-grok-conv-id":           sessionID,
		"x-grok-req-id":            reqID,
		"x-grok-turn-idx":          strconv.Itoa(turnIdx),
		"x-grok-model-override":    model,
		"x-xai-token-auth":         grokCLITokenAuth,
		"x-authenticateresponse":   "authenticate-response",
		"x-grok-client-mode":       "interactive",
		"x-grok-doom-loop-check":   "true",
		"x-compaction-at":          "400000",
	}
	if agentID := resolveGrokCLIAgentID(creds); agentID != "" {
		h["x-grok-agent-id"] = agentID
	}
	if email := creds.Extra["email"]; email != "" {
		h["x-email"] = email
	}
	if userID := firstNonEmpty(creds.Extra["userId"], creds.Extra["userid"], creds.Extra["user_id"]); userID != "" {
		h["x-grok-user-id"] = userID
	}
	return mergeHeaders(h, creds.Headers)
}

// validateHeaders are used for GET /models (and similar probes). Includes
// x-xai-token-auth which chat must never send.
func (c *GrokCLI) validateHeaders(creds core.Credentials) map[string]string {
	token := creds.AccessToken
	if token == "" {
		token = creds.APIKey
	}
	h := map[string]string{
		"Authorization":            bearer(token),
		"Accept":                   "application/json",
		"User-Agent":               grokCLIUserAgent,
		"x-xai-token-auth":         grokCLITokenAuth,
		"x-grok-client-version":    grokCLIVersion,
		"x-grok-client-identifier": grokCLIIdentifier,
		"x-grok-client-mode":       "headless",
	}
	if email := creds.Extra["email"]; email != "" {
		h["x-email"] = email
	}
	if userID := firstNonEmpty(creds.Extra["userId"], creds.Extra["userid"], creds.Extra["user_id"]); userID != "" {
		h["x-grok-user-id"] = userID
	}
	return mergeHeaders(h, creds.Headers)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// patchBody applies Grok CLI post-processing on a codec-rendered Responses body.
// Does not modify the global OpenAIResponsesCodec.
func (c *GrokCLI) patchBody(body []byte, model, effort string) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	m["model"] = model
	m["stream"] = true
	m["store"] = false

	reasoning, _ := m["reasoning"].(map[string]any)
	if reasoning == nil {
		reasoning = map[string]any{}
	}
	if _, ok := reasoning["summary"]; !ok {
		reasoning["summary"] = "concise"
	}
	if effort != "" {
		reasoning["effort"] = effort
	} else {
		delete(reasoning, "effort")
	}
	m["reasoning"] = reasoning

	// Encrypted reasoning continuity (CLI always requests this when reasoning is present).
	if effort != "none" {
		include := asStringSlice(m["include"])
		if !containsString(include, "reasoning.encrypted_content") {
			include = append(include, "reasoning.encrypted_content")
		}
		m["include"] = include
	}

	// 9router parity: drop item_reference / bare server-id strings from input.
	stripGrokCLIItemReferences(m)
	// Canonical history may originate from another Responses provider. Grok
	// rejects foreign encrypted reasoning, and custom tools must use function
	// call/output wire items.
	normalizeGrokCLIInput(m)
	// Flatten Chat Completions nested tools → Responses flat shape.
	flattenGrokCLITools(m)

	// Allowlist strip — keep parity with cli-chat-proxy Responses surface.
	for k := range m {
		if !grokCLIBodyAllowlist[k] {
			delete(m, k)
		}
	}
	return json.Marshal(m)
}

// stripGrokCLIItemReferences removes store/item_reference entries that
// cli-chat-proxy rejects when store=false (9router stripStoredItemReferences).
func stripGrokCLIItemReferences(m map[string]any) {
	raw, ok := m["input"].([]any)
	if !ok {
		return
	}
	out := make([]any, 0, len(raw))
	for _, item := range raw {
		switch v := item.(type) {
		case string:
			// Bare server ids (msg_… / rs_…) are not valid input items.
			if strings.HasPrefix(v, "msg_") || strings.HasPrefix(v, "rs_") || strings.HasPrefix(v, "fc_") {
				continue
			}
			out = append(out, item)
		case map[string]any:
			if t, _ := v["type"].(string); t == "item_reference" {
				continue
			}
			out = append(out, item)
		default:
			out = append(out, item)
		}
	}
	m["input"] = out
}

func stringifyGrokCLIValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

func normalizeGrokCLIInput(m map[string]any) {
	raw, ok := m["input"].([]any)
	if !ok {
		return
	}
	normalized := make([]any, 0, len(raw))
	callIDs := make(map[string]bool)
	for _, item := range raw {
		v, ok := item.(map[string]any)
		if !ok {
			normalized = append(normalized, item)
			continue
		}
		delete(v, "internal_chat_message_metadata_passthrough")
		typ, _ := v["type"].(string)
		switch typ {
		case "reasoning":
			provider, _ := v["signature_provider"].(string)
			id, _ := v["id"].(string)
			encrypted, _ := v["encrypted_content"].(string)
			if provider != "grok-cli" || id == "" || encrypted == "" {
				continue
			}
			v = map[string]any{"type": "reasoning", "id": id, "encrypted_content": encrypted}
		case "custom_tool_call":
			callID, _ := firstToolString(v["call_id"], v["id"])
			name, _ := v["name"].(string)
			if callID == "" || strings.TrimSpace(name) == "" {
				continue
			}
			arg := stringifyGrokCLIValue(firstToolAny(v["input"], v["arguments"]))
			v = map[string]any{"type": "function_call", "call_id": callID, "name": strings.TrimSpace(name), "arguments": stringifyGrokCLIValue(map[string]any{"input": arg})}
			callIDs[callID] = true
		case "function_call":
			callID, _ := firstToolString(v["call_id"], v["id"])
			name, _ := v["name"].(string)
			if callID == "" || strings.TrimSpace(name) == "" {
				continue
			}
			v["call_id"] = callID
			v["name"] = strings.TrimSpace(name)
			v["arguments"] = stringifyGrokCLIValue(v["arguments"])
			delete(v, "id")
			callIDs[callID] = true
		case "custom_tool_call_output", "function_call_output":
			callID, _ := firstToolString(v["call_id"], v["id"])
			if callID == "" {
				continue
			}
			v = map[string]any{"type": "function_call_output", "call_id": callID, "output": stringifyGrokCLIValue(v["output"])}
		}
		normalized = append(normalized, v)
	}
	out := normalized[:0]
	for _, item := range normalized {
		if v, ok := item.(map[string]any); ok && v["type"] == "function_call_output" {
			id, _ := v["call_id"].(string)
			if !callIDs[id] {
				continue
			}
		}
		out = append(out, item)
	}
	m["input"] = out
}

// grokCLIHostedToolTypes is the explicit allowlist of Responses hosted tool
// types passed through unchanged (no server-side execution).
var grokCLIHostedToolTypes = map[string]bool{
	"web_search":         true,
	"x_search":           true,
	"web_search_preview": true,
	"file_search":        true,
	"image_generation":   true,
	"code_interpreter":   true,
	"mcp":                true,
	"local_shell":        true,
}

const grokCLIMaxToolNameLen = 128

// flattenGrokCLITools converts nested Chat Completions tool shape
// ({type:"function", function:{name,description,parameters}}) into Responses
// flat shape ({type,name,description,parameters}). Hosted tools in
// grokCLIHostedToolTypes pass through; unknown non-function types are dropped.
// Function tool names are capped at 128 characters.
func flattenGrokCLITools(m map[string]any) {
	raw, ok := m["tools"].([]any)
	if !ok || len(raw) == 0 {
		return
	}
	out := make([]any, 0, len(raw))
	for _, item := range raw {
		tm, ok := item.(map[string]any)
		if !ok {
			continue
		}
		typ, _ := tm["type"].(string)
		fn, hasFn := tm["function"].(map[string]any)
		if hasFn {
			flat := map[string]any{"type": "function"}
			if typ != "" {
				flat["type"] = typ
			}
			if name, ok := firstToolString(tm["name"], fn["name"]); ok {
				flat["name"] = truncateGrokCLIToolName(name)
			}
			if desc, ok := firstToolString(tm["description"], fn["description"]); ok {
				flat["description"] = desc
			}
			if params := firstToolAny(tm["parameters"], fn["parameters"]); params != nil {
				flat["parameters"] = params
			}
			out = append(out, flat)
			continue
		}
		// Hosted tools pass through as-is (cli-chat-proxy executes them).
		if grokCLIHostedToolTypes[typ] {
			out = append(out, tm)
			continue
		}
		// Grok does not accept custom/freeform declarations. Mirror 9router by
		// converting them into a single-string function contract.
		if typ == "custom" || typ == "freeform" {
			name, _ := tm["name"].(string)
			if strings.TrimSpace(name) == "" {
				continue
			}
			out = append(out, map[string]any{
				"type": "function", "name": truncateGrokCLIToolName(strings.TrimSpace(name)), "description": tm["description"],
				"parameters": map[string]any{"type": "object", "properties": map[string]any{"input": map[string]any{"type": "string"}}, "required": []any{"input"}},
			})
			continue
		}
		// Already-flat function: keep, cap name.
		if typ == "function" || typ == "" {
			if name, ok := tm["name"].(string); ok && name != "" {
				tm["name"] = truncateGrokCLIToolName(name)
			}
			if typ == "" {
				tm["type"] = "function"
			}
			out = append(out, tm)
			continue
		}
		// Unknown non-function type — drop.
	}
	m["tools"] = out
}

func truncateGrokCLIToolName(name string) string {
	if len(name) <= grokCLIMaxToolNameLen {
		return name
	}
	return name[:grokCLIMaxToolNameLen]
}

func firstToolString(a, b any) (string, bool) {
	if s, ok := a.(string); ok && s != "" {
		return s, true
	}
	if s, ok := b.(string); ok && s != "" {
		return s, true
	}
	return "", false
}

func firstToolAny(a, b any) any {
	if a != nil {
		return a
	}
	return b
}

// Fields accepted by cli-chat-proxy Responses API (Codex allowlist + Grok extras).
var grokCLIBodyAllowlist = map[string]bool{
	"model":               true,
	"input":               true,
	"instructions":        true,
	"tools":               true,
	"tool_choice":         true,
	"stream":              true,
	"store":               true,
	"reasoning":           true,
	"include":             true,
	"temperature":         true,
	"top_p":               true,
	"max_output_tokens":   true,
	"parallel_tool_calls": true,
	"text":                true,
	"metadata":            true,
	"prompt_cache_key":    true,
}

func asStringSlice(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, e := range x {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func containsString(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

// Validate probes GET /models. HTTP 402 (spending limit) is treated as success:
// the token is accepted; credits may be exhausted (soft-402 for OAuth account create).
func (c *GrokCLI) Validate(ctx context.Context, creds core.Credentials) error {
	if creds.APIKey == "" && creds.AccessToken == "" {
		return fmt.Errorf("validation failed for %s: no API key or access token", c.id)
	}
	base := strings.TrimRight(c.baseURL(creds), "/")
	base = strings.TrimSuffix(base, "/responses")
	modelsURL := base + "/models"

	_, err := doJSONMethod(ctx, http.MethodGet, c.id, "validate", modelsURL, nil, c.validateHeaders(creds))
	if err != nil {
		if pe := core.AsProviderError(err); pe != nil && pe.StatusCode == http.StatusPaymentRequired {
			return nil
		}
		return fmt.Errorf("validation failed for %s: %w", c.id, err)
	}
	return nil
}

// Chat forces streaming upstream (Grok CLI requires stream=true) and aggregates
// into a unary ChatResponse for non-stream clients.
func (c *GrokCLI) Chat(ctx context.Context, req *core.ChatRequest, creds core.Credentials) (*core.ChatResponse, error) {
	stream, err := c.Stream(ctx, req, creds, core.StreamConfig{})
	if err != nil {
		return nil, err
	}
	return drainStreamToResponse(stream, req.Model)
}

// StreamRaw opens a streaming SSE connection and returns the raw response body.
func (c *GrokCLI) StreamRaw(ctx context.Context, req *core.ChatRequest, creds core.Credentials, cfg core.StreamConfig) (io.ReadCloser, http.Header, error) {
	body, model, err := c.renderBody(req)
	if err != nil {
		return nil, nil, err
	}
	resp, err := openStream(ctx, c.id, model, c.endpoint(creds), body, c.chatHeaders(creds, model, req))
	if err != nil {
		return nil, nil, err
	}
	return resp.Body, resp.Header, nil
}

// Stream performs a streaming Responses call against cli-chat-proxy.
func (c *GrokCLI) Stream(ctx context.Context, req *core.ChatRequest, creds core.Credentials, cfg core.StreamConfig) (<-chan core.StreamChunk, error) {
	body, model, err := c.renderBody(req)
	if err != nil {
		return nil, err
	}

	resp, err := openStream(ctx, c.id, model, c.endpoint(creds), body, c.chatHeaders(creds, model, req))
	if err != nil {
		return nil, err
	}

	out := make(chan core.StreamChunk, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()

		ttft := newTTFTTracker(cfg)
		terminalSeen := false

		scanner := sseScanner(resp.Body)
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			payload, ok := parseSSEData(scanner.Text())
			if !ok {
				continue
			}
			chunks, perr := c.codec.ParseStreamLine([]byte(payload), model)
			if perr != nil {
				if isResponsesTerminalPayload(payload) {
					terminalSeen = true
				}
				continue
			}
			if isResponsesTerminalPayload(payload) {
				terminalSeen = true
			}
			for _, ch := range chunks {
				if ch.Type == core.ChunkThinking && ch.Signature != "" {
					ch.SignatureProvider = "grok-cli"
				}
				ttft.maybeReport(ch)
				select {
				case out <- ch:
				case <-ctx.Done():
					return
				}
			}
		}
		if err := scanner.Err(); err != nil {
			terminalSeen = true
			sendStreamError(ctx, out, core.ErrTimeout, c.id, model, err)
		}
		if !terminalSeen {
			sendStreamError(ctx, out, core.ErrResponseIntegrity, c.id, model, errors.New("provider stream ended without a terminal event"))
		}
	}()
	return out, nil
}

func (c *GrokCLI) renderBody(req *core.ChatRequest) ([]byte, string, error) {
	upstream, effort := resolveGrokCLIModel(req.Model)
	// Work on a shallow copy so callers keep the virtual model id.
	r := *req
	r.Model = upstream
	r.Stream = true
	body, err := c.codec.RenderRequest(&r)
	if err != nil {
		return nil, "", &core.ProviderError{Kind: core.ErrInternal, Provider: c.id, Model: req.Model, Message: err.Error(), Cause: err}
	}
	body, err = c.patchBody(body, upstream, effort)
	if err != nil {
		return nil, "", &core.ProviderError{Kind: core.ErrInternal, Provider: c.id, Model: req.Model, Message: err.Error(), Cause: err}
	}
	return body, upstream, nil
}
