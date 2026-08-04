package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/mydisha/keirouter/backend/internal/core"
)

const (
	taskletProvider  = "tasklet"
	taskletTimeout   = 2 * time.Minute
	taskletReadLimit = 4 << 20
)

type Tasklet struct{ base string }

func NewTasklet(base string) *Tasklet    { return &Tasklet{base: strings.TrimRight(base, "/")} }
func (c *Tasklet) ID() string            { return taskletProvider }
func (c *Tasklet) Dialect() core.Dialect { return core.DialectOpenAI }
func (c *Tasklet) headers(token string) map[string]string {
	return map[string]string{"Authorization": bearer(token)}
}

func (c *Tasklet) Chat(ctx context.Context, req *core.ChatRequest, creds core.Credentials) (*core.ChatResponse, error) {
	stream, err := c.Stream(ctx, req, creds, core.StreamConfig{})
	if err != nil {
		return nil, err
	}
	var text, thinking string
	for chunk := range stream {
		switch chunk.Type {
		case core.ChunkText:
			text += chunk.Delta
		case core.ChunkThinking:
			thinking += chunk.Delta
		case core.ChunkError:
			return nil, chunk.Err
		}
	}
	parts := make([]core.ContentPart, 0, 2)
	if thinking != "" {
		parts = append(parts, core.ContentPart{Type: core.PartThinking, Text: thinking})
	}
	parts = append(parts, core.ContentPart{Type: core.PartText, Text: text})
	return &core.ChatResponse{
		ID: "tasklet", Model: req.Model,
		Message:      core.Message{Role: core.RoleAssistant, Content: parts},
		FinishReason: core.FinishStop,
	}, nil
}

func (c *Tasklet) Stream(ctx context.Context, req *core.ChatRequest, creds core.Credentials, cfg core.StreamConfig) (<-chan core.StreamChunk, error) {
	if req == nil || len(req.Messages) == 0 {
		return nil, taskletError(core.ErrBadRequest, core.FailureScopeRequest, "", http.StatusBadRequest, "messages are required", nil)
	}
	token := strings.TrimSpace(creds.APIKey)
	if token == "" {
		return nil, taskletError(core.ErrAuth, core.FailureScopeAccount, req.Model, http.StatusUnauthorized, "sessionToken API key is required", nil)
	}
	message := flattenTaskletMessages(req)
	if strings.TrimSpace(message) == "" {
		return nil, taskletError(core.ErrBadRequest, core.FailureScopeRequest, req.Model, http.StatusBadRequest, "message has no supported text", nil)
	}

	ctx, cancel := boundedTaskletContext(ctx)
	payload := map[string]any{
		"agentId": "new", "message": message, "timezone": "America/Los_Angeles", "fileIds": []string{}, "intelligence": "advanced",
		"modelConfig": map[string]any{"model": taskletModelID(req.Model), "thinkingEffort": "low", "chatHistory": "default", "serviceTier": "standard", "preset": "basic"},
		"agentConfig": map[string]any{"preview": true}, "workspaceId": creds.Extra["workspace_id"],
	}
	body, _ := json.Marshal(payload)
	response, err := doJSON(core.WithProxy(ctx, creds), taskletProvider, req.Model, joinURL(c.base, "api/sendChatMessage"), body, c.headers(token))
	if err != nil {
		cancel()
		return nil, taskletClassify(err, req.Model)
	}
	var envelope struct {
		AgentID string `json:"agentId"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil || envelope.AgentID == "" {
		cancel()
		if err == nil {
			err = errors.New("missing agentId")
		}
		return nil, taskletError(core.ErrResponseIntegrity, core.FailureScopeProvider, req.Model, http.StatusBadGateway, "invalid sendChatMessage response: "+err.Error(), err)
	}

	ws, _, err := c.dial(ctx, creds)
	if err != nil {
		cancel()
		return nil, taskletPostCreateError(transportError(ctx, taskletProvider, req.Model, err), req.Model)
	}
	ws.SetReadLimit(taskletReadLimit)
	if err := writeTaskletWS(ctx, ws, map[string]any{"type": "connect", "sessionToken": token}); err != nil {
		ws.CloseNow()
		cancel()
		return nil, taskletPostCreateError(transportError(ctx, taskletProvider, req.Model, err), req.Model)
	}

	out := make(chan core.StreamChunk, 16)
	go c.readStream(ctx, cancel, ws, envelope.AgentID, req.Model, cfg, out)
	return out, nil
}

func (c *Tasklet) readStream(ctx context.Context, cancel context.CancelFunc, ws *websocket.Conn, agentID, model string, cfg core.StreamConfig, out chan<- core.StreamChunk) {
	defer close(out)
	defer cancel()
	defer ws.CloseNow()
	var lastText, lastThinking string
	ttft := newTTFTTracker(cfg)
	connected := false
	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				emitTaskletError(ctx, out, taskletClassify(transportError(ctx, taskletProvider, model, err), model))
				return
			}
			status := websocket.CloseStatus(err)
			if status == websocket.StatusNormalClosure {
				emitTaskletError(ctx, out, taskletError(core.ErrResponseIntegrity, core.FailureScopeProvider, model, 0, "WebSocket closed before sync became idle", err))
			} else {
				emitTaskletError(ctx, out, taskletError(core.ErrUpstream, core.FailureScopeNetwork, model, 0, "WebSocket read failed: "+err.Error(), err))
			}
			return
		}
		var event taskletEvent
		if err := json.Unmarshal(data, &event); err != nil {
			emitTaskletError(ctx, out, taskletError(core.ErrResponseIntegrity, core.FailureScopeProvider, model, 0, "invalid WebSocket event", err))
			return
		}
		switch event.Type {
		case "connected":
			if connected {
				continue
			}
			connected = true
			if err := writeTaskletWS(ctx, ws, map[string]any{"type": "startSync", "agentId": agentID}); err != nil {
				emitTaskletError(ctx, out, taskletClassify(err, model))
				return
			}
			if err := writeTaskletWS(ctx, ws, map[string]any{"type": "subscribeBlocks", "runId": agentID, "pageSize": 50}); err != nil {
				emitTaskletError(ctx, out, taskletClassify(err, model))
				return
			}
		case "error":
			emitTaskletError(ctx, out, taskletSignalError(model, event.Error))
			return
		case "blocksUpdate":
			for _, block := range event.Updates {
				var chunk core.StreamChunk
				switch block.Type {
				case "agent_content":
					chunk = core.StreamChunk{Type: core.ChunkText, Delta: snapshotDelta(lastText, block.Content)}
					lastText = block.Content
				case "thinking":
					chunk = core.StreamChunk{Type: core.ChunkThinking, Delta: snapshotDelta(lastThinking, block.Content)}
					lastThinking = block.Content
				default:
					continue
				}
				if chunk.Delta != "" {
					ttft.maybeReport(chunk)
					if !sendTaskletChunk(ctx, out, chunk) {
						return
					}
				}
			}
		case "syncUpdate":
			switch event.State.RunState.Type {
			case "idle":
				sendTaskletChunk(ctx, out, core.StreamChunk{Type: core.ChunkFinish, FinishReason: core.FinishStop})
				return
			case "error":
				emitTaskletError(ctx, out, taskletSignalError(model, event.State.RunState.Error))
				return
			}
		}
	}
}

type taskletEvent struct {
	Type    string `json:"type"`
	Error   string `json:"error"`
	Updates map[string]struct {
		Type    string `json:"type"`
		Content string `json:"content"`
	} `json:"updates"`
	State struct {
		RunState struct {
			Type  string `json:"type"`
			Error string `json:"error"`
		} `json:"runState"`
	} `json:"state"`
}

func (c *Tasklet) dial(ctx context.Context, creds core.Credentials) (*websocket.Conn, *http.Response, error) {
	endpoint := joinURL(c.base, "api/sync")
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, nil, err
	}
	origin := u.Scheme + "://" + u.Host
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	opts := &websocket.DialOptions{HTTPClient: clientFor(creds)}
	if creds.RelayURL != "" {
		path := u.RequestURI()
		relay, parseErr := url.Parse(creds.RelayURL)
		if parseErr != nil {
			return nil, nil, parseErr
		}
		if relay.Scheme == "https" {
			relay.Scheme = "wss"
		} else {
			relay.Scheme = "ws"
		}
		u = relay
		opts.HTTPHeader = http.Header{"x-relay-target": []string{origin}, "x-relay-path": []string{path}}
	}
	return websocket.Dial(ctx, u.String(), opts)
}

func flattenTaskletMessages(req *core.ChatRequest) string {
	parts := make([]string, 0, len(req.Messages)+1)
	if strings.TrimSpace(req.System) != "" {
		parts = append(parts, "[System]: "+req.System)
	}
	for _, msg := range req.Messages {
		text := msg.TextContent()
		switch msg.Role {
		case core.RoleSystem, core.RoleDeveloper:
			if strings.TrimSpace(text) != "" {
				parts = append(parts, "[System]: "+text)
			}
		case core.RoleAssistant:
			if strings.TrimSpace(text) != "" {
				parts = append(parts, "[Assistant]: "+text)
			}
			for _, p := range msg.Content {
				if p.Type == core.PartToolCall && p.ToolCall != nil {
					parts = append(parts, "[Tool Call: "+defaultString(p.ToolCall.Name, "unknown")+"]")
				}
			}
		case core.RoleTool:
			name, content := msg.Name, text
			for _, p := range msg.Content {
				if p.Type == core.PartToolResult && p.ToolResult != nil {
					name, content = p.ToolResult.CallID, p.ToolResult.Content
					break
				}
			}
			if name == "" {
				name = "tool"
			}
			if len(content) > 3000 {
				content = content[:3000] + "...(truncated)"
			}
			parts = append(parts, "[Tool Result ("+name+")]: "+content)
		default:
			if strings.TrimSpace(text) != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n\n")
}

func taskletModelID(model string) string {
	return strings.NewReplacer(".", "_", "-", "_").Replace(model)
}
func snapshotDelta(previous, current string) string {
	if strings.HasPrefix(current, previous) {
		return current[len(previous):]
	}
	return current
}
func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
func writeTaskletWS(ctx context.Context, ws *websocket.Conn, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return ws.Write(ctx, websocket.MessageText, b)
}
func sendTaskletChunk(ctx context.Context, out chan<- core.StreamChunk, chunk core.StreamChunk) bool {
	select {
	case out <- chunk:
		return true
	case <-ctx.Done():
		return false
	}
}
func emitTaskletError(_ context.Context, out chan<- core.StreamChunk, err error) {
	// Terminal errors are best-effort but must remain observable after the
	// request context is canceled. The stream channel is buffered, so enqueue
	// when capacity remains and never block if a stalled consumer filled it.
	select {
	case out <- core.StreamChunk{Type: core.ChunkError, Err: err}:
	default:
	}
}
func boundedTaskletContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, taskletTimeout)
}

func taskletSignalError(model, message string) error {
	lower := strings.ToLower(message)
	if strings.Contains(lower, "quota") || strings.Contains(lower, "credit") || strings.Contains(lower, "balance") {
		return taskletError(core.ErrQuotaExhausted, core.FailureScopeAccount, model, 0, defaultString(message, "Tasklet credits exhausted"), nil)
	}
	if strings.Contains(lower, "model") && (strings.Contains(lower, "unavailable") || strings.Contains(lower, "not found") || strings.Contains(lower, "unsupported")) {
		return taskletError(core.ErrModelUnavailable, core.FailureScopeModel, model, 0, message, nil)
	}
	return taskletError(core.ErrUpstream, core.FailureScopeProvider, model, 0, defaultString(message, "Tasklet sync error"), nil)
}

func taskletClassify(err error, model string) error {
	if err == nil {
		return nil
	}
	pe := core.AsProviderError(err)
	copy := *pe
	copy.Provider, copy.Model = taskletProvider, model
	return &copy
}
func taskletPostCreateError(err error, model string) error {
	pe := core.AsProviderError(taskletClassify(err, model))
	copy := *pe
	copy.Scope = core.FailureScopeRequest
	copy.NonReplayable = true
	return &copy
}
func taskletError(kind core.ErrorKind, scope core.FailureScope, model string, status int, message string, cause error) error {
	return &core.ProviderError{Kind: kind, Scope: scope, Provider: taskletProvider, Model: model, StatusCode: status, Message: message, Cause: cause}
}

func (c *Tasklet) Validate(ctx context.Context, creds core.Credentials) error {
	if strings.TrimSpace(creds.APIKey) == "" {
		return taskletError(core.ErrAuth, core.FailureScopeAccount, "validate", http.StatusUnauthorized, "sessionToken API key is required", nil)
	}
	_, err := doJSONMethod(core.WithProxy(ctx, creds), http.MethodPost, taskletProvider, "validate", joinURL(c.base, "api/profile"), []byte("null"), c.headers(creds.APIKey))
	return taskletClassify(err, "validate")
}

func (c *Tasklet) FetchQuota(ctx context.Context, creds core.Credentials) (*QuotaResult, error) {
	organizationID := strings.TrimSpace(creds.Extra["organization_id"])
	if organizationID == "" {
		return &QuotaResult{Message: "Tasklet connected, but organizationId metadata is missing."}, nil
	}
	body, _ := json.Marshal(map[string]string{"organizationId": organizationID})
	response, err := doJSON(core.WithProxy(ctx, creds), taskletProvider, "quota", joinURL(c.base, "api/billing/creditGrants"), body, c.headers(creds.APIKey))
	if err != nil {
		return nil, taskletClassify(err, "quota")
	}
	var envelope struct {
		Grants []struct {
			Amount     *float64        `json:"amount"`
			Consumed   *float64        `json:"consumed"`
			Type       string          `json:"type"`
			Expiration json.RawMessage `json:"expiration"`
		} `json:"grants"`
	}
	if err := json.Unmarshal(response, &envelope); err != nil {
		return nil, fmt.Errorf("decode Tasklet credit grants: %w", err)
	}
	if len(envelope.Grants) == 0 {
		return &QuotaResult{Message: "Tasklet connected. No credit grants found."}, nil
	}
	result := &QuotaResult{}
	for _, grant := range envelope.Grants {
		if grant.Amount == nil || grant.Consumed == nil {
			return nil, errors.New("Tasklet credit grant missing amount or consumed")
		}
		limit, used := int(*grant.Amount), int(*grant.Consumed)
		remaining := limit - used
		if remaining < 0 {
			remaining = 0
		}
		result.Quotas = append(result.Quotas, QuotaEntry{ResourceType: defaultString(grant.Type, "credits"), Used: used, Limit: limit, Remaining: remaining, ResetAt: taskletExpiration(grant.Expiration)})
	}
	return result, nil
}

func taskletExpiration(raw json.RawMessage) string {
	text := strings.TrimSpace(string(raw))
	if text == "" || text == "null" {
		return ""
	}
	if strings.HasPrefix(text, `"`) {
		var value string
		if json.Unmarshal(raw, &value) == nil {
			return value
		}
		return ""
	}
	stamp, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return ""
	}
	if stamp > 10_000_000_000 {
		stamp /= 1000
	}
	return time.Unix(stamp, 0).UTC().Format(time.RFC3339)
}
