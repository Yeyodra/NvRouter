package connectors

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mydisha/keirouter/backend/internal/core"
	krhttp "github.com/mydisha/keirouter/backend/internal/httputil"
	"github.com/mydisha/keirouter/backend/internal/transform"
)

const (
	enterProvider = "enter-converge"
	enterUA       = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	enterImageMax = 10 << 20
)

var enterNativeModels = map[string]string{
	"gpt-5.6-sol": "openai/gpt-5.6-sol", "gpt-5.6-terra": "openai/gpt-5.6-terra", "gpt-5.6-luna": "openai/gpt-5.6-luna",
	"gpt-5.5": "openai/gpt-5.5", "gpt-5.4-pro": "openai/gpt-5.4-pro", "gpt-5.4": "openai/gpt-5.4", "gpt-5.2-pro": "openai/gpt-5.2-pro",
	"claude-opus-4.6": "anthropic/claude-opus-4.6", "claude-sonnet-4.5": "anthropic/claude-sonnet-4.5",
	// Legacy lock-only IDs remain mapped for migration/import/export compatibility;
	// they are intentionally absent from the active Enter catalog.
	"claude-opus-4.8": "anthropic/claude-opus-4.8", "claude-sonnet-5": "anthropic/claude-sonnet-5",
	"minimax-m3": "minimax/minimax-m3", "minimax-m2.7": "minimax/minimax-m2.7", "minimax-m2.5": "minimax/minimax-m2.5",
	"deepseek-v4-pro": "deepseek/deepseek-v4-pro",
	"qwen-3.7-plus":   "alibaba/qwen-3.7-plus", "qwen-3.7-max": "alibaba/qwen-3.7-max", "qwen-3.6-plus": "alibaba/qwen-3.6-plus", "qwen-3.6-max-preview": "alibaba/qwen-3.6-max-preview",
	"kimi-k3": "moonshotai/kimi-k3", "kimi-k2.7-code": "moonshotai/kimi-k2.7-code", "kimi-k2.6": "moonshotai/kimi-k2.6", "kimi-k2.5": "moonshotai/kimi-k2.5",
	"glm-5.2": "z-ai/glm-5.2", "glm-5.1": "z-ai/glm-5.1", "glm-5": "z-ai/glm-5",
}

// EnterNativeModelID converts NvRouter's canonical bare ID to Enter's wire ID.
// Already-native IDs remain accepted for migration compatibility.
func EnterNativeModelID(model string) string {
	if native, ok := enterNativeModels[model]; ok {
		return native
	}
	return model
}

var enterCanonicalModels = func() map[string]string {
	out := make(map[string]string, len(enterNativeModels))
	for canonical, native := range enterNativeModels {
		out[native] = canonical
	}
	return out
}()

// EnterCanonicalModelID converts a known Enter wire ID to its NvRouter ID.
func EnterCanonicalModelID(model string) string {
	if canonical, ok := enterCanonicalModels[model]; ok {
		return canonical
	}
	return model
}

type enterWorkspaceCacheEntry struct {
	id      string
	expires time.Time
}

// EnterConverge is Enter's API-key OpenAI-compatible transport.
type EnterConverge struct {
	base       string
	codec      transform.OpenAICodec
	cacheMu    sync.Mutex
	workspaces map[string]enterWorkspaceCacheEntry
}

func NewEnterConverge(base string) *EnterConverge {
	return &EnterConverge{base: strings.TrimRight(base, "/"), workspaces: map[string]enterWorkspaceCacheEntry{}}
}

func (c *EnterConverge) ID() string            { return enterProvider }
func (c *EnterConverge) Dialect() core.Dialect { return core.DialectOpenAI }

func (c *EnterConverge) headers(creds core.Credentials, workspace string) map[string]string {
	h := map[string]string{
		"Authorization": bearer(creds.APIKey),
		"Origin":        "https://enter.converge.ai",
		"Referer":       "https://enter.converge.ai/",
		"User-Agent":    enterUA,
		"Accept":        "application/json",
	}
	if workspace != "" {
		h["X-Workspace-ID"] = workspace
	}
	return mergeHeaders(h, creds.Headers)
}

func enterStoredWorkspace(creds core.Credentials) string {
	if v := strings.TrimSpace(creds.Extra["workspaceId"]); v != "" {
		return v
	}
	return strings.TrimSpace(creds.Extra["workspace_id"])
}

func parseEnterWorkspaceID(raw json.RawMessage) (string, error) {
	raw = json.RawMessage(strings.TrimSpace(string(raw)))
	if len(raw) == 0 || string(raw) == "null" {
		return "", fmt.Errorf("missing workspace id")
	}
	if raw[0] == '"' {
		var id string
		if err := json.Unmarshal(raw, &id); err != nil || strings.TrimSpace(id) == "" {
			return "", fmt.Errorf("invalid workspace id")
		}
		return strings.TrimSpace(id), nil
	}
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return "", fmt.Errorf("invalid workspace id: %w", err)
	}
	if _, err := n.Int64(); err != nil {
		return "", fmt.Errorf("workspace id must be an integer: %w", err)
	}
	return n.String(), nil
}

// ResolveWorkspace returns the configured or API-resolved workspace ID.
func (c *EnterConverge) ResolveWorkspace(ctx context.Context, creds core.Credentials) (string, error) {
	return c.workspace(ctx, creds)
}

func (c *EnterConverge) workspace(ctx context.Context, creds core.Credentials) (string, error) {
	if id := enterStoredWorkspace(creds); id != "" {
		return id, nil
	}
	if !strings.HasPrefix(creds.APIKey, "ek_") {
		return "", &core.ProviderError{Kind: core.ErrAuth, Scope: core.FailureScopeAccount, Provider: enterProvider, Message: "API key must start with ek_"}
	}
	c.cacheMu.Lock()
	cached, ok := c.workspaces[creds.APIKey]
	c.cacheMu.Unlock()
	if ok && cached.expires.After(time.Now()) {
		return cached.id, nil
	}
	body, err := doJSONMethod(core.WithProxy(ctx, creds), http.MethodGet, enterProvider, "workspace", joinURL(c.base, "workspaces"), nil, c.headers(creds, ""))
	if err != nil {
		return "", err
	}
	var envelope struct {
		Data struct {
			Workspaces []struct {
				ID json.RawMessage `json:"id"`
			} `json:"workspaces"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Data.Workspaces) == 0 {
		return "", &core.ProviderError{Kind: core.ErrAuth, Scope: core.FailureScopeAccount, Provider: enterProvider, Message: "no workspace available for API key", Cause: err}
	}
	id, err := parseEnterWorkspaceID(envelope.Data.Workspaces[0].ID)
	if err != nil {
		return "", &core.ProviderError{Kind: core.ErrAuth, Scope: core.FailureScopeAccount, Provider: enterProvider, Message: "workspace response contained invalid id", Cause: err}
	}
	c.cacheMu.Lock()
	c.workspaces[creds.APIKey] = enterWorkspaceCacheEntry{id: id, expires: time.Now().Add(30 * time.Minute)}
	c.cacheMu.Unlock()
	return id, nil
}

func cloneEnterRequest(req *core.ChatRequest) *core.ChatRequest {
	clone := *req
	clone.Reasoning = nil
	clone.Temperature = nil
	clone.TopP = nil
	if len(req.Extra) > 0 {
		clone.Extra = make(map[string]json.RawMessage, len(req.Extra))
		for k, v := range req.Extra {
			clone.Extra[k] = v
		}
		for _, k := range []string{"top_k", "temperature", "top_p", "min_p", "typical_p", "repetition_penalty", "frequency_penalty", "presence_penalty", "reasoning_effort", "thinking", "reasoning"} {
			delete(clone.Extra, k)
		}
	}
	clone.Messages = append([]core.Message(nil), req.Messages...)
	for i := range clone.Messages {
		clone.Messages[i].Content = append([]core.ContentPart(nil), req.Messages[i].Content...)
		for j := range clone.Messages[i].Content {
			if media := clone.Messages[i].Content[j].Media; media != nil {
				copy := *media
				clone.Messages[i].Content[j].Media = &copy
			}
		}
	}
	return &clone
}

func (c *EnterConverge) prepare(ctx context.Context, req *core.ChatRequest, creds core.Credentials, stream bool) ([]byte, string, error) {
	workspace, err := c.workspace(ctx, creds)
	if err != nil {
		return nil, "", err
	}
	clone := cloneEnterRequest(req)
	clone.Model = EnterNativeModelID(req.Model)
	clone.Stream = stream
	if err := c.prefetchImages(ctx, clone, creds); err != nil {
		return nil, "", &core.ProviderError{Kind: core.ErrBadRequest, Scope: core.FailureScopeRequest, Provider: enterProvider, Model: req.Model, Message: err.Error(), Cause: err}
	}
	body, err := c.codec.RenderRequestForProvider(clone, enterProvider)
	if err != nil {
		return nil, "", err
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, "", err
	}
	for _, k := range []string{"top_k", "temperature", "top_p", "min_p", "typical_p", "repetition_penalty", "frequency_penalty", "presence_penalty", "reasoning_effort", "thinking", "reasoning"} {
		delete(payload, k)
	}
	if strings.Contains(strings.ToLower(req.Model), "/gpt-") || strings.HasPrefix(strings.ToLower(req.Model), "gpt-") {
		if v, ok := payload["max_tokens"]; ok {
			payload["max_completion_tokens"] = v
			delete(payload, "max_tokens")
		}
	}
	body, err = json.Marshal(payload)
	return body, workspace, err
}

func (c *EnterConverge) prefetchImages(ctx context.Context, req *core.ChatRequest, creds core.Credentials) error {
	for mi := range req.Messages {
		for pi := range req.Messages[mi].Content {
			media := req.Messages[mi].Content[pi].Media
			if media == nil || media.URL == "" {
				continue
			}
			if err := krhttp.ValidateOutboundURL(media.URL); err != nil {
				return fmt.Errorf("image URL: %w", err)
			}
			fetchCtx, cancel := context.WithTimeout(core.WithProxy(ctx, creds), 10*time.Second)
			request, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, media.URL, nil)
			if err != nil {
				cancel()
				return err
			}
			proxyRewrite(fetchCtx, request)
			client := *proxyClient(fetchCtx)
			if creds.ProxyURL == "" && creds.RelayURL == "" {
				transport := sharedClient.Transport.(*http.Transport).Clone()
				transport.DialContext = krhttp.StrictSafeDialContext
				client.Transport = transport
			}
			client.CheckRedirect = func(next *http.Request, _ []*http.Request) error {
				return krhttp.ValidateOutboundURL(next.URL.String())
			}
			resp, err := client.Do(request)
			if err != nil {
				cancel()
				return fmt.Errorf("fetch image: %w", err)
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				resp.Body.Close()
				cancel()
				return fmt.Errorf("fetch image returned HTTP %d", resp.StatusCode)
			}
			ct, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
			if !strings.HasPrefix(ct, "image/") {
				resp.Body.Close()
				cancel()
				return fmt.Errorf("remote image content type %q is not an image", ct)
			}
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, enterImageMax+1))
			resp.Body.Close()
			cancel()
			if readErr != nil {
				return fmt.Errorf("read image: %w", readErr)
			}
			if len(data) > enterImageMax {
				return fmt.Errorf("remote image exceeds %d bytes", enterImageMax)
			}
			media.MIMEType, media.Data, media.URL = ct, base64.StdEncoding.EncodeToString(data), ""
		}
	}
	return nil
}

func enterError(err error, model string) error {
	pe := core.AsProviderError(err)
	if pe.StatusCode == http.StatusPaymentRequired {
		pe.Kind = core.ErrQuotaExhausted
		pe.Scope = core.FailureScopeModel
		pe.Model = model
		pe.CreditsExhausted = false
		pe.RetryAfter = 10 * 365 * 24 * time.Hour
	}
	return pe
}

func waitEnterRetry(ctx context.Context, attempt int) error {
	t := time.NewTimer(time.Duration(attempt) * 100 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *EnterConverge) Chat(ctx context.Context, req *core.ChatRequest, creds core.Credentials) (*core.ChatResponse, error) {
	body, workspace, err := c.prepare(ctx, req, creds, false)
	if err != nil {
		return nil, err
	}
	var response []byte
	for attempt := 1; attempt <= 3; attempt++ {
		response, err = doJSON(core.WithProxy(ctx, creds), enterProvider, req.Model, joinURL(c.base, "chat/completions"), body, c.headers(creds, workspace))
		if err == nil || core.AsProviderError(err).StatusCode != http.StatusBadGateway || attempt == 3 {
			break
		}
		if err = waitEnterRetry(ctx, attempt); err != nil {
			break
		}
	}
	if err != nil {
		return nil, enterError(err, req.Model)
	}
	resp, err := c.codec.ParseResponse(response, req.Model)
	if resp != nil {
		resp.Model = req.Model
	}
	return resp, err
}

func (c *EnterConverge) openStream(ctx context.Context, req *core.ChatRequest, creds core.Credentials) (*http.Response, error) {
	body, workspace, err := c.prepare(ctx, req, creds, true)
	if err != nil {
		return nil, err
	}
	for attempt := 1; attempt <= 3; attempt++ {
		resp, openErr := openStreamWithClient(core.WithProxy(ctx, creds), enterProvider, req.Model, joinURL(c.base, "chat/completions"), body, c.headers(creds, workspace), clientFor(creds))
		if openErr == nil || core.AsProviderError(openErr).StatusCode != http.StatusBadGateway || attempt == 3 {
			if openErr != nil {
				return nil, enterError(openErr, req.Model)
			}
			return resp, nil
		}
		if err := waitEnterRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, err
}

func (c *EnterConverge) Stream(ctx context.Context, req *core.ChatRequest, creds core.Credentials, cfg core.StreamConfig) (<-chan core.StreamChunk, error) {
	resp, err := c.openStream(ctx, req, creds)
	if err != nil {
		return nil, err
	}
	return scanOpenAISSE(ctx, enterProvider, req.Model, resp, c.codec, cfg), nil
}

func (c *EnterConverge) StreamRaw(ctx context.Context, req *core.ChatRequest, creds core.Credentials, _ core.StreamConfig) (io.ReadCloser, http.Header, error) {
	resp, err := c.openStream(ctx, req, creds)
	if err != nil {
		return nil, nil, err
	}
	return resp.Body, resp.Header, nil
}

func (c *EnterConverge) Validate(ctx context.Context, creds core.Credentials) error {
	workspace, err := c.workspace(ctx, creds)
	if err != nil {
		return err
	}
	_, err = doJSONMethod(core.WithProxy(ctx, creds), http.MethodGet, enterProvider, "validate", joinURL(c.base, "ai-capability/models"), nil, c.headers(creds, workspace))
	return err
}

func (c *EnterConverge) ListModels(ctx context.Context, creds core.Credentials) ([]ModelSpec, error) {
	workspace, err := c.workspace(ctx, creds)
	if err != nil {
		return nil, err
	}
	body, err := doJSONMethod(core.WithProxy(ctx, creds), http.MethodGet, enterProvider, "models", joinURL(c.base, "ai-capability/models"), nil, c.headers(creds, workspace))
	if err != nil {
		return nil, err
	}
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	allowed := map[string]ModelSpec{}
	for _, m := range ModelsForProvider(enterProvider) {
		allowed[EnterNativeModelID(m.ID)] = m
	}
	seen := map[string]bool{}
	var out []ModelSpec
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case []any:
			for _, item := range x {
				walk(item)
			}
		case map[string]any:
			id, _ := x["id"].(string)
			if spec, ok := allowed[id]; ok && !seen[id] {
				seen[id], out = true, append(out, spec)
			}
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(raw)
	return out, nil
}

func (c *EnterConverge) FetchQuota(ctx context.Context, creds core.Credentials) (*QuotaResult, error) {
	workspace, err := c.workspace(ctx, creds)
	if err != nil {
		return nil, err
	}
	base := joinURL(c.base, "workspaces/"+workspace)
	body, err := doJSONMethod(core.WithProxy(ctx, creds), http.MethodGet, enterProvider, "quota", base+"/credits/dashboard", nil, c.headers(creds, workspace))
	if err != nil {
		body, err = doJSONMethod(core.WithProxy(ctx, creds), http.MethodGet, enterProvider, "quota", base+"/credits", nil, c.headers(creds, workspace))
		if err != nil {
			return nil, err
		}
	}
	var envelope map[string]any
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	data, _ := envelope["data"].(map[string]any)
	balance, _ := data["credits_balance"].(map[string]any)
	remaining, found := numberValue(balance["total"])
	if !found {
		remaining, found = numberValue(data["credits"])
	}
	if !found {
		return &QuotaResult{Message: "Enter Converge connected, but credits payload was empty."}, nil
	}
	plan := "Unknown"
	limit := remaining
	if sub, subErr := doJSONMethod(core.WithProxy(ctx, creds), http.MethodGet, enterProvider, "quota", base+"/subscription/status", nil, c.headers(creds, workspace)); subErr == nil {
		var e map[string]any
		if json.Unmarshal(sub, &e) == nil {
			if d, ok := e["data"].(map[string]any); ok {
				status, _ := d["status"].(map[string]any)
				if status == nil {
					status = d
				}
				entitlement, _ := status["entitlement"].(map[string]any)
				if p := firstNonemptyString(status["plan_type"], entitlement["plan_type"], entitlement["name"]); p != "" {
					plan = p
				}
				if daily, ok := numberValue(entitlement["daily_credits"]); ok && daily > limit {
					limit = daily
				}
			}
		}
	}
	if limit < remaining {
		limit = remaining
	}
	return &QuotaResult{PlanName: plan, Quotas: []QuotaEntry{{ResourceType: "Credits", Used: int(limit - remaining), Limit: int(limit), Remaining: int(remaining), PlanName: plan}}}, nil
}

func numberValue(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

func firstNonemptyString(values ...any) string {
	for _, value := range values {
		if s, ok := value.(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}
