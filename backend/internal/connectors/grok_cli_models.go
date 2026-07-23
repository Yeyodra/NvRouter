package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// GrokCLIModelSource implements LiveModelSource for Grok CLI / Grok Build.
// GET {base}/models with probe headers (including x-xai-token-auth).
// Flexible envelope: data | models | results | bare array | object map.
// Merges with the static providerModels["grok-cli"] catalog on success.
//
// Context defaults (500k/64k for grok-build) from 9router are N/A: ModelSpec
// has no context/max-output fields (Task 4).
type GrokCLIModelSource struct {
	connector *GrokCLI
}

// NewGrokCLIModelSource builds a live model source backed by GrokCLI probe headers.
func NewGrokCLIModelSource(conn *GrokCLI) *GrokCLIModelSource {
	return &GrokCLIModelSource{connector: conn}
}

// ListModels fetches GET /models and returns live + static merged ModelSpecs.
// On network/HTTP/parse failure the caller (gateway) soft-skips; static catalog
// remains usable via ModelsForProvider without live discovery.
func (s *GrokCLIModelSource) ListModels(ctx context.Context, creds core.Credentials) ([]ModelSpec, error) {
	if s == nil || s.connector == nil {
		return nil, fmt.Errorf("grok-cli: ListModels: nil source")
	}
	if creds.APIKey == "" && creds.AccessToken == "" {
		return nil, fmt.Errorf("grok-cli: ListModels: no API key or access token")
	}

	base := strings.TrimRight(s.connector.baseURL(creds), "/")
	base = strings.TrimSuffix(base, "/responses")
	url := base + "/models"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range s.connector.validateHeaders(creds) {
		req.Header.Set(k, v)
	}

	resp, err := sharedClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("grok-cli: ListModels: read: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("grok-cli: GET /models returned %d: %s", resp.StatusCode, truncateError(body))
	}

	live, err := parseGrokCLIModels(body)
	if err != nil {
		return nil, err
	}
	return mergeGrokCLIModels(live, ModelsForProvider(s.connector.ID())), nil
}

// parseGrokCLIModels mirrors 9router parseGrokCliModels: flexible envelope.
func parseGrokCLIModels(raw []byte) ([]ModelSpec, error) {
	var top any
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, fmt.Errorf("grok-cli: decode /models: %w", err)
	}

	entries := grokCLIModelEntries(top)
	seen := make(map[string]struct{}, len(entries))
	out := make([]ModelSpec, 0, len(entries))

	for _, e := range entries {
		id, name := grokCLIEntryIDName(e)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, ModelSpec{ID: id, Name: name, Kind: core.ServiceLLM})
	}
	return out, nil
}

// grokCLIModelEntries extracts list items from data/models/results/array/map.
func grokCLIModelEntries(data any) []any {
	switch v := data.(type) {
	case []any:
		return v
	case map[string]any:
		for _, key := range []string{"data", "models", "results"} {
			if nested, ok := v[key]; ok {
				return grokCLIModelEntries(nested)
			}
		}
		// Object map of id → entry.
		out := make([]any, 0, len(v))
		for k, val := range v {
			switch item := val.(type) {
			case map[string]any:
				if _, has := item["id"]; !has {
					item["id"] = k
				}
				out = append(out, item)
			case string:
				out = append(out, map[string]any{"id": item})
			default:
				out = append(out, map[string]any{"id": k})
			}
		}
		return out
	default:
		return nil
	}
}

func grokCLIEntryIDName(raw any) (id, name string) {
	switch item := raw.(type) {
	case string:
		id = strings.TrimSpace(item)
		return id, id
	case map[string]any:
		id = firstString(item, "id", "model_id", "modelId", "model", "slug", "name")
		name = firstString(item, "display_name", "displayName", "name")
		if name == "" {
			name = id
		}
		return strings.TrimSpace(id), strings.TrimSpace(name)
	default:
		return "", ""
	}
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

// mergeGrokCLIModels: live models first (prefer live name), then static-only ids.
func mergeGrokCLIModels(live, static []ModelSpec) []ModelSpec {
	if len(live) == 0 {
		return static
	}
	if len(static) == 0 {
		return live
	}
	seen := make(map[string]struct{}, len(live)+len(static))
	out := make([]ModelSpec, 0, len(live)+len(static))
	for _, m := range live {
		if m.ID == "" {
			continue
		}
		if _, ok := seen[m.ID]; ok {
			continue
		}
		// Prefer static display name when live only echoed the id.
		if m.Name == "" || m.Name == m.ID {
			for _, s := range static {
				if s.ID == m.ID && s.Name != "" {
					m.Name = s.Name
					break
				}
			}
		}
		if m.Name == "" {
			m.Name = m.ID
		}
		if m.Kind == "" {
			m.Kind = core.ServiceLLM
		}
		seen[m.ID] = struct{}{}
		out = append(out, m)
	}
	for _, m := range static {
		if m.ID == "" {
			continue
		}
		if _, ok := seen[m.ID]; ok {
			continue
		}
		seen[m.ID] = struct{}{}
		out = append(out, m)
	}
	return out
}
