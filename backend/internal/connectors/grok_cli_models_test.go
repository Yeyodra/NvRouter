package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/stretchr/testify/require"
)

func TestParseGrokCLIModels_Envelopes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{
			name: "openai data",
			raw:  `{"data":[{"id":"grok-build","name":"Grok Build"},{"id":"grok-4.5"}]}`,
			want: []string{"grok-build", "grok-4.5"},
		},
		{
			name: "models key",
			raw:  `{"models":[{"model_id":"grok-build","display_name":"Grok Build"}]}`,
			want: []string{"grok-build"},
		},
		{
			name: "bare array of strings",
			raw:  `["grok-build","grok-4.5"]`,
			want: []string{"grok-build", "grok-4.5"},
		},
		{
			name: "object map",
			raw:  `{"grok-build":{"display_name":"Grok Build"},"grok-4.5":{}}`,
			want: []string{"grok-build", "grok-4.5"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseGrokCLIModels([]byte(tc.raw))
			require.NoError(t, err)
			ids := make([]string, len(got))
			for i, m := range got {
				ids[i] = m.ID
			}
			for _, w := range tc.want {
				require.Contains(t, ids, w)
			}
		})
	}
}

func TestMergeGrokCLIModels_KeepsStatic(t *testing.T) {
	live := []ModelSpec{{ID: "grok-build", Name: "grok-build", Kind: core.ServiceLLM}}
	static := ModelsForProvider("grok-cli")
	require.NotEmpty(t, static)

	merged := mergeGrokCLIModels(live, static)
	ids := map[string]bool{}
	for _, m := range merged {
		ids[m.ID] = true
	}
	require.True(t, ids["grok-build"])
	require.True(t, ids["grok-4.5"])
	require.True(t, ids["grok-4.5-high"], "static effort virtuals must remain")
	// Prefer static display name when live only echoed id.
	for _, m := range merged {
		if m.ID == "grok-build" {
			require.Equal(t, "Grok Build", m.Name)
		}
	}
}

func TestGrokCLIModelSource_ListModels_200(t *testing.T) {
	var sawAuth, sawTokenAuth, sawUA bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/models", r.URL.Path)
		if r.Header.Get("Authorization") == "Bearer tok" {
			sawAuth = true
		}
		if r.Header.Get("x-xai-token-auth") == grokCLITokenAuth {
			sawTokenAuth = true
		}
		if r.Header.Get("User-Agent") == grokCLIUserAgent {
			sawUA = true
		}
		require.Equal(t, grokCLIVersion, r.Header.Get("x-grok-client-version"))
		require.Equal(t, grokCLIIdentifier, r.Header.Get("x-grok-client-identifier"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]string{
				{"id": "grok-build", "name": "Grok Build"},
				{"id": "live-only-model"},
			},
		})
	}))
	t.Cleanup(srv.Close)

	gc := NewGrokCLI("grok-cli", srv.URL+"/v1")
	src := NewGrokCLIModelSource(gc)
	models, err := src.ListModels(context.Background(), core.Credentials{AccessToken: "tok"})
	require.NoError(t, err)
	require.True(t, sawAuth && sawTokenAuth && sawUA)

	ids := map[string]bool{}
	for _, m := range models {
		ids[m.ID] = true
	}
	require.True(t, ids["grok-build"])
	require.True(t, ids["live-only-model"])
	require.True(t, ids["grok-4.5-high"], "static catalog merged on success")
}

func TestGrokCLIModelSource_ListModels_500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	gc := NewGrokCLI("grok-cli", srv.URL+"/v1")
	src := NewGrokCLIModelSource(gc)
	_, err := src.ListModels(context.Background(), core.Credentials{AccessToken: "tok"})
	require.Error(t, err)

	// Soft fallback: static catalog still usable without live discovery.
	static := ModelsForProvider("grok-cli")
	require.NotEmpty(t, static)
	var hasBuild bool
	for _, m := range static {
		if m.ID == "grok-build" {
			hasBuild = true
		}
	}
	require.True(t, hasBuild)
}

func TestGrokCLIModelSource_ListModels_NetworkError(t *testing.T) {
	gc := NewGrokCLI("grok-cli", "http://127.0.0.1:1") // closed port
	src := NewGrokCLIModelSource(gc)
	_, err := src.ListModels(context.Background(), core.Credentials{AccessToken: "tok"})
	require.Error(t, err)
	// No panic; static still available.
	require.NotEmpty(t, ModelsForProvider("grok-cli"))
}

func TestRegistry_GrokCLI_LiveModelSource(t *testing.T) {
	_ = DefaultRegistry() // populates liveModelSources
	src := GetLiveModelSource("grok-cli")
	require.NotNil(t, src)
	_, ok := src.(*GrokCLIModelSource)
	require.True(t, ok)
}
