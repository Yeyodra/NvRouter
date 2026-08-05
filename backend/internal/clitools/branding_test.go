package clitools

import (
	"os"
	"path/filepath"
	"testing"

	toml "github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"
)

func TestConfiguratorUsesNvRouterDisplayNamesAndKeepsCompatibilityIDs(t *testing.T) {
	home := t.TempDir()

	require.NoError(t, (&CodexTool{}).Configure(home, "https://novela.biz.id", "test-key", []string{"public-model"}))
	raw, err := os.ReadFile(filepath.Join(home, ".codex", "config.toml"))
	require.NoError(t, err)
	var codex map[string]any
	require.NoError(t, toml.Unmarshal(raw, &codex))
	require.Equal(t, "keirouter", codex["model_provider"])
	providers := codex["model_providers"].(map[string]any)
	require.Equal(t, "NvRouter", providers["keirouter"].(map[string]any)["name"])

	require.NoError(t, (&DroidTool{}).Configure(home, "https://novela.biz.id", "test-key", nil))
	var droid map[string]any
	require.NoError(t, readJSON(filepath.Join(home, ".factory", "settings.json"), &droid))
	model := droid["customModels"].([]any)[0].(map[string]any)
	require.Equal(t, "custom:KeiRouter-0", model["id"])
	require.Equal(t, "gpt-4o (NvRouter)", model["displayName"])
}

func TestCopilotWritesNvRouterAndRecognizesLegacyKeiRouter(t *testing.T) {
	home := t.TempDir()
	tool := &CopilotTool{}
	require.NoError(t, tool.Configure(home, "https://novela.biz.id", "test-key", []string{"public-model"}))
	var entries []map[string]any
	require.NoError(t, readJSON(tool.configPath(home), &entries))
	require.Equal(t, "NvRouter", entries[0]["name"])

	entries[0]["name"] = "KeiRouter"
	require.NoError(t, writeJSON(tool.configPath(home), entries))
	_, configured, _, err := tool.DetectStatus(home)
	require.NoError(t, err)
	require.True(t, configured)
	require.NoError(t, tool.Remove(home))
}
