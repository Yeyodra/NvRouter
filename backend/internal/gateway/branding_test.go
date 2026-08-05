package gateway

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultBrandingUsesNvRouterPublicIdentity(t *testing.T) {
	branding := defaultBrandingSettings()
	require.Equal(t, "NvRouter", branding.Name)
}
