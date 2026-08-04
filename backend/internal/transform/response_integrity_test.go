package transform

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestValidateResponseIntegrityRejectsMalformedFinalToolArguments(t *testing.T) {
	resp := &core.ChatResponse{Message: core.Message{Content: []core.ContentPart{{
		Type:     core.PartToolCall,
		ToolCall: &core.ToolCall{Name: "Read", Arguments: json.RawMessage(`{"path":`)},
	}}}}

	err := ValidateResponseIntegrity(resp)
	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
}
