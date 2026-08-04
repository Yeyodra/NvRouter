package gateway

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestCopySanitizedStreamRejectsMalformedFrameBeforeEOF(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: {bad\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAI, nil)

	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
	if strings.Contains(out.String(), "{bad") {
		t.Fatalf("malformed frame was forwarded: %q", out.String())
	}
}

func TestCopySanitizedStreamRejectsMissingTerminal(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAI, nil)

	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
}

func TestCopySanitizedStreamPreservesValidStream(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAI, nil)
	if err != nil {
		t.Fatalf("copySanitizedStream() error = %v", err)
	}
	if out.String() != input {
		t.Fatalf("output = %q, want byte-identical %q", out.String(), input)
	}
}

func TestCopySanitizedStreamAcceptsOpenAIFinishReasonWithoutDone(t *testing.T) {
	input := "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAI, nil)
	if err != nil {
		t.Fatalf("copySanitizedStream() error = %v", err)
	}
	if out.String() != input {
		t.Fatalf("output = %q, want byte-identical %q", out.String(), input)
	}
}

func TestCopySanitizedStreamRejectsContentAfterTerminalButAllowsUsage(t *testing.T) {
	terminal := "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n"
	usage := "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n"
	content := "data: {\"choices\":[{\"delta\":{\"content\":\"late\"}}]}\n\n"

	var valid bytes.Buffer
	if _, err := copySanitizedStream(&valid, strings.NewReader(terminal+usage), core.DialectOpenAI, nil); err != nil {
		t.Fatalf("usage after terminal rejected: %v", err)
	}
	if valid.String() != terminal+usage {
		t.Fatalf("valid output = %q, want %q", valid.String(), terminal+usage)
	}

	var malformed bytes.Buffer
	_, err := copySanitizedStream(&malformed, strings.NewReader(terminal+content), core.DialectOpenAI, nil)
	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
	if strings.Contains(malformed.String(), "late") {
		t.Fatalf("post-terminal content was forwarded: %q", malformed.String())
	}
}

func TestCopySanitizedResponsesPreservesNativeTerminalOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
	}{
		{
			name:  "failed",
			input: "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_native\",\"status\":\"failed\",\"error\":{\"type\":\"server_error\",\"code\":\"native_code\",\"message\":\"native message\"}}}\n\n",
		},
		{
			name:  "incomplete",
			input: "event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_native\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			_, err := copySanitizedStream(&out, strings.NewReader(tt.input), core.DialectOpenAIResponses, nil)
			if tt.name == "failed" {
				var pe *core.ProviderError
				if !errors.As(err, &pe) || pe.Kind != core.ErrUpstream {
					t.Fatalf("error = %v, want upstream ProviderError", err)
				}
			} else if err != nil {
				t.Fatalf("copySanitizedStream() error = %v", err)
			}
			if out.String() != tt.input {
				t.Fatalf("output = %q, want native event %q", out.String(), tt.input)
			}
			if strings.Contains(out.String(), "response.completed") || strings.Contains(out.String(), `data: {\"error\"`) {
				t.Fatalf("native terminal was contradicted or replaced: %q", out.String())
			}
		})
	}
}

func TestCopySanitizedResponsesEmitsFailureAfterPartialEOF(t *testing.T) {
	input := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAIResponses, nil)
	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
	got := out.String()
	if !strings.Contains(got, "event: response.failed\n") || !strings.Contains(got, `"type":"response.failed"`) {
		t.Fatalf("output = %q, want native response.failed terminal", got)
	}
	if strings.Contains(got, "response.completed") {
		t.Fatalf("partial stream was marked completed: %q", got)
	}
}

func TestCopySanitizedResponsesEmitsFailureAfterTruncatedFrameEOF(t *testing.T) {
	input := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\nevent: response.output_text.delta\ndata: {\"type\":"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAIResponses, nil)
	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
	got := out.String()
	if !strings.Contains(got, `"delta":"partial"`) {
		t.Fatalf("complete delta was not forwarded: %q", got)
	}
	if strings.Count(got, "event: response.failed\n") != 1 {
		t.Fatalf("output = %q, want exactly one native response.failed terminal", got)
	}
	if strings.Contains(got, "response.completed") {
		t.Fatalf("truncated stream was marked completed: %q", got)
	}
}

func TestCopySanitizedResponsesRejectsContentAfterTerminal(t *testing.T) {
	terminal := "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
	content := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"late\"}\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(terminal+content), core.DialectOpenAIResponses, nil)
	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
	if strings.Contains(out.String(), "late") {
		t.Fatalf("post-terminal content was forwarded: %q", out.String())
	}
}

func TestCopySanitizedResponsesRejectsMalformedIncomplete(t *testing.T) {
	input := "event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\"}}\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAIResponses, nil)
	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrResponseIntegrity {
		t.Fatalf("error = %v, want response-integrity ProviderError", err)
	}
	if out.Len() != 0 {
		t.Fatalf("malformed incomplete was forwarded: %q", out.String())
	}
}
