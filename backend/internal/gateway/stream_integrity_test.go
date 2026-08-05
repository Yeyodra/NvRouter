package gateway

import (
	"bytes"
	"encoding/json"
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
	if !strings.Contains(out.String(), `"type":"upstream_error"`) {
		t.Fatalf("output = %q, want sanitized OpenAI error", out.String())
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
	if !strings.Contains(out.String(), `"type":"upstream_error"`) {
		t.Fatalf("output = %q, want sanitized OpenAI error", out.String())
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

func TestCopySanitizedResponsesSanitizesNativeFailure(t *testing.T) {
	input := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_native\",\"status\":\"failed\",\"error\":{\"type\":\"server_error\",\"code\":\"native_code\",\"message\":\"native message\"}}}\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAIResponses, nil)
	var pe *core.ProviderError
	if !errors.As(err, &pe) || pe.Kind != core.ErrUpstream {
		t.Fatalf("error = %v, want upstream ProviderError", err)
	}
	got := out.String()
	if !strings.Contains(got, "event: response.failed\n") || !strings.Contains(got, `"message":"upstream provider request failed"`) {
		t.Fatalf("output = %q, want generic response.failed", got)
	}
	if strings.Contains(got, "resp_native") || strings.Contains(got, "native_code") || strings.Contains(got, "native message") {
		t.Fatalf("output exposed upstream failure detail: %q", got)
	}
	if !strings.Contains(pe.Error(), "native message") || pe.Cause == nil || !strings.Contains(pe.Cause.Error(), "native message") {
		t.Fatalf("internal error lost upstream detail: %#v", pe)
	}
}

func TestCopySanitizedResponsesPreservesNativeIncomplete(t *testing.T) {
	input := "event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_native\",\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"
	var out bytes.Buffer

	_, err := copySanitizedStream(&out, strings.NewReader(input), core.DialectOpenAIResponses, nil)
	if err != nil {
		t.Fatalf("copySanitizedStream() error = %v", err)
	}
	if out.String() != input {
		t.Fatalf("output = %q, want native event %q", out.String(), input)
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

func TestProjectStreamFrameOnlyRewritesProtocolModel(t *testing.T) {
	tests := []struct {
		name    string
		dialect core.Dialect
		payload string
		path    []string
	}{
		{"openai", core.DialectOpenAI, `{"model":"internal","choices":[{"delta":{"tool_calls":[{"function":{"arguments":{"model":"customer-selected-value"}}}]}}]}`, []string{"model"}},
		{"responses", core.DialectOpenAIResponses, `{"type":"response.created","response":{"model":"internal","output":[{"arguments":{"model":"customer-selected-value"}}]}}`, []string{"response", "model"}},
		{"anthropic", core.DialectAnthropic, `{"type":"message_start","message":{"model":"internal","content":[{"input":{"model":"customer-selected-value"}}]}}`, []string{"message", "model"}},
		{"gemini", core.DialectGemini, `{"modelVersion":"internal","candidates":[{"content":{"parts":[{"functionCall":{"args":{"model":"customer-selected-value"}}}]}}]}`, []string{"modelVersion"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := []byte("data: " + tt.payload + "\n\n")
			out, err := projectStreamFrame(raw, []byte(tt.payload), tt.dialect, "public-safe-route")
			if err != nil {
				t.Fatal(err)
			}
			var projected map[string]any
			data := strings.TrimSpace(strings.TrimPrefix(string(out), "data:"))
			if err := json.Unmarshal([]byte(data), &projected); err != nil {
				t.Fatal(err)
			}
			value := any(projected)
			for _, key := range tt.path {
				value = value.(map[string]any)[key]
			}
			if value != "public-safe-route" {
				t.Fatalf("protocol model = %v", value)
			}
			if !strings.Contains(string(out), `"model":"customer-selected-value"`) {
				t.Fatalf("nested tool model was mutated: %s", out)
			}
		})
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
	got := out.String()
	if !strings.Contains(got, "event: response.failed\n") || !strings.Contains(got, "upstream provider response was invalid") {
		t.Fatalf("output = %q, want generic response.failed", got)
	}
	if strings.Contains(got, `"status":"incomplete"`) {
		t.Fatalf("malformed incomplete was forwarded: %q", got)
	}
}
