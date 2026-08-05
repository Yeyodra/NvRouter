package cache

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// Embedder turns a request's prompt into a vector for cache lookup.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// PromptText returns a deterministic representation of every request field that
// can affect a non-streaming response. Router-only request IDs and affinity are
// intentionally excluded.
func PromptText(req *core.ChatRequest) string {
	if req == nil {
		return "null"
	}
	input := struct {
		TenantID, ProjectID, APIKeyID, PublicModel string
		Model                                      string
		System                                     string
		Messages                                   []core.Message
		Tools                                      []core.Tool
		ToolChoice                                 any
		Temperature, TopP                          *float64
		MaxTokens                                  *int
		Stop                                       []string
		Reasoning                                  *core.ReasoningConfig
		ResponseFormat                             json.RawMessage
		Extra                                      map[string]json.RawMessage
	}{
		TenantID: req.Metadata.TenantID, ProjectID: req.Metadata.ProjectID,
		APIKeyID: req.Metadata.APIKeyID, PublicModel: req.Metadata.PublicModel,
		Model: req.Model, System: req.System, Messages: req.Messages, Tools: req.Tools,
		ToolChoice: req.ToolChoice, Temperature: req.Temperature, TopP: req.TopP,
		MaxTokens: req.MaxTokens, Stop: req.Stop, Reasoning: req.Reasoning,
		ResponseFormat: req.ResponseFormat, Extra: req.Extra,
	}
	encoded, _ := json.Marshal(input)
	return string(encoded)
}

// RequestKey is the exact isolation key paired with the semantic vector.
func RequestKey(req *core.ChatRequest) string {
	sum := sha256.Sum256([]byte(PromptText(req)))
	return hex.EncodeToString(sum[:])
}

// HashEmbedder is a deterministic, dependency-free embedder. Identical prompts
// map to identical vectors (cosine 1.0), giving exact-prompt caching with no
// embeddings provider required. For true semantic (near-match) caching, plug in
// a provider-backed embedder instead.
type HashEmbedder struct {
	dims int
}

// NewHashEmbedder builds a hash embedder producing vectors of the given length.
func NewHashEmbedder(dims int) *HashEmbedder {
	if dims <= 0 {
		dims = 16
	}
	return &HashEmbedder{dims: dims}
}

// Embed maps text to a deterministic unit-ish vector derived from its SHA-256
// digest. The mapping is stable, so identical text always yields the same
// vector; different text yields effectively unrelated vectors.
func (h *HashEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	vec := make([]float32, h.dims)
	// Expand the digest deterministically across the requested dimensions by
	// re-hashing with a counter.
	for i := 0; i < h.dims; i++ {
		var seed [8]byte
		binary.LittleEndian.PutUint64(seed[:], uint64(i))
		sum := sha256.Sum256(append([]byte(text), seed[:]...))
		// Map the first 4 bytes to a float in [-1, 1].
		u := binary.LittleEndian.Uint32(sum[:4])
		vec[i] = float32(int32(u))/float32(1<<31) // normalize to ~[-1,1]
	}
	return vec, nil
}