package gateway

import (
	"testing"

	"github.com/mydisha/keirouter/backend/internal/core"
)

func TestMergeStreamUsageRetainsSplitTotals(t *testing.T) {
	var usage core.Usage
	mergeStreamUsage(&usage, core.Usage{PromptTokens: 11, CachedTokens: 2, CacheWriteTokens: 3, TotalTokens: 11})
	mergeStreamUsage(&usage, core.Usage{CompletionTokens: 7, TotalTokens: 18})

	if usage.PromptTokens != 11 || usage.CompletionTokens != 7 || usage.TotalTokens != 18 || usage.CachedTokens != 2 || usage.CacheWriteTokens != 3 {
		t.Fatalf("merged usage = %+v, want prompt=11 completion=7 total=18 cache=2 write=3", usage)
	}
}
