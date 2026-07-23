package connectors

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/mydisha/keirouter/backend/internal/core"
)

// Process-local turn index store (9router resolveGrokCliTurnIdx).
// Never persisted; capped FIFO eviction.
const grokCLITurnStoreMax = 5000

// ponytail: no TTL yet (9router MEMORY_CONFIG.sessionTtlMs); add when multi-tenant
// process lifetime needs eviction beyond cap.
type grokCLITurnEntry struct {
	turn     int
	lastUsed int64 // unix ms
}

var (
	grokCLITurnMu    sync.Mutex
	grokCLITurnStore = make(map[string]grokCLITurnEntry)
	grokCLITurnOrder []string // insertion order for FIFO cap

	grokCLIAgentOnce sync.Once
	grokCLIAgentID   string
)

// countGrokCLIUserTurns counts role=user messages (1-based floor of 1).
func countGrokCLIUserTurns(req *core.ChatRequest) int {
	if req == nil {
		return 1
	}
	n := 0
	for _, m := range req.Messages {
		if m.Role == core.RoleUser {
			n++
		}
	}
	return max(1, n)
}

// resolveGrokCLITurnIdx returns a monotonic turn index for sessionID.
// Prefers user-message count; never decreases vs last value for the same session
// in this process (matches 9router resolveGrokCliTurnIdx with requestKey set).
func resolveGrokCLITurnIdx(sessionID string, fromInput int) int {
	if fromInput < 1 {
		fromInput = 1
	}
	if sessionID == "" {
		return fromInput
	}

	now := time.Now().UnixMilli()
	grokCLITurnMu.Lock()
	defer grokCLITurnMu.Unlock()

	prev := 0
	_, existed := grokCLITurnStore[sessionID]
	if existing, ok := grokCLITurnStore[sessionID]; ok {
		prev = existing.turn
		// Drop from order; re-append after so FIFO cap tracks recency.
		for i, k := range grokCLITurnOrder {
			if k == sessionID {
				grokCLITurnOrder = append(grokCLITurnOrder[:i], grokCLITurnOrder[i+1:]...)
				break
			}
		}
	}

	// Each call is a distinct request (like 9router requestKey=body): advance.
	turn := fromInput
	if prev > 0 && prev+1 > turn {
		turn = prev + 1
	}

	// Evict only when inserting a new session key.
	if !existed {
		for len(grokCLITurnStore) >= grokCLITurnStoreMax && len(grokCLITurnOrder) > 0 {
			old := grokCLITurnOrder[0]
			grokCLITurnOrder = grokCLITurnOrder[1:]
			delete(grokCLITurnStore, old)
		}
	}
	grokCLITurnOrder = append(grokCLITurnOrder, sessionID)
	grokCLITurnStore[sessionID] = grokCLITurnEntry{turn: turn, lastUsed: now}
	return turn
}

// resetGrokCLITurnStore clears the in-memory turn counters (tests only).
func resetGrokCLITurnStore() {
	grokCLITurnMu.Lock()
	defer grokCLITurnMu.Unlock()
	grokCLITurnStore = make(map[string]grokCLITurnEntry)
	grokCLITurnOrder = nil
}

// resolveGrokCLISessionID prefers stable conversation keys, then account, else uuid.
func resolveGrokCLISessionID(req *core.ChatRequest, creds core.Credentials) string {
	if req != nil {
		if k := strings.TrimSpace(req.Metadata.ContextAffinityKey); k != "" {
			return k
		}
		if v := grokCLIExtraString(req, "prompt_cache_key", "session_id", "conversation_id", "thread_id"); v != "" {
			return v
		}
		if v := grokCLIExtraMetaString(req, "session_id", "conversation_id", "thread_id", "prompt_cache_key"); v != "" {
			return v
		}
	}
	if id := firstNonEmpty(creds.AccountID, creds.Extra["connectionId"], creds.Extra["connection_id"]); id != "" {
		return id
	}
	return uuid.NewString()
}

// resolveGrokCLIAgentID returns Extra deviceId/agentId or a process-stable machine id.
func resolveGrokCLIAgentID(creds core.Credentials) string {
	if id := firstNonEmpty(
		creds.Extra["deviceId"],
		creds.Extra["device_id"],
		creds.Extra["agentId"],
		creds.Extra["agent_id"],
	); id != "" {
		return id
	}
	grokCLIAgentOnce.Do(func() {
		host, _ := os.Hostname()
		if host == "" {
			grokCLIAgentID = uuid.NewString()
			return
		}
		sum := sha256.Sum256([]byte("grok-cli-agent:" + host))
		mid := hex.EncodeToString(sum[:])
		// UUID-ish aesthetics (9router getConsistentMachineId shape).
		grokCLIAgentID = mid[0:8] + "-" + mid[8:12] + "-5" + mid[13:16] + "-a" + mid[17:20] + "-" + mid[0:12]
	})
	return grokCLIAgentID
}

func grokCLIExtraString(req *core.ChatRequest, keys ...string) string {
	if req == nil || len(req.Extra) == 0 {
		return ""
	}
	for _, key := range keys {
		raw, ok := req.Extra[key]
		if !ok || len(raw) == 0 {
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			if s = strings.TrimSpace(s); s != "" {
				return s
			}
		}
	}
	return ""
}

func grokCLIExtraMetaString(req *core.ChatRequest, keys ...string) string {
	if req == nil || len(req.Extra) == 0 {
		return ""
	}
	raw, ok := req.Extra["metadata"]
	if !ok || len(raw) == 0 {
		return ""
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return ""
	}
	for _, key := range keys {
		v, ok := meta[key]
		if !ok {
			continue
		}
		switch t := v.(type) {
		case string:
			if s := strings.TrimSpace(t); s != "" {
				return s
			}
		case float64:
			return strconv.FormatInt(int64(t), 10)
		}
	}
	return ""
}
