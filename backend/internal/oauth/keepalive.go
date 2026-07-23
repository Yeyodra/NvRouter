package oauth

import (
	"context"
	"log/slog"
	"time"

	"github.com/mydisha/keirouter/backend/internal/store"
)

const (
	// DefaultKeepAliveInterval is how often the background refresher checks
	// for near-expiry OAuth tokens. 30 minutes keeps tokens warm within the
	// typical 1-hour access token lifetime used by AWS SSO OIDC.
	DefaultKeepAliveInterval = 30 * time.Minute
)

// KeepAlive runs a background loop that proactively refreshes near-expiry
// OAuth access tokens. It prevents request-time latency from just-in-time
// refresh and detects expired refresh tokens early so the dashboard can show
// a "Reconnect" prompt.
type KeepAlive struct {
	interval time.Duration
	tokenMgr *TokenManager
	accounts *store.AccountRepo
	tenantID string
	log      *slog.Logger
}

// NewKeepAlive builds a KeepAlive.
func NewKeepAlive(tm *TokenManager, accounts *store.AccountRepo, tenantID string, log *slog.Logger) *KeepAlive {
	return &KeepAlive{
		interval: DefaultKeepAliveInterval,
		tokenMgr: tm,
		accounts: accounts,
		tenantID: tenantID,
		log:      log,
	}
}

// SetInterval overrides the default check interval.
func (k *KeepAlive) SetInterval(d time.Duration) {
	if d > 0 {
		k.interval = d
	}
}

// Run starts the keepalive loop. It blocks until ctx is cancelled. Callers
// should launch it as a goroutine tied to the application context.
func (k *KeepAlive) Run(ctx context.Context) {
	k.log.Info("oauth keepalive started", "interval", k.interval)

	// Run once immediately on startup to catch stale tokens early.
	k.refreshAll(ctx)

	ticker := time.NewTicker(k.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			k.log.Info("oauth keepalive stopped")
			return
		case <-ticker.C:
			k.refreshAll(ctx)
		}
	}
}

// refreshAll lists all OAuth accounts for the tenant and refreshes those that
// are near expiry. Failures are logged but do not stop the loop.
func (k *KeepAlive) refreshAll(ctx context.Context) {
	accs, err := k.accounts.ListByTenant(ctx, k.tenantID)
	if err != nil {
		k.log.Error("oauth keepalive: list accounts", "err", err)
		return
	}

	// Cap work per pass. Farm imports (thousands of OAuth rows) would otherwise
	// serial-refresh every expired token on startup and freeze the gateway.
	const maxRefreshPerPass = 32

	var refreshed, skipped, failed, reconnect, deferred int
	for _, acc := range accs {
		if refreshed+failed >= maxRefreshPerPass {
			// Leave the rest for later passes / request-time EnsureFresh.
			deferred++
			continue
		}
		if acc.AuthKind != store.AuthOAuth {
			continue
		}
		if acc.Disabled {
			continue
		}
		// Already flagged for reconnection; skip until the user re-authenticates.
		if acc.NeedsReconnect {
			reconnect++
			continue
		}
		// Only refresh tokens that are near expiry or recently expired.
		// Lead is per-provider (RefreshLead) or global refreshSkew when unset.
		// Long-expired farm tokens are skipped here — request path refreshes
		// only the few accounts actually selected for a chat attempt.
		if acc.TokenExpiresAt != nil {
			remaining := time.Until(*acc.TokenExpiresAt)
			lead := providerRefreshLead(acc.Provider)
			if remaining > lead {
				skipped++
				continue
			}
			// Expired more than 1h ago: do not thrash the token endpoint on keepalive.
			if remaining < -time.Hour {
				skipped++
				continue
			}
		}

		_, err := k.tokenMgr.EnsureFresh(ctx, acc)
		if err != nil {
			failed++
			k.log.Warn("oauth keepalive: refresh failed",
				"account", acc.ID,
				"provider", acc.Provider,
				"err", err,
			)
			continue
		}
		refreshed++
		k.log.Debug("oauth keepalive: refreshed",
			"account", acc.ID,
			"provider", acc.Provider,
		)
	}

	if refreshed > 0 || failed > 0 || reconnect > 0 || deferred > 0 {
		k.log.Info("oauth keepalive pass complete",
			"refreshed", refreshed,
			"skipped", skipped,
			"failed", failed,
			"needs_reconnect", reconnect,
			"deferred", deferred,
		)
	}
}
