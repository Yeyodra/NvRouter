package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/mydisha/keirouter/backend/internal/connectors"
	"github.com/mydisha/keirouter/backend/internal/core"
	"github.com/mydisha/keirouter/backend/internal/store"
)

const (
	quotaWorkerCount = 8
	quotaQueueSize   = 256
	quotaTimeout     = 12 * time.Second
	quotaCooldown    = 30 * time.Second
)

var errQuotaUnsupported = errors.New("upstream quota is unsupported")

type quotaPayload struct {
	PlanName          string                  `json:"plan_name,omitempty"`
	Message           string                  `json:"message,omitempty"`
	UpstreamQuotas    []connectors.QuotaEntry `json:"upstream_quotas,omitempty"`
	RemainingRatio    float64                 `json:"-"`
	HasRemainingRatio bool                    `json:"-"`
}

type quotaJob struct {
	account store.Account
	manual  bool
}

type quotaScheduler struct {
	repo     *store.QuotaSnapshotRepo
	accounts *store.AccountRepo
	fetch    func(context.Context, store.Account) (quotaPayload, error)
	queue    chan quotaJob
	priority chan quotaJob
	mu       sync.Mutex
	active   map[string]bool
	cooldown map[string]time.Time
	provider map[string]chan struct{}
	workers  sync.WaitGroup
}

func newQuotaScheduler(repo *store.QuotaSnapshotRepo, accounts *store.AccountRepo, fetch func(context.Context, store.Account) (quotaPayload, error)) *quotaScheduler {
	return &quotaScheduler{repo: repo, accounts: accounts, fetch: fetch, queue: make(chan quotaJob, quotaQueueSize), priority: make(chan quotaJob, quotaQueueSize), active: map[string]bool{}, cooldown: map[string]time.Time{}, provider: map[string]chan struct{}{}}
}

func (s *quotaScheduler) startWorkers(ctx context.Context) {
	for range quotaWorkerCount {
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			for {
				job, ok := s.nextJob(ctx)
				if !ok {
					return
				}
				s.run(ctx, job)
			}
		}()
	}
}

func (s *quotaScheduler) nextJob(ctx context.Context) (quotaJob, bool) {
	select {
	case job := <-s.priority:
		return job, true
	default:
	}
	select {
	case <-ctx.Done():
		return quotaJob{}, false
	case job := <-s.priority:
		return job, true
	case job := <-s.queue:
		return job, true
	}
}

func (s *quotaScheduler) enqueue(account store.Account, manual bool) bool {
	now := time.Now().UTC()
	s.mu.Lock()
	if s.active[account.ID] || (manual && now.Before(s.cooldown[account.ID])) {
		s.mu.Unlock()
		return false
	}
	s.mu.Unlock()
	if manual && s.repo != nil {
		if previous, err := s.repo.Get(context.Background(), account.ID); err == nil &&
			(previous.State == "queued" || previous.State == "refreshing" || now.Before(previous.LastAttemptAt.Add(quotaCooldown))) {
			return false
		}
	}
	s.mu.Lock()
	if s.active[account.ID] {
		s.mu.Unlock()
		return false
	}
	s.active[account.ID] = true
	s.mu.Unlock()

	queue := s.queue
	if manual {
		queue = s.priority
	}
	if s.repo != nil {
		if err := s.repo.UpsertState(context.Background(), account.ID, account.Provider, "queued", now, now); err != nil {
			s.mu.Lock()
			delete(s.active, account.ID)
			s.mu.Unlock()
			return false
		}
	}
	select {
	case queue <- quotaJob{account: account, manual: manual}:
		return true
	default:
		s.mu.Lock()
		delete(s.active, account.ID)
		s.mu.Unlock()
		if s.repo != nil {
			state := "pending"
			if snapshot, err := s.repo.Get(context.Background(), account.ID); err == nil && snapshot.FetchedAt != nil {
				state = "stale"
			}
			_ = s.repo.UpsertState(context.Background(), account.ID, account.Provider, state, now, now)
		}
		return false
	}
}

func (s *quotaScheduler) run(parent context.Context, job quotaJob) {
	s.mu.Lock()
	sem := s.provider[job.account.Provider]
	if sem == nil {
		sem = make(chan struct{}, 2)
		s.provider[job.account.Provider] = sem
	}
	s.mu.Unlock()
	select {
	case sem <- struct{}{}:
	case <-parent.Done():
		s.finish(job.account.ID, job.manual)
		return
	}
	defer func() { <-sem; s.finish(job.account.ID, job.manual) }()

	now := time.Now().UTC()
	if s.repo != nil {
		_ = s.repo.UpsertState(parent, job.account.ID, job.account.Provider, "refreshing", now, now)
	}
	ctx, cancel := context.WithTimeout(parent, quotaTimeout)
	payload, err := s.fetch(ctx, job.account)
	cancel()
	if s.repo == nil {
		return
	}
	failures := 0
	if previous, getErr := s.repo.Get(parent, job.account.ID); getErr == nil {
		failures = previous.ConsecutiveFailures
	}
	next := nextQuotaRefresh(now, payload, err, failures+1, hashJitter(job.account.ID))
	if errors.Is(err, errQuotaUnsupported) {
		_ = s.repo.UpsertState(parent, job.account.ID, job.account.Provider, "unsupported", now, time.Time{})
		return
	}
	if err != nil {
		_ = s.repo.RecordFailure(parent, job.account.ID, job.account.Provider, quotaErrorMessage(err), now, next)
		return
	}
	body, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		_ = s.repo.RecordFailure(parent, job.account.ID, job.account.Provider, marshalErr.Error(), now, next)
		return
	}
	_ = s.repo.RecordSuccess(parent, store.QuotaSnapshot{AccountID: job.account.ID, Provider: job.account.Provider, Payload: string(body), State: "reported", FetchedAt: &now, LastAttemptAt: now, NextRefreshAt: next})
}

func (s *quotaScheduler) finish(accountID string, manual bool) {
	s.mu.Lock()
	delete(s.active, accountID)
	if manual {
		s.cooldown[accountID] = time.Now().Add(quotaCooldown)
	}
	s.mu.Unlock()
}

func (s *quotaScheduler) runLoop(ctx context.Context) {
	defer s.workers.Wait()
	_ = s.repo.ResetTransient(ctx, time.Now().UTC())
	s.startWorkers(ctx)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		s.schedule(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *quotaScheduler) schedule(ctx context.Context) {
	accounts, err := s.accounts.ListByTenant(ctx, adminTenant)
	if err != nil {
		return
	}
	now := time.Now().UTC()
	byID := make(map[string]store.Account, len(accounts))
	ids := make([]string, len(accounts))
	for i, account := range accounts {
		byID[account.ID] = account
		ids[i] = account.ID
	}
	snapshots, err := s.repo.ListByAccounts(ctx, ids)
	if err != nil {
		return
	}
	for _, account := range accounts {
		snapshot, exists := snapshots[account.ID]
		switch {
		case account.Disabled:
			if !exists || snapshot.State != "paused" {
				_ = s.repo.UpsertState(ctx, account.ID, account.Provider, "paused", now, now.AddDate(10, 0, 0))
			}
		case connectors.GetQuotaSource(account.Provider) == nil:
			if !exists || snapshot.State != "unsupported" {
				_ = s.repo.UpsertState(ctx, account.ID, account.Provider, "unsupported", now, now.AddDate(10, 0, 0))
			}
		case !exists:
			_ = s.repo.UpsertState(ctx, account.ID, account.Provider, "pending", now, now)
		case snapshot.State == "paused" || snapshot.State == "unsupported":
			state := "pending"
			if snapshot.FetchedAt != nil {
				state = "stale"
			}
			_ = s.repo.UpsertState(ctx, account.ID, account.Provider, state, now, now)
		}
	}
	eligible, err := s.repo.ListEligible(ctx, now, quotaQueueSize)
	if err != nil {
		return
	}
	for _, item := range fairQuotaOrder(eligible) {
		if account, ok := byID[item.AccountID]; ok && !account.Disabled {
			s.enqueue(account, false)
		}
	}
}

func fairQuotaOrder(items []store.QuotaSnapshot) []store.QuotaSnapshot {
	providers := make([]string, 0)
	groups := map[string][]store.QuotaSnapshot{}
	for _, item := range items {
		if _, ok := groups[item.Provider]; !ok {
			providers = append(providers, item.Provider)
		}
		groups[item.Provider] = append(groups[item.Provider], item)
	}
	out := make([]store.QuotaSnapshot, 0, len(items))
	for round := 0; len(out) < len(items); round++ {
		for _, provider := range providers {
			if round < len(groups[provider]) {
				out = append(out, groups[provider][round])
			}
		}
	}
	return out
}

func nextQuotaRefresh(now time.Time, payload quotaPayload, err error, failures int, jitter uint32) time.Time {
	if errors.Is(err, errQuotaUnsupported) {
		return time.Time{}
	}
	if err == nil {
		minDelay, spread := 10*time.Minute, 5*time.Minute
		if payload.HasRemainingRatio && payload.RemainingRatio <= 0.1 {
			minDelay, spread = 2*time.Minute, 3*time.Minute
		}
		return now.Add(minDelay + time.Duration(jitter%uint32(spread/time.Second)+1)*time.Second)
	}
	pe := core.AsProviderError(err)
	if pe.Kind == core.ErrRateLimit && pe.RetryAfter > 0 {
		return now.Add(pe.RetryAfter)
	}
	if pe.Kind == core.ErrAuth {
		return now.Add(6 * time.Hour)
	}
	delay := time.Minute << min(max(failures, 0), 6)
	return now.Add(delay)
}

func quotaErrorMessage(err error) string {
	pe := core.AsProviderError(err)
	switch pe.Kind {
	case core.ErrAuth:
		return "quota authentication failed"
	case core.ErrRateLimit:
		return "upstream quota rate limited"
	case core.ErrTimeout:
		return "upstream quota refresh timed out"
	default:
		return "upstream quota refresh failed"
	}
}

func hashJitter(value string) uint32 {
	var out uint32
	for i := range len(value) {
		out = out*33 + uint32(value[i])
	}
	return out
}
