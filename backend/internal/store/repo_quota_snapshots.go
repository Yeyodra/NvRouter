package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// QuotaSnapshotRepo persists the latest upstream quota result per account.
type QuotaSnapshotRepo struct{ db *DB }

func (db *DB) QuotaSnapshots() *QuotaSnapshotRepo { return &QuotaSnapshotRepo{db: db} }

const quotaSnapshotColumns = `account_id, provider, payload, state, fetched_at,
	last_attempt_at, next_refresh_at, last_error, consecutive_failures`

func (r *QuotaSnapshotRepo) RecordSuccess(ctx context.Context, s QuotaSnapshot) error {
	q := r.db.rebind(`INSERT INTO quota_snapshots (` + quotaSnapshotColumns + `)
		VALUES (?, ?, ?, ?, ?, ?, ?, '', 0)
		ON CONFLICT(account_id) DO UPDATE SET
			provider = excluded.provider, payload = excluded.payload, state = excluded.state,
			fetched_at = excluded.fetched_at, last_attempt_at = excluded.last_attempt_at,
			next_refresh_at = excluded.next_refresh_at, last_error = '', consecutive_failures = 0`)
	_, err := r.db.sql.ExecContext(ctx, q, s.AccountID, s.Provider, s.Payload, s.State,
		nullTime(s.FetchedAt), formatTime(s.LastAttemptAt), formatTime(s.NextRefreshAt))
	return err
}

func (r *QuotaSnapshotRepo) RecordFailure(ctx context.Context, accountID, provider, message string, attemptedAt, nextRefreshAt time.Time) error {
	q := r.db.rebind(`INSERT INTO quota_snapshots (` + quotaSnapshotColumns + `)
		VALUES (?, ?, '{}', 'error', NULL, ?, ?, ?, 1)
		ON CONFLICT(account_id) DO UPDATE SET
			provider = excluded.provider,
			state = CASE WHEN quota_snapshots.fetched_at IS NULL THEN 'error' ELSE 'stale' END,
			last_attempt_at = excluded.last_attempt_at, next_refresh_at = excluded.next_refresh_at,
			last_error = excluded.last_error,
			consecutive_failures = quota_snapshots.consecutive_failures + 1`)
	_, err := r.db.sql.ExecContext(ctx, q, accountID, provider, formatTime(attemptedAt),
		formatTime(nextRefreshAt), sanitizeQuotaError(message))
	return err
}

func (r *QuotaSnapshotRepo) UpsertState(ctx context.Context, accountID, provider, state string, now, next time.Time) error {
	q := r.db.rebind(`INSERT INTO quota_snapshots (` + quotaSnapshotColumns + `)
		VALUES (?, ?, '{}', ?, NULL, ?, ?, '', 0)
		ON CONFLICT(account_id) DO UPDATE SET provider = excluded.provider, state = excluded.state,
			last_attempt_at = excluded.last_attempt_at, next_refresh_at = excluded.next_refresh_at`)
	_, err := r.db.sql.ExecContext(ctx, q, accountID, provider, state, formatTime(now), formatTime(next))
	return err
}

func (r *QuotaSnapshotRepo) ResetTransient(ctx context.Context, next time.Time) error {
	q := r.db.rebind(`UPDATE quota_snapshots SET state = CASE WHEN fetched_at IS NULL THEN 'pending' ELSE 'stale' END,
		next_refresh_at = ? WHERE state IN ('queued', 'refreshing')`)
	_, err := r.db.sql.ExecContext(ctx, q, formatTime(next))
	return err
}

func (r *QuotaSnapshotRepo) Get(ctx context.Context, accountID string) (QuotaSnapshot, error) {
	q := r.db.rebind(`SELECT ` + quotaSnapshotColumns + ` FROM quota_snapshots WHERE account_id = ?`)
	s, err := scanQuotaSnapshot(r.db.sql.QueryRowContext(ctx, q, accountID).Scan)
	if err == sql.ErrNoRows {
		return QuotaSnapshot{}, ErrNotFound
	}
	return s, err
}

func (r *QuotaSnapshotRepo) ListByAccounts(ctx context.Context, accountIDs []string) (map[string]QuotaSnapshot, error) {
	out := make(map[string]QuotaSnapshot, len(accountIDs))
	if len(accountIDs) == 0 {
		return out, nil
	}
	marks := make([]string, len(accountIDs))
	args := make([]any, len(accountIDs))
	for i, id := range accountIDs {
		marks[i], args[i] = "?", id
	}
	q := r.db.rebind(`SELECT ` + quotaSnapshotColumns + ` FROM quota_snapshots WHERE account_id IN (` + strings.Join(marks, ",") + `)`)
	rows, err := r.db.sql.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		s, err := scanQuotaSnapshot(rows.Scan)
		if err != nil {
			return nil, err
		}
		out[s.AccountID] = s
	}
	return out, rows.Err()
}

func (r *QuotaSnapshotRepo) ListEligible(ctx context.Context, now time.Time, limit int) ([]QuotaSnapshot, error) {
	q := r.db.rebind(`SELECT ` + quotaSnapshotColumns + ` FROM (
		SELECT ` + quotaSnapshotColumns + `,
			ROW_NUMBER() OVER (PARTITION BY provider ORDER BY next_refresh_at, account_id) AS provider_position
		FROM quota_snapshots
		WHERE next_refresh_at <= ? AND state NOT IN ('unsupported', 'paused', 'queued', 'refreshing')
	) eligible ORDER BY provider_position, provider, next_refresh_at, account_id LIMIT ?`)
	rows, err := r.db.sql.QueryContext(ctx, q, formatTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QuotaSnapshot
	for rows.Next() {
		s, err := scanQuotaSnapshot(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func scanQuotaSnapshot(scan func(...any) error) (QuotaSnapshot, error) {
	var s QuotaSnapshot
	var fetched sql.NullString
	var attempted, next string
	err := scan(&s.AccountID, &s.Provider, &s.Payload, &s.State, &fetched,
		&attempted, &next, &s.LastError, &s.ConsecutiveFailures)
	if err != nil {
		return s, err
	}
	if fetched.Valid {
		t := parseTime(fetched.String)
		s.FetchedAt = &t
	}
	s.LastAttemptAt, s.NextRefreshAt = parseTime(attempted), parseTime(next)
	return s, nil
}

func sanitizeQuotaError(message string) string {
	message = strings.TrimSpace(strings.Split(message, "\n")[0])
	lower := strings.ToLower(message)
	if strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "bearer") {
		return "upstream quota refresh failed"
	}
	if len(message) > 240 {
		message = message[:240]
	}
	if message == "" {
		return "upstream quota refresh failed"
	}
	return fmt.Sprintf("%s", message)
}
