CREATE TABLE quota_snapshots (
    account_id           TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    provider             TEXT NOT NULL,
    payload              TEXT NOT NULL DEFAULT '{}',
    state                TEXT NOT NULL,
    fetched_at           TEXT,
    last_attempt_at      TEXT NOT NULL,
    next_refresh_at      TEXT NOT NULL,
    last_error           TEXT NOT NULL DEFAULT '',
    consecutive_failures INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_quota_snapshots_next_refresh ON quota_snapshots(next_refresh_at, provider, account_id);
