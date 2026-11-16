CREATE TABLE IF NOT EXISTS raw_service_requests (
    unique_key TEXT PRIMARY KEY,
    source_updated_at TIMESTAMPTZ NOT NULL,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    status TEXT,
    agency TEXT,
    complaint_type TEXT,
    raw_payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_requests_updated
ON raw_service_requests (source_updated_at, unique_key);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    rows_read INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_unchanged INTEGER NOT NULL DEFAULT 0,
    rows_rejected INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    CONSTRAINT ingestion_runs_status_check
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT ingestion_runs_counts_check
        CHECK (
            rows_read >= 0 AND
            rows_inserted >= 0 AND
            rows_updated >= 0 AND
            rows_unchanged >= 0 AND
            rows_rejected >= 0
        )
);
