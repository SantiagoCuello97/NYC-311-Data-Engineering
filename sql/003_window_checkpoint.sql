CREATE TABLE IF NOT EXISTS window_sync_state (
    pipeline_name TEXT PRIMARY KEY,
    completed_through TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    CONSTRAINT window_checkpoint_dates CHECK (
        completed_through IS NULL
        OR last_success_at IS NOT NULL
    )
);

INSERT INTO window_sync_state (
    pipeline_name,
    completed_through,
    last_success_at
)
VALUES ('nyc311', NULL, NULL)
ON CONFLICT (pipeline_name) DO NOTHING;