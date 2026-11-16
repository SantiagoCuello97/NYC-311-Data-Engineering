CREATE TABLE IF NOT EXISTS sync_state (
    pipeline_name TEXT PRIMARY KEY,
    watermark_updated_at TIMESTAMPTZ,
    watermark_unique_key TEXT,
    last_success_at TIMESTAMPTZ,
    CONSTRAINT sync_state_cursor_pair CHECK (
        (watermark_updated_at IS NULL AND watermark_unique_key IS NULL)
        OR
        (watermark_updated_at IS NOT NULL AND watermark_unique_key IS NOT NULL)
    )
);

INSERT INTO sync_state (
    pipeline_name,
    watermark_updated_at,
    watermark_unique_key,
    last_success_at
)
VALUES ('nyc311', NULL, NULL, NULL)
ON CONFLICT (pipeline_name) DO NOTHING;