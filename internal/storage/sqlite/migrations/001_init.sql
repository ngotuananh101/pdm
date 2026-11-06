-- 001_init.sql — initial PDM schema (Stage 2, Phase 1.3).
--
-- Tables follow docs/ARCHITECTURE.md §5 (Download, Segment) and §9
-- (SQLite and migration):
--   downloads: one row per job, including queue_position for the
--     persisted queue order. total_bytes is NULL when unknown.
--     Timestamps are UTC RFC 3339 text. etag / last_modified are the
--     resource validators from §5; supports_range is 0/1 (the result of
--     an actual Range verification, never inferred from HEAD alone).
--   segments: composite key (download_id, "index"); end_offset is
--     inclusive per §5 and NULL when the size is unknown (single-stream
--     job). The foreign key cascades: deleting a download removes its
--     segments in the same transaction.
--   settings: key/value store for config.Config (Phase 3).
--   schema_migrations: applied migration versions (also created by the
--     runner before applying migrations; kept here so the file is
--     self-contained when inspected standalone).
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS downloads (
    id               TEXT PRIMARY KEY,
    url              TEXT NOT NULL,
    filename         TEXT NOT NULL,
    destination_dir  TEXT NOT NULL,
    temp_path        TEXT NOT NULL,
    final_path       TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL,
    total_bytes      INTEGER,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    etag             TEXT,
    last_modified    TEXT,
    supports_range   INTEGER NOT NULL DEFAULT 0,
    error_code       TEXT NOT NULL DEFAULT '',
    error_message    TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    queue_position   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS segments (
    download_id     TEXT NOT NULL,
    "index"         INTEGER NOT NULL,
    start_offset    INTEGER NOT NULL,
    end_offset      INTEGER,
    committed_bytes INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (download_id, "index"),
    FOREIGN KEY (download_id) REFERENCES downloads(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_downloads_queue_position
    ON downloads(queue_position);
