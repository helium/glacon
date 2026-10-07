-- One database per route.

-- Key/value markers about the database itself (e.g. when it was initialized).
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT, WITHOUT ROWID;

-- Every input file the route has fully processed. Drives the poller: the
-- newest row per input is its resume point, and listed files found here are
-- skipped.
CREATE TABLE processed_files (
    input TEXT NOT NULL,
    file_key TEXT NOT NULL,
    file_timestamp_ms INTEGER NOT NULL,
    processed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (input, file_key)
) STRICT, WITHOUT ROWID;

CREATE INDEX processed_files_by_input_ts ON processed_files (input, file_timestamp_ms);

-- Outputs that already durably took a file still being processed. Lets a
-- re-read after a crash skip outputs that were already written. Rows are
-- removed once the file lands in processed_files.
CREATE TABLE in_flight_deliveries (
    input TEXT NOT NULL,
    file_key TEXT NOT NULL,
    output TEXT NOT NULL,
    PRIMARY KEY (input, file_key, output)
) STRICT, WITHOUT ROWID;

-- Hashes of recently-seen record dedup keys, shared by every input.
CREATE TABLE dedup_keys (
    key_hash BLOB PRIMARY KEY,
    last_seen_ms INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

CREATE INDEX dedup_keys_by_last_seen ON dedup_keys (last_seen_ms);
