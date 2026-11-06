-- One database per route: the delivery ledger.

-- Key/value markers about the database itself (e.g. when it was initialized).
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT, WITHOUT ROWID;

-- Every batch an input read: in_flight until every output it was routed to
-- has it durably, then done. Rows store no records.
CREATE TABLE batches (
    input TEXT NOT NULL,
    batch_key TEXT NOT NULL,
    batch_ts_ms INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('in_flight', 'done')),
    read_at_ms INTEGER NOT NULL,
    done_at_ms INTEGER,
    PRIMARY KEY (input, batch_key)
) STRICT, WITHOUT ROWID;

CREATE INDEX batches_by_input_status_ts ON batches (input, status, batch_ts_ms);

-- Outputs that already durably took an in_flight batch: a replay of the
-- batch skips them. Deleted when the batch is done.
CREATE TABLE deliveries (
    input TEXT NOT NULL,
    batch_key TEXT NOT NULL,
    output TEXT NOT NULL,
    PRIMARY KEY (input, batch_key, output)
) STRICT, WITHOUT ROWID;
