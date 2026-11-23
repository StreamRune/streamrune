-- StreamRune event-store schema for SQLite.
--
-- Applied by SqliteDatabase.migrate(SqliteSchemaSeries.EVENT_STORE) from db/sqlite-migration and
-- recorded in its own Flyway history table, flyway_schema_history_streamrune, so it never shares a
-- version namespace with the application's own migrations. It mirrors the Postgres event-store
-- baseline (db/streamrune-migration): same tables, columns, constraint and index names, except
-- where noted below.
--
-- Once released this file is frozen; later schema changes ship as new versioned migrations.
--
-- TIMESTAMPS: every timestamp column is an INTEGER holding microseconds since 1970-01-01T00:00:00Z
-- (UTC), negative before 1970 (SqliteTimestamps converts). Microseconds are Postgres' TIMESTAMPTZ
-- precision, so an Instant reads back as it does on Postgres; sub-microsecond digits round half up,
-- as the Postgres driver rounds them. An integer sorts chronologically, answers range queries with
-- plain comparisons and needs no parsing; a 64-bit count of microseconds covers years -290308 to
-- 294247, so 2038 is no limit. ISO-8601 text was rejected: it sorts as text only while every value
-- has the same width, which a year before 0000 or after 9999 breaks, and it costs a parse on every
-- read. No timestamp column has a default: SQLite's clock is not used, the application Clock stamps
-- every value, so a statement that forgets a NOT NULL timestamp fails instead of being stamped by a
-- second clock. (Postgres defaults several of them to NOW().) To read one by hand:
-- strftime('%Y-%m-%dT%H:%M:%fZ', col / 1000000.0, 'unixepoch').
--
-- TYPE TRANSLATION. Every table is STRICT, so SQLite refuses a value of the wrong type as Postgres
-- does. BIGINT, INT and SMALLINT become INTEGER. VARCHAR(n) and TEXT become TEXT, and VARCHAR(n)
-- keeps its limit as CHECK (length(col) <= n), which counts characters as VARCHAR does. JSONB
-- becomes TEXT holding JSON, with CHECK (json_valid(col)) for Postgres' refusal of text that is not
-- JSON (SQLite keeps the text as written; JSONB normalizes it). BOOLEAN becomes INTEGER 0 or 1.
-- BIGINT[] becomes a TEXT JSON array. TIMESTAMPTZ becomes INTEGER, see above. The aggregate-type
-- syntax, Postgres' regex '^[a-z][a-z0-9_]{0,31}$', becomes GLOB patterns plus a length check, since
-- SQLite has no regex operator by default.
--
-- ROW IDS. No table uses AUTOINCREMENT. An INTEGER PRIMARY KEY column is SQLite's row id: an insert
-- that does not set it gets one more than the largest row id in the table. Postgres' BIGSERIAL
-- columns (audit_log.id, event_audit_log.id, outbox_events.seq) become such columns: increasing in
-- insert order among the rows that exist, which is what every query orders or compares by. Unlike a
-- sequence, the id of a deleted last row can be handed out again; nothing keeps an id beyond the
-- life of its row.
--
-- LEFT OUT: subscription_leases. Leadership on SQLite is always SubscriptionLeadership.NOOP (one
-- process per database file, enforced by the instance guard), so no lease is ever taken.


-- ===========================================================================================
-- event_stream — the append-only event log, the single source of truth.
-- ===========================================================================================
CREATE TABLE event_stream (
    -- Gapless, commit-ordered position in the global stream. The append sets it from
    -- global_offset_counter inside its own IMMEDIATE transaction, so a rolled-back append never
    -- leaves a hole. A plain row id without AUTOINCREMENT: SQLite never assigns it here.
    global_offset  INTEGER NOT NULL PRIMARY KEY,
    -- The stream key: the aggregate type the decider was registered under and the id the command's
    -- extractor returned, verbatim.
    aggregate_type TEXT    NOT NULL,
    aggregate_id   TEXT    NOT NULL CHECK (length(aggregate_id) <= 255),
    version        INTEGER NOT NULL,   -- per-stream version (optimistic concurrency)
    event_type     TEXT    NOT NULL CHECK (length(event_type) <= 255),
    payload        TEXT    NOT NULL CHECK (json_valid(payload)),   -- @Encrypted fields are ciphertext here
    metadata       TEXT    NOT NULL CHECK (json_valid(metadata)),
    created_at     INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,   -- event schema version read by upcasting
    -- One event per (stream, version): a concurrent append at the same version fails here.
    CONSTRAINT event_stream_stream_version_key UNIQUE (aggregate_type, aggregate_id, version),
    CONSTRAINT event_stream_aggregate_type_syntax CHECK (
        aggregate_type GLOB '[a-z]*' AND aggregate_type NOT GLOB '*[^a-z0-9_]*'
        AND length(aggregate_type) <= 32)
) STRICT;

-- Reads events by type (upcasting and type-filtered queries).
CREATE INDEX idx_event_type ON event_stream (event_type);


-- ===========================================================================================
-- global_offset_counter — the single-row counter every append reserves global offsets from.
-- Postgres: global_offset_sequence, same columns and semantics. An append runs
--   UPDATE global_offset_counter SET next_value = next_value + ? WHERE id = 1 RETURNING next_value
-- and inserts its events at the offsets up to the returned value, in the same transaction.
-- ===========================================================================================
CREATE TABLE global_offset_counter (
    id          INTEGER NOT NULL PRIMARY KEY CHECK (id = 1),   -- exactly one row
    next_value  INTEGER NOT NULL                               -- highest offset handed out so far
) STRICT;

-- The counter row must exist before the first append; an empty store starts at 0.
INSERT INTO global_offset_counter (id, next_value) VALUES (1, 0);


-- ===========================================================================================
-- snapshot_store — the latest aggregate snapshot per stream.
-- A purged stream keeps its row without a state (state_type and state_payload NULL): the row
-- carries purge_generation, which a snapshot save compares with the generation its load saw.
-- ===========================================================================================
CREATE TABLE snapshot_store (
    aggregate_type   TEXT    NOT NULL,
    aggregate_id     TEXT    NOT NULL CHECK (length(aggregate_id) <= 255),
    version          INTEGER NOT NULL,   -- stream version the snapshot was taken at
    state_type       TEXT    CHECK (length(state_type) <= 255),          -- NULL once purged
    state_payload    TEXT    CHECK (json_valid(state_payload)),          -- NULL once purged
    created_at       INTEGER,
    -- Snapshot schema version: a snapshot whose version differs from the configured one is
    -- migrated on load or discarded and rebuilt from events.
    snapshot_version INTEGER NOT NULL DEFAULT 1,
    -- How many times the snapshot purger has purged this stream's snapshot. A snapshot save writes
    -- only while the row still has the generation the command's load saw.
    purge_generation INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT snapshot_store_pkey PRIMARY KEY (aggregate_type, aggregate_id),
    CONSTRAINT snapshot_store_aggregate_type_syntax CHECK (
        aggregate_type GLOB '[a-z]*' AND aggregate_type NOT GLOB '*[^a-z0-9_]*'
        AND length(aggregate_type) <= 32),
    CONSTRAINT snapshot_store_state_present CHECK ((state_type IS NULL) = (state_payload IS NULL))
) STRICT;


-- ===========================================================================================
-- subscription_checkpoint — each projection's / subscription's checkpoint in the global stream.
-- ===========================================================================================
CREATE TABLE subscription_checkpoint (
    projection_name TEXT    NOT NULL PRIMARY KEY CHECK (length(projection_name) <= 255),
    last_offset     INTEGER NOT NULL,
    updated_at      INTEGER,
    -- Fencing token of the lease holder that last committed; always 0 on SQLite (no leadership),
    -- kept so the checkpoint contracts apply unchanged.
    epoch           INTEGER NOT NULL DEFAULT 0
) STRICT;


-- ===========================================================================================
-- projection_dead_letters — event batches a projection could not apply after its retries.
-- ===========================================================================================
CREATE TABLE projection_dead_letters (
    projection_name TEXT    NOT NULL CHECK (length(projection_name) <= 255),
    from_offset     INTEGER NOT NULL,
    to_offset       INTEGER NOT NULL,
    batch_size      INTEGER NOT NULL,
    error_type      TEXT    NOT NULL,
    error_message   TEXT,
    attempts        INTEGER NOT NULL,
    failed_at       INTEGER NOT NULL,
    PRIMARY KEY (projection_name, from_offset)
) STRICT;


-- ===========================================================================================
-- command_inbox — idempotency record per IdempotencyKey: a repeated key returns the recorded
-- result instead of re-running the command.
-- ===========================================================================================
CREATE TABLE command_inbox (
    idempotency_key TEXT    NOT NULL PRIMARY KEY CHECK (length(idempotency_key) <= 512),
    command_type    TEXT    NOT NULL CHECK (length(command_type) <= 255),
    -- The stream the key is bound to.
    aggregate_type  TEXT    NOT NULL,
    aggregate_id    TEXT    NOT NULL CHECK (length(aggregate_id) <= 255),
    final_version   INTEGER NOT NULL,
    -- Offsets of the events it appended, as a JSON array of integers (Postgres: BIGINT[]).
    global_offsets  TEXT    NOT NULL DEFAULT '[]'
                            CHECK (json_valid(global_offsets) AND json_type(global_offsets) = 'array'),
    processed_at    INTEGER NOT NULL,
    CONSTRAINT command_inbox_aggregate_type_syntax CHECK (
        aggregate_type GLOB '[a-z]*' AND aggregate_type NOT GLOB '*[^a-z0-9_]*'
        AND length(aggregate_type) <= 32)
) STRICT;

-- Serves the retention sweep (prune by age).
CREATE INDEX idx_command_inbox_processed_at ON command_inbox (processed_at);


-- ===========================================================================================
-- dead_letter_queue — commands that failed after their retries, kept for the DLQ retry runner.
-- ===========================================================================================
CREATE TABLE dead_letter_queue (
    command_id       TEXT    NOT NULL PRIMARY KEY CHECK (length(command_id) <= 255),
    command_type     TEXT    NOT NULL CHECK (length(command_type) <= 255),
    payload          TEXT    NOT NULL CHECK (json_valid(payload)),
    -- The stream the command targeted; both NULL when it had none.
    aggregate_type   TEXT,
    aggregate_id     TEXT    CHECK (length(aggregate_id) <= 255),
    error_type       TEXT    NOT NULL,
    error_message    TEXT,
    attempts         INTEGER NOT NULL,   -- attempts before the command was dead-lettered
    first_attempt_at INTEGER NOT NULL,
    published_at     INTEGER NOT NULL,
    dlq_attempts     INTEGER NOT NULL DEFAULT 0,  -- replay attempts made by the retry runner
    last_attempt_at  INTEGER,
    -- Request context of the original command, rebound on replay (NULL = none was bound).
    correlation_id   TEXT,
    user_id          TEXT,
    trace_id         TEXT,
    -- The original IdempotencyKey, so a replay and the client's own redelivery dedup on the same
    -- inbox row. NULL for commands that ran unkeyed.
    idempotency_key  TEXT    CHECK (length(idempotency_key) <= 512),
    CONSTRAINT dead_letter_queue_stream_both_or_neither CHECK ((aggregate_type IS NULL) = (aggregate_id IS NULL)),
    CONSTRAINT dead_letter_queue_aggregate_type_syntax CHECK (
        aggregate_type GLOB '[a-z]*' AND aggregate_type NOT GLOB '*[^a-z0-9_]*'
        AND length(aggregate_type) <= 32)
) STRICT;

CREATE INDEX idx_dlq_published_at ON dead_letter_queue (published_at DESC);
-- One entry per command under a key; a command is its type and its stream. Postgres declares the
-- index NULLS NOT DISTINCT so a keyed command without a stream is stored once; SQLite has no such
-- clause, so the stream columns are indexed through ifnull(..., ''). '' cannot be a real value
-- there: an aggregate type matches [a-z]..., and the id is NULL exactly when the type is.
CREATE UNIQUE INDEX idx_dlq_idempotency_key
    ON dead_letter_queue (idempotency_key, command_type, ifnull(aggregate_type, ''), ifnull(aggregate_id, ''))
    WHERE idempotency_key IS NOT NULL;


-- ===========================================================================================
-- audit_log — append-only record of command executions and audited queries.
-- ===========================================================================================
CREATE TABLE audit_log (
    id             INTEGER NOT NULL PRIMARY KEY,   -- Postgres: BIGSERIAL; see ROW IDS above
    command_id     TEXT,         -- NULL for query-origin entries; not unique (a retry adds a row)
    command_type   TEXT    NOT NULL,
    aggregate_id   TEXT,         -- NULL for query-origin entries
    -- NULL for query-origin entries and for subject entries, whose aggregate_id is a subject hash.
    aggregate_type TEXT,
    user_id        TEXT,
    occurred_at    INTEGER NOT NULL,
    outcome        TEXT    NOT NULL,
    error_message  TEXT,
    event_count    INTEGER NOT NULL DEFAULT 0,
    correlation_id TEXT,         -- ties the entry back to the originating request flow
    CONSTRAINT audit_log_aggregate_type_syntax CHECK (
        aggregate_type GLOB '[a-z]*' AND aggregate_type NOT GLOB '*[^a-z0-9_]*'
        AND length(aggregate_type) <= 32)
) STRICT;

CREATE INDEX idx_audit_command_id     ON audit_log (command_id);
CREATE INDEX idx_audit_aggregate      ON audit_log (aggregate_type, aggregate_id) WHERE aggregate_id IS NOT NULL;
CREATE INDEX idx_audit_user_id        ON audit_log (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_audit_occurred_at    ON audit_log (occurred_at DESC);
CREATE INDEX idx_audit_correlation_id ON audit_log (correlation_id) WHERE correlation_id IS NOT NULL;


-- ===========================================================================================
-- event_audit_log — opt-in, append-only record of every persisted event with its causation
-- metadata (streamrune.event-audit.enabled).
-- ===========================================================================================
CREATE TABLE event_audit_log (
    id              INTEGER NOT NULL PRIMARY KEY,   -- Postgres: BIGSERIAL; see ROW IDS above
    event_id        TEXT    NOT NULL UNIQUE,
    event_type      TEXT    NOT NULL,
    aggregate_type  TEXT    NOT NULL,
    aggregate_id    TEXT    NOT NULL CHECK (length(aggregate_id) <= 255),
    version         INTEGER NOT NULL,
    command_id      TEXT    NOT NULL,
    correlation_id  TEXT    NOT NULL,
    causation_id    TEXT,
    user_id         TEXT,
    occurred_at     INTEGER NOT NULL,
    CONSTRAINT event_audit_log_aggregate_type_syntax CHECK (
        aggregate_type GLOB '[a-z]*' AND aggregate_type NOT GLOB '*[^a-z0-9_]*'
        AND length(aggregate_type) <= 32)
) STRICT;

CREATE INDEX idx_event_audit_command_id     ON event_audit_log (command_id);
CREATE INDEX idx_event_audit_correlation_id ON event_audit_log (correlation_id);
CREATE INDEX idx_event_audit_causation_id   ON event_audit_log (causation_id) WHERE causation_id IS NOT NULL;
CREATE INDEX idx_event_audit_stream         ON event_audit_log (aggregate_type, aggregate_id, occurred_at);
CREATE INDEX idx_event_audit_user_id        ON event_audit_log (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_event_audit_occurred_at    ON event_audit_log (occurred_at DESC);


-- ===========================================================================================
-- outbox_events — transactional outbox: messages written in the append transaction and
-- delivered to external systems by the relay.
-- Status: PENDING -> IN_PROGRESS (claimed) -> DELIVERED | FAILED (retries exhausted; unresolved
-- until an operator replays it back to PENDING or skips it) -> SKIPPED (operator decision, audited).
-- Ordering mode is a property of the relay channel (OutboxOrderingMode on the store), not of the
-- row: in STRICT_PER_AGGREGATE a FAILED head blocks its stream's later rows in the claim query.
--
-- Postgres keys the table by entry_id and numbers it with a BIGSERIAL seq. SQLite can number only
-- the row id, so seq is the INTEGER PRIMARY KEY (see ROW IDS above) and entry_id is UNIQUE: lookups
-- by entry_id and the claim order by seq both keep an index.
-- ===========================================================================================
CREATE TABLE outbox_events (
    seq           INTEGER NOT NULL PRIMARY KEY,   -- strict claim order (insert order)
    entry_id      TEXT    NOT NULL UNIQUE CHECK (length(entry_id) <= 255),
    payload       TEXT    NOT NULL CHECK (json_valid(payload)),
    payload_type  TEXT    NOT NULL CHECK (length(payload_type) <= 255),
    status        TEXT    NOT NULL DEFAULT 'PENDING' CHECK (length(status) <= 50),
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    INTEGER NOT NULL,
    processed_at  INTEGER,   -- stamped on DELIVERED / FAILED; drives DELIVERED retention and the blockage-age gauge
    next_retry_at INTEGER,   -- backoff: NULL = deliverable now, future = skip until then
    -- The stream whose delivery order the consumer depends on (normally the event's own stream);
    -- both columns set, or both NULL. NULL-stream entries carry no ordering constraint.
    aggregate_type TEXT,
    aggregate_id  TEXT    CHECK (length(aggregate_id) <= 255),
    claimed_at    INTEGER,   -- lease bookkeeping of an IN_PROGRESS claim
    claimed_by    TEXT    CHECK (length(claimed_by) <= 255),
    skipped_at    INTEGER,   -- audited operator skip (FAILED -> SKIPPED): all three are set
    skipped_by    TEXT    CHECK (length(skipped_by) <= 255),   -- together, non-null iff SKIPPED
    skip_reason   TEXT,
    -- The span of the command whose append wrote the entry (W3C ids: 32 and 16 lowercase hex
    -- characters), both set or both NULL.
    trace_id      TEXT    CHECK (length(trace_id) <= 32),
    span_id       TEXT    CHECK (length(span_id) <= 16),
    CONSTRAINT outbox_events_stream_both_or_neither CHECK ((aggregate_type IS NULL) = (aggregate_id IS NULL)),
    CONSTRAINT outbox_events_aggregate_type_syntax CHECK (
        aggregate_type GLOB '[a-z]*' AND aggregate_type NOT GLOB '*[^a-z0-9_]*'
        AND length(aggregate_type) <= 32)
) STRICT;

CREATE INDEX idx_outbox_pending     ON outbox_events (seq)          WHERE status = 'PENDING';
CREATE INDEX idx_outbox_in_progress ON outbox_events (claimed_at)   WHERE status = 'IN_PROGRESS';
CREATE INDEX idx_outbox_delivered   ON outbox_events (processed_at) WHERE status = 'DELIVERED';
-- Unresolved failures: the blockage gauges and operator enumeration. Postgres INCLUDEs the stream
-- columns; SQLite has no INCLUDE, so they are trailing key columns, which covers the same reads.
CREATE INDEX idx_outbox_failed      ON outbox_events (processed_at, aggregate_type, aggregate_id) WHERE status = 'FAILED';
-- SKIPPED retention sweep.
CREATE INDEX idx_outbox_skipped     ON outbox_events (skipped_at)   WHERE status = 'SKIPPED';
-- Per-stream claim gate: the lowest-seq head per stream, over the two statuses a head can have.
CREATE INDEX idx_outbox_stream_seq ON outbox_events (aggregate_type, aggregate_id, seq)
    WHERE aggregate_id IS NOT NULL AND status IN ('PENDING', 'FAILED');
-- At most one IN_PROGRESS entry per stream, enforced by the database.
CREATE UNIQUE INDEX ux_outbox_one_inflight_per_stream ON outbox_events (aggregate_type, aggregate_id)
    WHERE status = 'IN_PROGRESS' AND aggregate_id IS NOT NULL;


-- ===========================================================================================
-- saga_state — one row per saga instance: its evolved state plus the recorded recovery facts
-- the saga runtime reads (it never infers them from row shape).
-- ===========================================================================================
CREATE TABLE saga_state (
    saga_id              TEXT    NOT NULL PRIMARY KEY CHECK (length(saga_id) <= 255),
    saga_type            TEXT    NOT NULL CHECK (length(saga_type) <= 255),
    status               TEXT    NOT NULL CHECK (length(status) <= 50),   -- SagaStatus
    state_json           TEXT    NOT NULL CHECK (json_valid(state_json)),
    version              INTEGER NOT NULL DEFAULT 1,   -- compare-and-set version
    created_at           INTEGER NOT NULL,   -- absolute start: forward timeout
    updated_at           INTEGER NOT NULL,   -- last activity: compensation timeout
    -- Durable identity of the compensation episode: the row version at which the saga entered
    -- COMPENSATING, and when. NULL = never entered a compensation episode.
    episode_version      INTEGER,
    episode_claimed_at   INTEGER,
    -- The id of the event that opened the compensation episode; NULL when it was opened with no
    -- event in hand and on a row that never entered one.
    episode_cause_event_id TEXT,
    -- 1 once the start event's forward step committed; 0 from the create-first write until then.
    genesis_applied      INTEGER NOT NULL DEFAULT 0 CHECK (genesis_applied IN (0, 1)),
    -- The status a fault replaced. NULL unless status = 'FAULTED', and NULL on a genesis-pending
    -- fault.
    pre_fault_status     TEXT    CHECK (length(pre_fault_status) <= 50),
    -- The shield: the saga owns at least one dead-letter entry; live correlated events are held
    -- while it is 1.
    dead_letter_pending  INTEGER NOT NULL DEFAULT 0 CHECK (dead_letter_pending IN (0, 1)),
    last_applied_offset  INTEGER,      -- highest global offset ever applied (live redelivery dedup)
    last_replayed_offset INTEGER,      -- last offset applied by a dead-letter replay feed
    -- The episode stamp rule: both halves of the stamp are written together or not at all, a row
    -- in a compensation episode (COMPENSATING, or FAULTED out of COMPENSATING) carries them, and a
    -- row names a cause only where it carries a stamp. Postgres' IS DISTINCT FROM is SQLite's IS NOT.
    CONSTRAINT saga_state_episode_stamped CHECK (
        (episode_version IS NULL) = (episode_claimed_at IS NULL)
        AND (episode_version IS NOT NULL
             OR (status <> 'COMPENSATING' AND pre_fault_status IS NOT 'COMPENSATING'))
        AND (episode_cause_event_id IS NULL OR episode_version IS NOT NULL)
    )
) STRICT;

-- Lookups by status, and by type and status.
CREATE INDEX idx_saga_status ON saga_state (status);
CREATE INDEX idx_saga_type_status ON saga_state (saga_type, status);
CREATE INDEX idx_saga_type_status_updated
    ON saga_state (saga_type, status, updated_at);
-- Timeout scan, forward branch (STARTED/RUNNING, on the absolute start). The status set must
-- match the query's predicate exactly for the planner to use this partial index.
CREATE INDEX idx_saga_state_created_timeout
    ON saga_state (saga_type, created_at)
    WHERE status IN ('STARTED', 'RUNNING');


-- ===========================================================================================
-- saga_dead_letters — quarantine for poison saga events, so one bad event does not block the
-- subscription. Metadata only: the event payload is deliberately NOT copied here; read it at
-- event_offset.
-- ===========================================================================================
CREATE TABLE saga_dead_letters (
    saga_id                 TEXT    CHECK (length(saga_id) <= 255),   -- NULL when the event could not be routed
    saga_type               TEXT    NOT NULL CHECK (length(saga_type) <= 255),
    event_offset            INTEGER NOT NULL,
    event_type              TEXT    NOT NULL CHECK (length(event_type) <= 255),
    error_type              TEXT    NOT NULL,
    error_message           TEXT,
    faulted_at              INTEGER NOT NULL,   -- refreshed by every re-quarantine
    -- First quarantine of this (saga_id, event_offset): kept by the re-quarantine upsert; anchors
    -- retention.
    first_faulted_at        INTEGER NOT NULL,
    -- Resolved target saga of a null-saga entry, stamped by the replayer. Kept by the upsert.
    target_saga_id          TEXT    CHECK (length(target_saga_id) <= 255),
    -- First replay attempt of this entry: written once, kept by the re-quarantine upsert.
    first_replay_started_at INTEGER,
    -- Idempotent re-quarantine upsert key. NULL semantics as in Postgres: null-saga rows at one
    -- offset are distinct.
    CONSTRAINT saga_dead_letters_uniq UNIQUE (saga_id, event_offset)
) STRICT;

CREATE INDEX idx_saga_dead_letters_saga
    ON saga_dead_letters (saga_id)
    WHERE saga_id IS NOT NULL;
-- findAll / findBySaga order by faulted_at DESC.
CREATE INDEX idx_saga_dead_letters_faulted_at
    ON saga_dead_letters (faulted_at DESC);
-- Retention sweep: prunes and orders by the first-fault anchor.
CREATE INDEX idx_saga_dead_letters_retention
    ON saga_dead_letters (first_faulted_at);
