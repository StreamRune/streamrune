-- StreamRune event-store schema.
--
-- Applied by PostgresEventStoreFactory.initializeSchema() from classpath:db/streamrune-migration and
-- recorded in its own Flyway history table, flyway_schema_history_streamrune, so it never shares a
-- version namespace with the application's own migrations. Every object is created UNQUALIFIED: it
-- lands in the connection's current schema, which is what lets a store run in a non-default schema
-- (currentSchema / search_path).
--
-- Once released this file is frozen; later schema changes ship as new versioned migrations.


-- ===========================================================================================
-- event_stream — the append-only event log, the single source of truth.
-- ===========================================================================================
CREATE TABLE event_stream (
    -- Gapless, commit-ordered position in the global stream. No DEFAULT on purpose: the append
    -- reserves it from global_offset_sequence inside its own transaction, so a rolled-back append
    -- never leaves a hole.
    global_offset  BIGINT       PRIMARY KEY,
    -- The stream key: the aggregate type the decider was registered under and the id the command's
    -- extractor returned, verbatim. No column stores the composed stream id; Java, JSON and logs
    -- show the pair as "<aggregate_type>:<aggregate_id>".
    aggregate_type VARCHAR(32)  NOT NULL,
    aggregate_id   VARCHAR(255) NOT NULL,
    version        BIGINT       NOT NULL,   -- per-stream version (optimistic concurrency)
    event_type     VARCHAR(255) NOT NULL,
    payload        JSONB        NOT NULL,   -- @Encrypted fields are ciphertext here
    metadata       JSONB        NOT NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    schema_version INT          NOT NULL DEFAULT 1,   -- event schema version read by upcasting
    -- One event per (stream, version): a concurrent append at the same version fails here. Its
    -- leading column also serves every per-type scan as an index prefix.
    CONSTRAINT event_stream_stream_version_key UNIQUE (aggregate_type, aggregate_id, version),
    CONSTRAINT event_stream_aggregate_type_syntax CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
);

-- Reads events by type (upcasting and type-filtered queries).
CREATE INDEX idx_event_type ON event_stream (event_type);


-- ===========================================================================================
-- global_offset_sequence — the single-row counter every append reserves global offsets from.
-- ===========================================================================================
CREATE TABLE global_offset_sequence (
    id          smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),   -- exactly one row
    next_value  bigint   NOT NULL                                -- highest offset handed out so far
);

-- The counter row must exist before the first append; an empty store starts at 0.
INSERT INTO global_offset_sequence (id, next_value) VALUES (1, 0);


-- ===========================================================================================
-- snapshot_store — the latest aggregate snapshot per stream.
-- ===========================================================================================
CREATE TABLE snapshot_store (
    aggregate_type   VARCHAR(32)  NOT NULL,
    aggregate_id     VARCHAR(255) NOT NULL,
    version          BIGINT       NOT NULL,   -- stream version the snapshot was taken at
    state_type       VARCHAR(255) NOT NULL,
    state_payload    JSONB        NOT NULL,
    created_at       TIMESTAMPTZ  DEFAULT NOW(),
    -- Snapshot schema version: a snapshot whose version differs from the configured one is
    -- migrated on load or discarded and rebuilt from events.
    snapshot_version INT          NOT NULL DEFAULT 1,
    CONSTRAINT snapshot_store_pkey PRIMARY KEY (aggregate_type, aggregate_id),
    CONSTRAINT snapshot_store_aggregate_type_syntax CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
);


-- ===========================================================================================
-- projection_offset — each projection's / subscription's checkpoint in the global stream.
-- ===========================================================================================
CREATE TABLE projection_offset (
    projection_name VARCHAR(255) PRIMARY KEY,
    last_offset     BIGINT       NOT NULL,
    updated_at      TIMESTAMPTZ  DEFAULT NOW(),
    -- Fencing token of the lease holder that last committed. A checkpoint commit carrying a lower
    -- epoch is rejected; 0 = unfenced (single-node, no leadership).
    epoch           BIGINT       NOT NULL DEFAULT 0
);


-- ===========================================================================================
-- subscription_leases — DB-clocked, epoch-fenced leadership leases (one leader per consumer).
-- ===========================================================================================
CREATE TABLE subscription_leases (
    consumer_name TEXT PRIMARY KEY,
    holder_id     TEXT        NOT NULL,   -- unique per replica process (UUID at startup)
    epoch         BIGINT      NOT NULL,   -- monotonic fencing token, +1 on every takeover
    lease_until   TIMESTAMPTZ NOT NULL    -- expiry, on the database clock
);


-- ===========================================================================================
-- projection_dead_letters — event batches a projection could not apply after its retries.
-- ===========================================================================================
CREATE TABLE projection_dead_letters (
    projection_name VARCHAR(255) NOT NULL,
    from_offset     BIGINT NOT NULL,
    to_offset       BIGINT NOT NULL,
    batch_size      INT NOT NULL,
    error_type      VARCHAR(512) NOT NULL,
    error_message   TEXT,
    attempts        INT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (projection_name, from_offset)
);


-- ===========================================================================================
-- command_inbox — idempotency record per IdempotencyKey: a repeated key returns the recorded
-- result instead of re-running the command.
-- ===========================================================================================
CREATE TABLE command_inbox (
    idempotency_key VARCHAR(512) PRIMARY KEY,
    command_type    VARCHAR(255) NOT NULL,
    -- The stream the key is bound to.
    aggregate_type  VARCHAR(32)  NOT NULL,
    aggregate_id    VARCHAR(255) NOT NULL,
    final_version   BIGINT       NOT NULL,
    global_offsets  BIGINT[]     NOT NULL DEFAULT '{}',   -- offsets of the events it appended
    processed_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT command_inbox_aggregate_type_syntax CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
);

-- Serves the retention sweep (prune by age).
CREATE INDEX idx_command_inbox_processed_at ON command_inbox (processed_at);


-- ===========================================================================================
-- dead_letter_queue — commands that failed after their retries, kept for the DLQ retry runner.
-- ===========================================================================================
CREATE TABLE dead_letter_queue (
    command_id       VARCHAR(255) PRIMARY KEY,   -- CommandId values are strings, not UUIDs
    command_type     VARCHAR(255) NOT NULL,
    payload          JSONB       NOT NULL,
    -- The stream the command targeted; both NULL when it had none.
    aggregate_type   VARCHAR(32),
    aggregate_id     VARCHAR(255),
    error_type       VARCHAR(255) NOT NULL,
    error_message    TEXT,
    attempts         INT         NOT NULL,   -- attempts before the command was dead-lettered
    first_attempt_at TIMESTAMPTZ NOT NULL,
    published_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dlq_attempts     INT         NOT NULL DEFAULT 0,  -- replay attempts made by the retry runner
    last_attempt_at  TIMESTAMPTZ,
    -- Request context of the original command, rebound on replay (NULL = none was bound): events
    -- keep their correlation and authorization re-evaluates as the original user.
    correlation_id   VARCHAR(255),
    user_id          VARCHAR(255),
    trace_id         VARCHAR(255),
    -- The original IdempotencyKey, so a replay and the client's own redelivery dedup on the same
    -- inbox row. NULL for commands that ran unkeyed (replayed under a synthetic key).
    idempotency_key  VARCHAR(512),
    CONSTRAINT dead_letter_queue_stream_both_or_neither CHECK ((aggregate_type IS NULL) = (aggregate_id IS NULL)),
    CONSTRAINT dead_letter_queue_aggregate_type_syntax CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
);

CREATE INDEX idx_dlq_published_at ON dead_letter_queue (published_at DESC);


-- ===========================================================================================
-- audit_log — append-only record of command executions and audited queries.
-- ===========================================================================================
CREATE TABLE audit_log (
    id             BIGSERIAL   PRIMARY KEY,
    command_id     TEXT,         -- NULL for query-origin entries; not unique (a retry adds a row)
    command_type   TEXT        NOT NULL,
    aggregate_id   TEXT,         -- NULL for query-origin entries
    -- NULL for query-origin entries and for subject entries, whose aggregate_id is a subject hash.
    aggregate_type VARCHAR(32),
    user_id        TEXT,
    occurred_at    TIMESTAMPTZ NOT NULL,
    outcome        TEXT        NOT NULL,
    error_message  TEXT,
    event_count    INT         NOT NULL DEFAULT 0,
    correlation_id TEXT,         -- ties the entry back to the originating request flow
    CONSTRAINT audit_log_aggregate_type_syntax CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
);

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
    id              BIGSERIAL    PRIMARY KEY,
    event_id        TEXT         NOT NULL UNIQUE,
    event_type      TEXT         NOT NULL,
    aggregate_type  VARCHAR(32)  NOT NULL,
    aggregate_id    VARCHAR(255) NOT NULL,
    version         BIGINT       NOT NULL,
    command_id      TEXT         NOT NULL,
    correlation_id  TEXT         NOT NULL,
    causation_id    TEXT,
    user_id         TEXT,
    occurred_at     TIMESTAMPTZ  NOT NULL,
    CONSTRAINT event_audit_log_aggregate_type_syntax CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
);

CREATE INDEX idx_event_audit_command_id     ON event_audit_log (command_id);
CREATE INDEX idx_event_audit_correlation_id ON event_audit_log (correlation_id);
CREATE INDEX idx_event_audit_causation_id   ON event_audit_log (causation_id) WHERE causation_id IS NOT NULL;
CREATE INDEX idx_event_audit_stream         ON event_audit_log (aggregate_type, aggregate_id, occurred_at);
CREATE INDEX idx_event_audit_user_id        ON event_audit_log (user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_event_audit_occurred_at    ON event_audit_log (occurred_at DESC);


-- ===========================================================================================
-- outbox_events — transactional outbox: messages written in the append transaction and
-- delivered to external systems by competing relay instances.
-- Status: PENDING -> IN_PROGRESS (claimed) -> DELIVERED | FAILED (retries exhausted; unresolved
-- until an operator replays it back to PENDING or skips it) -> SKIPPED (operator decision, audited).
-- Ordering mode is a property of the relay channel (OutboxOrderingMode on the store), not of the
-- row: in STRICT_PER_AGGREGATE a FAILED head blocks its stream's later rows in the claim query.
-- ===========================================================================================
CREATE TABLE outbox_events (
    entry_id      VARCHAR(255) NOT NULL PRIMARY KEY,
    payload       JSONB        NOT NULL,
    payload_type  VARCHAR(255) NOT NULL,
    status        VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    attempts      INT          NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed_at  TIMESTAMPTZ,   -- stamped on DELIVERED / FAILED; drives DELIVERED retention and the blockage-age gauge
    next_retry_at TIMESTAMPTZ,   -- backoff: NULL = deliverable now, future = skip until then
    seq           BIGSERIAL,     -- strict claim order (created_at ties within one transaction)
    -- The stream whose delivery order the consumer depends on (normally the event's own stream);
    -- both columns set, or both NULL. Mandatory on a STRICT_PER_AGGREGATE channel (enforced by
    -- the store). NULL-stream entries carry no ordering constraint.
    aggregate_type VARCHAR(32),
    aggregate_id  VARCHAR(255),
    claimed_at    TIMESTAMPTZ,   -- lease bookkeeping of an IN_PROGRESS claim; stale claims are
    claimed_by    VARCHAR(255),  -- reset to PENDING by other relays
    skipped_at    TIMESTAMPTZ,   -- audited operator skip (FAILED -> SKIPPED): all three are set
    skipped_by    VARCHAR(255),  -- together in one statement and are non-null iff status = 'SKIPPED'
    skip_reason   TEXT,
    CONSTRAINT outbox_events_stream_both_or_neither CHECK ((aggregate_type IS NULL) = (aggregate_id IS NULL)),
    CONSTRAINT outbox_events_aggregate_type_syntax CHECK (aggregate_type ~ '^[a-z][a-z0-9_]{0,31}$')
);

CREATE INDEX idx_outbox_pending     ON outbox_events (seq)          WHERE status = 'PENDING';
CREATE INDEX idx_outbox_in_progress ON outbox_events (claimed_at)   WHERE status = 'IN_PROGRESS';
CREATE INDEX idx_outbox_delivered   ON outbox_events (processed_at) WHERE status = 'DELIVERED';
-- Unresolved failures: the blockage gauges (count(DISTINCT (aggregate_type, aggregate_id))
-- FILTER (WHERE aggregate_id IS NOT NULL) — the filter keeps legacy (NULL, NULL) rows out of the
-- blocked-stream count, since a row value of NULLs is a non-null composite — min(processed_at),
-- count(*) FILTER (WHERE aggregate_id IS NULL)) as one index-only scan, and operator
-- enumeration. FAILED rows are never swept by retention.
CREATE INDEX idx_outbox_failed      ON outbox_events (processed_at) INCLUDE (aggregate_type, aggregate_id) WHERE status = 'FAILED';
-- SKIPPED retention sweep.
CREATE INDEX idx_outbox_skipped     ON outbox_events (skipped_at)   WHERE status = 'SKIPPED';
-- Per-stream claim gate: the lowest-seq head per stream. Partial over the two statuses a head can
-- have (PENDING; FAILED in strict mode) so both claim statements read it in
-- (aggregate_type, aggregate_id, seq) order with no sort; DELIVERED/SKIPPED rows are not in it.
-- The in-flight probe is served by ux_outbox_one_inflight_per_stream. NULL-stream entries are
-- claimed through idx_outbox_pending.
CREATE INDEX idx_outbox_stream_seq ON outbox_events (aggregate_type, aggregate_id, seq)
    WHERE aggregate_id IS NOT NULL AND status IN ('PENDING', 'FAILED');
-- At most one IN_PROGRESS entry per stream, enforced by the database. Also the IN_PROGRESS probe
-- index of the claim's NOT EXISTS.
CREATE UNIQUE INDEX ux_outbox_one_inflight_per_stream ON outbox_events (aggregate_type, aggregate_id)
    WHERE status = 'IN_PROGRESS' AND aggregate_id IS NOT NULL;


-- ===========================================================================================
-- saga_state — one row per saga instance: its evolved state plus the recorded recovery facts
-- the saga runtime reads (it never infers them from row shape).
-- ===========================================================================================
CREATE TABLE saga_state (
    saga_id              VARCHAR(255) NOT NULL PRIMARY KEY,
    saga_type            VARCHAR(255) NOT NULL,
    status               VARCHAR(50)  NOT NULL,   -- SagaStatus
    state_json           JSONB        NOT NULL,
    version              BIGINT       NOT NULL DEFAULT 1,   -- compare-and-set version
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),   -- absolute start: forward timeout
    updated_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),   -- last activity: compensation timeout
    -- Durable identity of the compensation episode: the row version at which the saga entered
    -- COMPENSATING (a claim, or a forward write persisting an evolved COMPENSATING status), and
    -- when. Written by that one entering statement, untouched by fault / unfault / terminal writes,
    -- so a replayed resume re-derives the same compensation idempotency keys and the key-age guards
    -- measure from the same instant. NULL = never entered a compensation episode.
    episode_version      BIGINT,
    episode_claimed_at   TIMESTAMPTZ,
    -- TRUE once the start event's forward step (evolve -> handle -> dispatch loop) committed;
    -- FALSE from the create-first write until then.
    genesis_applied      BOOLEAN      NOT NULL DEFAULT FALSE,
    -- The status a fault replaced. NULL unless status = 'FAULTED', and NULL on a genesis-pending
    -- fault (a replay re-evolves from the initial state).
    pre_fault_status     VARCHAR(50),
    -- The shield: the saga owns at least one dead-letter entry (named, or as the resolved target
    -- of a null-saga entry); live correlated events are held while TRUE.
    dead_letter_pending  BOOLEAN      NOT NULL DEFAULT FALSE,
    last_applied_offset  BIGINT,      -- highest global offset ever applied (live redelivery dedup)
    last_replayed_offset BIGINT,      -- last offset applied by a dead-letter replay feed
    -- The episode stamp rule: both halves of the stamp are written together or not at
    -- all, and a row in a compensation episode — COMPENSATING, or FAULTED out of COMPENSATING —
    -- carries them. The saga runtime derives the compensation idempotency keys and the key-age
    -- anchors from these two columns with no fallback and refuses a row that lacks them; this
    -- constraint makes such a row impossible to write in the first place.
    CONSTRAINT saga_state_episode_stamped CHECK (
        (episode_version IS NULL) = (episode_claimed_at IS NULL)
        AND (episode_version IS NOT NULL
             OR (status <> 'COMPENSATING' AND pre_fault_status IS DISTINCT FROM 'COMPENSATING'))
    )
);

-- Lookups by status, and by type and status.
CREATE INDEX idx_saga_status ON saga_state (status);
CREATE INDEX idx_saga_type_status ON saga_state (saga_type, status);
CREATE INDEX idx_saga_type_status_updated
    ON saga_state (saga_type, status, updated_at);
-- Timeout scan, forward branch (STARTED/RUNNING, on the absolute start). The status set must
-- match the query's predicate exactly for the planner to use this partial index. The scan's
-- COMPENSATING re-pick branch is served by idx_saga_type_status_updated.
CREATE INDEX idx_saga_state_created_timeout
    ON saga_state (saga_type, created_at)
    WHERE status IN ('STARTED', 'RUNNING');


-- ===========================================================================================
-- saga_dead_letters — quarantine for poison saga events, so one bad event does not block the
-- subscription. Metadata only: the event payload is deliberately NOT copied here — event_stream
-- stays the single crypto-governed, shreddable copy; read it at event_offset.
-- ===========================================================================================
CREATE TABLE saga_dead_letters (
    saga_id                 VARCHAR(255),   -- NULL when the event could not be routed to a saga
    saga_type               VARCHAR(255) NOT NULL,   -- the quarantining runner's saga type
    event_offset            BIGINT       NOT NULL,
    event_type              VARCHAR(255) NOT NULL,
    error_type              VARCHAR(512) NOT NULL,
    error_message           TEXT,
    faulted_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW(),   -- refreshed by every re-quarantine
    -- First quarantine of this (saga_id, event_offset): kept by the re-quarantine upsert; anchors
    -- retention.
    first_faulted_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- Resolved target saga of a null-saga entry, stamped by the replayer; lets retention protect
    -- the entry while that saga is still FAULTED. Kept by the re-quarantine upsert.
    target_saga_id          VARCHAR(255),
    -- First replay attempt of this entry: written once (establish-only), kept by the re-quarantine
    -- upsert; anchors the replayer's command-inbox key-age guard.
    first_replay_started_at TIMESTAMPTZ,
    -- Idempotent re-quarantine upsert key. SQL NULL semantics: null-saga rows at one offset are
    -- distinct (an accepted edge case for rare routing failures).
    CONSTRAINT saga_dead_letters_uniq UNIQUE (saga_id, event_offset)
);

CREATE INDEX idx_saga_dead_letters_saga
    ON saga_dead_letters (saga_id)
    WHERE saga_id IS NOT NULL;
-- findAll / findBySaga order by faulted_at DESC.
CREATE INDEX idx_saga_dead_letters_faulted_at
    ON saga_dead_letters (faulted_at DESC);
-- Retention sweep: prunes and orders by the first-fault anchor.
CREATE INDEX idx_saga_dead_letters_retention
    ON saga_dead_letters (first_faulted_at);
