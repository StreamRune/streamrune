-- StreamRune read-model series for SQLite — baseline.
--
-- Applied by SqliteDatabase.migrate(SqliteSchemaSeries.READ_MODEL) from db/sqlite-readmodel-migration
-- and recorded in its own Flyway history table, flyway_schema_history_readmodel: it is applied to
-- the database that holds the read models, which need not be the event-store database. It mirrors
-- the Postgres read-model baseline (db/readmodel-migration): same table and columns.
--
-- Once released this file is frozen; later schema changes ship as new versioned migrations.
--
-- The type translation and the timestamp representation are those of the event-store baseline
-- (db/sqlite-migration/V001__baseline.sql): a STRICT table, VARCHAR(n) as TEXT with
-- CHECK (length(col) <= n), BIGINT as INTEGER, and TIMESTAMPTZ as INTEGER microseconds since
-- 1970-01-01T00:00:00Z (UTC) with no default, stamped by the application Clock where Postgres
-- defaults it to NOW().

-- ===========================================================================================
-- readmodel_checkpoint — each projection's checkpoint, stored with its read model.
-- ===========================================================================================
CREATE TABLE readmodel_checkpoint (
    projection_name TEXT    NOT NULL PRIMARY KEY CHECK (length(projection_name) <= 255),
    last_offset     INTEGER NOT NULL,
    updated_at      INTEGER,
    -- Fencing token of the lease holder that last committed. A checkpoint commit carrying a lower
    -- epoch is rejected; 0 = unfenced (single-node, no leadership), which SQLite always is.
    epoch           INTEGER NOT NULL DEFAULT 0
) STRICT;
