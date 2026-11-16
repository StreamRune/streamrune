-- StreamRune read-model series — baseline.
--
-- Shipped under db/readmodel-migration (NOT the default db/migration) as a SEPARATE Flyway HISTORY,
-- like db/crypto-migration: it is applied to the database that holds the read models, which need
-- not be the event-store database.

-- ===========================================================================================
-- readmodel_checkpoint — each projection's checkpoint, stored with its read model.
-- ===========================================================================================
CREATE TABLE readmodel_checkpoint (
    projection_name VARCHAR(255) PRIMARY KEY,
    last_offset     BIGINT       NOT NULL,
    updated_at      TIMESTAMPTZ  DEFAULT NOW(),
    -- Fencing token of the lease holder that last committed. A checkpoint commit carrying a lower
    -- epoch is rejected; 0 = unfenced (single-node, no leadership).
    epoch           BIGINT       NOT NULL DEFAULT 0
);
