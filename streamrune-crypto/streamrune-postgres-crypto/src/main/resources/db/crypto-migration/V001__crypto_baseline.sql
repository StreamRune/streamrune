-- StreamRune crypto schema: per-subject keys and GDPR crypto-shredding tombstones.
--
-- Shipped under db/crypto-migration (NOT the default db/migration) as a SEPARATE Flyway HISTORY,
-- never as a second location on your main configuration: this series and the event-store series
-- both start at V001 and Flyway locations share ONE version namespace, so appending this location
-- to an existing flyway.locations aborts with "Found more than one migration with version 1".
-- Apply it as its own Flyway run against flyway_schema_history_crypto (what
-- PostgresEventStoreFactory.initializeSchema does), or copy the DDL below into your own series.
-- See streamrune-postgres-crypto/README.md. Every object is created UNQUALIFIED, in the
-- connection's current schema.
--
-- Every table is created only when ABSENT (IF NOT EXISTS): applications may provision some or all
-- of these tables themselves (the README's copy-the-DDL route, a tombstone-only KMS/Vault
-- deployment, a custom-named key table next to them), and PostgresEventStoreFactory always applies
-- this series from baseline 0. Missing tables are created; a present table is left untouched and
-- SchemaValidator reports any column it lacks. Later crypto migrations must be idempotent and
-- presence-tolerant for the same reason.
--
-- Once released this file is frozen; later schema changes ship as new versioned migrations.

-- Quiet PostgreSQL's NOTICE for each table that already exists: on a database where the application
-- provisioned some of these tables itself, CREATE TABLE IF NOT EXISTS reports "relation already
-- exists, skipping", which Flyway logs at WARN and which reads like a failure. SET LOCAL lasts only
-- for this migration's transaction. Not part of the copy-the-DDL block in the README.
SET LOCAL client_min_messages = warning;


-- ===========================================================================================
-- encryption_keys — the per-subject AES-256 key of PostgresCryptoEngine (its default key-table
-- name; configurable). Deleting a subject's row crypto-shreds every @Encrypted value of it.
-- ===========================================================================================
CREATE TABLE IF NOT EXISTS encryption_keys (
    subject_id  VARCHAR(255) PRIMARY KEY,   -- SHA-256 hex of the subject id, never the raw value
    key_bytes   BYTEA        NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- Generation of the CURRENT key. Every ciphertext carries its generation in the key-version
    -- header byte: equal -> decrypt; lower -> that key was destroyed by an erasure -> [REDACTED].
    -- No default: every key insert names the generation it minted.
    key_version SMALLINT     NOT NULL
);


-- ===========================================================================================
-- forgotten_subjects — erasure tombstones, shared by every crypto backend. A row marks the
-- subject FORGOTTEN, so a later encrypt fails with SubjectForgottenException instead of minting a
-- fresh key for an erased subject; it survives the key DELETE. reinstate removes it.
-- ===========================================================================================
CREATE TABLE IF NOT EXISTS forgotten_subjects (
    subject_id     VARCHAR(255) PRIMARY KEY,   -- SHA-256 hex of the subject id
    forgotten_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- Backend-sourced creation instant of the erased key (written by the Vault backend), so
    -- reinstate can prove the key's identity without comparing different hosts' clocks.
    -- NULL = identity unprovable: reinstate fails closed.
    key_created_at TIMESTAMPTZ
);


-- ===========================================================================================
-- erased_key_generations — per subject, the highest key generation an erasure ever destroyed;
-- the next mint uses max_erased_version + 1. It deliberately SURVIVES reinstate, which is what
-- keeps generations monotonic across erase/reinstate cycles. No personal data, no key material.
-- ===========================================================================================
CREATE TABLE IF NOT EXISTS erased_key_generations (
    subject_id          VARCHAR(255) PRIMARY KEY,   -- SHA-256 hex of the subject id
    max_erased_version  SMALLINT     NOT NULL
);
