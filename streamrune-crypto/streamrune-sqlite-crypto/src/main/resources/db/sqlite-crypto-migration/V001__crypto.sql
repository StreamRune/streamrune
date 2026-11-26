-- StreamRune crypto series for SQLite: per-subject keys and GDPR crypto-shredding tombstones.
--
-- Applied by SqliteDatabase.migrate(SqliteSchemaSeries.CRYPTO) from db/sqlite-crypto-migration and
-- recorded in its own Flyway history table, flyway_schema_history_crypto. It mirrors the Postgres
-- crypto baseline (db/crypto-migration/V001__crypto_baseline.sql of streamrune-postgres-crypto):
-- same tables and columns.
--
-- Every table is created only when ABSENT (IF NOT EXISTS), as on Postgres: an application may
-- provision some of these tables itself (a tombstone-only Vault or AWS KMS deployment, a
-- custom-named key table next to them). Missing tables are created; a present table is left
-- untouched and validation reports any column it lacks.
--
-- Once released this file is frozen; later schema changes ship as new versioned migrations.
--
-- The type translation and the timestamp representation are those of the event-store baseline
-- (db/sqlite-migration/V001__baseline.sql of streamrune-sqlite): STRICT tables, VARCHAR(n) as TEXT
-- with CHECK (length(col) <= n), SMALLINT as INTEGER with its range as a CHECK, BYTEA as BLOB, and
-- TIMESTAMPTZ as INTEGER microseconds since 1970-01-01T00:00:00Z (UTC) with no default, stamped by
-- the application Clock where Postgres defaults it to NOW().


-- ===========================================================================================
-- encryption_keys: the per-subject AES-256 key of SqliteCryptoEngine (its default key-table
-- name; configurable). Deleting a subject's row crypto-shreds every @Encrypted value of it.
-- ===========================================================================================
CREATE TABLE IF NOT EXISTS encryption_keys (
    -- SHA-256 hex of the subject id, never the raw value
    subject_id  TEXT    NOT NULL PRIMARY KEY CHECK (length(subject_id) <= 255),
    key_bytes   BLOB    NOT NULL,
    created_at  INTEGER NOT NULL,
    -- Generation of the CURRENT key. Every ciphertext carries its generation in the key-version
    -- header byte: equal -> decrypt; lower -> that key was destroyed by an erasure -> [REDACTED].
    -- No default: every key insert names the generation it minted.
    key_version INTEGER NOT NULL CHECK (key_version BETWEEN -32768 AND 32767)
) STRICT;


-- ===========================================================================================
-- forgotten_subjects: erasure tombstones, shared by every crypto engine on this database. A row
-- marks the subject FORGOTTEN, so a later encrypt fails with SubjectForgottenException instead of
-- minting a fresh key for an erased subject; it survives the key DELETE. reinstate removes it.
-- ===========================================================================================
CREATE TABLE IF NOT EXISTS forgotten_subjects (
    -- SHA-256 hex of the subject id
    subject_id     TEXT    NOT NULL PRIMARY KEY CHECK (length(subject_id) <= 255),
    forgotten_at   INTEGER NOT NULL,
    -- Backend-sourced creation instant of the erased key (written by the Vault engine), so
    -- reinstate can prove the key's identity without comparing different hosts' clocks.
    -- NULL = identity unprovable: reinstate fails closed.
    key_created_at INTEGER
) STRICT;


-- ===========================================================================================
-- erased_key_generations: per subject, the highest key generation an erasure ever destroyed;
-- the next mint uses max_erased_version + 1. It deliberately SURVIVES reinstate, which is what
-- keeps generations monotonic across erase/reinstate cycles. No personal data, no key material.
-- ===========================================================================================
CREATE TABLE IF NOT EXISTS erased_key_generations (
    -- SHA-256 hex of the subject id
    subject_id         TEXT    NOT NULL PRIMARY KEY CHECK (length(subject_id) <= 255),
    max_erased_version INTEGER NOT NULL CHECK (max_erased_version BETWEEN -32768 AND 32767)
) STRICT;
