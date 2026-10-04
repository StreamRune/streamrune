# StreamRune Postgres Crypto

PostgreSQL-backed `CryptoEngine` implementation. Encryption keys are stored in a dedicated table alongside the application's existing data.

## Prerequisites

Three tables must exist before the engine can operate — the engine never auto-creates them. Without `encryption_keys`, the first `encrypt(...)` fails with a `CryptoOperationException` caused by PostgreSQL's `relation "encryption_keys" does not exist`. The `forgotten_subjects` table backs terminal crypto-shredding (see *Terminal Erasure*) and is required by `deleteKey`/`encrypt`. The `erased_key_generations` table records the highest key generation each erasure destroyed and is required by `deleteKey` and by every `encrypt` that mints a key — without it erasure itself fails.

All three are reported by `requiredCryptoTables()`, so a self-managed schema missing any of them fails **fast at startup** (`SchemaValidationException` from `PostgresEventStoreFactory.create()`), not silently on the first erasure.

> `forgotten_subjects` is **shared by every crypto backend** and is validated as a whole, not per engine. `SchemaValidator` requires all of `subject_id`, `forgotten_at` and `key_created_at` on it — even though only the *vault* backend reads the last two. A shared table cannot be validated per engine without letting a partial one boot green here and then fail the vault backend's erasure (a GDPR operation) at first use, so the check fails closed. Provision the table in full whichever backend you run.

The schema ships in this artifact as one Flyway migration, `db/crypto-migration/V001__crypto_baseline.sql` (the `db/crypto-migration/` directory of the jar is the authoritative list). It creates all three tables, each **only when it is absent** (`CREATE TABLE IF NOT EXISTS`), so it can be applied to a database where you created some of them yourself; it never alters a table that already exists.

## Applying the crypto migrations

They live outside Flyway's default `db/migration` location because they are a **separate Flyway
_history_** — not a second location on your main Flyway configuration.

> **StreamRune never uses Flyway's defaults.** `classpath:db/migration` and `flyway_schema_history`
> belong to *your* application. The framework ships **two** namespaced series, each on
> its own history table:
>
> | Series | Location | History table |
> |---|---|---|
> | Event store (`streamrune-postgres`) | `classpath:db/streamrune-migration` | `flyway_schema_history_streamrune` |
> | Crypto (`streamrune-postgres-crypto`, this artifact) | `classpath:db/crypto-migration` | `flyway_schema_history_crypto` |
>
> Every `db/migration` below therefore refers to **your own** migrations, not to StreamRune's.

> **Do not do this — it is a guaranteed boot failure:**
>
> ```properties
> # WRONG
> flyway.locations=classpath:db/migration,classpath:db/crypto-migration
> ```
>
> Flyway locations share **one** version namespace. Your own series almost certainly starts at
> `V1`/`V001` and so does the crypto series, so a single Flyway configuration resolving both aborts
> with `FlywayException: Found more than one migration with version 1`, before any StreamRune code
> runs. The same collision is why the event-store series lives off the default location too: adding
> either StreamRune series to your own `locations` is always wrong.

The crypto series must be applied by its own Flyway run whose schema-history table is
`flyway_schema_history_crypto`. That is exactly what `PostgresEventStoreFactory.initializeSchema()`
does when schema auto-initialization is on (the default) — **if you let StreamRune manage the
schema there is nothing to configure**; it provisions the crypto tables whenever the active
`CryptoEngine` reports it needs them.

If you manage the schema yourself, pick one of the following.

### Out of band (Flyway CLI / Maven / Gradle plugin)

Run Flyway once per history — your own, then StreamRune's two. Each run differs only in `locations`
and `table`:

```bash
# 1. your application's own migrations (Flyway's defaults — untouched by StreamRune)
flyway -url=jdbc:postgresql://db:5432/app -user=app \
       -locations=classpath:db/migration migrate

# 2. StreamRune's event store — skip this one if you leave
#    streamrune.event-store.schema.auto-initialize=true (the default)
flyway -url=jdbc:postgresql://db:5432/app -user=app \
       -locations=classpath:db/streamrune-migration \
       -table=flyway_schema_history_streamrune migrate

# 3. this artifact's crypto series
flyway -url=jdbc:postgresql://db:5432/app -user=app \
       -locations=classpath:db/crypto-migration \
       -table=flyway_schema_history_crypto migrate
```

On a non-empty schema that has no `flyway_schema_history_crypto` yet (your application's own tables,
or crypto tables you created yourself), add `-baselineOnMigrate=true -baselineVersion=0` — the
configuration `PostgresEventStoreFactory.initializeSchema()` always uses. Baseline 0 skips nothing:
the baseline creates whichever crypto tables are missing and leaves existing ones as they are. The
recipes below set the same two options; they are inert once `flyway_schema_history_crypto` exists.

### Spring Boot

Spring Boot models exactly **one** Flyway configuration (`spring.flyway.*`), so there is no
property-only second history — and declaring your own `Flyway`-typed bean would trip
`@ConditionalOnMissingBean(Flyway.class)` on `FlywayAutoConfiguration` and disable auto-migration
entirely. Use the supported `FlywayMigrationStrategy` hook, which *replaces* the default
`migrate()` call (so it must invoke `flyway.migrate()` itself):

```java
// org.springframework.boot.flyway.autoconfigure.FlywayMigrationStrategy (Spring Boot 4)
@Bean
FlywayMigrationStrategy cryptoMigrations(DataSource dataSource) {
    return flyway -> {
        flyway.migrate(); // YOUR history: classpath:db/migration on flyway_schema_history
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/crypto-migration")
            .table("flyway_schema_history_crypto")
            .baselineOnMigrate(true)   // the schema already holds your tables
            .baselineVersion("0")      // skips nothing: the whole crypto series still applies
            .load()
            .migrate();
    };
}
```

### Quarkus

`quarkus.flyway.*` is keyed by **datasource name**, so a second history needs a second named
datasource pointing at the same database:

```properties
# your own migrations, on Flyway's defaults
quarkus.flyway.locations=db/migration
quarkus.flyway.migrate-at-start=true

quarkus.datasource."crypto".db-kind=postgresql
quarkus.datasource."crypto".jdbc.url=${quarkus.datasource.jdbc.url}
quarkus.datasource."crypto".username=${quarkus.datasource.username}
quarkus.datasource."crypto".password=${quarkus.datasource.password}
quarkus.flyway."crypto".locations=db/crypto-migration
quarkus.flyway."crypto".table=flyway_schema_history_crypto
quarkus.flyway."crypto".baseline-on-migrate=true
quarkus.flyway."crypto".baseline-version=0
quarkus.flyway."crypto".migrate-at-start=true
```

Note that `locations` is build-time-fixed in Quarkus, the extra datasource opens a second Agroal
pool, and other injection points may need `@DataSource("crypto")` / `quarkus.hibernate-orm.datasource`
to stay unambiguous.

### Micronaut

`micronaut-flyway` binds one configuration per key under `flyway.datasources`, and a key that is not
a `DataSource` bean name may carry its own `url`/`user`/`password` — so a second history needs no
second datasource bean (the key must **not** collide with an existing datasource name, or the
migration runs twice):

```yaml
flyway:
  datasources:
    default:
      # your own migrations, on Flyway's defaults
      enabled: true
      locations: classpath:db/migration
    crypto:
      enabled: true
      url: jdbc:postgresql://db:5432/app
      user: app
      password: secret
      locations: classpath:db/crypto-migration
      table: flyway_schema_history_crypto
      baseline-on-migrate: true
      baseline-version: 0
```

`table`, `baseline-on-migrate` and `baseline-version` bind through micronaut-flyway's
`@ConfigurationBuilder` delegate (Flyway's `FluentConfiguration`) rather than explicit setters, so
they are absent from the published property table; the doc-listed equivalents go through the
`properties` map with Flyway's own keys (`flyway.table`, `flyway.baselineOnMigrate`,
`flyway.baselineVersion`).

### Or copy the DDL into your own migration series

Copy **all** of it — the engine reads `encryption_keys.key_version` on every key load and writes
`erased_key_generations` on every erasure, so an inventory that stops at the first two tables boots
and then fails on the first `encrypt`/`deleteKey`:

```sql
CREATE TABLE IF NOT EXISTS encryption_keys (
    subject_id  VARCHAR(255) PRIMARY KEY,
    key_bytes   BYTEA        NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    key_version SMALLINT     NOT NULL
);

CREATE TABLE IF NOT EXISTS forgotten_subjects (
    subject_id     VARCHAR(255) PRIMARY KEY,
    forgotten_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    key_created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS erased_key_generations (
    subject_id          VARCHAR(255) PRIMARY KEY,
    max_erased_version  SMALLINT     NOT NULL
);
```

This is exactly the DDL of `V001__crypto_baseline.sql` (a test keeps the two identical).
Back up and restore `erased_key_generations` together with `forgotten_subjects`. `reinstate` never
reconstructs a missing generation row, so a tombstone restored without it lets the next key reuse a
destroyed generation, and the subject's pre-erasure ciphertext then fails loudly
(`CryptoMappingException`) instead of reading as `"[REDACTED]"`.

**Custom key-table name.** If you set `streamrune.crypto.postgres.table-name`, the DDL above is
yours to adapt: the shipped series only ever creates the default-named `encryption_keys`, so a
custom table must be created — and kept up to date, `key_version` included — by you. Re-applying the
crypto series will not add a missing column to it (nor to any table that already exists: the
baseline creates tables only when absent). `forgotten_subjects` and `erased_key_generations` keep
their fixed names and are created by the series. The engine folds the name to lower case, as
PostgreSQL does with an unquoted identifier: `table-name=CryptoKeys` uses, and validates at startup,
the table `cryptokeys`. Create it unquoted (or as `cryptokeys`), not as the quoted `"CryptoKeys"`.

## Usage

```java
DataSource dataSource = ...; // HikariCP, Agroal, etc.

PostgresCryptoEngine engine = PostgresCryptoEngine.builder()
    .dataSource(dataSource)
    .build();

SubjectId subject = SubjectId.of("customer-42");

// Encrypt — creates key if absent
byte[] ciphertext = engine.encrypt(subject, "PII data".getBytes(StandardCharsets.UTF_8));

// Decrypt
byte[] plaintext = engine.decrypt(subject, ciphertext);

// Check existence (advisory)
boolean exists = engine.isKeyAvailable(subject);

// Delete key (crypto-shredding — terminal erasure)
engine.deleteKey(subject);

// Re-encrypting a forgotten subject now FAILS LOUDLY:
//   engine.encrypt(subject, ...) -> throws SubjectForgottenException

// Explicit escape hatch for legitimate re-registration:
engine.reinstate(subject);              // deletes the tombstone row (old data stays unrecoverable)
engine.encrypt(subject, ...);           // mints a brand-new key, one generation higher
```

## Terminal Erasure (crypto-shredding)

`deleteKey(subjectId)` is the GDPR Article 17 operation and is **terminal**. In one transaction it deletes the `encryption_keys` row, inserts a tombstone into `forgotten_subjects` and, when a key existed, records its generation in `erased_key_generations`. Subject ids are stored as SHA-256 hashes in all three tables, never verbatim. A subsequent `encrypt(subjectId, …)` for a forgotten subject throws `SubjectForgottenException` (in `org.streamrune.core.crypto`) instead of silently minting a fresh key — so a retried command or a re-imported event cannot un-do an erasure by collecting new recoverable PII under a new key.

`decrypt` of the subject's **old** ciphertext after a forget throws `KeyNotFoundException` (which `CryptoShreddingModule` maps to `"[REDACTED]"`).

`engine.reinstate(subjectId)` is the only way to re-register a forgotten subject: it deletes that subject's `forgotten_subjects` row, and there is no engine-wide opt-out. The old key and old ciphertext stay unrecoverable; only future encrypts are re-enabled. The next key is minted one generation above the destroyed one, so the subject's pre-erasure ciphertext keeps reading as `"[REDACTED]"` rather than failing the GCM tag check. A subject supports at most 126 erase/re-register cycles. In an application, call it through `ForgetSubjectService.reinstate(subjectId, requester)`, which records the reinstate in the audit log as `GDPR_REINSTATE`; the engine method alone writes no audit record.

## Key Components

| Component | Description |
|---|---|
| `PostgresCryptoEngine` | Implements `CryptoEngine` with JDBC-backed key storage |
| `Builder` | `.dataSource(DataSource)` — required; `.tableName(String)` — key table, default `encryption_keys` (a plain SQL identifier; see *Custom key-table name*) |
| `JdbcForgottenSubjectStore` | Durable `ForgottenSubjectStore` on the `forgotten_subjects` table, for the Vault and AWS KMS engines |
| `PostgresCryptoForgetSignal` | `CryptoForgetSignal` over PostgreSQL LISTEN/NOTIFY, so a forget on one replica evicts `CachedCryptoEngine` entries on every peer. Listens on a dedicated one-connection pool opened from the `DataSource`'s JDBC URL (`HikariDataSource`, or a `getJdbcUrl()`/`getUrl()`/`getURL()` accessor); for any other `DataSource` pass the LISTEN pool with `new PostgresCryptoForgetSignal(dataSource, () -> listenPool)` |

## Requirements

- Java 25
- PostgreSQL 17 or newer (tested on 17 and 18; `PostgresEventStoreFactory.create()` refuses an older server at startup)
- Depends on `streamrune-core` and `streamrune-crypto-api`

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-postgres-crypto:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-postgres-crypto:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Best Practices

- **Use with CachedCryptoEngine** — wrap with `CachedCryptoEngine` for aggregate replay performance. Without caching, every event replay issues a DB call per unique subjectId.
- **Let the crypto baseline create the tables** — through the event-store factory's schema auto-initialization, or your own Flyway run on `flyway_schema_history_crypto` (see *Applying the crypto migrations*).
- **Separate tablespace** — consider placing `encryption_keys` on encrypted tablespace/disk for additional protection.

## Threat Model & Key Custody

Keys are stored as **plaintext AES-256 bytes** in `encryption_keys` — there is no envelope encryption (no KEK wrapping the per-subject keys). Design for two consequences:

- **Same-instance compromise** — if `encryption_keys` lives in the same PostgreSQL instance and backup set as the event stream, any attacker or backup leak that reaches the database obtains keys *and* ciphertext together; encryption at rest adds nothing against that attacker. Place `encryption_keys` in a separate database/instance with its own credentials, or use `streamrune-vault-crypto` / `streamrune-aws-kms-crypto` so key material never reaches your database at all.
- **Crypto-shredding is only as strong as backup hygiene** — any backup of `encryption_keys` taken *before* a subject was forgotten resurrects that subject's key on restore. Align key-table backup retention with your GDPR erasure deadlines and never restore `encryption_keys` from a pre-forget snapshot.

Envelope encryption (per-subject keys wrapped by a KEK held in Vault/KMS/HSM) is the standard remedy and is on the roadmap; until then the controls above are operational, not technical.

## Known Limitations

- **Single DataSource** — no multi-master HA support.
- **`deleteKey()` is terminal** — it deletes the key and records a `forgotten_subjects` tombstone in one transaction; a later `encrypt` for that subject throws `SubjectForgottenException` rather than resurrecting the key. Use `reinstate(subjectId)` to re-register a subject. See *Terminal Erasure*.
- **No key rotation** — rotating a key requires re-encrypting all events. Ciphertext carries a 2-byte `[format version][key version]` header (followed by the 12-byte GCM IV and the AES-256-GCM ciphertext+tag). The key-version byte is the key generation, which only a post-erasure re-mint advances. A blob with an unknown format or key version, a truncated blob or a GCM tag mismatch fails with `CryptoMappingException` (deterministic — quarantined, not retried) instead of a generic error; a blob from a destroyed generation reads as `"[REDACTED]"`.
- **No envelope encryption** — key bytes are stored unwrapped; see *Threat Model & Key Custody*.
