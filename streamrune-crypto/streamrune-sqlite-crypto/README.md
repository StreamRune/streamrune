# StreamRune SQLite Crypto

SQLite-backed `CryptoEngine` implementation, for the [SQLite backend](../../streamrune-sqlite/README.md).
Encryption keys are stored in a table of the application's SQLite file.

## Overview

The module is the crypto side of `streamrune.backend=sqlite`. It holds the key-store engine and
what every crypto engine needs from the backend on SQLite: the forgotten-subject store and the
forget signal.

| Type | What it is |
|---|---|
| `SqliteCryptoEngine` | One AES-256 key per subject in the `encryption_keys` table, with the semantics of the PostgreSQL engine: a terminal `deleteKey`, key generations that keep pre-erasure ciphertext reading `[REDACTED]` after a reinstate, verified encryption. |
| `SqliteForgottenSubjectStore` | The tombstones of forgotten subjects in the SQLite file; the Vault and AWS KMS engines use it on SQLite. |
| `InProcessCryptoForgetSignal` | The forget signal of a backend that serves one instance: it reaches the caches of this process, which has no peer to tell. |
| `SqliteCryptoBackend` | The `CryptoBackend` named `sqlite`: builds the three above over a `DataSource`. With `streamrune.backend=sqlite` and a crypto engine the integrations load it by its class name. |

The [SQLite guide](../../docs/guide/sqlite.md#crypto-and-gdpr-erasure) is the reference for crypto
and GDPR erasure on SQLite.

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-sqlite-crypto:1.0.0")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-sqlite-crypto:1.0.0")
```

> A release resolves from `mavenCentral()`; the [Status](../../README.md#status) of the README says
> whether it is published yet. The unsigned preview of the next release has a version of its
> own and needs the Maven Central snapshot repository: see
> [Preview builds](../../README.md#preview-builds). The [CHANGELOG](../../CHANGELOG.md) says what
> each release contains.

The module depends on `streamrune-sqlite`. It is needed only with a crypto engine: with
`streamrune.backend=sqlite` and a crypto engine enabled, a start without it fails naming the module.

## Usage

With an integration, select the engine with `streamrune.crypto.sqlite.enabled=true`. Without one,
build the engine first and hand it to the backend's bundle:

```java
CryptoEngine engine = new SqliteCryptoBackend()
    .keyStoreEngine(pool, new CryptoBackend.KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));

BackendStores stores = new SqliteBackend().open(new BackendContext(
    pool, engine, Clock.systemUTC(), StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP,
    true, true, true));
```

The event store factory applies the crypto schema series (`db/sqlite-crypto-migration`, history
table `flyway_schema_history_crypto`) when the engine reports crypto tables.

## What to know

- **The keys live in the same file as the ciphertext.** Protect the file, and its backups, as you
  protect a key store.
- **Never erase inside a projection batch.** `deleteKey`, `reinstate` and the forgotten-subject
  store's `forget` and `reinstate` refuse at once with an `IllegalStateException` on a thread that
  runs a projection batch or replay on the same file. Run the erasure from a command handler or a
  saga step.
- **A key minted inside a batch joins the batch.** When a projection writes an `@Encrypted` field of
  a subject without a key, the engine mints the key inside the batch's transaction, so it commits or
  rolls back with the read models encrypted under it.
- **An application transaction on the same file cannot mint a key**: the engine's write waits for
  the write lock that transaction holds. See
  [Application transactions are never joined](../../docs/guide/sqlite.md#application-transactions-are-never-joined).
- `streamrune.crypto.sqlite.enabled=true` with `streamrune.backend=postgres` fails the start, as
  `streamrune.crypto.postgres.enabled=true` does with `sqlite`.

## Related

- [`streamrune-crypto-api`](../streamrune-crypto-api/README.md): the crypto-shredding Jackson module
  and `CachedCryptoEngine`.
- [`streamrune-postgres-crypto`](../streamrune-postgres-crypto/README.md): the engine with keys in
  PostgreSQL.
