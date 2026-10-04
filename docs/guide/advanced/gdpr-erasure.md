# GDPR Erasure (Crypto-Shredding)

## When to use

Use crypto-shredding when your event streams contain personal data and you must honour a GDPR
Article 17 "right to erasure" request. Because an event store is append-only and immutable, you
cannot delete individual events. Instead, StreamRune encrypts each subject's personal fields under a
per-subject key and *forgets the key* — the ciphertext remains in the immutable log but is
permanently undecryptable, so the data is effectively erased.

Use it whenever a `@Encrypted` field would hold PII: email, name, address, phone, national ID, and
so on. The subject id groups all of a person's data under one key.

Choose an opaque subject id — a UUID, not an email or a username. After an erasure the framework
keeps the subject id's unsalted SHA-256 hash for good (the tombstone, the GDPR audit rows, log
lines). For an opaque id that hash identifies nobody; for a guessable one, anyone holding a list of
candidate emails can recompute the hashes and learn who was erased and when — hashing does not make
a guessable identifier anonymous.

Do **not** use crypto-shredding as a substitute for authorization or transport encryption — it
protects data *at rest and after erasure*, not access control.

Do **not** put a subject id itself inside an `@Encrypted` field — you need it in cleartext to locate
and delete the right key.

## How it works

There are four moving parts: the `@Encrypted` annotation on your event, state and command
records, a `CryptoEngine` backend that holds the per-subject keys, the `CryptoShreddingModule`
Jackson module that encrypts on serialize and decrypts on deserialize, and the GDPR services that
delete keys and purge read models.

### `@Encrypted` — mark the PII fields

`@org.streamrune.core.crypto.Encrypted` targets **record components** (`@Target(RECORD_COMPONENT)`,
`@Retention(RUNTIME)`). Its single attribute `subjectId` names the sibling component that carries the
subject id under which the field is encrypted:

```java
public record CustomerRegistered(
        String customerId,
        @Encrypted(subjectId = "customerId") String email,
        @Encrypted(subjectId = "customerId") String fullName) implements DomainEvent {}
```

The annotated component **must be a `String`.** The annotation itself carries no type constraint, but
`CryptoShreddingModule` enforces String-only at runtime: a non-`String` `@Encrypted` component throws
`CryptoOperationException` when the module scans the type. Model PII as `String`.

### `CryptoEngine` — the key store

`org.streamrune.core.crypto.CryptoEngine` is the SPI that holds keys and performs the actual
encrypt/decrypt:

```java
byte[] encrypt(SubjectId subjectId, byte[] plaintext);
byte[] decrypt(SubjectId subjectId, byte[] ciphertext);
void   deleteKey(SubjectId subjectId);            // terminal — GDPR Art. 17 — on every shipped backend
default void reinstate(SubjectId subjectId);      // default throws UnsupportedOperationException; every
                                                   // shipped backend overrides it to actually work
boolean isKeyAvailable(SubjectId subjectId);
```

Four production backends ship, each obtained via `builder()`. All four enforce **terminal**
erasure: after `deleteKey(subjectId)`, `encrypt` for that subject throws `SubjectForgottenException`
and `decrypt` of old ciphertext throws `KeyNotFoundException` (mapped to `[REDACTED]` by
`CryptoShreddingModule`).

| Backend | Class (package) | Construction | Notes |
|---|---|---|---|
| filesystem | `org.streamrune.filesystem.FileSystemCryptoEngine` | `.keyDirectory(Path)` | AES-256-GCM; `deleteKey` writes a disk tombstone, deletes the key **file** and sweeps stray `*.tmp` hard links left by a crashed mint (see Caveats) — the key material is gone for good. Each erasure step is fsynced before the next, and the delete before `deleteKey` returns; a freshly minted key is fsynced before `encrypt` uses it (see Caveats). `encrypt` re-checks the tombstone **after** claiming a freshly minted key file, so an erasure completing in another process mid-mint is refused instead of resurrected (see Caveats — the guarantee is POSIX-local; NFS caveat). Every key/tombstone existence probe fails **closed**: if the key directory becomes unreadable or its volume unavailable, operations throw `CryptoOperationException` rather than reporting the key as absent (which would read as a completed erasure and redact live PII into read models) |
| postgres | `org.streamrune.crypto.postgres.PostgresCryptoEngine` | `.dataSource(DataSource)` | keys in `encryption_keys`, tombstones in `forgotten_subjects`; `deleteKey` deletes the key row and writes the tombstone in one transaction — the key material is gone for good |
| vault | `org.streamrune.vault.VaultCryptoEngine` | `.vaultAddress(...).token(...).httpClient(...).forgottenSubjectStore(...)` | HashiCorp Vault transit engine; `deleteKey` deletes the Vault transit key **and** records a durable tombstone (Vault would otherwise silently re-mint the key on the next `encrypt`) — terminal, matching filesystem/postgres/aws-kms. `deleteKey` sets `deletion_allowed=true` on the key before deleting it, so the Vault token needs `update` on `<mount>/keys/+/config` as well as `delete` on `<mount>/keys/+` (full policy in the [module README](../../../streamrune-crypto/streamrune-vault-crypto/README.md)); without them the forget fails and the error names the missing capability. `encrypt` re-checks the tombstone **after** the Vault call and deletes a key that an in-flight encrypt racing the erasure resurrected (see Caveats). **`forgottenSubjectStore(...)` is mandatory** — see below |
| aws-kms | `org.streamrune.aws.AwsKmsCryptoEngine` | `.kmsKeyId(...)` plus `.kmsClient(...)` or `.region(...)`, `.forgottenSubjectStore(...)` | a **single shared** Customer Managed Key (CMK) encrypts every subject — there is no per-subject key to delete. `deleteKey` records a durable per-subject **tombstone** instead (it does **not** throw, and the CMK itself is never touched). `encrypt` re-checks the tombstone **after** the KMS call and discards the ciphertext if the erasure completed mid-flight — the CMK is never destroyed, so such a ciphertext would otherwise stay decryptable forever. A missing or inaccessible CMK is an **infrastructure fault**, never an erasure: `decrypt` and `isKeyAvailable` fail closed with `CryptoOperationException` instead of reporting every subject as erased. **`forgottenSubjectStore(...)` is mandatory** — see below |

> ⚠️ **Never delete the shared CMK to erase one subject (aws-kms).** `deleteKey(subjectId)` never
> touches the Customer Managed Key — it only records that one subject's tombstone. Scheduling
> deletion of, or otherwise destroying, the CMK itself as an "erasure" step for a single Article 17
> request destroys **every** subject's data system-wide, irreversibly. Always erase via
> `deleteKey`/`ForgetSubjectService.forget(...)`; never delete or schedule deletion of the CMK for a
> per-subject request.

### `reinstate(SubjectId)` — undoing an erasure

`reinstate(SubjectId)` lifts the tombstone so `encrypt` is allowed again for legitimate
re-registration under the same subject id. It is the only way to re-register a forgotten subject:
explicit and per subject, on every backend — no engine offers an engine-wide opt-out of terminal
erasure. What happens to *pre-erasure* ciphertext afterwards is
backend-specific — but every backend guarantees the same invariant: **a reinstate never converts
erased history into replay-blocking poison.**

**Reinstate through `ForgetSubjectService.reinstate(SubjectId, UserId)`, not through the engine.**
The engine method writes no audit record, so calling it directly bypasses the GDPR audit trail. The
service calls it and records the decision in `audit_log` as `GDPR_REINSTATE` — `SUCCESS`, or
`FAILURE` with the engine's reason (for example vault's refusal below) — under the same hashed
subject id as the erasure's `GDPR_FORGET` rows, naming the requester. A reinstate reverses part of
an erasure (on aws-kms all of it, see below), so make it an authorized decision: the service records
who asked, it does not check whether they may. The `SUCCESS` row is written after the tombstone is
lifted; if that write fails, `reinstate` throws the audit store's failure with a note saying the
tombstone is already lifted. Re-run `reinstate` once the audit store works — the engine's reinstate
is a no-op on a subject that is no longer forgotten, so the re-run writes only the missing row — or
`forget` the subject again. A process that dies between the two steps leaves the same state, and the
same re-run records it.

- **postgres / filesystem** — full same-id re-registration. After a *completed* `deleteKey` the
  key material is gone forever; `reinstate` allows a *new* key to be minted on the next `encrypt`,
  and that key gets a **higher key generation** (the generation of every destroyed key is recorded
  durably — postgres in the `erased_key_generations` table, filesystem in a per-subject
  `<hash>.generation` file; both store only the SHA-256 subject hash plus a small integer, no PII
  and no key material, and both deliberately survive the reinstate). Every ciphertext blob carries
  its generation in the key-version header byte, so decrypting pre-erasure ciphertext keeps
  yielding `KeyNotFoundException` → `[REDACTED]` — aggregate loads, replays and full projection
  rebuilds keep working exactly as they did between the forget and the reinstate. Tampering with
  *current*-generation ciphertext (a failed GCM tag check), a truncated blob, an unknown format
  byte, or a key-version byte outside `1..current` still fails loudly — with
  `CryptoMappingException`, the deterministic subtype of `CryptoOperationException` that the
  framework's read paths quarantine as poison rather than retry as a key-store outage; the
  destroyed-generation tolerance is strictly bounded to generations `1..current-1`.
  - Exception — a **crashed** erasure on filesystem (tombstone written, key file survived; the
    safe ordering: a tombstoned-but-live key fails closed, the reverse would resurrect).
    `reinstate` clears only the tombstone, so it puts that **pre-erasure key back in service** —
    same key, same generation — and its ciphertext becomes readable again. Re-run `deleteKey`
    first — it is idempotent and converges — whenever an erasure may not have completed (on
    filesystem, `isKeyAvailable(subjectId)` returning `true` for a tombstoned subject is exactly
    this residue). Postgres has no such window: it deletes the key, records its generation and
    writes the tombstone in one transaction.
- **vault** — `reinstate` **refuses after a completed erasure** (throws
  `CryptoOperationException` with remediation guidance). Vault's transit engine owns the
  ciphertext wire format (`vault:vN:…`), and a re-minted transit key restarts at `v1` — so this
  backend *cannot* discriminate destroyed key generations, and lifting the tombstone would arm a
  trap: the first re-encrypt upserts a fresh transit key, and from then on every pre-erasure blob
  fails to decrypt with an HTTP 400 — a loud failure instead of `[REDACTED]`, raised as
  `CryptoMappingException` (poison) when Vault's error text identifies the blob as invalid or
  unauthenticated, and as a plain `CryptoOperationException` otherwise. Re-register the returning
  person under a **new subject id** instead. The **crashed**-erasure case (tombstone committed, the transit `DELETE` never landed)
  is still reinstatable — but the probe demands key **identity**, not mere existence:
  a live transit key can also be the residue of
  an encrypt that raced the erasure (the Vault transit upsert whose best-effort cleanup failed),
  which is a *different*, post-forget key — reviving it would arm the same poison. `reinstate`
  therefore proves key **identity by equality**: `deleteKey` reads the transit key *before*
  destroying it and records its oldest version's `creation_time` in the tombstone
  (`forgotten_subjects.key_created_at`), and `reinstate` accepts only a
  live key reporting that same instant. Both values come from Vault, so the proof carries **no
  cross-clock term at all**.

  The evidence is written once, at erasure time, and never invented. `deleteKey` records it only
  when the key already out-ages that erasure by
  `VaultCryptoEngine.Builder.revivalIdentityMargin(...)` (default 60 s) — a comparison between
  two Vault-sourced instants, the key's `creation_time` and the same response's HTTP `Date`
  header. That margin is what keeps the later equality check unambiguous despite Vault's
  epoch-second granularity: a re-created key can only report the *same* creation timestamp as the
  original if it lands within one granule of it. Raising it is always safe; lowering it is not.
  Its cost is that a subject who first encrypts and then erases *within* the margin (ordinary
  under GDPR) records no evidence and cannot be revived — fail-closed, so only the convenience is
  lost. **An erasure never fails for want of evidence:** every unavailable case (Vault
  unreachable, an unreadable `creation_time`, a stripped or unparseable `Date` header, a key
  inside the margin) still erases and logs a WARN naming the reason.

  Everything unprovable then fails closed at `reinstate` and keeps the tombstone: Vault
  unreachable, an unreadable creation time, a live key whose creation time differs from the
  recorded one, a tombstone that carries **no** evidence, or a store that does not implement
  `keyCreatedAt` (third-party implementations inherit a safe empty default — they lose only the
  revival convenience, never erasure safety). The only honest value for a destroyed key's creation
  time is none, so there is deliberately no fallback: re-register such a subject under a new
  subject id.

  **The tombstone is lifted last, and it is the only durable write `reinstate` makes**.
  A `deleteKey` in flight commits its tombstone
  *before* issuing its transit `DELETE`, so the identity probe can see the still-live original
  while that `DELETE` is about to land; `reinstate` therefore re-reads the key as its last act
  before deciding, and refuses — with the tombstone never lifted — if it has vanished. **Every
  crash converges, because there is no intermediate state to crash into:** a crash before the lift
  leaves the tombstone and its evidence exactly as the erasure wrote them, so the subject stays
  gated and a rerun re-decides from scratch; a crash after it means the revival completed, which
  is correct, and a rerun agrees (an idempotent no-op).

  *One residual remains:* a transit `DELETE` landing **after** the verification still escapes
  detection and leaves a completed erasure with no tombstone, after which the next `encrypt`
  passes the pre-check and transit silently upserts a fresh key for an erased subject. No local
  ordering can close it — that would need a transaction spanning Vault and the tombstone store —
  and it is not self-healing. **Whenever an erasure may have been concurrent or crashed, re-run
  the idempotent `deleteKey` first** (it also sweeps the residue of a racing encrypt, making the refusal fire
  correctly), as the engine docs instruct; `reinstate` logs a WARN naming that residual and its
  remedy immediately before the lift.
- **aws-kms** — the shared CMK is never modified, so `reinstate` also makes *pre-erasure*
  ciphertext decryptable again, because erasure was enforced purely by the tombstone gate rather
  than by destroying key material. On aws-kms, the tombstone store's durability and integrity
  **is** the erasure boundary, and a reinstate is a full un-erasure — reinstate only through
  `ForgetSubjectService.reinstate(...)`, so the un-erasure is on record.

Operational notes for the postgres/filesystem generation scheme:

- A subject supports at most **126 completed erase → reinstate → re-encrypt cycles**; the mint
  then fails loudly with guidance to use a fresh subject id (the generation byte caps at 127 and
  must never wrap — a wrapped generation would collide with a destroyed one).
- **Back up and restore the generation records with the tombstones** (postgres:
  `erased_key_generations` with `forgotten_subjects`; filesystem: the `<hash>.generation` files with
  the `<hash>.forgotten` files), and erase only through `deleteKey`. The generation record is
  written by `deleteKey` when it destroys a key; `reinstate` never reconstructs it. Without it — a
  record lost in a partial restore, or a key row/file deleted outside the engine before the forget
  — the next key reuses a destroyed generation, and that subject's pre-erasure ciphertext fails
  loudly (`CryptoMappingException`) instead of reading as `[REDACTED]`.
- **Accepted residual:** the generation byte is routing metadata outside the GCM tag. An attacker
  with write access to stored ciphertext can roll a *reinstated* subject's current-generation
  blob down to a destroyed generation, turning it into `[REDACTED]` (surfaced via the
  `streamrune.crypto.subject_redacted` metric and its WARN log) instead of a loud tag failure.
  This is bounded to subjects that actually went through erasure + reinstate; for everyone else a
  flipped version byte still fails loudly (`CryptoMappingException`).

### Durable erasure on vault and aws-kms (production)

Because vault and aws-kms erasure is enforced by a tombstone rather than by destroying a
per-subject key, `Builder#build()` on both engines **fails closed**: it throws
`IllegalStateException` unless a `org.streamrune.crypto.ForgottenSubjectStore` is supplied via
`forgottenSubjectStore(...)` — there is no silent in-memory default.

- `InMemoryForgottenSubjectStore` is process-local — acceptable for tests/ephemeral use only. Its
  tombstones vanish on restart, silently un-forgetting subjects.
- `JdbcForgottenSubjectStore` (in `streamrune-postgres-crypto`) persists tombstones in the shared
  `forgotten_subjects` table and survives restarts. The Spring/Micronaut/Quarkus auto-configs
  auto-wire it automatically whenever a `DataSource` bean is present; with no `DataSource`, the
  engine bean fails to build instead of silently falling back to an in-memory store.
- `forgotten_subjects` (and, for postgres, `encryption_keys` and `erased_key_generations`) live in
  the opt-in `db/crypto-migration` Flyway location. When schema auto-initialization is on (the default) and the
  configured `CryptoEngine` reports it needs crypto tables (`CryptoEngine.requiredCryptoTables()`),
  `PostgresEventStoreFactory.initializeSchema()` auto-applies `db/crypto-migration` too (on its
  own Flyway history table), so durable erasure works out of the box through the shipped auto-config
  path. If you manage the schema yourself and omit that migration, `SchemaValidator
  .validateCryptoTables(...)` fails fast at startup with an actionable message instead of only
  throwing on the first `@Encrypted` operation or the first `forget(...)`. Apply it yourself with a
  *separate* Flyway run on `flyway_schema_history_crypto` — **never** by adding the location to your
  main `flyway.locations`, which aborts with `Found more than one migration with version 1` because
  both series start at `V001` (see *Applying the crypto migrations* in
  `streamrune-crypto/streamrune-postgres-crypto/README.md`).

Wrap any backend in `org.streamrune.crypto.CachedCryptoEngine` (from `streamrune-crypto-api`) to
cache decrypt results and avoid a key round-trip on every read:

```java
CryptoEngine engine = CachedCryptoEngine.builder()
    .delegate(PostgresCryptoEngine.builder().dataSource(dataSource).build())
    .maximumSize(10_000)
    .expireAfterWrite(Duration.ofMinutes(5))   // decrypted PII never lives in cache indefinitely
    .build();
```

The cache always applies a bounded TTL (default `CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE`,
5 minutes) even when you set none — decrypted plaintext must not linger past erasure. In a
multi-replica deployment, also wire a `CryptoForgetSignal` (e.g. `PostgresCryptoForgetSignal`) via
`CachedCryptoEngine.Builder#forgetSignal(...)` so a `deleteKey` on one replica promptly evicts the
forgotten subject's cached plaintext on peer replicas, instead of relying solely on the TTL. The
framework auto-configs wire this automatically for all four backends when a `DataSource` is present.

`PostgresCryptoForgetSignal` publishes on a connection borrowed from the `DataSource` and listens on
one connection of its own: a dedicated pool of one connection per replica, never a slot of the
application pool — size `max_connections` for it. It opens that pool from the `DataSource`'s
connection settings: a `HikariDataSource`, a `DataSource` exposing
`getJdbcUrl()`, `getUrl()` or `getURL()` such as `PGSimpleDataSource`, or — on Quarkus — the Agroal
datasource, whose URL, credentials (or credentials provider) and JDBC properties the Quarkus
integration copies. A `DataSource` of any other shape (a proxy or wrapper hiding the pool) cannot be
read: the replica logs `Crypto-forget LISTEN disabled` at startup and keeps publishing, but serves a
subject that *another* replica forgot from its cache until the cache write-TTL elapses. Treat that
WARN as a failed erasure guarantee, not noise: either build the `CachedCryptoEngine` yourself (with
the backend's auto-config switched off) and hand it a LISTEN pool through
`new PostgresCryptoForgetSignal(dataSource, () -> listenPool)` — a pool of one connection, dedicated
to the signal, that validates connections on borrow; the signal closes it — or set
`expireAfterWrite` to the longest delay you can accept between a forget and the last plaintext read.

`InMemoryCryptoEngine` (from `streamrune-test`) is a test fixture only; do not use it in production.

### `CryptoShreddingModule` — encrypt on serialize, `[REDACTED]` after forget

`org.streamrune.crypto.CryptoShreddingModule` is a Jackson `SimpleModule` constructed from a
`CryptoEngine`:

```java
ObjectMapper mapper = new ObjectMapper();
mapper.registerModule(new CryptoShreddingModule(cryptoEngine));
```

When the module is registered:

- On **serialize**, each `@Encrypted` component is encrypted under `encrypt(subjectId, ...)` and
  stored as Base64 ciphertext.
- On **deserialize**, the component is decrypted back to plaintext before the record's constructor
  runs. Only `@Encrypted` components are touched: every other component reads back exactly as
  it would without the module — a `BigDecimal` keeps its scale, an `Instant` or `Duration` its
  nanoseconds.
- After the subject's key is deleted, decryption can no longer succeed and the component
  deserializes to the constant `CryptoShreddingModule.REDACTED` (`"[REDACTED]"`). The ciphertext in
  the log is untouched — it is simply no longer recoverable.

You rarely register the module by hand. The event store does it for you when it is made
crypto-aware (below), and the saga store does the same for saga state.

### Making the event store crypto-aware

The event store must serialize events through a `CryptoShreddingModule`. Wire the engine into the
store through the **`eventStoreFactoryProvider`** overload of the `StreamRune` builder, which threads
the engine (and the type registry) into the store you construct. Its lambda returns the factory
itself rather than an already-built store; the facade calls `create()` on it:

```java
static final AggregateType CUSTOMER = AggregateType.of("customer");

StreamRune runtime = StreamRune.builder()
    .eventStoreFactoryProvider(settings ->               // ConfiguredEventStoreFactoryProvider
        new PostgresEventStoreFactory(dataSource, settings.typeRegistry())
            .cryptoEngine(settings.cryptoEngine()))      // engine reaches the store here
    .cryptoEngine(engine)                                // collected into EventStoreSettings
    .registerEventType("CustomerRegistered", CustomerRegistered.class)
    .register(CUSTOMER, CustomerCommand.class, c -> AggregateId.of(c.customerId()), new CustomerDecider())
    .build();
```

Do **not** try to combine a pre-built `.eventStore(store)` with `.cryptoEngine(engine)` on the
`StreamRune` builder — that throws `IllegalStateException` at build time, because the facade cannot
thread the engine into a store it did not construct. A pre-built store must have the engine set on its
own factory (`new PostgresEventStoreFactory(...).cryptoEngine(engine).create()`) and you must then
**not** call `.cryptoEngine(...)` on the `StreamRune` builder.

## Quick example

Erase a subject: delete the key, then purge any read models holding their rows.

```java
ForgetSubjectService forget = ForgetSubjectService.builder()
    .cryptoEngine(engine)
    .auditStore(auditStore)                              // records the erasure in the audit log
    .purgers(List.of(new CustomerReadModelPurger()))     // one per read model with the subject's rows
    .queryCache(cachingQueryBus)                         // evicted after the last purger (optional)
    .build();

ForgetResult result = forget.forget(SubjectId.of("customer-42"), UserId.of("dpo-admin"));
if (!result.fullyErased()) {
    // key is gone, but these read models (or, when !result.queryCacheEvicted(), cached query
    // answers) may still hold the subject's data — call forget again
    List<String> retry = result.failedPurgers();
}

// Later, a legitimate re-registration under the same id: lift the tombstone, on record as
// GDPR_REINSTATE (see "reinstate(SubjectId) — undoing an erasure" above)
forget.reinstate(SubjectId.of("customer-42"), UserId.of("dpo-admin"));
```

`forget(SubjectId, UserId)` deletes the crypto key first (the terminal-erasure step — see
*`reinstate(SubjectId)`* above for exactly what each backend can and cannot undo afterwards), then
runs each registered `SubjectDataPurger` in turn, then evicts the query cache when one is set (see
*Cached query answers are evicted* below). Every engine tombstones the subject even when it
never had a key, so a later `encrypt` for that id is refused until a reinstate: issue a forget only
for subject ids known to exist. The `UserId` — who requested the erasure — is
recorded in the audit log and may be `null`. If `deleteKey` itself throws, `forget` audits a
`FAILURE` and rethrows; no purger runs, because the key was not deleted. A purger that throws does
**not** abort the erasure and is not rethrown (the key is already gone): the failure is audited per
purger, counted as `streamrune.gdpr.purge_failed` when the builder was given `metrics(...)`, the
remaining purgers still run, and the returned `ForgetResult` reports it — do not tell the data
subject the erasure completed while `fullyErased()` is `false`.

An `Error` after the key is gone (from the `SUCCESS` audit write, a purger, a per-purge audit write
or the metrics backend) does not cut the erasure short either: every purger is still attempted,
then the first `Error` propagates instead of a `ForgetResult`. It carries a suppressed note saying
the crypto-shred completed, whether the `SUCCESS` audit row is missing, and which purgers purged and
which failed; any later `Error` rides along on it as a suppressed exception. Treat it as a partial
erasure, not a failed one: re-run `forget` once the cause is fixed. It is idempotent (`deleteKey`
is a no-op on a deleted key) and runs every purger again.

A purger's `name()` is read once, when `ForgetSubjectService.builder()...build()` runs, before any
key can be deleted; `forget` never calls it. Each purger needs a name of its own: `build()` throws
`IllegalArgumentException` naming every name two or more purgers share, so with the auto-configured
service two purger beans under one name stop the application from starting. A `name()` that throws,
or returns `null` or a blank string, does not stop the build: the purger is named by its class name
(the simple name; for an anonymous class, its binary name without the package) in the
`ForgetResult`, the audit rows and the metric, and a WARN at build time names the class so you can
fix it. Two such instances of one class share that class name, so they are refused as well. An
`Error` from `name()` propagates from `build()`. Every purger name goes through
`LogSanitizer.sanitizeForLog` before it lands in a log line, an audit summary or the refusal
message (control characters stripped, 64 characters at most); `ForgetResult` keeps the name as the
purger gave it.

A `SubjectDataPurger` deletes one read model's rows for the subject:

```java
public final class CustomerReadModelPurger implements SubjectDataPurger {
    private final JdbcTemplate jdbc;
    CustomerReadModelPurger(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override public String name() { return "customer-summary"; }

    @Override public void purge(SubjectId subjectId) {           // must be idempotent
        jdbc.update("DELETE FROM customer_summary WHERE customer_id = ?", subjectId.value());
    }
}
```

## Full example

Export a subject's data (GDPR Article 15, "right of access") before erasing it. Registered
`SubjectDataCollector`s each contribute a section:

```java
ExportSubjectDataService exporter = ExportSubjectDataService.builder()
    .collectors(List.of(new CustomerProfileCollector()))
    .auditStore(auditStore)
    .build();

SubjectExport data = exporter.export(SubjectId.of("customer-42"), UserId.of("dpo-admin"));
// -> immutable SubjectExport with one JSON section per collector, in registration order
```

`export(SubjectId, UserId)` throws `IllegalStateException` if no collector was registered, and
`GdprExportException` (carrying the partial sections and the failing collector's name) if a collector
throws an exception. An `Error` from a collector (for example `OutOfMemoryError`) is audited as
FAILURE and rethrown unchanged.

A collector's `name()` is read once, when `build()` runs; every export keys that collector's
section by it, and `export` never calls it. Each collector needs a name of its own: `build()` throws
`IllegalArgumentException` naming every name two or more collectors share, since one section would
otherwise replace the other and the export would be incomplete without anything failing. With the
auto-configured service, two collector beans under one name stop the application from starting. A
`name()` that throws, or returns `null` or a blank string, does not stop the build: the collector's
section is keyed by its class name (the simple name; for an anonymous class, its binary name without
the package), and a WARN at build time names the class so you can fix it. Two such instances of one
class share that class name, so they are refused as well. An `Error` from `name()` propagates from
`build()`. Every collector name goes through `LogSanitizer.sanitizeForLog` before it lands in a log
line, an audit summary, the refusal message or the `GdprExportException` message (control
characters stripped, 64 characters at most); the `SubjectExport` keeps a name as the collector gave
it, since it is the key a consumer parses.

After a successful erasure, reading an event for the forgotten subject yields `[REDACTED]` where the
PII used to be:

```java
// Before forget:
CustomerRegistered(customerId="customer-42", email="ada@example.com", fullName="Ada Lovelace")

// After forget.forget(SubjectId.of("customer-42"), ...):
CustomerRegistered(customerId="customer-42", email="[REDACTED]", fullName="[REDACTED]")
```

## Saga state is encrypted too

`@Encrypted` fields on **saga state** are crypto-shredded on the same footing as event fields.
`PostgresSagaStore` registers a `CryptoShreddingModule` on its ObjectMapper when a `CryptoEngine` is
supplied, so `@Encrypted` saga-state fields are encrypted at rest and fall within the scope of a
`forget(...)`.

`PostgresSagaStore(DataSource, ObjectMapper)` is the **only** constructor — the mapper is never
implicit. Build it with `PostgresSagaStore.createObjectMapper(cryptoEngine)`:

```java
SagaStore sagaStore =
    new PostgresSagaStore(dataSource, PostgresSagaStore.createObjectMapper(cryptoEngine));
```

Passing `null` for the engine (or supplying your own mapper without `CryptoShreddingModule`) yields
a **crypto-blind** store: `@Encrypted` saga-state fields are written as **plaintext**, are never
decrypted, and are outside the scope of `forget(...)` — deleting the subject's key does nothing to
them. That is a deliberate, visible choice at the call site; there is no one-argument constructor
that makes it for you. The auto-configurations (Spring/Quarkus/Micronaut) always pass the
application's `CryptoEngine` when one is configured.

## Dead-lettered commands are encrypted only when the command carries `@Encrypted`

A command that fails on infrastructure is recorded in the command dead-letter queue
(`dead_letter_queue.payload`) as JSON, so it can be replayed later. That payload is the **command
record**, not the events the command would have produced. The bus serializes it with
`DeadLetterRetryRunner.createObjectMapper(cryptoEngine)` — the auto-configured bus always does, and
a bus you build yourself must be given that mapper with `.objectMapper(...)` — so the command's own
`@Encrypted` components are stored as ciphertext under the subject's key, and `forget(...)` makes
them unreadable along with the events. A retry runner reads the entries with the same kind of
mapper; an entry whose subject was forgotten while it waited decrypts to `[REDACTED]` and is never
replayed (see [Retry and resilience](retry-and-resilience.md)).

A command that carries personal data **without** `@Encrypted` is stored as plaintext. Annotating
only the events it produces is not enough: the queued copy keeps the name and email readable after
the key is deleted, until the retention sweeper (`streamrune.dead-letter.retention-max-age`, default
30 days) deletes the row. Annotate the personal data on the command exactly as on its events:

```java
public record RegisterCustomer(
        String customerId,
        @Encrypted(subjectId = "customerId") String email,
        @Encrypted(subjectId = "customerId") String fullName) implements Command {}
```

An erasure therefore reaches every copy of the subject's data only when all of these hold: the
events, saga state and commands mark their personal data `@Encrypted` (deleting the key covers
them), a purger removes every read-model row, snapshots are purged (next section), and the query
cache is evicted (see *Cached query answers are evicted* below).

## ⚠️ Aggregate snapshots are NOT purged by `forget(...)` — register a purger

**`snapshot_store` is framework-owned infrastructure, and `ForgetSubjectService.forget(...)` never
touches it unless you tell it to.** Crypto-shredding makes an `@Encrypted` field in the LATEST
snapshot read back as `[REDACTED]` (same as any other post-forget read), but it does nothing for
non-`@Encrypted` DERIVED data an aggregate's `evolve()` copied out of PII into a plain field — an
age computed from a birth date, an email domain extracted for analytics, anything similar. That
plaintext survives in the last snapshot taken before the forget **indefinitely**: no new snapshot
overwrites it without new commands against that exact aggregate, and nothing else ever deletes the
row.

Register [`PostgresSnapshotStorePurger`](../../../streamrune-eventstore/streamrune-postgres/src/main/java/org/streamrune/postgres/PostgresSnapshotStorePurger.java)
alongside your other purgers to close this gap:

```java
ForgetSubjectService.builder()
    .cryptoEngine(engine)
    .purgers(List.of(
        new CustomerReadModelPurger(jdbc),
        new PostgresSnapshotStorePurger(dataSource,
            subjectId -> List.of(StreamId.of(CUSTOMER, new AggregateId(subjectId.value()))))))
    .build();
```

(`CUSTOMER` is the `AggregateType` the customer decider is registered under; the mapping here says
the customer's stream carries the subject id as its aggregate id.)

It is **opt-in**, not automatic, because `snapshot_store` is keyed by `(aggregate_type, aggregate_id)`
(a typed stream), not `subject_id` — unlike your own read models (typically `DELETE ... WHERE subject_id = ?`),
there is no indexed subject column to purge against, and the framework has no general way to
derive one: whether a stream's aggregate id equals a subject id, or an aggregate's state merely CARRIES a
subject's PII under an unrelated stream, is a decision only your domain model can answer. You
supply that mapping (`Function<SubjectId, Collection<StreamId>>`) when constructing the purger —
the same kind of lookup every other `SubjectDataPurger` already performs internally against its
own read model, made explicit here because `snapshot_store` has none of its own to query.

Under `SnapshotPolicy.never()`, no *new* rows are written to `snapshot_store` — but this is **not**
"nothing to purge": if this aggregate was ever actively snapshotted before switching to `never()`,
its pre-switch rows persist indefinitely, never read again and never overwritten (see the *Switching
an actively-snapshotted aggregate to `never()`* caveat in
[`snapshot-versioning.md`](snapshot-versioning.md#caveats)), and they carry exactly the derived
plaintext this section is about. Register [`PostgresSnapshotStorePurger`](../../../streamrune-eventstore/streamrune-postgres/src/main/java/org/streamrune/postgres/PostgresSnapshotStorePurger.java)
anyway if the table may still hold rows from before the switch, or purge the table once
by hand when you make the switch. A fully custom `EventStore` that is not `PostgresEventStore`-backed
has no `snapshot_store` table for this particular purger to purge — you would need your own store's
equivalent instead.

## Cached query answers are evicted

A `@Cacheable` query answered before the erasure holds the subject's data as it was then: an
`@Encrypted` field the handler decrypted, or a read-model row a purger is about to delete. Left in
the `CachingQueryBus`, that answer would be served until its TTL ran out, after the key was
shredded and the row purged. `forget(...)` therefore evicts the query cache given to
`ForgetSubjectService.builder().queryCache(cachingQueryBus)`:

- **Every query type, every caller's partition.** The service cannot tell which cached answers
  hold the subject, so it evicts them all (`CachingQueryBus.evictAll()`). Erasures are rare; the
  cost is one cache miss per query type afterwards.
- **After the last purger.** A query answered while a purge is running may cache the row that
  purge then deletes; only an eviction after it removes that answer. A load still in flight when
  the eviction runs drops its own answer once stored, so it is not served either.
- **Only when the key was deleted.** A forget whose `deleteKey` throws changed nothing and evicts
  nothing. After the key is gone, an eviction failure never stops the erasure, and it is handled
  like a failed purge: it is logged at WARN, counted in `streamrune.gdpr.purge_failed` under the
  name `query-cache`, recorded as a `FAILURE` audit row `query-cache eviction failed: ...`, and
  reported as `ForgetResult.queryCacheEvicted() == false`, so `fullyErased()` is `false`. An `Error`
  is also thrown once the erasure's other steps are done, like any other post-shred `Error`.
- **Wired for you.** With `streamrune.query-cache.enabled=true`, the Spring, Quarkus and Micronaut
  integrations pass their `CachingQueryBus` to the auto-configured `ForgetSubjectService`. Two
  `CachingQueryBus` beans with no default fail startup (Spring: `ifAvailable` finds more than one
  candidate; Quarkus: an ambiguous `Instance`; Micronaut: a non-unique bean) rather than leave one
  of them un-evicted. A `ForgetSubjectService` you build yourself evicts only if you call
  `queryCache(...)`.
- **This process only.** The cache is in memory. Another instance of the application keeps its
  cached answers until their TTL runs out, because a forget raises no event the other instances
  see. In a multi-instance deployment, give `@Cacheable` queries whose answers carry personal data
  a TTL you can accept as the longest time such an answer may outlive an erasure, or evict on
  every instance yourself (for example from a purger that broadcasts the eviction).

## When a forget does not finish: call it again

There is no durable job behind `forget(...)`. If the process dies, or a step fails, anywhere
between the engine's tombstone, the key deletion, the audit rows and the purges, **call
`forget(subjectId, requester)` again** for the same subject. The re-call completes the erasure from
any point a previous call stopped at:

| Where the previous call stopped | What is left | What the re-call does |
|---|---|---|
| Before or inside `deleteKey` (a `FAILURE` row if the engine threw, no row if the process died) | Nothing changed, or the tombstone is recorded and the key file or Vault key still exists | Records the tombstone again (a no-op) and deletes the key |
| After `deleteKey`, before the `SUCCESS` row | Key deleted, tombstone recorded, no audit row | Writes the `SUCCESS` row and runs every purger |
| Between purgers, or between a purge and its audit row | Some read models purged, some audit rows missing | Runs every purger again (a no-op where the rows are gone) and writes their rows |
| After the last purge row | Erasure complete | Repeats every step harmlessly; the audit log gains one more attempt |

Every step is safe on what a partial run left behind: `deleteKey` is idempotent, the audit log is
append-only (a repeated row is one more attempt on record), purgers must be idempotent, and the
eviction clears whatever is cached. A partial run never makes the subject readable again: every
engine records the tombstone before, or in the same transaction as, the key deletion, so a deleted
key always has its tombstone. These guarantees are pinned crash point by crash point in the
framework's tests.

**Detecting an incomplete erasure.** An erasure is incomplete when any of these holds:

- `forget(...)` threw: an exception from `deleteKey` (a `GDPR_FORGET` `FAILURE` row records it), or
  an `Error` carrying the crypto-shred-completed note (it says whether the `SUCCESS` row is missing
  and which purgers failed).
- `forget(...)` returned a `ForgetResult` with `fullyErased() == false`.
- The audit log lacks, for the subject's hash (`SubjectId.redacted()`, stored in
  `audit_log.aggregate_id`), a `GDPR_FORGET` `SUCCESS` row with no summary (the shred row)
  followed by a `SUCCESS` row `purged read model '<name>'` for every registered purger. This is
  the check that catches a process that died mid-erasure: the missing `SUCCESS` row, or a missing
  per-purger row after it.
- The audit log holds, for the subject's hash, a `FAILURE` row `query-cache eviction failed: ...`
  newer than its last shred row: cached query answers may still hold the subject's data until
  their TTL runs out.

A process that dies before `deleteKey` writes anything leaves no audit row at all, so record each
erasure request in your own store before calling `forget(...)`, and mark it done only when
`fullyErased()` returned `true`; re-call every request still open. See the production guide's
runbook for the audit-log queries.

## Configuration

| Setting | Where | Effect |
|---|---|---|
| `.cryptoEngine(engine)` on the `StreamRune` builder | must be paired with `eventStoreFactoryProvider(...)` (or `eventStoreFactory(ConfiguredEventStoreFactory)`) | makes the event store crypto-aware |
| `@Encrypted(subjectId = "...")` | on a `String` record component | field is encrypted under that subject's key |
| `CachedCryptoEngine` TTL | `.expireAfterWrite(Duration)` | bounds how long decrypted PII stays cached (default 5 minutes) |
| `streamrune.crypto.cache.enabled` | Spring/Quarkus/Micronaut property (default `true`) | when `false`, the `CachedCryptoEngine` wrapper is skipped entirely on all three integrations, so no decrypted PII is ever retained in heap — the deliberate no-cache GDPR posture, stronger than shortening the TTL |
| `ForgetSubjectService.builder().queryCache(bus)` | auto-wired by the integrations when `streamrune.query-cache.enabled=true` | every `forget(...)` evicts the `CachingQueryBus` after its last purger, in this process (see *Cached query answers are evicted*) |

### Startup fail-fast

If any registered event or state type has an `@Encrypted` component (directly or in a nested
record) but **no `CryptoEngine` is configured**, building the event store **fails fast with
`IllegalStateException`** rather than silently persisting that PII as plaintext. The check runs in
`CryptoConfigValidator.validate(...)` from every builder of a `PostgresEventStore` —
`PostgresEventStoreFactory.create()` (the path all three integrations use),
`PostgresEventStore.create(...)` and `PostgresEventStore.builder().build()` — before the database is
touched. A custom `EventTypeRegistry` that does not implement `registeredTypes()` is refused there
too, because it cannot be checked. Each integration additionally validates its command types on
boot.

Two paths are not checked. The `PostgresEventStore` constructors that take a ready-made
`ObjectMapper` use it as given: build it with `PostgresEventStore.createObjectMapper(cryptoEngine,
metrics)` when a registered type carries `@Encrypted`. Command types registered with the plain-Java
`StreamRune` builder are not scanned: a dead-lettered command's `@Encrypted` components are
encrypted only when the bus's `.objectMapper(...)` is
`DeadLetterRetryRunner.createObjectMapper(cryptoEngine)`.

This is deliberate: an `@Encrypted` field with no engine is a compliance bug, so StreamRune refuses to
start rather than leak.

## Caveats

- **`deleteKey` requires a persistent `ForgottenSubjectStore` on vault and aws-kms.** Both engines'
  `build()` fails closed (`IllegalStateException`) without one — see *Durable erasure on vault and
  aws-kms* above. Never delete the shared aws-kms CMK itself to erase a single subject — that
  destroys every subject's data.
- **`reinstate(SubjectId)` never recovers key material a *completed* `deleteKey` destroyed on
  filesystem/postgres/vault** — it only allows a fresh key to be minted; pre-erasure ciphertext
  stays permanently unreadable. On postgres/filesystem "unreadable" means the designed
  replay-tolerant form — `[REDACTED]` via the key-generation scheme — even after the subject is
  re-keyed; on vault a completed-erasure reinstate is refused outright (re-register under a new
  subject id). After a **crashed** erasure on filesystem/vault the key material outlived the
  tombstone, so `reinstate` revives the pre-erasure key — re-run `deleteKey` first (see
  *`reinstate(SubjectId)` — undoing an erasure* above). On aws-kms the shared CMK is untouched, so
  `reinstate` always restores access to pre-erasure ciphertext — treat the aws-kms tombstone store
  as the actual erasure boundary.
- **`CryptoEngine.reinstate(SubjectId)` writes no audit record.** Reinstate through
  `ForgetSubjectService.reinstate(SubjectId, UserId)`, which records a `GDPR_REINSTATE` row naming
  the requester (see *`reinstate(SubjectId)` — undoing an erasure* above).
- **`@Encrypted` components must be `String`.** A non-`String` component throws
  `CryptoOperationException` at type-scan time.
- **`@JsonUnwrapped(prefix/suffix)` over an `@Encrypted`-carrying record is supported — on
  jackson-databind 2.19+.** The encrypting writer survives Jackson's rename derivation and the
  prefixed/suffixed field is ciphertext at rest. Reading the unwrapped
  record back requires jackson-databind ≥ 2.19 (2.18 cannot populate creator properties of an
  unwrapped *record* target at all — fields deserialize as `null` even without encryption).
- **`@JsonFormat(shape = ARRAY)` is not supported on records carrying `@Encrypted` components.**
  Positional array serialization bypasses the encrypting writer and carries no property name for
  the decrypt side to resolve, so serialization fails closed with `CryptoOperationException`
  instead of ever writing the PII as plaintext. Serialize such records as JSON objects.
- **The wire form of an `@Encrypted`-carrying record must be a plain JSON object with per-property
  slots.** Any class-level mechanism that replaces the property-based bean
  serializer fails closed with `CryptoOperationException` at write time instead of leaking
  plaintext: `@JsonValue`, class-level `@JsonSerialize(using = …)`, `@JsonSerialize(converter = …)`,
  `@JsonSerialize(as = …)`, and using the record as a **Map key** (JSON object keys stringify via
  `toString()`, dumping every component). A single opaque wire value has no slot for ciphertext +
  subjectId, so no encrypted round-trip can exist — remove the override, or drop `@Encrypted` and
  encrypt at the container level. *Property-level* overrides on **another type's field** of the record type are a
  separate, deliberately unguarded blind spot — see *Property-level (de)serializer overrides …*
  below.
- **The subjectId component referenced by an `@Encrypted` field must itself be serialized.**
  `@JsonIgnore` on the subjectId component would let the write *succeed* — the
  encrypting writer reads the subjectId reflectively off the live record — while the stored JSON
  carried no subjectId property, so every later read would find no subjectId and silently return
  the Base64 ciphertext as the field value: permanently undecryptable data with zero error signal.
  Serializer construction therefore fails closed with `CryptoOperationException` naming the field,
  the subjectId component, and the remedy. Renaming
  the subjectId with `@JsonProperty` is fully supported (write and read resolve it through the
  same rename mapping, including under `@JsonUnwrapped` prefixes) — use a rename, not
  `@JsonIgnore`, when the Java name must not appear on the wire.
- **By-name property filters must not name an `@Encrypted` component or its subjectId.**
  `@JsonIgnoreProperties` / `@JsonIncludeProperties` on a property *referencing*
  an `@Encrypted`-carrying record (directly, via mixin, on a `List`/`Map`/array member whose
  content is the record, or a declared-`Object` member) is applied when the record's serializer is
  contextualized for that property — after every construction-time check — and would drop the
  subjectId writer (ciphertext stored without its key reference: permanently undecryptable) or the
  encrypting writer itself (the PII silently vanished from the stored event). Class-level
  `@JsonIgnoreProperties` / `@JsonIncludeProperties` on the record itself (and per-class config
  overrides) would remove the writers through a second, equally silent route. Both fail closed
  with `CryptoOperationException` at write time. Matching uses external/wire names (renames and naming
  strategies honored); filters naming only unrelated properties keep working. To keep a name off
  the wire, use a `@JsonProperty` rename — never filter the subjectId or the encrypted field.
- **An active `@JsonView`, or a `@JsonFilter`, must not hide an `@Encrypted` component or its
  subjectId.** Two more routes reach the same corruption as the by-name filters
  above, but decide it *per serialization* instead of per contextualization, so no
  construction-time check can see them: `@JsonView` is applied from a **separate** filtered-writer
  array that Jackson only consults when the writer sets an active view (`writerWithView(...)`), and
  a `@JsonFilter`'s per-property decision comes from a filter supplied at write time. Both fail
  closed with `CryptoOperationException` **at write time**, where the active view and the actual
  filter are known:
  - a view that excludes the subjectId writes ciphertext with no key reference (permanently
    undecryptable — the record then reads back as `customerId=null, email=<raw Base64>`, silently);
  - with `MapperFeature.DEFAULT_VIEW_INCLUSION` disabled (Spring Boot's default), a view that does
    not name the `@Encrypted` component drops the PII from the stored event entirely;
  - a `@JsonFilter` can do either, per property.

  The checks are **write-time by design, and only fire on the shapes that actually corrupt**: the
  same view-annotated record serializes normally under a matching view or with no active view at
  all, and a `@JsonFilter` that keeps both properties keeps working — the filter is *asked*, per
  bean, rather than the annotation being refused. `@JsonUnwrapped` forms are covered too (the
  message names the prefixed wire names). Keep the `@Encrypted` component and its subjectId in
  every view the mapper writes with, and never filter either out.
- **Class-level `@JsonDeserialize(using/converter/builder)` is not supported on
  `@Encrypted`-carrying records.** All three routes replace the bean deserializer
  before the decrypting wrapper can hook, so every read would construct the record with the raw
  Base64 ciphertext as the field value — silently: ciphertext in aggregates/read models/APIs, no
  `[REDACTED]` after a forget, no redaction observability, and snapshots re-encrypting the
  ciphertext one nesting level per cycle. Reads therefore fail closed with `CryptoOperationException`
  at deserializer construction. Writes are unaffected and correct, so data written under the
  annotation is fully recoverable — just remove the override. Property-level `@JsonDeserialize` on
  components *of* the record keeps working (the decrypting deserializer still wraps). Overrides on
  **another type's field** of the record type are the deliberately unguarded blind spot below.
- **Never put a property-level `@JsonSerialize`/`@JsonDeserialize` override on a field whose type
  carries `@Encrypted` components — this is unguarded, and the write side leaks PLAINTEXT PII.**
  The constraint is symmetric and covers every per-property form on the *referencing* field:
  `@JsonSerialize(using = …)`, `@JsonSerialize(contentUsing = …)`, `@JsonSerialize(converter = …)`,
  `@JsonSerialize(contentConverter = …)`, `@JsonSerialize(keyUsing = …)` and the matching
  `@JsonDeserialize(using/contentUsing/converter/contentConverter)`. Each replaces the record's own
  (de)serializer *for that one property*, with no `BeanSerializerModifier`/`BeanDeserializerModifier`
  hook firing for the record.

  The two sides fail very differently, and the write side is the severe one:

  | Side | What happens | Recoverability |
  |---|---|---|
  | **Write** | The override writes the record itself, so the encrypting property writers never run: the `@Encrypted` value lands in the append-only event store as **plaintext PII**, with no ciphertext and no error. Verified against jackson-databind 2.19.2 for all four of `using`, `contentUsing`, `converter`, `contentConverter`. | **None.** No key was ever minted, so no later `forget(subjectId)` can reach it — the plaintext is permanent unless the events are rewritten out-of-band. |
  | **Read** | The override receives that property's raw JSON, so the record is constructed holding the **Base64 ciphertext as the field value** — silently, with no `[REDACTED]` after a forget and no redaction metric. | **Full.** The stored ciphertext and subjectId are correct; removing the override restores correct reads. |

  Both routes are *observable* (`findSerializer`/`findDeserializer(AnnotatedMember)`), and both are
  deliberately left unguarded because the same annotation expresses a legitimate pattern that a
  guard cannot distinguish from a bypass:

  - a custom (de)serializer that pre-processes and then **delegates back** to the mapper
    (`provider.defaultSerializeValue(…)` / `ctxt.readValue(…)`) encrypts and decrypts correctly —
    only running the user's code tells the two apart;
  - a `converter` whose **output type is the record itself** (a normalizing pass-through) likewise
    keeps encrypting correctly — verified — so even the converter forms have a benign shape;
  - a read-side false positive is a replay DoS (every aggregate load and projection rebuild stops),
    and a write-side false positive stops the application persisting events at all.

  So: put custom (de)serializers on the **containing** type, or on components *of* the record
  (both fully supported), never on a field *of* the `@Encrypted`-carrying record type itself.
- **`deleteKey` sweeps a crashed mint's stray hard link (filesystem backend).** A hard crash between claiming a fresh key file (`Files.createLink`)
  and removing its temp name leaves TWO directory entries for the key inode. `deleteKey` therefore
  deletes the primary key file **and** sweeps the subject's stray `*.tmp` links, so the AES key
  cannot stay readable through a leftover link after an erasure that reported success. The mint
  itself does not heal such an orphan — cross-process it could delete a peer's live in-flight temp
  (see *The filesystem mint does not delete stray `*.tmp` files before minting* below) — so the erasure-time sweep is the load-bearing one.
- **An encrypt racing a completed erasure cannot resurrect a Vault transit key.**
  Vault transit **upserts** the named key, so an `encrypt` whose
  tombstone pre-check passed while a concurrent `deleteKey` completed in full would land its POST
  after the transit DELETE and re-create a live key for the erased subject. `encrypt` therefore
  re-checks the tombstone after the Vault call, best-effort deletes the resurrected key (enabling
  `deletion_allowed` first) and throws `SubjectForgottenException` instead of returning the
  ciphertext. One residue remains: a client that crashes or times out between its Vault POST and
  the post-check — or whose best-effort delete fails, which it logs at ERROR without throwing —
  leaves the stray key behind, carrying only brand-new random material that decrypts nothing (the
  subject's historical ciphertexts used the already-deleted key, and no post-forget ciphertext was
  returned), so it is compliance hygiene, not recoverable PII. Detect it by comparing live transit
  keys against tombstones: `vault list transit/keys` (adjust the mount) — any key named
  `subject-<hash>` whose `<hash>` appears in `SELECT subject_id FROM forgotten_subjects` is
  residue. Remove it by re-running the forget for that subject (`deleteKey` is idempotent), or
  manually: set `deletion_allowed=true` via `transit/keys/subject-<hash>/config`, then
  `vault delete transit/keys/subject-<hash>`.
- **A cross-process encrypt racing a completed erasure cannot re-mint a filesystem key
  — but the guarantee is POSIX-local.** The filesystem engine's
  encrypt-vs-deleteKey serialization is a per-JVM stripe lock, so with several JVMs sharing one
  key directory (two replicas on a shared volume; an admin job running forgets beside the app) an
  `encrypt` whose tombstone pre-check passed while another process's `deleteKey` ran to
  completion could mint a brand-new live key for the erased subject after the forget had already
  reported success. `encrypt` therefore re-checks the tombstone after the mint race resolves — on both the `link(2)` winner leg (it removes its
  freshly created key file plus stray `*.tmp` links and throws `SubjectForgottenException`) and
  the loser leg (it refuses to adopt, and removes, a race-window key when the tombstone is
  visible — the only live guard left if the winning process died before its own re-check) — and
  refuses outright when a key file coexists with a tombstone. Sound because `deleteKey` writes
  the tombstone *before* deleting the key file: a mint claiming after the erasure's delete
  necessarily sees the tombstone; one claiming before is removed by that delete.
  **Visibility scope (NFS).** The re-check assumes POSIX-coherent metadata — one kernel: a local
  filesystem, all sharing JVMs on one host, or a volume mounted by a single node. NFS clients
  cache attributes and directory entries (`acregmin`/`acdirmin`, negative-entry caching), and
  close-to-open consistency governs file *content* on open — it does not make one client's
  freshly created tombstone promptly visible to another client's `Files.exists`, and the
  erasure's own `deleteIfExists`/sweep resolve names through the same client cache. Across NFS
  *clients* the re-check therefore narrows the resurrection window to the attribute-cache
  timeout (commonly 3–60 s) but cannot close it. For hard cross-client erasure ordering, run
  every writer of a key directory on one host, mount with `actimeo=0`/`noac` (per-operation
  server round-trips), or use the postgres/vault/aws-kms backends, whose erasure serialization
  lives in the shared database.
  **Residue (crash window).** A mint that dies between its `createLink` and its
  re-check leaves a key file (possibly plus a `*.tmp` link) beside the tombstone. This is the
  filesystem twin of the Vault crashed-client residue: brand-new random material returned to
  nobody — nothing was ever encrypted under it, every later `encrypt` refuses while the
  tombstone exists, and a `decrypt` attempted against it fails the GCM tag check (fail-closed) —
  compliance hygiene, not recoverable PII. It is self-identifying: a `<hash>.subjectId` file
  whose `<hash>.forgotten` sibling exists. Remove it by re-running the forget for that subject
  (`deleteKey` is idempotent: tombstone re-write no-ops, key delete + sweep clear the residue)
  or by deleting the key file and its `*.tmp` links manually.
- **The filesystem mint does not delete stray `*.tmp` files before minting
  — pre-erasure orphans persist until the erasure sweep.**
  Every matching temp is crash residue only within one JVM. With several JVMs sharing a key
  directory, a matching temp can be a peer process's *live* pre-link mint, and deleting it would
  make that peer's `createLink` fail: a legitimate concurrent first-encrypt (and the command
  carrying it) would die with `CryptoOperationException` instead of adopting the race winner's
  key. Temp sweeping therefore happens only under the subject's governing tombstone — `deleteKey`'s erasure sweep and the post-mint
  refusal — where killing an in-flight mint is the documented erasure-wins semantics.
  **Operational note:** a mint that crashes between writing its temp and claiming it leaves an
  inert orphan (`<hash>.subjectId<random>.tmp` holding a never-claimed key that nothing was
  encrypted under) until the subject's erasure sweeps it. `find <key-directory> -name '*.tmp'`
  finds them; while the subject's primary key file is live (or absent with no
  tombstone) such an orphan is safe to delete at any time — but prefer letting the erasure sweep
  handle it unless directory hygiene matters, since a hit may also be a peer's in-flight mint if
  taken while the application is running.
- **Filesystem erasure is fsynced step by step, and so are the key mint, `reinstate` and
  the first use of a key.**
  `deleteKey` forces the tombstone (file, shard directory, key directory and any directory it
  created above it) to stable storage before it records the generation, forces the
  `<hash>.generation` record (before its rename, then the renamed entry) before it deletes the key
  file, and forces the delete before it returns. Without that, nothing orders those steps on disk: a
  power loss after `ForgetSubjectService` had audited the erasure as complete could keep the
  key-file delete but lose the generation record (the next `reinstate` + `encrypt` re-mints the
  destroyed generation, and the subject's pre-erasure events fail with `CryptoMappingException`
  instead of reading as `[REDACTED]`), lose the tombstone too (a retried command re-mints without
  any `reinstate`), or lose the delete itself (the key file comes back and pre-erasure PII decrypts
  again) — and nothing re-runs a forget that reported success. A power loss mid-erasure now leaves
  only the residue a crash leaves (the crashed-erasure notes above), which re-running the forget
  completes; a failed fsync fails the erasure loudly before the next step runs. Directory fsync
  needs a POSIX filesystem: on Windows/NTFS a directory cannot be opened to force it, so it is
  skipped there and directory-entry durability is whatever NTFS provides. A subject's first
  `encrypt` forces the new key's bytes while the key is still a temp file, publishes it with
  `link(2)`, removes the temp name, and forces the shard directory, the key directory and any
  directory it created before the key is used — also when it loses a mint race and adopts another
  process's key. An event committed under a fresh key therefore never outlives the key across a
  power loss: a power loss earlier in the mint loses only a key nothing was encrypted under (and
  leaves at most an inert `*.tmp` that the subject's erasure sweeps), and the next `encrypt` loads
  the surviving key or mints a new one. A mint whose fsync fails returns no key, and the same engine
  instance forces that published key again before its next `encrypt` uses it. A mint refused by a
  concurrent erasure forces its cleanup before reporting the refusal, and `reinstate` forces the
  tombstone removal before it returns (a power loss before that brings the tombstone back, which
  fails closed: `encrypt` keeps refusing until `reinstate` is re-run). The same rule covers a key
  file an engine instance did not mint: one a peer instance or process sharing the directory
  published (and may not have forced yet, or died before forcing), or one this application's
  previous run published. The first `encrypt` in an engine instance that would use such a key
  forces the key file, its shard directory and the key directory before it encrypts anything under
  it, and fails with no ciphertext if that fsync fails (the next `encrypt` forces it again). A power
  loss before those forces can only take a key that no instance had forced yet — and no ciphertext
  exists under such a key, because every instance forces a key before its first `encrypt` under
  it; the next `encrypt` loads the surviving key or mints a new one. The instance then remembers
  the key file (by a fingerprint of its content, for the 16,384 most recently used keys), so later
  encrypts cost no fsync, and a key file replaced under the same name — erased, reinstated and
  re-minted by another process — is forced again on its first use. `decrypt` never forces: it
  produces no ciphertext, and every ciphertext it can read was written by an `encrypt` that had
  already forced its key. The key directory itself is assumed provisioned, or created by a mint
  that completed its forces: a first use forces nothing above the key directory, so a key
  directory a peer's first-ever mint created and died before forcing can still be lost with every
  key under it. When several instances or processes share a key directory, create it ahead of
  first use.
- **`[REDACTED]` is a sentinel, not proof of erasure.** A field whose plaintext literally equals
  `"[REDACTED]"` round-trips unchanged while its key exists; confirm erasure via
  `CryptoEngine.isKeyAvailable(subjectId)`, not by string comparison.
- **The audit log stores `userId` as plaintext, by design** — the audit trail must survive the
  subject's erasure. Do not put the subject's PII in audit fields.
- **Purgers must be idempotent.** A `forget(...)` may run more than once; purging a subject with no
  rows, or purging twice, must be a safe no-op.
- **Give every purger a literal, stable, unique `name()`.** It is the label in the audit trail, the
  `purge_failed` metric tag and `ForgetResult.failedPurgers()`; a shared name is refused at build
  time. A class name stands in when `name()` fails, but it changes when the class is renamed or
  moved.
- **Give every collector a literal, stable, unique `name()`.** It is the section key in the
  `SubjectExport`, which a consumer parses; a shared name is refused at build time. A class name
  stands in when `name()` fails, but it is an internal detail that changes when the class is
  renamed or moved.
- **Cache TTL matters after erasure.** A too-long `CachedCryptoEngine` TTL keeps decrypted PII
  readable in memory after `deleteKey`; keep it short, or set `streamrune.crypto.cache.enabled=false`
  to skip the cache entirely.

## Related guides

| Guide | What it covers |
|---|---|
| [audit.md](audit.md) | Recording who requested each erasure and export — the audit trail behind GDPR actions |
| [authorization.md](authorization.md) | Restricting who may issue `forget`/`export` requests |
| [../concepts.md](../concepts.md) | Aggregates, events, and the immutable event store crypto-shredding erases against |
