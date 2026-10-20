# StreamRune AWS KMS Crypto

AWS KMS-backed `CryptoEngine` implementation, using envelope encryption under one AWS KMS Customer Managed Key (CMK). Uses AWS SDK v2 for Java. The CMK never leaves AWS: KMS generates and wraps per-subject data keys, and the engine encrypts each value locally with AES-256-GCM under such a data key.

## Prerequisites

- An AWS KMS symmetric Customer Managed Key (CMK) in your account
- AWS credentials configured (env vars, ~/.aws/credentials, IAM role)
- `kms:GenerateDataKey`, `kms:Decrypt`, `kms:DescribeKey` IAM permissions on the CMK (`kms:Encrypt` is not used)

## Usage

```java
AwsKmsCryptoEngine engine = AwsKmsCryptoEngine.builder()
    .region("eu-central-1")
    .kmsKeyId("arn:aws:kms:eu-central-1:123456789012:key/1234abcd")
    // Required: the shared CMK is never deleted, so terminal GDPR erasure needs a durable
    // tombstone store. build() throws IllegalStateException without it.
    .forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource))
    .build();

// Or inject a pre-configured KmsClient
KmsClient client = KmsClient.builder().region(Region.of("eu-central-1")).build();
AwsKmsCryptoEngine engine = AwsKmsCryptoEngine.builder()
    .kmsClient(client)
    .kmsKeyId("arn:aws:kms:eu-central-1:123456789012:key/1234abcd")
    .forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource))
    // Optional: the data-key cache (defaults shown; 0 disables it)
    .dataKeyCacheMaximumSize(1_000)
    .dataKeyCacheExpireAfterWrite(Duration.ofMinutes(5))
    .build();

SubjectId subject = SubjectId.of("customer-42");

// Encrypt
byte[] ciphertext = engine.encrypt(subject, "PII data".getBytes(StandardCharsets.UTF_8));

// Decrypt
byte[] plaintext = engine.decrypt(subject, ciphertext);

// Check existence (advisory)
boolean exists = engine.isKeyAvailable(subject);
```

The Spring, Quarkus and Micronaut integrations wire a `JdbcForgottenSubjectStore` on the
application `DataSource` automatically, with the default data-key cache; without a `DataSource`
the engine does not build.

## Envelope Encryption

`encrypt` asks KMS `GenerateDataKey` for an AES-256 data key under the CMK, with the encryption
context `{"streamrune-subject": sha256hex(subjectId)}` (CloudTrail records the subject hash, never
the raw id), encrypts the value locally with AES-256-GCM, and stores the wrapped data key in the
ciphertext. `decrypt` asks KMS `Decrypt` to unwrap that data key under the same encryption context
and the configured key id, then decrypts locally. KMS only ever sees data keys, never values.

### Ciphertext format

```
[magic 'S' 'R' 'K' 'C'][version 2][wrapped-key length: 2 bytes, big-endian][wrapped data key]
[nonce: 12 bytes][AES-256-GCM ciphertext][tag: 16 bytes]
```

The GCM associated data is every byte before the nonce followed by `sha256hex(subjectId)`. A value
is bound to its subject twice: KMS refuses to unwrap the data key under another subject's
encryption context, and the GCM tag fails for another subject even if a data key were unwrapped. A
ciphertext is therefore never readable as another subject's. The `CryptoEngine` contract carries
no field identity, so the values of one subject are not bound to the field they were written for —
on every backend.

The overhead is about 220 bytes per value (a wrapped AES-256 data key under a symmetric CMK is about
184 bytes). The engine reads and writes version 2 only; a blob with another version byte is refused
with `CryptoMappingException`.

### Limits

- **Value size** — a value of any size up to `AwsKmsCryptoEngine.MAX_PLAINTEXT_LENGTH` (the Java
  array limit less the envelope, just under 2 GiB) is encrypted. KMS `Encrypt` would refuse more
  than 4,096 bytes; this engine does not call it. A longer value is refused on encrypt with
  `CryptoMappingException`, before any KMS call: the same value fails the same way on every attempt,
  so no classifier retries it.
- **Data-key use** — one cached data key encrypts at most `MAX_ENCRYPTIONS_PER_DATA_KEY` (2^24)
  values before the engine asks KMS for a new one, far below the 2^32 random-nonce limit of AES-GCM.

### Data-key cache and cost

The engine keeps a bounded in-process cache: per subject, the data key it encrypts under, and the
data keys it unwrapped, each map at most `dataKeyCacheMaximumSize` entries (default 1,000,
least-recently-used first out) and each entry at most `dataKeyCacheExpireAfterWrite` (default
5 minutes) after it was stored, however often it is used.

| Operation | KMS calls |
|---|---|
| encrypt, cache on | one `GenerateDataKey` per subject per window on the replica that writes it (or per 2^24 values) |
| decrypt, cache on | one `Decrypt` per data key per window on each replica that reads it; none on the replica that generated the key |
| encrypt or decrypt, cache off (`dataKeyCacheMaximumSize(0)`) | one call per value, and every value carries its own data key |
| `isKeyAvailable` | one `DescribeKey` |

The cache holds plaintext data keys in the process heap. The expiry bounds how long; `deleteKey`
drops the forgotten subject's keys at once on the replica that runs it. It never decides whether a
key is used: see below. `CachedCryptoEngine` in front of this engine still saves the local
decryption and the tombstone read on a cache hit.

### Key rotation

- **Automatic CMK rotation** is transparent: KMS unwraps data keys wrapped under any earlier
  backing key of the CMK, and new data keys are wrapped under the current one.
- **Data keys** rotate on their own: a new one per subject after each cache window or 2^24 values.
  Ciphertext keeps the data key it was written with.
- **Moving to another CMK** (a new `kmsKeyId`) leaves earlier ciphertext wrapped under the old CMK:
  `decrypt` pins the configured key id, so it fails with the bare `CryptoOperationException`
  (`IncorrectKeyException`) until that data is re-encrypted.

## Key Deletion & Terminal Erasure

A single CMK wraps the data keys of all subjects, so **there is no per-subject KMS key to hard-delete**. Instead of failing GDPR erasure, this engine records a **per-subject crypto-shredding tombstone** (in a pluggable `ForgottenSubjectStore`):

- `deleteKey(subjectId)` records the tombstone (idempotent) and drops the subject's cached data keys on this replica.
- After erasure, `decrypt(subjectId, …)` throws `KeyNotFoundException` — which `CryptoShreddingModule` maps to `"[REDACTED]"` — and `encrypt(subjectId, …)` throws `SubjectForgottenException`, matching the filesystem and PostgreSQL backends.
- Both read the tombstone on every call, before they look at the data-key cache or call KMS. The tombstone store is shared, so a data key another replica still caches is never used for a subject forgotten anywhere; it only stays in that replica's heap until its entry expires.
- `reinstate(subjectId)` clears the tombstone for legitimate re-registration. On this engine that is a full un-erasure (see *Known Limitations*), so call it through `ForgetSubjectService.reinstate(subjectId, requester)`, which records it in the audit log as `GDPR_REINSTATE`; the engine method alone writes no audit record.

The auto-wired `ForgetSubjectService` therefore completes on KMS: the crypto-shred step succeeds and the registered read-model purgers run.

**Durability is mandatory — the engine fails closed.** The shared CMK is never modified, so erasure is only as durable as the tombstone store. There is no default store: `build()` throws `IllegalStateException` unless `forgottenSubjectStore(...)` was called. Use the durable `JdbcForgottenSubjectStore` (in `streamrune-postgres-crypto`; it needs the `forgotten_subjects` table from the crypto baseline) in production. `InMemoryForgottenSubjectStore` is process-local — its tombstones are lost on restart, after which pre-erasure ciphertext becomes decryptable again under the intact CMK — so pass it only explicitly, for tests or single-process ephemeral use.

Complementary controls for defense in depth (the CMK itself remains intact):
- AWS KMS multi-region key rotation with manual key retirement
- AWS Config rules to detect and flag non-compliant usage

## Error Handling

Only a tombstoned subject yields `KeyNotFoundException` (→ `"[REDACTED]"`); that check runs before any KMS call. No KMS-reported key state is treated as a crypto-shred, because with one shared CMK it would redact every subject at once:

- A disabled, pending-deletion or otherwise invalid-state CMK (`DisabledException`, `KmsInvalidStateException`), a missing or inaccessible CMK (`NotFoundException`), an `IncorrectKeyException` from a wrong `kmsKeyId`, throttling and other service failures throw the bare `CryptoOperationException`, from `GenerateDataKey` on encrypt as from `Decrypt` on decrypt — reversible or configuration faults, which the read-path classifiers treat as an infrastructure failure and retry.
- `CryptoMappingException`, the deterministic subtype that the classifiers quarantine instead of retrying, covers every verdict on the value itself: a blob without the engine's envelope, an unsupported envelope version, a truncated envelope, `InvalidCiphertextException` on the wrapped data key (corrupted, tampered with or wrapped under another subject's encryption context), an unwrapped key that is not a 256-bit key, a GCM tag failure, and on encrypt a value longer than `MAX_PLAINTEXT_LENGTH`. The envelope checks and the size check run before any KMS call: a raw KMS ciphertext is never sent to KMS.

`isKeyAvailable` answers `false` for a tombstoned subject or a `Disabled`/`PendingDeletion` CMK, and throws `CryptoOperationException` when the CMK cannot be described at all.

## Key Components

| Component | Description |
|---|---|
| `AwsKmsCryptoEngine` | Implements `CryptoEngine` with envelope encryption under an AWS KMS CMK |
| `Builder` | `.region(String)` (used when no `KmsClient` is given), `.kmsKeyId(String)` (required), `.kmsClient(KmsClient)`, **`.forgottenSubjectStore(ForgottenSubjectStore)` (required — see *Key Deletion & Terminal Erasure*)**, `.dataKeyCacheMaximumSize(int)` (entries per map, default 1,000; 0 disables), `.dataKeyCacheExpireAfterWrite(Duration)` (default 5 minutes) |

## Requirements

- Java 25
- AWS SDK v2 (`software.amazon.awssdk:kms:2.x`)
- Depends on `streamrune-core` and `streamrune-crypto-api`
- A `ForgottenSubjectStore` for the required tombstone. The `JdbcForgottenSubjectStore` used in the
  examples above lives in `streamrune-postgres-crypto` (`org.streamrune.crypto.postgres`); any
  `ForgottenSubjectStore` implementation works.

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-aws-kms-crypto:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-aws-kms-crypto:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Best Practices

- **Keep the data-key cache on** — it turns one KMS round trip (~1-5 ms, charged per call) per value into one per subject and window.
- **Use with CachedCryptoEngine** for aggregate replay — a cache hit skips the tombstone read and the local decryption as well.
- **Use CMK, not AWS managed key** — customer managed keys allow key policies and rotation.
- **Enable key rotation** — configure automatic annual rotation in KMS.
- **Credential management** — prefer IAM roles over static credentials.

## Known Limitations

- **Erasure durability equals the tombstone store** — the shared CMK is immutable, so `deleteKey()` records a per-subject tombstone rather than deleting a key. `reinstate(subjectId)` makes pre-erasure ciphertext readable again, because the CMK was never destroyed.
- **Data keys in heap** — cached plaintext data keys live in the process heap for up to `dataKeyCacheExpireAfterWrite`; a heap dump of a running replica exposes them. Disable the cache (`dataKeyCacheMaximumSize(0)`) where that matters more than KMS cost and latency.
- **Latency** — a cache miss is a network call to AWS.
- **Cost** — AWS KMS charges per API call; see *Data-key cache and cost*.
