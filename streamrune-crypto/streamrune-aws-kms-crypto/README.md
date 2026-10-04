# StreamRune AWS KMS Crypto

AWS KMS-backed `CryptoEngine` implementation. Uses AWS SDK v2 for Java. Encryption and decryption are performed by AWS KMS; key material never leaves AWS.

## Prerequisites

- An AWS KMS Customer Managed Key (CMK) in your account
- AWS credentials configured (env vars, ~/.aws/credentials, IAM role)
- `kms:Encrypt`, `kms:Decrypt`, `kms:DescribeKey` IAM permissions

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
application `DataSource` automatically; without a `DataSource` the engine does not build.

## Key Deletion & Terminal Erasure

A single CMK encrypts all subjects, so **there is no per-subject KMS key to hard-delete**. Instead of failing GDPR erasure, this engine records a **per-subject crypto-shredding tombstone** (in a pluggable `ForgottenSubjectStore`):

- `deleteKey(subjectId)` records the tombstone (idempotent).
- After erasure, `decrypt(subjectId, …)` throws `KeyNotFoundException` — which `CryptoShreddingModule` maps to `"[REDACTED]"` — and `encrypt(subjectId, …)` throws `SubjectForgottenException`, matching the filesystem and PostgreSQL backends.
- `reinstate(subjectId)` clears the tombstone for legitimate re-registration. On this engine that is a full un-erasure (see *Known Limitations*), so call it through `ForgetSubjectService.reinstate(subjectId, requester)`, which records it in the audit log as `GDPR_REINSTATE`; the engine method alone writes no audit record.

The auto-wired `ForgetSubjectService` therefore completes on KMS: the crypto-shred step succeeds and the registered read-model purgers run.

**Durability is mandatory — the engine fails closed.** The shared CMK is never modified, so erasure is only as durable as the tombstone store. There is no default store: `build()` throws `IllegalStateException` unless `forgottenSubjectStore(...)` was called. Use the durable `JdbcForgottenSubjectStore` (in `streamrune-postgres-crypto`; it needs the `forgotten_subjects` table from the crypto baseline) in production. `InMemoryForgottenSubjectStore` is process-local — its tombstones are lost on restart, after which pre-erasure ciphertext becomes decryptable again under the intact CMK — so pass it only explicitly, for tests or single-process ephemeral use.

Complementary controls for defense in depth (the CMK itself remains intact):
- AWS KMS multi-region key rotation with manual key retirement
- AWS Config rules to detect and flag non-compliant usage

## Error Handling

Only a tombstoned subject yields `KeyNotFoundException` (→ `"[REDACTED]"`); that check runs before any KMS call. No KMS-reported key state is treated as a crypto-shred, because with one shared CMK it would redact every subject at once:

- A disabled, pending-deletion or otherwise invalid-state CMK (`DisabledException`, `KmsInvalidStateException`), a missing or inaccessible CMK (`NotFoundException`), an `IncorrectKeyException` from a wrong `kmsKeyId`, throttling and other service failures throw the bare `CryptoOperationException` — reversible or configuration faults, which the read-path classifiers treat as an infrastructure failure and retry.
- `InvalidCiphertextException` (the ciphertext itself is corrupted, tampered with or bound to another encryption context), a blob without the engine's ciphertext envelope and an unsupported envelope version throw `CryptoMappingException`, the deterministic subtype that the read-path classifiers quarantine instead of retrying. The two envelope checks run before any KMS call: decrypt always re-supplies the subject's EncryptionContext, so a raw KMS ciphertext is never decrypted.

`isKeyAvailable` answers `false` for a tombstoned subject or a `Disabled`/`PendingDeletion` CMK, and throws `CryptoOperationException` when the CMK cannot be described at all.

## Key Components

| Component | Description |
|---|---|
| `AwsKmsCryptoEngine` | Implements `CryptoEngine` backed by AWS KMS |
| `Builder` | `.region(String)` (used when no `KmsClient` is given), `.kmsKeyId(String)` (required), `.kmsClient(KmsClient)`, **`.forgottenSubjectStore(ForgottenSubjectStore)` (required — see *Key Deletion & Terminal Erasure*)** |

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

- **Use with CachedCryptoEngine** — AWS KMS latency (~1-5ms) makes caching essential for aggregate replay.
- **Use CMK, not AWS managed key** — customer managed keys allow key policies and rotation.
- **Enable key rotation** — configure automatic annual rotation in KMS.
- **Credential management** — prefer IAM roles over static credentials.

## Known Limitations

- **Erasure durability equals the tombstone store** — the shared CMK is immutable, so `deleteKey()` records a per-subject tombstone rather than deleting a key. `reinstate(subjectId)` makes pre-erasure ciphertext readable again, because the CMK was never destroyed.
- **Latency** — each encrypt/decrypt is a network call to AWS. Use caching.
- **Cost** — AWS KMS charges per API call. Caching reduces costs significantly.
