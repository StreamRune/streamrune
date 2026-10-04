# StreamRune Vault Crypto

HashiCorp Vault Transit Engine-backed `CryptoEngine` implementation. Encryption and decryption are performed by the Vault Transit Engine; key material never leaves Vault.

## Prerequisites

- HashiCorp Vault running with Transit Engine enabled
- A Vault token whose policy grants the capabilities in *Vault Token Policy* below
- The Vault address and token, passed to the builder (`vaultAddress` defaults to `http://localhost:8200`, a plain-HTTP development convenience — use an `https://` address in production); the engine reads no environment variables itself

Enable Transit Engine in Vault:
```bash
vault secrets enable transit
```

## Vault Token Policy

The engine needs these capabilities on its transit mount. The policy below is for a mount named `transit`; use your `engineMount` in its place:

```hcl
path "transit/encrypt/+" {
  capabilities = ["create", "update"]
}
path "transit/decrypt/+" {
  capabilities = ["update"]
}
path "transit/keys/+" {
  capabilities = ["read", "delete"]
}
path "transit/keys/+/config" {
  capabilities = ["update"]
}
```

- `create` and `update` on `encrypt/+`: transit creates a subject's key on the subject's first encrypt, which needs `create`. With `update` alone, Vault refuses to encrypt for a subject that has no key yet.
- `update` on `decrypt/+`.
- `read` on `keys/+`: `isKeyAvailable`, the key-identity record a forget keeps for `reinstate`, and `reinstate` itself.
- `delete` on `keys/+` and `update` on `keys/+/config`: a forget sets `deletion_allowed=true` on the subject's key and then deletes it. `+` matches one path segment, so `keys/+` does not cover the `/config` path and the fourth rule is needed.

The policy covers every key in the mount, so give StreamRune a transit mount of its own.

## Usage

```java
HttpClient httpClient = HttpClient.newHttpClient();

VaultCryptoEngine engine = VaultCryptoEngine.builder()
    .httpClient(httpClient)
    .vaultAddress("http://localhost:8200")
    .token(System.getenv("VAULT_TOKEN"))
    .engineMount("transit")
    // Required: Vault's transit engine re-creates a deleted key on the next encrypt, so terminal
    // GDPR erasure needs a durable tombstone store. build() throws IllegalStateException without it.
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

## Key Components

| Component | Description |
|---|---|
| `VaultCryptoEngine` | Implements `CryptoEngine` backed by Vault Transit Engine |
| `Builder` | `.httpClient(HttpClient)`, `.vaultAddress(String)`, `.token(String)`, `.engineMount(String)`, **`.forgottenSubjectStore(ForgottenSubjectStore)` (required — see *Terminal Erasure*)**, `.requestTimeout(Duration)` (default 10s), `.maxRetries(int)` (default 2), `.retryDelay(Duration)` (default 200ms), `.revivalIdentityMargin(Duration)` (default 60s — a safety bound for `reinstate`; raising it is always safe) |

## Resilience

Transient failures — connection errors/timeouts and HTTP 429/5xx — are retried up to `maxRetries` times with exponential backoff starting at `retryDelay` and doubling per retry up to a 30s ceiling (a `retryDelay` above 30s is used as configured for every retry). All operations are idempotent against Vault transit, so retrying is safe. `isKeyAvailable` answers `false` only on a definitive HTTP 404; any other non-200 (403 token expiry, 5xx outage) throws so a live key is never misreported as absent.

## Decrypt Error Classification

Vault transit answers HTTP 400 for several distinct causes, and the engine maps them by the response body:

- `"encryption key not found"` → `KeyNotFoundException` (→ `"[REDACTED]"`).
- A deterministic verdict on the ciphertext itself — Vault's `"invalid ciphertext ..."` family (shape, base64, length) or the forwarded AEAD failure text `"message authentication failed"` → `CryptoMappingException`, the deterministic subtype the read-path classifiers quarantine instead of retrying. A stored value that is not `vault:v<N>:<base64>` is rejected the same way before any request is sent.
- Every other 400 stays the bare `CryptoOperationException` with Vault's error text preserved — including a version refused by the key's `min_decryption_version` policy (`"... disallowed by policy (too old)"`) and `"version is too new"`, which shares the `"invalid ciphertext"` prefix but is reversible (a lagging node, a restore from an older snapshot) rather than a verdict on the blob.

## Key Naming

Transit key names are `subject-<SHA-256 hex of the subjectId's UTF-8 bytes>`. The raw subjectId never appears in Vault request paths, key listings, or audit logs — it cannot redirect requests to other Vault paths (path injection) and subject IDs that are themselves PII (emails, usernames) are not leaked to Vault operators.

## Requirements

- Java 25
- Depends on `streamrune-core` and `streamrune-crypto-api`
- A `ForgottenSubjectStore` for the required tombstone (see *Terminal Erasure*). The
  `JdbcForgottenSubjectStore` used in the example above lives in `streamrune-postgres-crypto`
  (`org.streamrune.crypto.postgres`); any `ForgottenSubjectStore` implementation works.

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-vault-crypto:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-vault-crypto:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Best Practices

- **Use with CachedCryptoEngine** — Vault round-trip latency (~1-5ms) makes caching essential for aggregate replay.
- **Token management** — use Vault Agent Sidecar or kubernetes auth for token renewal; short-lived tokens reduce risk.
- **Network security** — run Vault on a private network; use TLS in production.
- **Wrap with CachedCryptoEngine** — reduces Vault API calls and costs.

## Terminal Erasure (crypto-shredding)

This backend **enforces terminal erasure via a per-subject tombstone**, exactly like the filesystem and PostgreSQL engines — which is why `build()` requires a persistent `ForgottenSubjectStore` (see *Usage*). Vault's transit engine re-creates a deleted key on the next `encrypt`, so a durable tombstone is what makes a forget terminal rather than a silent resurrection.

`deleteKey(subjectId)` writes the tombstone to the `ForgottenSubjectStore` **before** deleting the Vault transit key (safe ordering: a tombstoned-but-still-live key fails closed, whereas a deleted key with no tombstone would resurrect).

Transit creates each subject's key on its first encrypt with `deletion_allowed=false`, and refuses to delete such a key. After the tombstone, `deleteKey` therefore sets `deletion_allowed=true` on the key's config and then deletes the key, so a forget needs no step outside the engine. If Vault refuses (for example, the token lacks `update` on `<mount>/keys/+/config`), `deleteKey` throws a `CryptoOperationException` that names the capability. The subject is then already gated by its tombstone and only the key material remains: fix the policy and re-run the forget (`deleteKey` is idempotent).

After a forget:

- `encrypt(subjectId, ...)` throws `SubjectForgottenException` — and re-checks the tombstone *after* the Vault call, so a request racing the erasure cannot slip a fresh key past it.
- `decrypt(subjectId, oldCiphertext)` throws `KeyNotFoundException`, which `CryptoShreddingModule` maps to `"[REDACTED]"`.
- `reinstate(subjectId)` lifts the tombstone **only** for the documented crashed-erasure residue (the tombstone was committed but the transit DELETE never landed): the surviving transit key's `creation_time` must equal the one `deleteKey` recorded with the tombstone. A key created *after* the forget is not the same subject and is refused. In an application, call it through `ForgetSubjectService.reinstate(subjectId, requester)`, which records the reinstate, or the refusal, in the audit log as `GDPR_REINSTATE`; the engine method alone writes no audit record.

You do not need to enforce erasure outside this engine; the tombstone store is the enforcement.

## Known Limitations

- **Token auth only** — AppRole / Kubernetes auth not in this scope.
- **No key rotation** — Vault handles this natively via key versioning.
