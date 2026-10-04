# StreamRune FileSystem Crypto

File system-backed `CryptoEngine` implementation. Keys are stored as raw AES-256 bytes in a two-level directory structure, avoiding the single-directory file limit.

## Overview

```
keyStorage/
├── ab/
│   ├── ab123456abcdef.subjectId    <- raw AES-256 key bytes
│   ├── ab123456abcdef.forgotten    <- crypto-shredding tombstone (created by deleteKey)
│   └── ab123456abcdef.generation   <- highest key generation an erasure destroyed
├── cd/
│   └── cd789xyz.subjectId
└── ...
```

The directory name is derived from the first two characters of the SHA-256 hash of the subject ID (hashed as its UTF-8 bytes, so paths are identical across JVMs regardless of platform default charset). File names are the full SHA-256 hex string with a `.subjectId` suffix (keys), a `.forgotten` suffix (tombstones) or a `.generation` suffix (the destroyed key generation, a small integer — no PII, no key material). This distributes keys evenly across ~256 top-level directories. A first-generation key file holds the 32 raw key bytes; a key re-minted after an erasure carries one extra generation byte.

## Usage

```java
Path keyDirectory = Path.of("/var/streamrune/keys");

FileSystemCryptoEngine engine = FileSystemCryptoEngine.builder()
    .keyDirectory(keyDirectory)
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
engine.reinstate(subject);              // lifts the tombstone (old data stays unrecoverable)
engine.encrypt(subject, ...);           // mints a brand-new key, one generation higher
                                        // (while its .generation record is intact)
```

## Terminal Erasure (crypto-shredding)

`deleteKey(subjectId)` is the GDPR Article 17 operation and is **terminal**. Beyond removing the key file it writes an on-disk tombstone marker (`<hash>.forgotten`) in the same two-level directory. A subsequent `encrypt(subjectId, …)` for a forgotten subject throws `SubjectForgottenException` (in `org.streamrune.core.crypto`) instead of silently minting a fresh key — so a retried command or a re-imported event cannot un-do an erasure by collecting new recoverable PII under a new key. The tombstone is on disk, so it survives JVM restarts, and it is fsynced: each erasure step reaches stable storage before the next one runs — the tombstone, then the `<hash>.generation` record, then the key-file delete — and the delete before `deleteKey` returns. A power loss after an erasure reported success therefore cannot bring the key file back or drop the tombstone or the generation record, and a power loss mid-erasure leaves the same residue as a crash, which re-running the forget completes. On Windows/NTFS the engine cannot fsync a directory, so there directory-entry durability is whatever NTFS provides.

`decrypt` of the subject's **old** ciphertext after a forget throws `KeyNotFoundException` (which `CryptoShreddingModule` maps to `"[REDACTED]"`).

`engine.reinstate(subjectId)` is the only way to re-register a forgotten subject: it removes that subject's tombstone, and there is no engine-wide opt-out. The old key and old ciphertext stay unrecoverable; only future encrypts are re-enabled. When `deleteKey` destroyed a key it also wrote that key's generation to `<hash>.generation`, and while that record is intact the next key is minted one generation above the destroyed one, so the subject's pre-erasure ciphertext keeps reading as `"[REDACTED]"` rather than failing the GCM tag check. `reinstate` only deletes the tombstone: it never rebuilds a missing `.generation` file (see *Best Practices* on backups). A subject supports at most 126 erase/re-register cycles. In an application, call it through `ForgetSubjectService.reinstate(subjectId, requester)`, which records the reinstate in the audit log as `GDPR_REINSTATE`; the engine method alone writes no audit record.

## Durability

Every key-directory change is forced to stable storage (fsync) before the call that made it returns:

- **Key mint** — a subject's first `encrypt` writes the new key to a temp file and forces it, publishes it under its final name with `link(2)`, removes the temp name, and forces the shard directory, the key directory and any directory it had to create — all before the key is used to encrypt. An event committed under a fresh key therefore never outlives that key across a power loss: a power loss earlier in the mint loses only a key that nothing was encrypted under, and the next `encrypt` mints or loads a complete one. The same forcing applies when two processes race to mint and one adopts the other's key.
- **`deleteKey`** — each erasure step is forced before the next one runs (see *Terminal Erasure*).
- **`reinstate`** — the tombstone removal is forced before `reinstate` returns.
- **First use of a key this engine instance did not mint** — a key file another instance or process sharing the directory published (and may not have forced yet, or died before forcing), or that the application's previous run published, is forced — the file, its shard directory and the key directory — by the first `encrypt` in this engine instance that uses it, before anything is encrypted under it. The engine instance then remembers that key file (by a fingerprint of its content, for the 16,384 most recently used keys), so later encrypts force nothing; a key file replaced under the same name is forced again on its first use. `decrypt` never forces: it produces no ciphertext, and every ciphertext it can read was written by an `encrypt` that had already forced its key. The key directory itself is assumed provisioned, or created by a mint that completed its forces: a first use forces nothing above the key directory, so if a peer's first-ever mint created the directory and died before forcing it, a power loss can still take the directory with every key under it. When several instances or processes share a key directory, create it ahead of first use.

A failed fsync fails the call loudly: a mint or first use whose fsync failed returns no key (the same engine instance forces that key again before its next `encrypt` uses it), and an erasure or reinstate whose fsync failed reports failure; re-running the call converges. On Windows/NTFS the engine cannot fsync a directory, so there directory-entry durability is whatever NTFS provides. See the [GDPR erasure guide](../../docs/guide/advanced/gdpr-erasure.md) for the crash points.

## Key Components

| Component | Description |
|---|---|
| `FileSystemCryptoEngine` | Implements `CryptoEngine` with file-backed key storage |
| `Builder` | `.keyDirectory(Path)` — required, path to key storage root |

## Requirements

- Java 25
- Depends on `streamrune-core`

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-filesystem-crypto:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-filesystem-crypto:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Best Practices

- **Back up the key directory** — key material is stored as raw bytes. Lose the directory, lose all data.
- **Restrict filesystem permissions** — only the application user should have read/write access to the key directory.
- **Share a key directory only within one kernel's view of the filesystem** — several JVMs on one host, or a volume mounted by a single node, may share one directory: key minting claims the key file first-writer-wins, and erasure writes the tombstone before deleting the key. Across NFS clients, attribute caching can hide another client's tombstone, so the erasure/mint race cannot be closed there. Use `streamrune-postgres-crypto`, Vault or AWS KMS for key storage shared across hosts.
- **Use a dedicated volume** — place the key directory on a volume with regular snapshots/backups.
- **Back up the key directory only as a consistent whole** — from a frozen snapshot of the volume (`fsfreeze`, or the snapshot tool's quiesce option), from a crash-consistent snapshot on a journaled filesystem that keeps metadata operations in order (for example ext4 or XFS; the engine fsyncs its mint, erasure and `reinstate` steps in order, so a crash image holds only states a crash can leave), or from a copy taken while no `deleteKey`, `reinstate` or `encrypt` runs against it. Never take a live file-by-file copy (for example `rsync` of a running directory): `deleteKey` takes three separate steps — it writes `<hash>.forgotten`, then `<hash>.generation`, then deletes `<hash>.subjectId` — and a live copy can see some of them and not others, for example a tombstone without its generation record. Never restore `.forgotten` files without the `.generation` files from the same point in time. `reinstate` does not rebuild a missing generation record: after reinstate the next `encrypt` reuses a destroyed generation, and the subject's pre-erasure events then fail with `CryptoMappingException` instead of reading as `"[REDACTED]"`, so the aggregate cannot be loaded and projection replays quarantine those events. This cannot be repaired once the new key has encrypted anything, because old and new ciphertext carry the same generation byte. Restore into a key directory no running engine instance uses (stop the application first), or sync the restored files (`sync`) before an instance uses them: a running instance remembers a key it already forced by the content of its file, so a file restored under the same name with identical bytes is not forced again by that instance. See the operational notes in the [GDPR erasure guide](../../docs/guide/advanced/gdpr-erasure.md) (section *`reinstate(SubjectId)` — undoing an erasure*).

## Threat Model & Key Custody

Keys are stored as **plaintext AES-256 bytes** on disk — there is no envelope encryption (no KEK wrapping the per-subject keys):

- An attacker (or backup/snapshot leak) with access to both the key directory and the event store obtains keys *and* ciphertext together. Keep the key directory on a separate volume with separate access controls from the database, or use `streamrune-vault-crypto` / `streamrune-aws-kms-crypto` so key material never touches application-accessible storage.
- **Crypto-shredding is only as strong as backup hygiene** — any snapshot of the key directory taken *before* a subject was forgotten resurrects that subject's key on restore. Align snapshot retention with your GDPR erasure deadlines and never restore the key directory from a pre-forget snapshot. Restore `.forgotten` tombstones only together with the `.generation` records taken at the same point (see *Best Practices*).

Envelope encryption (per-subject keys wrapped by a KEK held in Vault/KMS/HSM) is the standard remedy and is on the roadmap; until then the controls above are operational, not technical.

## Known Limitations

- **Keys lost if disk fails** — no replication or backup mechanism built in.
- **`deleteKey()` is terminal** — it writes an on-disk `.forgotten` tombstone (fsynced; survives JVM restarts and power loss); a later `encrypt` for that subject throws `SubjectForgottenException` rather than resurrecting the key. Use `reinstate(subjectId)` to re-register a subject. See *Terminal Erasure*.
- **No key rotation** — rotating a key requires re-encrypting all events. Ciphertext carries a 2-byte `[format version][key version]` header (followed by the 12-byte GCM IV and the AES-256-GCM ciphertext+tag). The key-version byte is the key generation, which only a post-erasure re-mint advances. A blob with an unknown format or key version, a truncated blob or a GCM tag mismatch fails with `CryptoMappingException` (deterministic — quarantined, not retried) instead of a generic error; a blob from a destroyed generation reads as `"[REDACTED]"`.
- **No envelope encryption** — key bytes are stored unwrapped; see *Threat Model & Key Custody*.
