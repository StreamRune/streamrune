# StreamRune Crypto

Umbrella module for field-level encryption of event payloads. Supports crypto shredding (GDPR right to erasure) via the `CryptoEngine` interface -- when a subject's key is deleted, encrypted fields return `"[REDACTED]"` instead of throwing, so event replay continues safely. Erasure is terminal on every shipped engine: encrypting for a forgotten subject throws `SubjectForgottenException`.

## Modules

| Module | Description |
|---|---|
| [streamrune-crypto-api](streamrune-crypto-api/README.md) | `CryptoShreddingModule` (Jackson), `CachedCryptoEngine`, the `ForgottenSubjectStore` tombstone SPI. The `CryptoEngine` interface and `@Encrypted` live in `streamrune-core`; the in-memory test engine lives in `streamrune-test` |
| [streamrune-aws-kms-crypto](streamrune-aws-kms-crypto/README.md) | AWS KMS-backed engine -- key material never leaves AWS |
| [streamrune-filesystem-crypto](streamrune-filesystem-crypto/README.md) | File system-backed engine -- AES-256 keys stored in a two-level directory structure |
| [streamrune-postgres-crypto](streamrune-postgres-crypto/README.md) | PostgreSQL-backed engine -- keys stored in an `encryption_keys` table |
| [streamrune-vault-crypto](streamrune-vault-crypto/README.md) | HashiCorp Vault Transit Engine-backed engine -- key material never leaves Vault |

## Requirements

- Java 25+
- `streamrune-core`

Each backend module adds its own dependencies (AWS SDK, Vault client, PostgreSQL driver, etc.).
