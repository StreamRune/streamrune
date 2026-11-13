# Crypto-shredding benchmarks

`CryptoShreddingBenchmark` measures the serialization of an event with `@Encrypted` fields on a real
AES-256-GCM engine: `FileSystemCryptoEngine`, with its keys in a temporary directory. The event goes
through the `ObjectMapper` that `PostgresEventStore` writes and reads every event with
(`PostgresEventStore.createObjectMapper`, which registers `CryptoShreddingModule` on the engine), so
the benchmark measures the event store's own serialization path. No network, no database. That
method is package-private, so the benchmark is declared in the package `org.streamrune.postgres`;
JMH runs it on the class path, where that package may span this source set and the store's jar.

| Benchmark | Parameter | What one operation is |
| --- | --- | --- |
| `serializeEncrypting` | `encryptedFields` 1, 5 | the event to JSON, each `@Encrypted` field encrypted |
| `deserializeDecrypting` | `encryptedFields` 1, 5 | the JSON back to the event, each `@Encrypted` field decrypted |

The engine is used bare, as the event store calls it, without the decrypt cache an integration can
put in front of it (`CachedCryptoEngine`): a cached decryption would measure the cache, not the
cipher. Each encryption takes a fresh cipher and checks its result with a second one before it
returns it, and each decryption takes a fresh cipher. All fields of the event belong to one subject;
the trial's first serialization creates its key file, which every operation reads again from the
operating system's page cache.

## Running

```bash
./gradlew :streamrune-crypto:streamrune-filesystem-crypto:jmh
./gradlew :streamrune-crypto:streamrune-filesystem-crypto:jmh -PjmhIncludes=deserializeDecrypting
```

`-PbenchCi`, the nightly settings and the results file are described in
`streamrune-runtime/src/jmh/README.md`. `CryptoShreddingBenchmarkSmokeTest` runs every method twice
outside the harness in `jmhSmokeTest`, part of `check`, and checks that the serialized event carries
no plaintext.
