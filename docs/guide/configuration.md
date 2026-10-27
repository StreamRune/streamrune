# Configuration reference

Every configuration property of the Spring Boot, Quarkus and Micronaut integrations, with its
default. The three bind the same keys under the `streamrune.` prefix, with the same defaults;
Spring and Micronaut also declare them in their configuration metadata, so an IDE completes them.
A property that exists on one integration only is described in that integration's README, such as
Micronaut's switch for its runner lifecycle. The guides explain what each setting does; this page
lists them all.

## How defaults are written

A default is written in one form in this guide and in every other table with a `Default` column:

- a duration as the shortest exact one of `Nd`, `Nh`, `Nm`, `Ns` and `Nms`: `7d`, `1m`, `30s`,
  `500ms`. All three integrations accept that form as it is; Quarkus also reads ISO-8601, so `7d`
  or `PT168H`, and `1m` or `PT60S`, there;
- a boolean or a number as its value: `true`, `100`, `2.0`; a text as it is: `operator-only`;
- `—` where the property has no default.

The tests of each integration (`ConfigurationDocLintTest`) compare every such table with the
defaults the integration binds, and fail on a key that the guides do not name or that the
integration does not bind.

## Commands, locking and retries

| Property | Default | Meaning |
|---|---|---|
| `streamrune.snapshot-every-n-events` | `100` | Events between two snapshots of an aggregate under the default snapshot policy. |
| `streamrune.retry-max-attempts` | `3` | Attempts of a command whose append met an optimistic-lock conflict. |
| `streamrune.retry-initial-delay-ms` | `50` | Delay before the first retry, in milliseconds. |
| `streamrune.retry-backoff-multiplier` | `2.0` | Factor by which the retry delay grows with each attempt. |
| `streamrune.lock-timeout` | `5s` | How long a command waits for its aggregate's lock; `0` tries once, a negative value fails the start. |
| `streamrune.stripe-count` | `1024` | Stripes of the in-process aggregate locker. |
| `streamrune.max-in-flight-async-commands` | `10000` | `executeAsync` commands admitted and not yet complete beyond which the next one is refused. |
| `streamrune.circuit-breaker-failure-threshold` | `5` | Consecutive command failures that open the circuit breaker. |
| `streamrune.circuit-breaker-cooldown` | `30s` | How long an open circuit waits before it lets a probe through. |
| `streamrune.validation-enabled` | `true` | Whether the Bean Validation command interceptor is registered. |
| `streamrune.security.trust-user-id-header` | `false` | `true` takes the identity from `X-User-Id`: only behind a gateway that authenticates every caller and sets it. |
| `streamrune.metadata.baggage-allowlist` | — | OpenTelemetry baggage keys a request may carry into event metadata; none by default. |

## Event store, projections and subscriptions

| Property | Default | Meaning |
|---|---|---|
| `streamrune.event-store.schema.auto-initialize` | `true` | Whether the framework's Flyway migrations run at startup. |
| `streamrune.event-store.statement-timeout` | `30s` | Bound on every statement of the event store; `0` sets none of its own. |
| `streamrune.polling-interval-ms` | `5000` | Catch-up polling interval of the auto-configured projection subscriptions, in milliseconds. |
| `streamrune.polling-jitter-ms` | `1000` | Random jitter added to that interval, in milliseconds. |
| `streamrune.projections.auto-discovery.enabled` | `true` | Whether the integration builds and starts the runners of the `@ProjectionConfig` projections. |
| `streamrune.projections.<name>.enabled` | `true` | Whether the projection of that name is discovered. |
| `streamrune.projections.<name>.error-strategy` | — | The error strategy of that projection; unset, the one its `@ProjectionConfig` names. |
| `streamrune.projection-dlq-enabled` | `false` | Whether the PostgreSQL projection dead-letter store is created (with a `DataSource`). |
| `streamrune.subscription.health.lag-threshold` | `1000` | Lag, in events, at or above which a running subscription reads `DEGRADED`. |
| `streamrune.subscription.single-active-consumer.enabled` | `true` | Whether a lease elects one replica per subscription (with a `DataSource`). |
| `streamrune.subscription.single-active-consumer.lease-ttl` | `15s` | How long a lease outlives its last renewal: the bound on failover. |
| `streamrune.event-audit-enabled` | `false` | Whether the event audit store and its query are created (with a `DataSource`). |
| `streamrune.query-cache.enabled` | `false` | Whether the `CachingQueryBus` is created. |

## Sagas

| Property | Default | Meaning |
|---|---|---|
| `streamrune.saga.enabled` | `true` | Whether the PostgreSQL saga stores are created (with a `DataSource`). |
| `streamrune.saga.forward-fence` | `auto` | The forward fence of the runners that set none: `auto` follows single-active-consumer, `on`, `off`. |
| `streamrune.saga.compensation-retry-enabled` | `true` | Whether the compensation-retry sweeper re-drives stuck compensations; `false` leaves it sampling the gauges. |
| `streamrune.saga.compensation-retry-interval` | `1m` | How often the sweeper re-drives, and the age an episode reaches before it is re-driven. |
| `streamrune.saga.compensation-retry-give-up-after` | `1h` | How long a saga may stay `COMPENSATING` before the sweeper faults it. |
| `streamrune.saga.dead-letter-retention-max-age` | `7d` | How long saga dead letters are kept; at most the inbox window. |

## Command inbox and dead letters

| Property | Default | Meaning |
|---|---|---|
| `streamrune.inbox.retention-max-age` | `7d` | How long processed command-inbox rows are kept; `0` or negative keeps them. |
| `streamrune.dead-letter.enabled` | `true` | Whether the retry runner replays dead-lettered commands automatically. |
| `streamrune.dead-letter.max-retries` | `5` | Replays of an entry before it is exhausted. |
| `streamrune.dead-letter.retry-interval-ms` | `60000` | Interval between two replays, in milliseconds. |
| `streamrune.dead-letter.retention-max-age` | `7d` | How long dead-letter entries are kept; at most the inbox window when a command inbox exists. |
| `streamrune.dead-letter.unkeyed-replay` | `operator-only` | Whether entries of commands without an idempotency key replay automatically (`automatic`) or wait for an operator. |

## Transactional outbox

| Property | Default | Meaning |
|---|---|---|
| `streamrune.outbox.enabled` | `false` | Whether the outbox relay runs. |
| `streamrune.outbox.batch-size` | `100` | Entries the relay claims per poll. |
| `streamrune.outbox.flush-interval-ms` | `5000` | Interval between two polls of the relay, in milliseconds. |
| `streamrune.outbox.retry-max-attempts` | `10` | Delivery attempts a rejected entry may accrue before it is `FAILED`. |
| `streamrune.outbox.retry-initial-delay-ms` | `1000` | First retry delay, in milliseconds; it grows by the multiplier, up to 60s. |
| `streamrune.outbox.retry-multiplier` | `2.0` | Factor by which the retry delay grows with each attempt. |
| `streamrune.outbox.retention-max-age` | `7d` | How long delivered entries are kept; `0` or negative keeps them. |
| `streamrune.outbox.skipped-retention-max-age` | `30d` | How long skipped entries are kept; `0` or negative keeps them. |

## Server-Sent Events

| Property | Default | Meaning |
|---|---|---|
| `streamrune.sse.enabled` | `false` | Whether the Server-Sent Events endpoint and its feed exist. |
| `streamrune.sse.timeout` | `5m` | How long a stream stays open; `0` or negative sets no timeout. |
| `streamrune.sse.keep-alive-interval` | `30s` | Interval of the keepalive comment frame; `0` or negative writes none. |
| `streamrune.sse.polling-interval` | `1s` | How often the feed reads the global stream; at least `1ms`. |

## Metrics and startup

| Property | Default | Meaning |
|---|---|---|
| `streamrune.metrics.enabled` | `true` | Whether the Micrometer `StreamRuneMetrics` bean is created. |
| `streamrune.metrics.prefix` | `streamrune` | The prefix of every meter name. |
| `streamrune.startup-log` | `true` | Whether the integration logs its banner at startup. |

## GDPR and encryption

| Property | Default | Meaning |
|---|---|---|
| `streamrune.gdpr.snapshot-purge` | `required` | `required`: the forget service needs a `SnapshotPurger` bean; `none`: no snapshot was ever stored. |
| `streamrune.crypto.cache.enabled` | `true` | Whether decrypted values are cached (`CachedCryptoEngine`); `false` also turns the AWS KMS data-key cache off. |
| `streamrune.crypto.cache.maximum-size` | `10000` | Decrypted entries the cache holds. |
| `streamrune.crypto.cache.expire-after-write` | `5m` | How long a cached entry lives after it was written. |
| `streamrune.crypto.cache.expire-after-access` | — | How long a cached entry lives after its last access; unset, entries expire by write age only. |
| `streamrune.crypto.filesystem.enabled` | `false` | Whether the file-system key engine is active. |
| `streamrune.crypto.filesystem.key-directory` | `/var/lib/streamrune/keys` | Directory of the file-system engine's keys. |
| `streamrune.crypto.postgres.enabled` | `false` | Whether the PostgreSQL key engine is active. |
| `streamrune.crypto.postgres.table-name` | `encryption_keys` | Table of the PostgreSQL engine's keys. |
| `streamrune.crypto.vault.enabled` | `false` | Whether the Vault transit engine is active. |
| `streamrune.crypto.vault.address` | `http://localhost:8200` | Address of the Vault server. |
| `streamrune.crypto.vault.token` | — | Vault token; required when the engine is active. |
| `streamrune.crypto.vault.engine-mount` | `transit` | Mount path of Vault's transit engine. |
| `streamrune.crypto.aws.enabled` | `false` | Whether the AWS KMS engine is active. |
| `streamrune.crypto.aws.region` | — | AWS region; required when the engine is active. |
| `streamrune.crypto.aws.kms-key-id` | — | KMS key id or ARN; required when the engine is active. |
| `streamrune.crypto.aws.data-key-cache.maximum-size` | `1000` | Plaintext data keys cached per map; `0` turns the cache off. |
| `streamrune.crypto.aws.data-key-cache.expire-after-write` | `5m` | How long a cached data key lives after it was stored. |
