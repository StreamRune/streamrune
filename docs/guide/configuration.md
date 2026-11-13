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
  `500ms`. All three integrations accept that form as it is, and ISO-8601 (`PT168H` for `7d`,
  `PT60S` for `1m`, `PT0.05S` for `50ms`) as well;
- a boolean or a number as its value: `true`, `100`, `2.0`; a text as it is: `operator-only`;
- `—` where the property has no default.

The tests of each integration (`ConfigurationDocLintTest`) compare every such table with the
defaults the integration binds, and fail on a key that the guides do not name or that the
integration does not bind.

## What the integrations refuse

The three integrations read a value the same way, or refuse to start on it with a message that
names the property and the value:

- **A duration without its unit.** `5000` would be milliseconds on Spring and seconds on Quarkus,
  so all three refuse a number without a unit, zero included: write `5000ms`, and zero as `0s`.
- **A value an integration cannot read as the property's type**, such as `soon` for a duration or
  `many` for a number. Spring and Quarkus refuse it when they bind it. Micronaut would bind the
  property's default in its place, so its integration refuses it at startup: a unit in upper case
  or after a space (`1D`, `1 d`), a decimal (`1.5d`), and a number of milliseconds above 2147483647
  (`2592000000ms`; write `30d`) among them.
- **A switch spelled other than `true` or `false`**, in lower case: `TRUE`, `yes`, `on` and `1`
  are refused, because the frameworks read them differently and a bean condition can read a
  spelling one way while the bound setting reads it the other. That holds for a value that reaches
  the integration as text: a properties file, an environment variable, a system property or quoted
  YAML. Spring's and Micronaut's YAML parser reads an unquoted `yes`, `on`, `no` or `off` as a YAML
  boolean before StreamRune sees it, so there it arrives as `true` or `false` and is accepted;
  Quarkus's YAML parser keeps it as text and refuses it. Spring and Micronaut refuse an empty switch
  too; Quarkus reads it as not set. The per-projection switches
  (`streamrune.projections.<name>.enabled`, `streamrune.projections.auto-discovery.enabled`) and
  `streamrune.subscription.single-active-consumer.enabled` are read by one parser on all three,
  for the setting and the bean condition alike, which takes `true`/`false`, `on`/`off`, `yes`/`no`,
  `y`/`n`, `t`/`f` and `1`/`0` in any case.
- **A name that reads like a property but is none.** Quarkus refuses every `streamrune.` name its
  configuration mapping does not declare. Spring and Micronaut ignore a name they do not bind, with
  two exceptions they refuse, naming the property to set instead: a duration property written with
  a `-ms` suffix, and the outbox retry multiplier written as `retry-multiplier` instead of
  `retry-backoff-multiplier`.

## Commands, locking and retries

| Property | Default | Meaning |
|---|---|---|
| `streamrune.snapshot-every-n-events` | `100` | Events between two snapshots of an aggregate under the default snapshot policy. |
| `streamrune.retry-max-attempts` | `3` | Attempts of a command whose append met an optimistic-lock conflict. |
| `streamrune.retry-initial-delay` | `50ms` | Delay before the first retry. |
| `streamrune.retry-backoff-multiplier` | `2.0` | Factor by which the retry delay grows with each attempt. |
| `streamrune.lock-timeout` | `5s` | How long a command waits for its aggregate's lock; `0s` tries once, a negative value fails the start. |
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
| `streamrune.event-store.statement-timeout` | `30s` | Bound on every statement of the event store; `0s` sets none of its own. |
| `streamrune.polling-interval` | `5s` | Catch-up polling interval of the auto-configured projection subscriptions; at least `1ms`. |
| `streamrune.polling-jitter` | `1s` | Random jitter added to that interval; `0s` disables it, a positive value below `1ms` fails the start. |
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
| `streamrune.inbox.retention-max-age` | `7d` | How long processed command-inbox rows are kept; `0s` or negative keeps them. |
| `streamrune.dead-letter.enabled` | `true` | Whether the retry runner replays dead-lettered commands automatically. |
| `streamrune.dead-letter.max-retries` | `5` | Replays of an entry before it is exhausted. |
| `streamrune.dead-letter.retry-interval` | `1m` | Interval between two replays. |
| `streamrune.dead-letter.retention-max-age` | `7d` | How long dead-letter entries are kept; at most the inbox window when a command inbox exists. |
| `streamrune.dead-letter.unkeyed-replay` | `operator-only` | Whether entries of commands without an idempotency key replay automatically (`automatic`) or wait for an operator. |

## Transactional outbox

| Property | Default | Meaning |
|---|---|---|
| `streamrune.outbox.enabled` | `false` | Whether the outbox relay runs. |
| `streamrune.outbox.batch-size` | `100` | Entries the relay claims per poll. |
| `streamrune.outbox.flush-interval` | `5s` | Interval between two polls of the relay. |
| `streamrune.outbox.retry-max-attempts` | `10` | Delivery attempts a rejected entry may accrue before it is `FAILED`. |
| `streamrune.outbox.retry-initial-delay` | `1s` | First retry delay; it grows by the multiplier, up to 60s. |
| `streamrune.outbox.retry-backoff-multiplier` | `2.0` | Factor by which the retry delay grows with each attempt. |
| `streamrune.outbox.retention-max-age` | `7d` | How long delivered entries are kept; `0s` or negative keeps them. |
| `streamrune.outbox.skipped-retention-max-age` | `30d` | How long skipped entries are kept; `0s` or negative keeps them. |

## Server-Sent Events

| Property | Default | Meaning |
|---|---|---|
| `streamrune.sse.enabled` | `false` | Whether the Server-Sent Events endpoint and its feed exist. |
| `streamrune.sse.timeout` | `5m` | How long a stream stays open; `0s` or negative sets no timeout. |
| `streamrune.sse.keep-alive-interval` | `30s` | Interval of the keepalive comment frame; `0s` or negative writes none. |
| `streamrune.sse.polling-interval` | `1s` | How often the feed reads the global stream; at least `1ms`. |

## Metrics and startup

| Property | Default | Meaning |
|---|---|---|
| `streamrune.metrics.enabled` | `true` | Whether the Micrometer `StreamRuneMetrics` bean is created. |
| `streamrune.metrics.prefix` | `streamrune` | The prefix of every meter name. |
| `streamrune.startup-log` | `true` | Whether the integration logs its banner at startup. |
| `streamrune.jvm.allow-graal-jit` | `false` | `true` starts on a JVM the startup check refuses (one that runs the Graal JIT, or a GraalVM runtime whose options the check cannot read), with an `ERROR` line; see [Supported JVMs](production.md#supported-jvms). |

## Tracing

The switches of the spans StreamRune starts. They take effect when the application has
OpenTelemetry: an `OpenTelemetry` bean, or else a registered global OpenTelemetry, such as the
OpenTelemetry Java agent's. Without it nothing is traced and the switches do nothing. See
[Tracing](production.md#tracing).

| Property | Default | Meaning |
|---|---|---|
| `streamrune.tracing.enabled` | `true` | Master switch: `false` starts no StreamRune span at all. |
| `streamrune.tracing.command.enabled` | `true` | Whether the `command/{CommandType}` spans are started. |
| `streamrune.tracing.subscription.enabled` | `true` | Whether the `subscription/{name}/deliver` spans are started. |
| `streamrune.tracing.projection.enabled` | `true` | Whether the `projection/{name}/process` spans are started. |
| `streamrune.tracing.event-store.enabled` | `true` | Whether the `eventstore/…` spans, snapshots included, are started. |
| `streamrune.tracing.lock.enabled` | `true` | Whether the `lock/acquire` spans are started. |
| `streamrune.tracing.outbox.enabled` | `true` | Whether the `outbox/relay` and `outbox/publish` spans are started. |
| `streamrune.tracing.saga.enabled` | `true` | Whether the `saga/{SagaType}/…` spans are started. |

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
