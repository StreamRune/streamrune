# StreamRune Quarkus

Quarkus integration for StreamRune using ArC (CDI) producers. No Quarkus extension build step is
required: the jar ships a `META-INF/beans.xml` marker **and** a Jandex index
(`META-INF/jandex.idx`), so Quarkus indexes it and discovers the producers, the
`@ConfigMapping` interfaces, the JAX-RS provider, and the lifecycle observers automatically.

## Overview

Configure StreamRune via `application.properties`, inject the buses anywhere, and let
`StreamRuneLifecycle` manage background runners.

```java
@Inject
CommandBus commandBus;

@POST
public Response placeOrder(PlaceOrderCommand cmd) {
    // Audited, authorized and recorded as the request's caller: see "Request context propagation".
    var result = commandBus.execute(cmd);
    ...
}
```

## Installation

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-quarkus:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Configuration

All keys live under the `streamrune.` prefix and bind through `StreamRuneQuarkusProperties` and
`StreamRuneQuarkusCryptoProperties` (`@ConfigMapping`); the
[configuration reference](../../docs/guide/configuration.md) says what each one does. The listing
below sets every key to its default, durations in ISO-8601, the form SmallRye Config reads; a key
without a default and the keys of one projection are commented out. A `.properties` comment must
start its own line — text after a value is part of the value.

```properties
# Commands, locking and retries
streamrune.snapshot-every-n-events=100
streamrune.retry-max-attempts=3
streamrune.retry-initial-delay-ms=50
streamrune.retry-backoff-multiplier=2.0
streamrune.lock-timeout=PT5S
streamrune.stripe-count=1024
streamrune.max-in-flight-async-commands=10000
streamrune.circuit-breaker-failure-threshold=5
streamrune.circuit-breaker-cooldown=PT30S
streamrune.validation-enabled=true
streamrune.security.trust-user-id-header=false
# streamrune.metadata.baggage-allowlist=

# Event store, projections and subscriptions
streamrune.event-store.schema.auto-initialize=true
# bound on every event-store statement (per transaction or statement, never on the
# Agroal session); PT0S sets none
streamrune.event-store.statement-timeout=PT30S
streamrune.polling-interval-ms=5000
streamrune.polling-jitter-ms=1000
streamrune.projections.auto-discovery.enabled=true
# streamrune.projections.<name>.enabled=true
# streamrune.projections.<name>.error-strategy=
streamrune.projection-dlq-enabled=false
streamrune.subscription.health.lag-threshold=1000
streamrune.subscription.single-active-consumer.enabled=true
streamrune.subscription.single-active-consumer.lease-ttl=PT15S
streamrune.event-audit-enabled=false
# build-time flag: decides whether the CachingQueryBus bean exists
streamrune.query-cache.enabled=false

# Sagas
streamrune.saga.enabled=true
streamrune.saga.forward-fence=auto
streamrune.saga.compensation-retry-enabled=true
streamrune.saga.compensation-retry-interval=PT60S
streamrune.saga.compensation-retry-give-up-after=PT1H
streamrune.saga.dead-letter-retention-max-age=PT168H

# Command inbox and dead letters
streamrune.inbox.retention-max-age=PT168H
streamrune.dead-letter.enabled=true
streamrune.dead-letter.max-retries=5
streamrune.dead-letter.retry-interval-ms=60000
streamrune.dead-letter.retention-max-age=PT168H
streamrune.dead-letter.unkeyed-replay=operator-only

# Transactional outbox
streamrune.outbox.enabled=false
streamrune.outbox.batch-size=100
streamrune.outbox.flush-interval-ms=5000
streamrune.outbox.retry-max-attempts=10
streamrune.outbox.retry-initial-delay-ms=1000
streamrune.outbox.retry-multiplier=2.0
streamrune.outbox.retention-max-age=PT168H
streamrune.outbox.skipped-retention-max-age=PT720H

# Server-Sent Events
# build-time flag: decides whether SseController and its event feed exist (deny-all
# SseAuthorizer unless you produce your own)
streamrune.sse.enabled=false
streamrune.sse.timeout=PT5M
streamrune.sse.keep-alive-interval=PT30S
# how often the SSE feed reads the global stream: the usual commit-to-frame delay; at least
# PT0.001S
streamrune.sse.polling-interval=PT1S

# Metrics and startup
streamrune.metrics.enabled=true
streamrune.metrics.prefix=streamrune
streamrune.startup-log=true

# GDPR and encryption
streamrune.gdpr.snapshot-purge=required
streamrune.crypto.cache.enabled=true
streamrune.crypto.cache.maximum-size=10000
streamrune.crypto.cache.expire-after-write=PT5M
# streamrune.crypto.cache.expire-after-access=
# crypto engine selection is fixed at build time (see below)
streamrune.crypto.filesystem.enabled=false
streamrune.crypto.filesystem.key-directory=/var/lib/streamrune/keys
streamrune.crypto.postgres.enabled=false
streamrune.crypto.postgres.table-name=encryption_keys
streamrune.crypto.vault.enabled=false
streamrune.crypto.vault.address=http://localhost:8200
# streamrune.crypto.vault.token=
streamrune.crypto.vault.engine-mount=transit
streamrune.crypto.aws.enabled=false
# streamrune.crypto.aws.region=
# streamrune.crypto.aws.kms-key-id=
streamrune.crypto.aws.data-key-cache.maximum-size=1000
streamrune.crypto.aws.data-key-cache.expire-after-write=PT5M
```

**More than one replica: a database-wide aggregate lock.** The produced `AggregateLocker` is the
in-process `LocalStripedLocker`, which serializes one aggregate's commands within one replica only.
On more than one replica produce `PgAdvisoryLocker.withDedicatedPool(...)` as the `AggregateLocker`
bean; the integration's producer is a `@DefaultBean` and steps aside:

```java
@ApplicationScoped
public class AggregateLockerProducer {

    @Produces
    @Singleton
    AggregateLocker aggregateLocker(
            @ConfigProperty(name = "quarkus.datasource.jdbc.url") String url,
            @ConfigProperty(name = "quarkus.datasource.username") String username,
            @ConfigProperty(name = "quarkus.datasource.password") String password) {
        return PgAdvisoryLocker.withDedicatedPool(url, username, password, 16);
    }

    void close(@Disposes AggregateLocker locker) throws Exception {
        ((AutoCloseable) locker).close(); // the locker owns its pool
    }
}
```

A command holds its lock connection for its whole run and borrows a second one from the Agroal pool
for its load and its append, so size the lock pool for the commands one replica runs at once and the
Agroal pool for the load and the append alone; the two pools of every replica together stay below
PostgreSQL's `max_connections` (see
[the aggregate lock across replicas](../../docs/guide/production.md#the-aggregate-lock-across-replicas)).

### Server-Sent Events

With `streamrune.sse.enabled=true` the integration serves
`GET /api/sse/{aggregateType}/{aggregateId}` — the live events of one stream, the registered
aggregate type plus its id as two path segments; an invalid part answers `400`.

- **What it emits** — one frame per domain event of that stream: `id` is the global offset, `data`
  the decrypted event as a JSON object, written by your application's JSON extension
  (`quarkus-rest-jackson` or `quarkus-rest-jsonb`). The integration does not bring one: without a
  JSON extension Quarkus REST has only its text writer and `data` carries the event's `toString()`.
  `: keepalive` comment frames are written every `streamrune.sse.keep-alive-interval` (default
  `30s`), and the server completes a stream after `streamrune.sse.timeout` (default `5m`); an
  `EventSource` reconnects on its own.
- **Opening frame** — as soon as the client is subscribed the endpoint writes one `: keepalive`
  comment frame, whatever the keepalive interval. A client that has read it receives every event
  of the stream stored from then on, while it stays connected. Quarkus REST sends the status line
  and the headers just before it subscribes the client, so on Quarkus this frame, not the response
  head (an `EventSource`'s `onopen`), is the signal that the client is subscribed.
- **Who feeds it** — the integration. It runs one `SseEventFeed` per application instance: a
  polling subscription that starts at the head of the global stream when the application starts
  and publishes every event stored from then on to the clients of the event's own stream, every
  `streamrune.sse.polling-interval` (default `1s`, at least `1ms`). No stored offset, no replay of
  history. The feed reads events only while a client is subscribed to this instance. With none, a
  poll is one read of the head of the global stream (`SELECT max(global_offset)` on PostgreSQL):
  no event is read or decrypted. With at least one, on any stream, the instance reads and decrypts
  every event of the global stream. A client that connects to an idle feed misses nothing: every
  event stored after its opening frame reaches it. Do not call `SseEventPublisher.publish`
  yourself for this endpoint — every frame would be written twice. To run a feed of your own,
  declare an `SseEventFeed` bean: it replaces the integration's, and you start and stop it.
- **Authorization** — every stream is denied (`403`) until you provide an `SseAuthorizer` bean:
  `boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId)`. `caller` is
  the request context `StreamRuneRequestFilter` built for the request, the one a command from that
  request authorizes against: `caller.userId()` is the caller (`null` when unauthenticated) and
  `caller.authority()` the roles and permissions your `UserRoleResolver` resolved for that user at
  the request edge, so a rule by role needs no request-scoped state of the web framework.
  `caller.authority()` is `null` when nothing was captured — an unauthenticated caller, no
  `UserRoleResolver`, a resolver that does not declare `requiresRequestContext()` (a lookup by
  user id: ask it yourself, `resolver.resolve(caller.userId())`), or a resolver that threw at the
  request edge (asked again it throws again, and the request ends without a stream). `null` means
  the roles are unknown, never "no roles": a rule that reads a role refuses on `null` first,
  whether the role would admit or refuse. Decide by `caller.userId()` and `caller.authority()`
  only: both are established at the request edge. In `caller.baggage()` the `role` entry is the
  forwarded `X-User-Role` header in trusted-gateway mode and absent in every other mode; every
  other entry is the client's own W3C `baggage` header in every identity mode and must not carry
  an access decision. A request the
  request-scoped `StreamRuneRequestContextHolder` holds no context for (the filter excluded from
  the application) or one whose context names another user than the `RequestIdentityPolicy`
  resolves is answered `403` and the authorizer is not called; the log says which.
  The authorizer is asked once, when the stream opens: a caller whose access ends afterwards keeps
  reading until the stream ends, so `streamrune.sse.timeout` (default `5m`) is also the bound on
  that. Keep it finite where access can change.

  ```java
  @Produces
  @Singleton
  SseAuthorizer sseAuthorizer() {
    return (caller, streamId) -> {
      UserId user = caller.userId();
      if (user == null) {
        return false;
      }
      UserAuthority authority = caller.authority();
      if (authority == null) {
        return false; // roles unknown: refuse before any role is read
      }
      if (authority.hasRole("SUPPORT")) {
        return true;
      }
      return streamId.aggregateType().value().equals("customer")
          && streamId.aggregateId().value().equals(user.value());
    };
  }
  ```
- **Threads** — the resource method is `@Blocking`: Quarkus REST runs the request filter and the
  `SseAuthorizer` on a worker thread, never on a Vert.x event loop, so an authorizer may read a
  database. The CDI request scope is active there: the endpoint reads the request context the
  filter stored in `StreamRuneRequestContextHolder` and hands it to the authorizer. The worker is
  held until the method has returned, not for the life of the stream; Quarkus REST then subscribes
  the client from an event loop, and neither that subscription nor the writing of a frame blocks.
- **Delivery guarantee** — live, best-effort, at-most-once. A frame reaches a client only while it
  is connected; nothing is redelivered, and `Last-Event-ID` is not honoured. Events stored before a
  client connected, while it was reconnecting, or while the instance was down are not sent to it.
  One exception: an event of its stream stored in the polling interval or so before the client
  subscribed, which the feed had not passed yet, can still arrive.
  Read the current state from a query after every (re)connect, and use a projection for anything
  that must see every event.
- **Lifecycle** — `streamrune.sse.enabled` is read at build time (it decides whether the resource
  and the feed exist) and again at runtime: switched off at runtime, the endpoint answers `404` and
  the feed does not start. The feed starts on the startup event and stops on the shutdown event.
- **Shutdown** — Quarkus stops its HTTP server before it fires the shutdown event, so what a
  connected client sees depends on a build-time property of your application.
  Built with `quarkus.shutdown.delay-enabled=true`, the application fires the shutdown-delay event
  at the start of its shutdown: the resource completes every open stream then (an `EventSource`
  sees a normal end and reconnects), answers a stream opened later in the shutdown already
  complete, and the graceful phase has no stream to wait for.
  Built without it (the Quarkus default), the server closes the connections as it stops: a
  connected client sees its connection cut, not a completed stream (an `EventSource` reconnects
  either way), and with `quarkus.shutdown.timeout` set a connected client holds the shutdown for
  that whole timeout, because an open stream is a request the graceful phase waits for. Build
  with `quarkus.shutdown.delay-enabled=true` wherever you set `quarkus.shutdown.timeout`.
- **Health** — the feed is the `sse-event-feed` component of the readiness check: `DOWN` when its
  polling thread has died, `DEGRADED` while its reads fail and are retried. A polling thread dies
  only of a JVM `Error`, and nothing restarts it: the check stays `DOWN` until the application
  restarts. The integration's feed is not a bean; to restart a feed without restarting the
  application, declare your own `SseEventFeed` bean (see "Who feeds it") and call `start()` on it.

Crypto engines bind through `StreamRuneQuarkusCryptoProperties`
(`streamrune.crypto.*`). Engine selection is **fixed at build time** via
`@IfBuildProperty` on the documented keys:

```properties
streamrune.crypto.filesystem.enabled=true
streamrune.crypto.filesystem.key-directory=/var/lib/streamrune/keys
# or
streamrune.crypto.postgres.enabled=true
# or (token required, fails fast when missing)
streamrune.crypto.vault.enabled=true
streamrune.crypto.vault.token=${VAULT_TOKEN}
# or (region and kms-key-id required, fail fast when missing)
streamrune.crypto.aws.enabled=true
streamrune.crypto.aws.region=eu-central-1
streamrune.crypto.aws.kms-key-id=arn:aws:kms:...
# optional: the AWS KMS engine's data-key cache (defaults shown; 0 disables it)
streamrune.crypto.aws.data-key-cache.maximum-size=1000
streamrune.crypto.aws.data-key-cache.expire-after-write=PT5M
```

A `.properties` comment must start its own line — text after a value is part of the value.

The Vault and AWS KMS engines record GDPR tombstones in a `JdbcForgottenSubjectStore` on the
application `DataSource`; without a `DataSource` their engine refuses to build.

With the crypto cache on (`streamrune.crypto.cache.enabled`, the default) and a `DataSource`
present, a subject forgotten on one replica is evicted from every other replica's cache through
PostgreSQL `LISTEN/NOTIFY`. Each replica listens on one extra connection: a separate Agroal pool of
one connection that copies the application datasource's URL, credentials (or credentials provider)
and JDBC properties, so it never takes a slot of the application pool. Its sessions are named
`<application name>-streamrune-crypto-forget-listen` in `pg_stat_activity` (just
`streamrune-crypto-forget-listen` when the application sets no `ApplicationName`). A `DataSource` that is not
Agroal goes through the generic `PostgresCryptoForgetSignal` derivation (see the
[GDPR erasure guide](../../docs/guide/advanced/gdpr-erasure.md)).

## Request identity

`StreamRuneRequestFilter` binds `RequestContext.userId` — the user every
`CommandAuthorizationPolicy`, `@RequireRole`/`@RequirePermission` and
`Authorization.requireOwner(...)` check trusts, and the user the audit records — and the SSE
endpoint hands that request context to your `SseAuthorizer`. Both resolve the user through one
`RequestIdentityPolicy` bean (the same rule as the Spring integration), which fails closed:

| Your setup | Identity | `X-User-Id` header | `X-User-Role` header |
|---|---|---|---|
| Quarkus Security present (or your own `AuthenticatedUserResolver` bean) | the authenticated `SecurityIdentity` principal; `null` when unauthenticated | ignored (a header naming another user is logged) | ignored |
| `streamrune.security.trust-user-id-header=true` | the `X-User-Id` header | the identity | baggage `role` |
| neither | none — every request is anonymous | ignored | ignored |

`X-User-Role` is read only in the trusted-gateway mode, where it becomes baggage `role` — recorded
in every event's metadata, so the gateway must forward the caller's role or strip the header. In
the other modes it is ignored and there is no `role` (an authenticated principal's roles are the
captured authority, not baggage), and a W3C `baggage: role=` entry never reaches the key.

`X-Correlation-Id`, `X-Trace-Id` and the allow-listed baggage are caller-supplied in every mode,
the trusted-gateway mode included; nothing may be addressed, authorized or deduplicated by them.
A correlation id is kept exactly as sent, with one exception: the `streamrune:` prefix is reserved
for the correlation ids the framework binds itself. `StreamRuneRequestFilter`
aborts a request that carries a correlation id with that prefix, or a correlation id, trace id or
user id that is longer than 255 characters or holds a control character, with status `400 Bad
Request`, content type `text/plain;charset=UTF-8` and the body `The request's correlation id, trace
id or user id is not acceptable` — the answer of the Spring and Micronaut filters, byte for byte.
The resource method does not run and `StreamRuneRequestContextHolder` stays empty. Of an `X-Correlation-Id` or `X-Trace-Id` header sent more than once the first
value is the header, as on the other two integrations; a blank first value counts as an absent
header. An authenticated principal the request edge refuses is logged at `WARN` once
per reason (its length, a control character). See
[What a client controls in event metadata](../../docs/guide/production.md#what-a-client-controls-in-event-metadata).

```properties
# Trusted-gateway mode. Only when a gateway in front of this service authenticates every caller,
# overwrites X-User-Id with the authenticated id and strips any client-supplied value (and owns
# X-User-Role the same way), and the service is reachable only through it — otherwise any client
# can act as any user.
streamrune.security.trust-user-id-header=true
```

`StreamRuneRequestIdentityValidator` logs the mode once at startup (`Request identity: …`, WARN for
the trusted gateway). In a Quarkus REST application, startup **fails** when a command bus runs an
identity-consuming authorization interceptor — `AuthorizationCommandInterceptor` (produced for a
`CommandAuthorizationPolicy` bean) or `AnnotationAuthorizationInterceptor` (produced for a
`UserRoleResolver` bean), or either one in the chain of a `VirtualThreadCommandBus` you produce
yourself — and the mode is anonymous: add Quarkus Security (or an `AuthenticatedUserResolver`
bean), or set the trusted-gateway flag. An
application without Quarkus REST derives no identities from HTTP requests and is not checked. See
[Authorization](../../docs/guide/advanced/authorization.md#where-the-identity-comes-from-security).

## Command interceptors

The command bus runs the framework's interceptors — audit (for an `AuditStore` bean),
authorization (for a `CommandAuthorizationPolicy` bean), annotation authorization (for a
`UserRoleResolver` bean), Bean Validation, the circuit breaker and OpenTelemetry (for an
`OpenTelemetry` bean) — together with every `CommandInterceptor` bean the application declares,
sorted into the canonical chain (audit outside authorization, validation after authorization,
breaker innermost). An application interceptor takes its position with
`@jakarta.annotation.Priority` on its class (lower runs first; `2500` sits between audit and
authorization) and runs innermost without one. The bus reads the annotation from the
interceptor's class, so a `@Priority` on a `@Produces` method does not position it. An application bean of a framework interceptor's type — a
`@Produces AuditCommandInterceptor` — replaces that framework interceptor, as
`@ConditionalOnMissingBean` does on Spring and `@Requires(missingBeans = …)` on Micronaut; the
replacement is logged at startup.

The beans are collected through the `BeanManager`, not by iterating an
`Instance<CommandInterceptor>`: Arc resolves an `Instance` iteration (and an `@All List`) with
ambiguity resolution, which drops every `@DefaultBean` framework interceptor as soon as the
application declares one interceptor bean of its own. The two startup validators read the chain
back from every command bus bean (`VirtualThreadCommandBus.interceptors()`), so they check exactly
what each bus runs.

Quarkus removes unused beans at build time by default (`quarkus.arc.remove-unused-beans=all`), and
a `BeanManager` lookup is invisible to that analysis. The bus producer therefore also declares an
`@Any Instance<CommandInterceptor>` injection point, which it never reads: it matches every
interceptor bean, so Arc keeps the framework's interceptor producers and every application
interceptor bean, even one nothing else injects. The startup validators inject the command buses,
which keeps the bus producer itself even in an application that injects no `CommandBus`. No
`@Unremovable` is needed on an application interceptor.

## Key Components

| Component | Description |
|---|---|
| `StreamRuneProducers` | CDI producers for the buses, stores, and interceptors |
| `ProjectionProducer` | Discovers `@ProjectionConfig` projections, builds runners |
| `StreamRuneLifecycle` | Starts/stops projection runners, outbox poller, DLQ retry runner and the retention sweepers on `StartupEvent`/`ShutdownEvent` |
| `SseEventFeedLifecycle` | With `streamrune.sse.enabled=true`: starts the `SseEventFeed` that publishes stored events to the SSE endpoint on `StartupEvent` and stops it on `ShutdownEvent` |
| `StreamRuneQuarkusProperties` | `@ConfigMapping` for `streamrune.*` properties |
| `StreamRuneRequestFilter` | JAX-RS `@Provider` that builds the `RequestContext`: `X-Trace-Id` / `X-Correlation-Id` (generated when missing), and the user id and `X-User-Role` (baggage `role`, trusted-gateway mode only) as `RequestIdentityPolicy` allows (see [Request identity](#request-identity)); aborts with `400` for an id the request edge refuses |
| `StreamRuneRequestIdentityValidator` | Startup observer: logs the request-identity mode once and refuses startup when a command bus authorizes against the request identity but requests can have no identity |
| `StreamRuneAuthorizationValidator` | Startup observer: refuses startup when a command bus registers `@RequireRole`/`@RequirePermission` commands without an `AnnotationAuthorizationInterceptor` in its chain |
| `StreamRuneRequestContextHolder` | Request-scoped holder carrying the parsed `RequestContext`; `ofCurrentRequest()` answers it for the request the calling thread serves, the source the command and query buses dispatch as (see [Request context propagation](#request-context-propagation)) |

## Request context propagation

JAX-RS filters cannot wrap the resource-method invocation, so the filter cannot bind the
`ScopedValue` for you. The filter parses headers into a `RequestContext` (generating a trace id /
correlation id when absent, `X-User-Role` placed into baggage under `role` in the trusted-gateway
mode only) and stores it in
`StreamRuneRequestContextHolder`. A request whose correlation id, trace id or user id the request
edge refuses is aborted with `400` instead, and the holder stays empty (see
[Request identity](#request-identity)). The user id is the identity authorization checks run against, so
it is never taken from the client unless you say so: it comes from the authenticated
`SecurityIdentity` when Quarkus Security is present, from `X-User-Id` only with
`streamrune.security.trust-user-id-header=true`, and is otherwise absent (see
[Request identity](#request-identity)). Only
allowlisted OpenTelemetry baggage keys (`streamrune.metadata.baggage-allowlist`; none by
default) reach the context, because baggage is written into every event's metadata.

**The command bus and the query bus bind it themselves.** The produced `VirtualThreadCommandBus` is
built with `StreamRuneRequestContextHolder::ofCurrentRequest` as its unbound-request-context source
(`VirtualThreadCommandBus.Builder#unboundRequestContext`): it answers the holder's context while a
CDI request context is active on the calling thread. The `QueryBus` bean is an
`UnboundRequestContextQueryBus` (`streamrune-runtime`) over the same source. Quarkus REST
activates a request's CDI request context on every thread it runs the request on — the event loop,
a worker thread, a virtual thread — so a command or a query a resource method dispatches, with
nothing bound around it, is authorized, audited and recorded as the request's caller. So are a command sent through `executeAsync` from the method and the commands of a saga
replay it runs (`SagaDeadLetterReplayer`). A dispatch with `StreamRuneContext.CURRENT` already bound
keeps its binding, and a dispatch with no request context active (a runner, a scheduled job, a
thread the application starts) runs without a context.

Two things stay yours. Code that reads the caller itself, outside a bus dispatch —
`Authorization.currentUserId()` or `StreamRuneContext.capture()` in a resource method before it
dispatches — binds the context with `StreamRuneRequestFilter.withContext(...)` /
`callWithContext(...)`, or reads `StreamRuneRequestContextHolder.context()`. And a bus you produce
yourself takes the same source: a `VirtualThreadCommandBus` is built with
`.unboundRequestContext(StreamRuneRequestContextHolder::ofCurrentRequest)`, and a `QueryBus` is
wrapped in `new UnboundRequestContextQueryBus(bus, StreamRuneRequestContextHolder::ofCurrentRequest)`.

## Lifecycle

`StreamRuneLifecycle` observes `StartupEvent` and starts whichever of these beans exist:
`MultiProjectionRunner` (its `AtomicBatchProcessor` is the `JdbcProjectionRepository` bean when a
registration declares `TRANSACTIONAL_LOCAL`/`EXTERNAL_EFFECT` or leadership must fence, otherwise
`AtomicBatchProcessor.nonAtomicAtLeastOnce()`), `ScheduledProjectionRunner`, `OutboxPoller`, the outbox, inbox, saga
dead-letter and command dead-letter retention sweepers, and `DeadLetterRetryRunner`. On
`ShutdownEvent` it closes them in reverse order and closes the `VirtualThreadCommandBus`. Do not
inject the runner types directly — ask `StreamRuneLifecycle` for the running instances.

**Your own runner replaces the framework's.** When the application declares its own bean of any of
these runner types (a plain `@Produces DeadLetterRetryRunner`, say), the framework neither builds
nor starts its default of that type, and it does not start yours either: you start and stop it
(for example from your own `StartupEvent`/`ShutdownEvent` observers). Spring
(`@ConditionalOnMissingBean`) and Micronaut (`@Requires(missingBeans = ...)`) behave the same.

**Projection runner ownership.** The framework starts and stops only the projection runner it
auto-assembles from your `@ProjectionConfig` beans, and only while
`streamrune.projections.auto-discovery.enabled` is `true` (the default). A
`MultiProjectionRunner`/`ScheduledProjectionRunner` bean you define yourself is never started or
stopped by the framework — you own its lifecycle. To supply and fully manage your own runner, set
`streamrune.projections.auto-discovery.enabled=false`; the framework then assembles and starts
nothing. This contract is identical across the Spring, Quarkus, and Micronaut integrations.

## Known Limitations

- **Requires Quarkus REST** for `StreamRuneRequestFilter` and `SseController` (both are inert
  without it).
- **Call StreamRune outside `@Transactional`.** The stores commit their own writes on the
  connection they borrow from the Agroal pool. Inside a JTA transaction Agroal enlists that
  connection and refuses the commit, so a command, an outbox replay or skip, or a dead-letter
  discard called from a `@Transactional` method fails and breaks the surrounding transaction.
  Start the call outside the transaction, or on its own thread.
- **A proxied `BaseProjection` needs `quarkus.arc.transform-unproxyable-classes=true`** (the
  Quarkus default). `BaseProjection.process(List, ProjectionRepository)` is final; a
  `BaseProjection` subclass that is normal-scoped (`@ApplicationScoped`) or carries an interceptor
  binding gets a generated client proxy or interceptor subclass, and Quarkus can generate it only
  by removing that `final` at build time. With the property set to `false` the build fails because
  the proxy cannot be created.
- **Crypto engine selection is build-time** — `streamrune.crypto.filesystem.enabled` and the
  `postgres`, `vault` and `aws` switches are read by `@IfBuildProperty`; overriding one to `false` at
  runtime fails fast at startup.
- **Native image** — `StreamRuneReflectionConfig` registers the framework's own value types
  and records, and the module
  ships `META-INF/native-image/org.streamrune/streamrune-quarkus/reachability-metadata.json` with the
  HikariCP metadata the framework's dedicated LISTEN and lock connections need (the event store
  borrows from your Agroal pool; Quarkus ships no HikariCP metadata; without
  it `new HikariDataSource(config)` copies an empty configuration and startup fails with *Failed to
  initialize pool: null*), and with the Flyway migration scripts schema auto-initialization reads by
  name (`db/streamrune-migration/*.sql`, `db/crypto-migration/*.sql`, Flyway's `version.txt`) as
  resources. A second directory, `META-INF/native-image/org.streamrune/streamrune-quarkus-flyway`,
  holds what Flyway reaches by name when it initializes the schema; a Quarkus build registers no
  `ServiceLoader` provider on its own and does not apply the GraalVM reachability-metadata
  repository, so without it the binary fails at startup inside Flyway. Its
  `reachability-metadata.json` registers the `META-INF/services` file Flyway reads its plugins
  from and every plugin that file names (the PostgreSQL database type among them), the declared
  fields of the configuration extensions Flyway copies field by field, and the SLF4J log creator
  Flyway instantiates from its class name (`streamrune-postgres` tells Flyway to log through
  SLF4J). Its `native-image.properties` initializes Flyway's `InsertRowLock` at run time: Quarkus
  initializes classes at image build time, and that class keeps a `java.util.Random` in a static
  field, which native-image refuses in the image heap. `FlywayNativeImageMetadataTest` checks the
  list against the Flyway on the class path. An application that adds a Flyway module of its own
  (another database type) registers that module's plugins the same way, or sets
  `quarkus.native.auto-service-loader-registration=true`: `ServiceLoader` refuses the lookup when
  a service file names a provider the image cannot load. The module also carries a native-image
  substitution for Flyway's class-path scanner
  (`org.streamrune.quarkus.graal`): Quarkus links every class at build time, and since Flyway 13 a
  `Flyway` instance reaches that scanner, whose JBoss VFS and OSGi branches need optional Flyway
  dependencies (`jboss-vfs`, `org.osgi.core`), so the build failed with *Discovered unresolved
  type during parsing: org.jboss.vfs.VirtualFileFilter*. The substitution leaves out those two
  branches, which a native image's class path can never take, and keeps Flyway's others; nothing
  changes on the JVM. The application registers its own records with `@RegisterForReflection`.
  **Sealed command/event interfaces need a `{"type": "…"}` entry in the application's
  `META-INF/native-image/<groupId>/<artifactId>/reachability-metadata.json`**:
  `@RegisterForReflection` on a sealed type writes a legacy `reflect-config.json` entry without its
  permitted subclasses, so the image reports the type as sealed with none, and the dead-letter
  runner's `registerCommand(Root.class)` and the `@Encrypted` / authorization startup checks refuse
  to start the application (see Quickstart §10). The demo's `quarkus-app` does exactly this and its
  native smoke test replays a dead-lettered command through its sealed root.
