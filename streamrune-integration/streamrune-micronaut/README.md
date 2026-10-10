# StreamRune Micronaut

Micronaut integration for StreamRune using DI and `ApplicationEventListener`.

## Overview

Inject `CommandBus` (`org.streamrune.core.CommandBus`) via constructor injection and use Micronaut's
context lifecycle for startup and shutdown. The module produces a `VirtualThreadCommandBus`, which
implements `CommandBus`. There is no injectable `StreamRune` bean — the `StreamRune` facade is the
programmatic bootstrap API only.

```java
@Singleton
public class OrderService {
    private final CommandBus commandBus;

    public OrderService(CommandBus commandBus) {
        this.commandBus = commandBus;
    }

    public void placeOrder(PlaceOrderCommand cmd) {
        commandBus.execute(cmd);
    }
}
```

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-micronaut:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-micronaut:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Configuration

```yaml
streamrune:
  snapshot-every-n-events: 100
  retry-max-attempts: 3
  lock-timeout: 5s
  stripe-count: 1024
  event-store:
    # bound on every event-store statement, per transaction or statement (never on the pooled
    # session); 0s sets none
    statement-timeout: 30s
```

The event store borrows its connections from your `DataSource` bean as given and builds no pool of
its own; size the datasource pool for everything StreamRune runs.

`streamrune.security.trust-user-id-header` (default `false`) selects where the request identity
comes from — see [Request identity](#request-identity).

## Key Components

| Component | Description |
|---|---|
| `StreamRuneMicronautModule` | Factory producing framework default beans (`@Requires(missingBeans = ...)` — application beans of the same type always win) |
| `StreamRuneMicronautProperties` | `@ConfigurationProperties` record bound from `streamrune.*` |
| `StreamRuneContextFilter` | HTTP filter that builds the request context — `X-Trace-Id` / `X-Correlation-Id` (generated when missing), and the user id and `X-User-Role` (baggage `role`, trusted-gateway mode only) as `RequestIdentityPolicy` allows (see [Request identity](#request-identity)) — and binds `StreamRuneContext.CURRENT` around the request |
| `StreamRuneRequestIdentityValidator` | Startup listener: logs the request-identity mode once and refuses startup when a command bus authorizes against the request identity but requests can have no identity |
| `StreamRuneAuthorizationValidator` | Startup listener: refuses startup when a command bus registers `@RequireRole`/`@RequirePermission` commands without an `AnnotationAuthorizationInterceptor` in its chain |
| `StreamRuneContextHelper` | ThreadLocal mirror of the request context for threads `ScopedValue` bindings cannot reach (kept in sync via Micronaut's `PropagatedContext`) |
| `StreamRuneLifecycle` | Starts `MultiProjectionRunner`, `ScheduledProjectionRunner`, `OutboxPoller`, the outbox, inbox, saga dead-letter and command dead-letter retention sweepers, and `DeadLetterRetryRunner` on context startup and closes them on shutdown |
| `SseEventFeedLifecycle` | With `streamrune.sse.enabled=true`: starts the `SseEventFeed` that publishes stored events to the SSE endpoint on `StartupEvent` and stops it when the context closes; independent of `streamrune.runner-lifecycle-enabled` |

## Lifecycle

Background runners auto-start on `StartupEvent` and close on shutdown via `StreamRuneLifecycle`.
Opt out with:

```yaml
streamrune:
  runner-lifecycle-enabled: false
```

**Projection runner ownership.** The framework starts and stops only the projection runner it
auto-assembles from your `@ProjectionConfig` beans, and only while
`streamrune.projections.auto-discovery.enabled` is `true` (the default). A
`MultiProjectionRunner`/`ScheduledProjectionRunner` bean you define yourself is never started or
stopped by the framework — you own its lifecycle. To supply and fully manage your own runner, set
`streamrune.projections.auto-discovery.enabled=false`; the framework then assembles and starts
nothing. This contract is identical across the Spring, Quarkus, and Micronaut integrations.

## Transactional outbox

Declare an `OutboxEventMapper` bean and a `PostgresOutboxStore` bean; the factory method may
return either `OutboxStore` or `PostgresOutboxStore`, because every framework consumer (the event
store, `OutboxPoller`, `OutboxRetentionSweeper`, `OutboxFailedReplayer`) injects the `OutboxStore`
interface. The event store writes the outbox entries inside the append transaction, which only a
`PostgresOutboxStore` can do, so startup fails when the mapper is paired with any other
`OutboxStore`; supply your own `EventStoreFactory` for such a store. With
`streamrune.outbox.enabled=true` the relay and the retention sweeper start too, and startup fails
when no `OutboxStore` bean exists, or when neither an `OutboxPublisher` bean
(`streamrune-kafka-outbox`, `streamrune-rabbitmq-outbox` or an `HttpOutboxPublisher`) nor your own
`OutboxPoller` bean exists.

## Server-Sent Events

With `streamrune.sse.enabled=true` the integration serves
`GET /api/sse/{aggregateType}/{aggregateId}` — the live events of one stream, the registered
aggregate type plus its id as two path segments; an invalid part answers `400`.

- **What it emits** — one frame per domain event of that stream: `id` is the global offset, `data`
  the decrypted event as JSON (the event types must be serializable by the application's JSON
  mapper — with Micronaut Serialization, `@Serdeable`). `:keepalive` comment frames are written
  every `streamrune.sse.keep-alive-interval` (default `30s`), and the server completes a stream
  after `streamrune.sse.timeout` (default `5m`); an `EventSource` reconnects on its own.
- **Who feeds it** — the integration. It runs one `SseEventFeed` per application instance: a
  polling subscription that starts at the head of the global stream when the application starts
  and publishes every event stored from then on to the clients of the event's own stream, every
  `streamrune.sse.polling-interval` (default `1s`). No stored offset, no replay of history. Do not
  call `SseEventPublisher.publish` yourself for this endpoint — every frame would be written twice.
- **Authorization** — every stream is denied (`403`) until you provide an `SseAuthorizer` bean; it
  receives the caller the request filter resolved and the requested `StreamId`.
- **Delivery guarantee** — live, best-effort, at-most-once. A frame reaches a client only while it
  is connected; nothing is redelivered, and `Last-Event-ID` is not honoured. Events stored before a
  client connected, while it was reconnecting, or while the instance was down are never sent to it.
  Read the current state from a query after every (re)connect, and use a projection for anything
  that must see every event.
- **Lifecycle** — the feed starts on the startup event and stops when the application context
  closes, which also completes every open stream.

## Request identity

`StreamRuneContextFilter` binds `RequestContext.userId` — the user every
`CommandAuthorizationPolicy`, `@RequireRole`/`@RequirePermission` and
`Authorization.requireOwner(...)` check trusts, and the user the audit records — and the SSE
endpoint hands the same user to your `SseAuthorizer`. Both resolve it through one
`RequestIdentityPolicy` bean (the same rule as the Spring and Quarkus integrations), which fails
closed:

| Your setup | Identity | `X-User-Id` header | `X-User-Role` header |
|---|---|---|---|
| Micronaut Security present and enabled (or your own `AuthenticatedUserResolver` bean) | the authenticated principal (`SecurityService`); `null` when unauthenticated | ignored (a header naming another user is logged) | ignored |
| `streamrune.security.trust-user-id-header=true` | the `X-User-Id` header | the identity | baggage `role` |
| neither | none — every request is anonymous | ignored | ignored |

`X-User-Role` is read only in the trusted-gateway mode, where it becomes baggage `role` — recorded
in every event's metadata, so the gateway must forward the caller's role or strip the header. In
the other modes it is ignored and there is no `role` (an authenticated principal's roles are the
captured authority, not baggage), and a W3C `baggage: role=` entry never reaches the key.

Micronaut Security with `micronaut.security.enabled=false` counts as absent: its beans are not
created, so there is no authenticated principal to resolve.

```yaml
streamrune:
  security:
    # Trusted-gateway mode. Only when a gateway in front of this service authenticates every
    # caller, overwrites X-User-Id with the authenticated id and strips any client-supplied value
    # (and owns X-User-Role the same way), and the service is reachable only through it —
    # otherwise any client can act as any user.
    trust-user-id-header: true
```

`StreamRuneRequestIdentityValidator` logs the mode once at startup (`Request identity: …`, WARN for
the trusted gateway). Startup **fails** when a command bus runs an identity-consuming
authorization interceptor — `AuthorizationCommandInterceptor` (produced for a
`CommandAuthorizationPolicy` bean) or `AnnotationAuthorizationInterceptor` (produced for a
`UserRoleResolver` bean), or either one in the chain of a `VirtualThreadCommandBus` you build
yourself — and the mode is anonymous: add Micronaut Security (or an
`AuthenticatedUserResolver` bean), or set the trusted-gateway flag. The check runs on the
context-level `StartupEvent`, before any background runner starts. It applies wherever
`StreamRuneContextFilter` is a bean, and this module brings the Netty HTTP server, so that is every
application that has not replaced the filter — including a context started without the embedded
server. See
[Authorization](../../docs/guide/advanced/authorization.md#where-the-identity-comes-from-security).

Only allowlisted OpenTelemetry baggage keys (`streamrune.metadata.baggage-allowlist` on top of the
framework defaults) reach the context, because baggage is written into every event's metadata.

## Context Propagation

`StreamRuneContextFilter` runs on a blocking executor and proceeds synchronously, so request
handlers (and the command bus they call) observe `StreamRuneContext.CURRENT`. For code hopping to
other threads (`@ExecuteOn`, reactive pipelines), read the context from
`StreamRuneContextHelper.get()` — Micronaut's `PropagatedContext` keeps it in sync.

## Validation

Commands are validated by `BeanValidationInterceptor` using the container's
`jakarta.validation.Validator` (Micronaut Validation). Micronaut's validator is compile-time:
annotate commands with `@Introspected` (or register them via `@Introspected(classes = ...)`) to
enable constraint validation — non-introspected commands are skipped with a one-time warning.

## Native image

The module ships `META-INF/native-image/org.streamrune/streamrune-micronaut/reflect-config.json`
for the framework's own value types and records, plus `Projection` and `BaseProjection`, whose
`process(List, ProjectionRepository)` `Projection#writesThroughRepository` resolves with
`getMethod` (without them the image throws `NoSuchMethodException` and the lifecycle cannot build
the projection runner). Its `@Requires(condition = ...)` classes hold no state: Micronaut keeps a
condition instance in the compile-time annotation metadata, which the image stores in its heap, and
a framework enum held there failed the build (*An object of type
'org.streamrune.core.ProjectionConfig$Mode' was found in the image heap*); `ArchitectureTest` keeps
it that way.

It also ships `resource-config.json`, which registers the Flyway migration scripts schema
auto-initialization reads by name (`db/streamrune-migration/*.sql`, `db/crypto-migration/*.sql`)
and Flyway's own `version.txt` as resources; without them the image cannot initialize the schema.

The application registers its own records (a `reflect-config.json` of its own, or `@TypeHint` /
`@ReflectiveAccess`), plus any projection class that declares `process(List,
ProjectionRepository)` itself. **Sealed command/event interfaces need a `{"type": "…"}` entry in the
application's `META-INF/native-image/<groupId>/<artifactId>/reachability-metadata.json`**:
`@TypeHint` and `@ReflectiveAccess` register a type through `RuntimeReflection.register(Class)`,
which leaves its permitted subclasses unregistered (measured on Micronaut 4.10 / GraalVM CE
25.0.1), so the image reports the type as sealed with none, and the `@Encrypted` startup check when
the command bus is built, the dead-letter runner's `registerCommand(Root.class)` and the
authorization startup check refuse to start the application (see Quickstart §10). The demo's
`micronaut-app` does exactly this, and its native smoke test replays a dead-lettered command
through its sealed root.

**Build the image with `-H:+SharedArenaSupport`** (an experimental GraalVM option, so between
`-H:+UnlockExperimentalVMOptions` and `-H:-UnlockExperimentalVMOptions`). Micronaut's HTTP server
runs on Netty 4.2, which on Java 25 frees every direct buffer it allocates outside its pool (one
above 1 MiB, and each pooled chunk when an event loop exits) through `Arena.ofShared().close()`.
GraalVM 25 supports that call only with this option. Without it the call throws
`UnsupportedFeatureError: Support for Arena.ofShared is not active` and the memory is never
returned. In the demo's binary (GraalVM CE 25.0.1) a request with a 1.5 MiB body answered 500 and
six event-loop threads threw at shutdown. Quickstart §10 has the Gradle snippet. The demo's `micronaut-app`
sets the option, and its native smoke test serves a 1.5 MiB read model and fails on any
`UnsupportedFeatureError` in the binary's output, shutdown included.
