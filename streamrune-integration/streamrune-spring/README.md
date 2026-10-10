# StreamRune Spring Boot

Spring Boot 4 auto-configuration for StreamRune. Registers beans, configures `VirtualThreadCommandBus`, and provides an opt-in SSE endpoint.

## Overview

Add the dependency, configure via `application.yml`, register each decider as a
`DeciderRegistration` bean (it names the `AggregateType` the decider's streams carry), and inject `CommandBus` (`org.streamrune.core.CommandBus`). The
auto-configuration builds a `VirtualThreadCommandBus`, which implements it. There is no injectable
`StreamRune` bean — the `StreamRune` facade is the programmatic bootstrap API only.

```java
static final AggregateType ORDER = AggregateType.of("order");

@Bean
DeciderRegistration<OrderCommand, OrderState, OrderEvent> orderDecider() {
    return new DeciderRegistration<>(
        ORDER, OrderCommand.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider());
}

@RestController
class OrderController {
    private final CommandBus bus;

    OrderController(CommandBus bus) {
        this.bus = bus;
    }

    @PostMapping("/orders")
    void place(@RequestBody PlaceOrder cmd) {
        bus.execute(cmd);
    }
}
```

## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-spring:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-spring:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Configuration

```yaml
streamrune:
  snapshot-every-n-events: 100
  retry-max-attempts: 3
  retry-initial-delay-ms: 50
  retry-backoff-multiplier: 2.0
  polling-interval-ms: 5000
  polling-jitter-ms: 1000
  lock-timeout: 5s
  stripe-count: 1024
  event-store:
    # bound on every event-store statement, per transaction or statement (never on the pooled
    # session); 0 sets none
    statement-timeout: 30s
```

The event store borrows its connections from your `DataSource` bean as given — Boot's HikariCP pool
— and builds no pool of its own; size `spring.datasource.hikari.maximum-pool-size` for everything
StreamRune runs.

## Server-Sent Events

With `streamrune.sse.enabled=true` on a servlet + Spring MVC application the integration serves
`GET /api/sse/{aggregateType}/{aggregateId}` — the live events of one stream, the registered
aggregate type plus its id as two path segments; an invalid part answers `400`.

```yaml
streamrune:
  sse:
    enabled: true            # default false
    polling-interval: 1s     # how often the feed reads the global stream; must be positive
    timeout: 5m              # the server completes a stream after this; 0 disables
    keep-alive-interval: 30s # ": keepalive" comment frames; 0 disables
```

- **What it emits** — one frame per domain event of that stream: `id` is the global offset, `data`
  the decrypted event as JSON (written by the application's `HttpMessageConverter`s). An
  `EventSource` reconnects on its own when the server completes the stream.
- **Opening frame** — as soon as the client is subscribed the endpoint writes one `: keepalive`
  comment frame, whatever the keepalive interval, and with it the status line and the headers: the
  stream is open at once (an `EventSource` fires `onopen`), not with the first event. Nothing of
  the response is written before the client is subscribed, so a client that has received the first
  bytes receives every event of the stream stored from then on, while it stays connected.
- **Who feeds it** — the integration. It runs one `SseEventFeed` per application instance: a
  polling subscription that starts at the head of the global stream when the application starts
  and publishes every event stored from then on to the clients of the event's own stream, every
  `streamrune.sse.polling-interval` (default `1s`, at least `1ms`). No stored offset, no replay of
  history. While the endpoint is enabled every instance reads and decrypts every event of the
  global stream, whether or not a client is connected. Do not call `SseEventPublisher.publish`
  yourself for this endpoint — every frame would be written twice. To run a feed of your own,
  declare an `SseEventFeed` bean: it replaces the integration's, and you start and stop it.
- **Authorization** — every stream is denied (`403`) until you provide an `SseAuthorizer` bean; it
  receives the caller the request filter resolved and the requested `StreamId`.
- **Delivery guarantee** — live, best-effort, at-most-once. A frame reaches a client only while it
  is connected; nothing is redelivered, and `Last-Event-ID` is not honoured. Events stored before a
  client connected, while it was reconnecting, or while the instance was down are never sent to it.
  Read the current state from a query after every (re)connect, and use a projection for anything
  that must see every event.
- **Lifecycle** — the feed is a `SmartLifecycle`-managed bean: started on context refresh before the
  web server accepts requests, stopped on context close. A non-servlet application (a headless
  worker sharing the same configuration) gets neither the endpoint nor the feed.
- **Health** — the feed is the `sse-event-feed` component of `StreamRuneHealthIndicator`: `DOWN`
  when its polling thread has died, `DEGRADED` while its reads fail and are retried.

## Request identity

`ScopedValueFilter` binds `RequestContext.userId` — the user every `CommandAuthorizationPolicy`,
`@RequireRole`/`@RequirePermission` and `Authorization.requireOwner(...)` check trusts, and the
user the audit records — and the SSE endpoint hands the same user to your `SseAuthorizer`. Both
resolve it through one `RequestIdentityPolicy` bean, which fails closed:

| Your setup | Identity | `X-User-Id` header | `X-User-Role` header |
|---|---|---|---|
| Spring Security on the classpath (or your own `AuthenticatedUserResolver` bean) | the authenticated principal; `null` when unauthenticated | ignored (a header naming another user is logged) | ignored |
| `streamrune.security.trust-user-id-header=true` | the `X-User-Id` header | the identity | baggage `role` |
| neither | none — every request is anonymous | ignored | ignored |

`X-User-Role` is read only in the trusted-gateway mode, where it becomes baggage `role` — recorded
in every event's metadata, so the gateway must forward the caller's role or strip the header. In
the other modes it is ignored and there is no `role` (an authenticated principal's roles are the
captured authority, not baggage), and a W3C `baggage: role=` entry never reaches the key.

```yaml
streamrune:
  security:
    # Trusted-gateway mode. Only when a gateway in front of this service authenticates every
    # caller, overwrites X-User-Id with the authenticated id and strips any client-supplied value
    # (and owns X-User-Role the same way), and the service is reachable only through it —
    # otherwise any client can act as any user.
    trust-user-id-header: true
```

The mode is logged once at startup. In a servlet application, startup **fails** when a command bus
runs an identity-consuming authorization interceptor — `AuthorizationCommandInterceptor` (registered
for a `CommandAuthorizationPolicy` bean) or `AnnotationAuthorizationInterceptor` (registered for a
`UserRoleResolver` bean), or either one in the chain of a `VirtualThreadCommandBus` you build
yourself — and the mode is anonymous: add Spring Security (or an `AuthenticatedUserResolver` bean),
or set the trusted-gateway flag. See
[Authorization](../../docs/guide/advanced/authorization.md#where-the-identity-comes-from-security).

## Key Components

| Component | Description |
|---|---|
| `StreamRuneAutoConfiguration` | Auto-configuration class |
| `StreamRuneProperties` | `@ConfigurationProperties` binding |
| `ScopedValueFilter` | Propagates HTTP request metadata into `StreamRuneContext`; the user comes from `RequestIdentityPolicy` (see [Request identity](#request-identity)) |
| `SseController` | `GET /api/sse/{aggregateType}/{aggregateId}` — live event stream of one aggregate; only with `streamrune.sse.enabled=true`, and every stream is denied until you provide an `SseAuthorizer` bean |
| `SseEventFeed` | Publishes every event stored after the application started to the SSE endpoint's clients; registered with `SseController`, started and stopped with the application context |

## Projection runners

**Projection runner ownership.** The framework starts and stops only the projection runner it
auto-assembles from your `@ProjectionConfig` beans, and only while
`streamrune.projections.auto-discovery.enabled` is `true` (the default). A
`MultiProjectionRunner`/`ScheduledProjectionRunner` bean you define yourself is never started or
stopped by the framework — you own its lifecycle. To supply and fully manage your own runner, set
`streamrune.projections.auto-discovery.enabled=false`; the framework then assembles and starts
nothing. This contract is identical across the Spring, Quarkus, and Micronaut integrations.

`ScopedValueFilter` is registered only in servlet web applications
(`@ConditionalOnWebApplication(SERVLET)`), so a headless worker
(`WebApplicationType.NONE`) boots without a servlet API on the classpath.

## Best Practices

- **Consider virtual request threads** — `CommandBus.execute(...)` runs synchronously on the calling thread and blocks it on aggregate locks and JDBC; with `spring.threads.virtual.enabled=true` that blocking is cheap. `executeAsync(...)` always runs the command on its own virtual thread.
- **Keep deciders stateless** — the command bus holds the one decider instance from each `DeciderRegistration` and uses it for every command; bean scope does not give each command a fresh instance.
- **Let the integration own shutdown** — the runners are `SmartLifecycle` beans stopped when the application context closes, and the command bus is closed with it; do not build or close a `StreamRune` facade next to the auto-configuration.
- **Authorize SSE streams** — provide an `SseAuthorizer` bean that checks per-stream access before enabling the endpoint.
- **Keep `trust-user-id-header` off unless a gateway owns `X-User-Id` and `X-User-Role`** — without Spring Security and without the flag every request is anonymous; with the flag, both headers are believed as-is.

## Known Limitations

- **`@StreamRuneComponent` is a `@Component` stereotype** — annotated classes must live in packages covered by the application's component scan; deciders still need an explicit `DeciderRegistration` bean to reach the command bus.
- **Event appends bypass `@Transactional`** — the PostgreSQL event store manages its own connections; derive read models from events via projections instead of dual writes.
- **SSE is opt-in, live and not durable** — `streamrune.sse.enabled` defaults to `false`; delivery is best-effort and at-most-once (see [Server-Sent Events](#server-sent-events)); a subscriber that cannot keep up is disconnected and must reconnect. On shutdown the controller completes every open stream before the web server's graceful-shutdown drain begins, so a connected client neither holds the shutdown for `spring.lifecycle.timeout-per-shutdown-phase` nor has its connection cut; clients must reconnect — an `EventSource` does so on its own — and a subscription that arrives while the application is stopping receives a stream that is already complete.
