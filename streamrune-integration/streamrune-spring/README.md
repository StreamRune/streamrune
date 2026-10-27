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

Every key and its default are in the [configuration reference](../../docs/guide/configuration.md);
the ones most applications set:

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

**More than one replica: a database-wide aggregate lock.** The auto-configured `AggregateLocker` is
the in-process `LocalStripedLocker`, which serializes one aggregate's commands within one replica
only. On more than one replica declare `PgAdvisoryLocker.withDedicatedPool(...)` as the
`AggregateLocker` bean; the auto-configured locker is
`@ConditionalOnMissingBean(AggregateLocker.class)` and steps aside:

```java
@Bean(destroyMethod = "close") // the locker owns its pool
AggregateLocker aggregateLocker(
        @Value("${spring.datasource.url}") String url,
        @Value("${spring.datasource.username}") String username,
        @Value("${spring.datasource.password}") String password) {
    return PgAdvisoryLocker.withDedicatedPool(url, username, password, 16);
}
```

A command holds its lock connection for its whole run and borrows a second one from the HikariCP
pool for its load and its append, so size the lock pool for the commands one replica runs at once
and the HikariCP pool for the load and the append alone; the two pools of every replica together
stay below PostgreSQL's `max_connections` (see
[the aggregate lock across replicas](../../docs/guide/production.md#the-aggregate-lock-across-replicas)).

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
  history. The feed reads events only while a client is subscribed to this instance. With none, a
  poll is one read of the head of the global stream (`SELECT max(global_offset)` on PostgreSQL):
  no event is read or decrypted. With at least one, on any stream, the instance reads and decrypts
  every event of the global stream. A client that connects to an idle feed misses nothing: every
  event stored after its opening frame reaches it. Do not call `SseEventPublisher.publish`
  yourself for this endpoint — every frame would be written twice. To run a feed of your own,
  declare an `SseEventFeed` bean: it replaces the integration's, and you start and stop it.
- **Authorization** — every stream is denied (`403`) until you provide an `SseAuthorizer` bean:
  `boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId)`. `caller` is
  the request context `ScopedValueFilter` built for the request, the one a command from that
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
  an access decision. A request with no request context
  bound (the filter registered with URL patterns that leave `/api/sse` out) or with one that names
  another user than the `RequestIdentityPolicy` resolves (the filter registered ahead of the
  Spring Security filter chain) is answered `403` and the authorizer is not called; the log says
  which.
  The authorizer is asked once, when the stream opens: a caller whose access ends afterwards keeps
  reading until the stream ends, so `streamrune.sse.timeout` (default `5m`) is also the bound on
  that. Keep it finite where access can change.

  ```java
  @Bean
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
- **Delivery guarantee** — live, best-effort, at-most-once. A frame reaches a client only while it
  is connected; nothing is redelivered, and `Last-Event-ID` is not honoured. Events stored before a
  client connected, while it was reconnecting, or while the instance was down are not sent to it.
  One exception: an event of its stream stored in the polling interval or so before the client
  subscribed, which the feed had not passed yet, can still arrive.
  Read the current state from a query after every (re)connect, and use a projection for anything
  that must see every event.
- **Lifecycle** — the feed is a `SmartLifecycle`-managed bean: started on context refresh before the
  web server accepts requests (the two start in one lifecycle phase, the feed first), stopped on
  context close. A non-servlet application (a headless
  worker sharing the same configuration) gets neither the endpoint nor the feed.
- **Clients that stall or vanish** — a client that stops reading is disconnected once 256 events
  are queued for it; the feed is not held while its blocked write times out. A client that resets
  its connection before the response is written is unsubscribed at once and its request ended.
- **Health** — the feed is the `sse-event-feed` component of `StreamRuneHealthIndicator`: `DOWN`
  when its polling thread has died, `DEGRADED` while its reads fail and are retried. A polling
  thread dies only of a JVM `Error`, and nothing restarts it: the indicator stays `DOWN` until the
  application restarts or calls `start()` on the `SseEventFeed` bean, which begins again at the
  head of the stream.

## Request identity

`ScopedValueFilter` binds `RequestContext.userId` — the user every `CommandAuthorizationPolicy`,
`@RequireRole`/`@RequirePermission` and `Authorization.requireOwner(...)` check trusts, and the
user the audit records — and the SSE endpoint hands that request context to your `SseAuthorizer`.
Both resolve the user through one `RequestIdentityPolicy` bean, which fails closed:

| Your setup | Identity | `X-User-Id` header | `X-User-Role` header |
|---|---|---|---|
| Spring Security on the classpath (or your own `AuthenticatedUserResolver` bean) | the authenticated principal; `null` when unauthenticated | ignored (a header naming another user is logged) | ignored |
| `streamrune.security.trust-user-id-header=true` | the `X-User-Id` header | the identity | baggage `role` |
| neither | none — every request is anonymous | ignored | ignored |

`X-User-Role` is read only in the trusted-gateway mode, where it becomes baggage `role` — recorded
in every event's metadata, so the gateway must forward the caller's role or strip the header. In
the other modes it is ignored and there is no `role` (an authenticated principal's roles are the
captured authority, not baggage), and a W3C `baggage: role=` entry never reaches the key.

`X-Correlation-Id`, `X-Trace-Id` and the allow-listed baggage are caller-supplied in every mode,
the trusted-gateway mode included; nothing may be addressed, authorized or deduplicated by them.
A correlation id is kept exactly as sent, with one exception: the `streamrune:` prefix is reserved
for the correlation ids the framework binds itself. `ScopedValueFilter` answers
a correlation id with that prefix, and a correlation id, trace id or user id that is longer than 255
characters or holds a control character, with status `400 Bad Request`, content type
`text/plain;charset=UTF-8` and the body `The request's correlation id, trace id or user id is not
acceptable` — the answer of the Quarkus and Micronaut filters, byte for byte. It writes that answer
itself and starts no error dispatch, so your error pages, your `@ControllerAdvice` and the rule
your Spring Security filter chain has for `/error` take no part, and the rest of the filter chain
does not run.

**CORS and the `400`.** The filter sits at order `-90`
(`StreamRuneAutoConfiguration.SCOPED_VALUE_FILTER_ORDER`), and its `400` carries the CORS headers
only of what ran before it. Spring Security's `cors()` runs before it (the security chain sits at
`-100`), and so does a `CorsFilter` you register with an order below `-90`: a page of another
origin reads the `400`. CORS configured in Spring MVC — `WebMvcConfigurer.addCorsMappings`,
`@CrossOrigin` — is applied inside the `DispatcherServlet`, and a `CorsFilter` bean without an
order is registered last: both run behind the filter, the refusal carries no
`Access-Control-Allow-Origin`, and the browser reports a network error to the page instead of the
status. To let a browser client read the refusal, configure CORS in Spring Security or register
the `CorsFilter` through a `FilterRegistrationBean` with an order below `-90`. A preflight carries
no `X-Correlation-Id` value and is never refused.

Of an `X-Correlation-Id` or `X-Trace-Id` header sent more than once the first
value is the header, as on the other two integrations; a blank first value counts as an absent
header. An authenticated principal the request edge refuses is logged at `WARN` once
per reason (its length, a control character). See
[What a client controls in event metadata](../../docs/guide/production.md#what-a-client-controls-in-event-metadata).

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
| `ScopedValueFilter` | Propagates HTTP request metadata into `StreamRuneContext`; the user comes from `RequestIdentityPolicy` (see [Request identity](#request-identity)); answers `400` to an id the request edge refuses |
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

**A `BaseProjection` must not be a CGLIB proxy.** When an aspect applies to a projection class
(a class-level `@Transactional`, `@Async` or `@Validated`, a metrics or tracing aspect, a scoped
proxy), the bean in the context is a generated subclass that forwards each overridable method to
the real projection. `BaseProjection.process(List, ProjectionRepository)` is final, so it cannot be
forwarded: a runner's call would run it on the uninitialised proxy and fail on the first batch. The
auto-configuration therefore refuses a `CONTINUOUS` or `SCHEDULED` projection bean that is a CGLIB
proxy of a `BaseProjection` subclass, naming the bean; it does not unwrap the proxy, which would run
the projection without its advice. Move the advice into a bean the projection calls, or implement
`Projection` directly and override both `process` methods and `writesThroughRepository()`. An
`INLINE` projection is not affected: the inline interceptor calls the one-argument `process`.
The check covers the projection beans the auto-configuration discovers. A projection the
application hands to a runner it builds itself (`streamrune.projections.auto-discovery.enabled=false`),
including one wrapped in `TracingProjectionDecorator`, `ValidatingProjectionDecorator` or
`CacheAwareProjection`, is the application's to keep free of such proxies: the decorators forward
the two-argument `process` to what they wrap.

`ScopedValueFilter` is registered only in servlet web applications
(`@ConditionalOnWebApplication(SERVLET)`), so a headless worker
(`WebApplicationType.NONE`) boots without a servlet API on the classpath.

## Best Practices

- **Consider virtual request threads** — `CommandBus.execute(...)` runs synchronously on the calling thread and blocks it on aggregate locks and JDBC; with `spring.threads.virtual.enabled=true` that blocking is cheap. `executeAsync(...)` always runs the command on its own virtual thread.
- **Keep deciders stateless** — the command bus holds the one decider instance from each `DeciderRegistration` and uses it for every command; bean scope does not give each command a fresh instance.
- **Let the integration own shutdown** — the runners are `SmartLifecycle` beans stopped when the application context closes, and the command bus is closed with it; do not build or close a `StreamRune` facade next to the auto-configuration.
- **Authorize SSE streams** — provide an `SseAuthorizer` bean that checks per-stream access, by the caller's user id and roles, before enabling the endpoint.
- **Keep `trust-user-id-header` off unless a gateway owns `X-User-Id` and `X-User-Role`** — without Spring Security and without the flag every request is anonymous; with the flag, both headers are believed as-is.

## Known Limitations

- **`@StreamRuneComponent` is a `@Component` stereotype** — annotated classes must live in packages covered by the application's component scan; deciders still need an explicit `DeciderRegistration` bean to reach the command bus.
- **Event appends bypass `@Transactional`** — the PostgreSQL event store manages its own connections; derive read models from events via projections instead of dual writes.
- **SSE is opt-in, live and not durable** — `streamrune.sse.enabled` defaults to `false`; delivery is best-effort and at-most-once (see [Server-Sent Events](#server-sent-events)); a subscriber that cannot keep up is disconnected and must reconnect. On shutdown the controller completes every open stream before the web server's graceful-shutdown drain begins, so a connected client neither holds the shutdown for `spring.lifecycle.timeout-per-shutdown-phase` nor has its connection cut; clients must reconnect — an `EventSource` does so on its own — and a subscription that arrives while the application is stopping receives a stream that is already complete.
