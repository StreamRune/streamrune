# Query Result Caching

## When to use

Use query result caching when a read model is dispatched far more often than the underlying events change, and recomputing the result on every dispatch is measurably expensive (a heavy projection read, a join, an aggregation).

Caching is **opt-in per query type** via the `@Cacheable` annotation. Un-annotated queries are never cached — they pass straight through to the delegate `QueryBus` with no behavior change. This keeps the default path (no annotation) exactly as fast as an uncached bus, and makes caching an explicit, readable decision on each query record.

Do **not** cache queries whose results must always reflect the latest write with zero staleness — use the delegate bus directly, or accept the TTL and event-driven invalidation window below.

## How it works

`CachingQueryBus` is a `QueryBus` decorator: it wraps a delegate bus (typically `SimpleQueryBus`) and consults the `@org.streamrune.core.Cacheable` annotation on each dispatched query's runtime class.

- **Un-annotated query** → dispatched straight to the delegate, no caching.
- **`@Cacheable` query** → served from a per-query-type [Caffeine](https://github.com/ben-manes/caffeine) cache, with TTL and maximum-size eviction taken from the annotation attributes. On a miss, the delegate computes the result and it is stored.

The cache key is the query instance itself (via `equals`/`hashCode`). `record` query types get canonical implementations for free; non-record query classes must implement `equals`/`hashCode` or every call is a miss.

For `Scope.USER` queries (the default) the key additionally includes the calling user's `UserId` from `StreamRuneContext`, so one user's cached results are never served to another. Callers without a bound user (anonymous requests) **bypass the cache entirely** — neither served from it nor stored into it — rather than sharing one anonymous partition across every unidentified caller (see `CachingQueryBus`'s javadoc for why a shared partition would be unsafe on Quarkus/Micronaut, where the request context is not always bound); such a dispatch is computed by the delegate and counted as a miss. `Scope.GLOBAL` keys by the query alone and stays fully cached for unbound callers, since it asserts caller-independence.

## Marking a query cacheable

Annotate the query record with `@Cacheable`. All attributes have defaults, so `@Cacheable` alone caches with a 60-second TTL and up to 1000 entries.

```java
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;

@Cacheable(
    ttlSeconds = 300,
    maxEntries = 500,
    invalidateOn = {OrderPlaced.class, OrderCancelled.class})
public record GetOrderCount(String customerId) implements Query<Long> {}
```

| Attribute | Type | Default | Meaning |
|---|---|---|---|
| `ttlSeconds` | `long` | `60` | Entry is evicted this many seconds after insertion, regardless of access. Must be `> 0`. |
| `maxEntries` | `long` | `1000` | Per-query-type cache size; the least-recently-used entry is evicted when full. Must be `> 0`. |
| `invalidateOn` | `Class<?>[]` | `{}` | Event types that trigger event-driven eviction (see below). Empty means TTL-only. |
| `scope` | `Cacheable.Scope` | `Scope.USER` | `USER` keys entries per calling user; `GLOBAL` shares one entry across all callers. |

`ttlSeconds` and `maxEntries` are validated on the **first dispatch** of the query type, not at compile time — a non-positive value throws `IllegalStateException` from `CachingQueryBus`.

Choose `Scope.GLOBAL` only for read models whose result is identical for every caller. Never use it for a handler that reads the ambient user from `StreamRuneContext`, or one user's results will leak to another.

## Authorization — a cache hit skips the handler

**A cache HIT never calls the delegate.** If a query handler performs its own authorization check internally (a common pattern — "only the order's owner may read it"), that check simply does not run on a hit: the caller receives the cached answer computed for whoever triggered the original miss, for as long as the entry survives. A permission revoked *after* an entry is cached has no effect until the entry expires (`ttlSeconds`) or is evicted (`invalidateOn`/manual) — **TTL is a caching control, not a security control**, and neither trigger reacts to a permission change.

Once caching is turned on for a query whose result depends on the caller's access, authorization **must live in an `org.streamrune.core.QueryAuthorizer`**, not only inside the handler:

```java
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.QueryAuthorizer;
import org.streamrune.runtime.CachingQueryBus;

QueryAuthorizer ordersAuthorizer = (query, user) -> {
  if (query instanceof GetOrder q && !orderAccessControl.canRead(user, q.orderId())) {
    throw new AuthorizationException("Caller may not read order " + q.orderId());
  }
};

QueryBus bus = CachingQueryBus.builder()
    .delegate(delegate)
    .authorizer(ordersAuthorizer)
    .build();
```

`CachingQueryBus` consults the configured authorizer on **every** dispatch of a `@Cacheable` query — hit or miss, `USER`- or `GLOBAL`-scoped, identified or not — strictly *before* the cache is touched. A denial serves nothing (no stale hit) and stores nothing (a miss is never computed or cached). This means revocation is honoured **immediately, on the very next dispatch** — not after the TTL, and not only for a fresh miss.

Write the check at the **object level**: the query instance carries whatever identifiers the check needs (e.g. the order id a `GetOrder` query targets). A role-only check cannot express "does this caller own *this* resource", which is exactly the kind of authorization a handler would otherwise perform internally — and exactly what a cache hit would then skip.

Absent (the default), `CachingQueryBus` has no authorization opinion of its own — caching is purely a performance optimization, and the handler remains fully responsible for authorization, with the cache-hit caveat above.

## Wiring the bus manually

Obtain a `CachingQueryBus` via its builder. `delegate(...)` is required; `metrics(...)` and `authorizer(...)` are optional (see [Authorization](#authorization--a-cache-hit-skips-the-handler) above for the latter; with a Micrometer-backed `StreamRuneMetrics`, metrics records the `streamrune.queries.cache.hits` / `streamrune.queries.cache.misses` counters, tagged `query.type` with the query's simple class name).

```java
import org.streamrune.core.QueryBus;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.SimpleQueryBus;

SimpleQueryBus delegate = new SimpleQueryBus();

QueryBus bus = CachingQueryBus.builder()
    .delegate(delegate)
    .metrics(metrics)          // optional; omit for no cache metrics
    .authorizer(authorizer)    // optional; omit to leave authorization entirely to the handler
    .build();

bus.register(GetOrderCount.class, new GetOrderCountHandler());

long count = bus.dispatch(new GetOrderCount("c-1"));  // miss → delegate
long again = bus.dispatch(new GetOrderCount("c-1"));  // hit  → cache
```

`register(...)` delegates to the wrapped bus, so register handlers on the `CachingQueryBus` exactly as you would on the delegate.

## Event-driven invalidation

TTL bounds staleness by time; `invalidateOn` bounds it by events. When any event of a listed type is processed, **all** cached entries for that query type are evicted as soon as the batch that processed it has committed. Both triggers apply when both are configured — an entry is evicted on whichever fires first. Type matching is assignable-from, so a supertype in `invalidateOn` matches its subtypes.

Wire the bus's `cacheInvalidator()` into event processing so it is notified after each successful projection batch, once that batch's read-model writes are committed. The shipped decorator `CacheAwareProjection` does exactly that — it calls the invalidator after its delegate processes a batch without throwing and the batch has committed:

```java
import org.streamrune.runtime.CacheAwareProjection;
import org.streamrune.runtime.CacheInvalidator;

CachingQueryBus cachingBus = CachingQueryBus.builder().delegate(delegate).build();
CacheInvalidator invalidator = cachingBus.cacheInvalidator();

// Register the wrapped projection with your runner instead of the plain one:
Projection cacheAware = new CacheAwareProjection(orderProjection, invalidator);

// Or, in your own event processing, after a batch's writes were committed:
invalidator.onEventsProcessed(batch);   // List<EventEnvelope>
```

**The eviction runs after the commit, not before.** For a `TRANSACTIONAL_LOCAL` / `EXTERNAL_EFFECT` registration under a transactional `AtomicBatchProcessor` (`JdbcProjectionRepository`) the projection runs inside the read-model transaction, and its writes stay invisible to other connections until the commit. Evicting at that point would let a query landing before the commit miss the cache, read the old rows and cache them — and the cache would then serve the pre-batch read model until the TTL ran out. `CacheAwareProjection` therefore registers the eviction with `ProjectionRepository.afterCommit(...)` on the transaction-scoped repository it is handed: the transaction runs it once it has committed and drops it if it rolls back, so a rolled-back batch leaves the cache as it was. With a non-transactional processor (`AtomicBatchProcessor.nonAtomicAtLeastOnce()`, which hands the projection a `null` repository), and under `AT_LEAST_ONCE_IDEMPOTENT` on any processor, where the runner hands no transaction-scoped repository, the projection's own writes are already committed when it returns, so the cache is invalidated right after `process` returns, before the checkpoint — the cheaper of the two stale windows. If you call `onEventsProcessed(...)` yourself from inside a transactional batch, register it the same way — `repository.afterCommit(() -> invalidator.onEventsProcessed(batch))` — and if you decorate a `ProjectionRepository`, forward `afterCommit` to the repository you wrap; the inherited default runs the action at once, which inside a transaction means before the commit.

A query that is still loading when an eviction runs does not keep its answer: it may have read the rows from before the batch, so `CachingQueryBus` removes that answer once it is stored, and the next dispatch loads again.

If the wrapped projection throws, the invalidator is not called — nothing was written, so there is nothing to invalidate. `CacheAwareProjection` forwards `process(batch, repository)` and `processDeadLetterReplay(batch)` to its delegate (a dead-letter replay invalidates only when the delegate reports it applied something), so it is safe to wrap a transactional or self-fencing projection.

The returned invalidator inspects each event's type and evicts every per-query-type cache whose `@Cacheable.invalidateOn` includes that type (or a supertype). It swallows all runtime exceptions and logs a warning, so a runtime failure in invalidation never propagates back into event processing. An `Error` is not swallowed: it propagates, and the projection runner stops and reports it as it would an `Error` from the projection itself.

## Manual eviction

Two administrative methods on `CachingQueryBus` clear entries outside the TTL/event path:

```java
cachingBus.evict(GetOrderCount.class);   // clear one query type's cache
cachingBus.evictAll();                   // clear every query type's cache
```

Caches are created lazily on the first dispatch of a query type, so `evict(...)` is a no-op if that type has never been dispatched.

## Framework integration

All three integrations auto-decorate the `QueryBus` — but **caching is off by default**. Set `streamrune.query-cache.enabled=true` to have the integration produce a `CachingQueryBus` (registered as the primary `QueryBus`, wrapping `SimpleQueryBus`) plus a `CacheInvalidator` bean backed by it.

```properties
streamrune.query-cache.enabled=true
```

With the property set, injecting `org.streamrune.core.QueryBus` gives you the caching bus, and a `CacheInvalidator` bean is available for wiring into your event/projection processing. The integrations produce that bean but do not call it: wrap the projections that feed cached queries in `CacheAwareProjection` (or call `onEventsProcessed(...)` yourself), or `invalidateOn` never fires and entries expire by TTL only. When the property is absent or `false`, no `CachingQueryBus` bean is created and the plain `SimpleQueryBus` is used — annotations on your queries have no effect until you enable the cache.

All three integrations also auto-detect an application-supplied `org.streamrune.core.QueryAuthorizer` bean and wire it into the caching bus when present — no separate property. Define no bean and caching runs with no authorization opinion of its own, exactly as described in [Authorization](#authorization--a-cache-hit-skips-the-handler) above.

If you are not using an integration, wire the bus manually as shown above.

## Caveats

- **TTL and event-driven invalidation are caching controls, not security controls.** Neither reacts to a permission change — a revoked caller keeps receiving a cached answer until the entry naturally expires or is evicted for an unrelated reason. Configure a `QueryAuthorizer` for any cached query whose result depends on the caller's access; see [Authorization](#authorization--a-cache-hit-skips-the-handler) above.
- Caching is opt-in per query type; a missing `@Cacheable` annotation means the query is never cached, even when the caching bus is in place.
- `ttlSeconds` / `maxEntries` are validated on first dispatch, not at compile time — a non-positive value throws `IllegalStateException` then.
- `Scope.GLOBAL` shares one entry across all callers. Never use it for handlers that read the ambient user, or results leak across users.
- Event-driven invalidation evicts **all** entries for a query type when a matching event is seen — it is coarse-grained, not per-key.
- `cacheInvalidator()` only evicts when it is actually called after event processing. You are responsible for that call in every setup — the integrations produce the `CacheInvalidator` bean but do not wire it into your projections; use `CacheAwareProjection` or invoke `onEventsProcessed(...)` yourself.

## Related guides

| Guide | What it covers |
|---|---|
| [idempotency.md](idempotency.md) | Effectively-once command execution via `IdempotencyKey` and the command inbox |
| [audit.md](audit.md) | Recording who read and wrote what — the `AuditingQueryBus` that wraps the caching bus |
| [retry-and-resilience.md](retry-and-resilience.md) | `RetryPolicy`, circuit breaker, and the command dead-letter queue |
