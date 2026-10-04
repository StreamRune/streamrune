package org.streamrune.runtime;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Authorization;
import org.streamrune.core.Cacheable;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.Query;
import org.streamrune.core.QueryAuthorizer;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.UserId;

/**
 * {@link QueryBus} decorator that caches dispatch results for query types annotated with {@link
 * Cacheable}.
 *
 * <p>Non-cacheable queries pass through to the delegate with no overhead beyond a memoized
 * annotation lookup. Cacheable queries use a per-query-type Caffeine cache instance with TTL and
 * maximum-size eviction controlled by the annotation attributes.
 *
 * <p>For {@link Cacheable.Scope#USER}-scoped queries (the default) the cache key combines the query
 * with the calling user's {@link UserId} from {@link org.streamrune.core.StreamRuneContext}, so one
 * user's results are never served to another. When no user is bound the query <b>bypasses the cache
 * entirely</b> — neither served nor stored — rather than sharing one anonymous partition; use
 * {@link Cacheable.Scope#GLOBAL} for answers that genuinely do not depend on the caller, which stay
 * cached in that case. {@link Cacheable.Scope#GLOBAL} queries are keyed by the query alone.
 *
 * <p>Thread-safe. Obtain instances via {@link #builder()}.
 *
 * <pre>{@code
 * CachingQueryBus bus = CachingQueryBus.builder()
 *     .delegate(simpleQueryBus)
 *     .build();
 * }</pre>
 *
 * @see Cacheable
 * @see CachingQueryBusBuilder
 * @see CacheInvalidator
 */
public final class CachingQueryBus implements QueryBus {

  private static final Logger LOG = LoggerFactory.getLogger(CachingQueryBus.class);

  /** Sentinel to distinguish "annotation absent" from "not yet checked" in annotationCache. */
  private static final Cacheable NO_ANNOTATION = noAnnotationSentinel();

  private final QueryBus delegate;
  private final Ticker ticker;
  private final StreamRuneMetrics metrics;
  private final QueryAuthorizer authorizer;

  /** Per-query-type cache instances, created lazily on first dispatch. */
  private final Map<Class<?>, QueryCache> caches = new ConcurrentHashMap<>();

  /**
   * Memoized annotation lookup. Maps query class → annotation (or {@link #NO_ANNOTATION} if
   * absent).
   */
  private final Map<Class<?>, Cacheable> annotationCache = new ConcurrentHashMap<>();

  /** Query types already warned about for the fail-closed bypass; one WARN each. */
  private final java.util.Set<Class<?>> unidentifiedScopeWarned = ConcurrentHashMap.newKeySet();

  /** Package-private — use {@link #builder()}. */
  CachingQueryBus(
      QueryBus delegate, Ticker ticker, StreamRuneMetrics metrics, QueryAuthorizer authorizer) {
    this.delegate = delegate;
    this.ticker = ticker;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    this.authorizer = authorizer;
  }

  /** Returns a new {@link CachingQueryBusBuilder}. */
  public static CachingQueryBusBuilder builder() {
    return new CachingQueryBusBuilder();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R dispatch(Query<R> query) {
    Class<?> type = query.getClass();
    Cacheable ann = resolveAnnotation(type);
    if (ann == NO_ANNOTATION) {
      return delegate.dispatch(query);
    }
    // Built before the authorizer/fail-closed checks below so an invalid @Cacheable configuration
    // still fails fast on the first dispatch, whether or not a caller identity is available.
    QueryCache cache = caches.computeIfAbsent(type, t -> new QueryCache(buildCache(t, ann)));
    // Consulted on EVERY dispatch of a cacheable query — hit or miss,
    // USER- or GLOBAL-scoped, identified or not — strictly BEFORE the cache is touched below. A
    // denial (AuthorizationException) propagates from here: nothing is served (no stale hit) and
    // nothing is computed or stored (neither cache.get nor delegate.dispatch below is ever
    // reached). This is what closes the gap a cache HIT otherwise creates — a hit never calls the
    // delegate, so any authorization the handler performs internally never runs for it. Absent
    // (the default), behavior is unchanged.
    if (authorizer != null) {
      authorizer.authorize(query, Authorization.currentUserId());
    }
    if (isUnidentifiedUserScope(ann)) {
      // Fail closed. Nothing may be served from, or written to, a partition we cannot
      // attribute to a caller. The dispatch still counts as a MISS — the delegate computed it.
      warnUnidentifiedUserScopeOnce(type);
      R fresh = delegate.dispatch(query);
      recordMetric(
          () -> metrics.recordQueryCacheMiss(type.getSimpleName()), "recordQueryCacheMiss");
      return fresh;
    }
    // Caffeine invokes the mapping function only on a cache miss; the flag distinguishes a hit
    // (served from cache) from a miss (computed by the delegate) so each can be metered.
    Object key = cacheKey(query, ann);
    long evictionsBeforeLoad = cache.evictions().get();
    AtomicBoolean miss = new AtomicBoolean(false);
    R result =
        (R)
            cache
                .entries()
                .get(
                    key,
                    k -> {
                      miss.set(true);
                      return delegate.dispatch(query);
                    });
    String queryType = type.getSimpleName();
    if (miss.get()) {
      cache.dropIfEvictedDuringLoad(key, result, evictionsBeforeLoad);
      recordMetric(() -> metrics.recordQueryCacheMiss(queryType), "recordQueryCacheMiss");
    } else {
      recordMetric(() -> metrics.recordQueryCacheHit(queryType), "recordQueryCacheHit");
    }
    return result;
  }

  /** Runs a metric-recording action, logging and swallowing any failure. */
  private void recordMetric(Runnable recorder, String meter) {
    try {
      recorder.run();
    } catch (RuntimeException e) {
      LOG.warn("Metrics {} failed", meter, e);
    }
  }

  /**
   * Whether this dispatch is a {@link Cacheable.Scope#USER}-scoped query that the framework cannot
   * attribute to a caller, and must therefore bypass the cache entirely.
   *
   * <p><b>Why bypassing, not a shared "anonymous" partition.</b> The old key was {@code
   * UserScopedKey(Authorization.currentUserId(), query)}, and {@code currentUserId()} returns
   * {@code null} whenever {@code StreamRuneContext.CURRENT} is unbound. Every unbound caller then
   * produced the byte-identical key and shared ONE partition — so the first caller's rows were
   * served to the next. That is not confined to genuinely anonymous traffic: Spring is the only
   * integration whose filter wraps the handler in a {@code ScopedValue.where(CURRENT, ctx)}. On
   * Quarkus the sole binding is the STATIC {@code StreamRuneRequestFilter.callWithContext} helper
   * the application must call by hand (nothing in the framework calls it for the query path), and
   * on Micronaut any {@code @ExecuteOn} or reactive route whose body runs off the filter thread is
   * unbound as well. On those runtimes a {@code @Cacheable} USER-scoped query silently cross-served
   * authenticated users' data.
   *
   * <p>The two ways to be wrong are not symmetric: bypassing costs a delegate dispatch that could
   * have been a hit, while sharing discloses one user's rows to another. If the answer genuinely
   * does not depend on the caller, the query should declare {@link Cacheable.Scope#GLOBAL} — which
   * stays fully cached for unbound callers, precisely because it asserts caller-independence.
   *
   * <p>This closes the guarantee at the one place that can enforce it for every entry point,
   * including the off-request ones (async dispatch, DLQ replay, schedulers) that no HTTP filter can
   * reach. Binding {@code CURRENT} on Quarkus/Micronaut remains worth doing on its own merits — it
   * would turn these bypasses back into hits — but it is not what makes the guarantee hold.
   */
  private static boolean isUnidentifiedUserScope(Cacheable ann) {
    return ann.scope() != Cacheable.Scope.GLOBAL && Authorization.currentUserId() == null;
  }

  /**
   * Warns ONCE per query type that its cache is inert because no caller identity is bound. Silence
   * would turn a security fix into an unexplained cache-hit-rate collapse; per-dispatch logging
   * would flood. The operator's two remedies are named explicitly.
   */
  private void warnUnidentifiedUserScopeOnce(Class<?> type) {
    if (unidentifiedScopeWarned.add(type)) {
      LOG.warn(
          "@Cacheable(scope = USER) query {} was dispatched with no caller identity bound"
              + " (StreamRuneContext.CURRENT is unbound), so it BYPASSES the cache rather than"
              + " sharing one anonymous partition across callers. Either bind the request context"
              + " around the dispatch, or declare scope = GLOBAL if the answer does not depend on"
              + " the caller.",
          type.getSimpleName());
    }
  }

  /**
   * Builds the cache key for a query. {@link Cacheable.Scope#GLOBAL} queries are keyed by the query
   * alone; {@link Cacheable.Scope#USER} queries (the default) are additionally keyed by the calling
   * user's {@link UserId} so cached results never leak across users. Only reached with a non-null
   * user — {@link #isUnidentifiedUserScope} has already diverted the unattributable case.
   */
  private static Object cacheKey(Query<?> query, Cacheable ann) {
    if (ann.scope() == Cacheable.Scope.GLOBAL) {
      return query;
    }
    return new UserScopedKey(Authorization.currentUserId(), query);
  }

  /** Composite key for {@link Cacheable.Scope#USER}-scoped entries. */
  private record UserScopedKey(UserId userId, Query<?> query) {}

  /**
   * One query type's Caffeine cache, plus the number of times it has been evicted.
   *
   * <p>Caffeine's {@code invalidateAll()} does not discard a load that is still in flight. A query
   * that missed and read the read model before a projection batch committed could therefore store
   * its pre-commit answer AFTER the eviction that batch triggered, and the cache served that answer
   * until the entry's TTL ran out: the same stale pin as evicting before the commit, only with a
   * narrower window. A load now reads {@link #evictions} before it starts and calls {@link
   * #dropIfEvictedDuringLoad} once it has stored its answer; an eviction in between removes that
   * answer again, and the next dispatch reloads.
   *
   * <p>Why no stale answer survives: an eviction increments {@link #evictions} and then clears the
   * entries. If the load read the counter after the increment, it started after the commit that
   * triggered the eviction, so its answer is not stale. Otherwise either the load checks the
   * counter after the increment, sees it changed and removes its own answer, or it checked before
   * the increment, so it stored its answer before the eviction cleared the entries, and the
   * eviction removed it. A load that was not stale but overlapped an eviction is dropped too, which
   * costs one extra miss.
   */
  private record QueryCache(Cache<Object, Object> entries, AtomicLong evictions) {

    QueryCache(Cache<Object, Object> entries) {
      this(entries, new AtomicLong());
    }

    /** Evicts every entry; a load in flight meanwhile drops its answer once stored. */
    void evictAll() {
      evictions.incrementAndGet();
      entries.invalidateAll();
    }

    /**
     * Removes the answer a load just stored under {@code key} if the cache was evicted since {@code
     * evictionsBeforeLoad} was read. The entry is removed only while it still holds that answer, so
     * a different answer stored by a later load stays; a {@code null} answer, which Caffeine does
     * not store, removes nothing.
     */
    void dropIfEvictedDuringLoad(Object key, Object answer, long evictionsBeforeLoad) {
      if (evictions.get() != evictionsBeforeLoad) {
        entries.asMap().remove(key, answer);
      }
    }
  }

  @Override
  public <Q extends Query<R>, R> void register(Class<Q> queryType, QueryHandler<Q, R> handler) {
    delegate.register(queryType, handler);
  }

  /**
   * Evicts all cached entries across all query types. Useful for testing and administrative
   * operations.
   *
   * <p>Caches are created lazily on first dispatch; this method only clears caches that have been
   * materialized.
   */
  public void evictAll() {
    caches.values().forEach(QueryCache::evictAll);
  }

  /**
   * Evicts all cached entries for a single query type. No-op if {@code queryType} has no cache
   * instance yet.
   *
   * <p>No-op if no dispatch for this query type has occurred yet — the per-type cache is created
   * lazily on first dispatch.
   *
   * @param queryType the query class whose cache should be invalidated
   */
  public void evict(Class<?> queryType) {
    QueryCache cache = caches.get(queryType);
    if (cache != null) {
      cache.evictAll();
    }
  }

  /**
   * Runs pending Caffeine maintenance (eviction, expiry) for the given query type's cache. This is
   * a testing aid — Caffeine eviction is normally performed amortized during operations.
   *
   * <p>Package-private visibility keeps this out of the public API.
   *
   * @param queryType the query class whose cache should be cleaned up
   */
  void cleanUp(Class<?> queryType) {
    QueryCache cache = caches.get(queryType);
    if (cache != null) {
      cache.entries().cleanUp();
    }
  }

  /**
   * Returns a {@link CacheInvalidator} backed by this bus. The invalidator iterates the batch,
   * looks up each event's type, and evicts every per-query-type cache whose
   * {@code @Cacheable.invalidateOn} includes that type (or a supertype of it).
   *
   * <p>The returned invalidator swallows all runtime exceptions and logs a WARNING — it must never
   * propagate a failure to the caller. An {@link Error} is not swallowed: it propagates to the
   * projection runner, which stops and reports as for an Error from the projection itself.
   *
   * @return a non-null {@link CacheInvalidator}
   */
  public CacheInvalidator cacheInvalidator() {
    return batch -> {
      try {
        for (EventEnvelope env : batch) {
          Class<?> eventType = env.event().getClass();
          for (Map.Entry<Class<?>, QueryCache> entry : caches.entrySet()) {
            Cacheable ann = annotationCache.get(entry.getKey());
            if (ann == null || ann == NO_ANNOTATION) {
              continue;
            }
            for (Class<?> trigger : ann.invalidateOn()) {
              if (trigger.isAssignableFrom(eventType)) {
                entry.getValue().evictAll();
                break;
              }
            }
          }
        }
      } catch (RuntimeException e) {
        LOG.warn("Cache invalidation failed", e);
      }
    };
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  private Cacheable resolveAnnotation(Class<?> type) {
    return annotationCache.computeIfAbsent(
        type,
        t -> {
          Cacheable a = t.getAnnotation(Cacheable.class);
          return a != null ? a : NO_ANNOTATION;
        });
  }

  private Cache<Object, Object> buildCache(Class<?> type, Cacheable ann) {
    if (ann.ttlSeconds() <= 0) {
      throw new IllegalStateException(type.getSimpleName() + ": @Cacheable ttlSeconds must be > 0");
    }
    if (ann.maxEntries() <= 0) {
      throw new IllegalStateException(type.getSimpleName() + ": @Cacheable maxEntries must be > 0");
    }
    return Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofSeconds(ann.ttlSeconds()))
        .maximumSize(ann.maxEntries())
        .ticker(ticker)
        .build();
  }

  /** Creates a synthetic {@link Cacheable} instance used as a sentinel for absent annotations. */
  @SuppressWarnings("ClassExplicitlyAnnotation")
  private static Cacheable noAnnotationSentinel() {
    return new Cacheable() {
      @Override
      public Class<? extends java.lang.annotation.Annotation> annotationType() {
        return Cacheable.class;
      }

      @Override
      public long ttlSeconds() {
        return -1;
      }

      @Override
      public long maxEntries() {
        return -1;
      }

      @Override
      public Class<?>[] invalidateOn() {
        return new Class<?>[0];
      }

      @Override
      public Scope scope() {
        return Scope.USER;
      }
    };
  }
}
