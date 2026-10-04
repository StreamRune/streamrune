package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.streamrune.core.Cacheable;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

@ExtendWith(MockitoExtension.class)
class CachingQueryBusTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  @Mock private QueryBus delegate;

  private CachingQueryBus bus;

  // -------------------------------------------------------------------------
  // Test query and event types
  // -------------------------------------------------------------------------

  /** Non-cacheable query — no @Cacheable annotation. */
  record PlainQuery(String id) implements Query<String> {}

  // The cache-MECHANICS query types below (per-type cache instance, TTL, size eviction,
  // invalidation, evict/evictAll) declare GLOBAL scope so they exercise those mechanics without
  // also depending on a caller identity: a USER-scoped dispatch with no bound
  // context bypasses the cache entirely, so keeping them on the default USER scope would have
  // turned every one of these into a test of the bypass instead. USER-scope keying has its own
  // tests further down (and in CachingQueryBusUnboundUserScopeTest).

  /** Standard cacheable query with 60-second TTL. */
  @Cacheable(ttlSeconds = 60, scope = Cacheable.Scope.GLOBAL)
  record TestQuery(String id) implements Query<String> {}

  /** Cacheable query that is invalidated by OrderEvent. */
  @Cacheable(
      ttlSeconds = 60,
      scope = Cacheable.Scope.GLOBAL,
      invalidateOn = {OrderEvent.class})
  record OrderQuery(String id) implements Query<String> {}

  /** Second cacheable type for multi-type tests. */
  @Cacheable(
      ttlSeconds = 60,
      scope = Cacheable.Scope.GLOBAL,
      invalidateOn = {ShipEvent.class})
  record ShipQuery(String id) implements Query<String> {}

  /** Query with short TTL of 1 second — used for TTL expiry test. */
  @Cacheable(ttlSeconds = 1, scope = Cacheable.Scope.GLOBAL)
  record ShortTtlQuery(String id) implements Query<String> {}

  /** Query with maxEntries=1 to test size eviction deterministically. */
  @Cacheable(ttlSeconds = 3600, maxEntries = 1, scope = Cacheable.Scope.GLOBAL)
  record SmallCacheQuery(String id) implements Query<String> {}

  /** USER-scoped query (the default scope) — entries are partitioned per calling user. */
  @Cacheable(ttlSeconds = 60)
  record UserScopedQuery(String id) implements Query<String> {}

  /** GLOBAL-scoped query — entries are shared across all users. */
  @Cacheable(ttlSeconds = 60, scope = Cacheable.Scope.GLOBAL)
  record GlobalQuery(String id) implements Query<String> {}

  /** Query with invalid ttlSeconds=0. */
  @Cacheable(ttlSeconds = 0)
  record BadTtlQuery(String id) implements Query<String> {}

  /** Query with invalid maxEntries=0. */
  @Cacheable(ttlSeconds = 60, maxEntries = 0)
  record BadMaxEntriesQuery(String id) implements Query<String> {}

  interface OrderEvent extends DomainEvent {}

  interface ShipEvent extends DomainEvent {}

  record OrderPlaced(String orderId) implements OrderEvent {}

  record OrderCancelled(String orderId) implements OrderEvent {}

  record ShipmentSent(String shipId) implements ShipEvent {}

  record UnrelatedEvent(String id) implements DomainEvent {}

  // -------------------------------------------------------------------------
  // Helper: controllable ticker for TTL tests
  // -------------------------------------------------------------------------

  static class FakeTicker implements Ticker {
    private final AtomicLong nanos = new AtomicLong(0);

    @Override
    public long read() {
      return nanos.get();
    }

    void advanceSeconds(long seconds) {
      nanos.addAndGet(seconds * 1_000_000_000L);
    }
  }

  // -------------------------------------------------------------------------
  // Helper: EventEnvelope factory
  // -------------------------------------------------------------------------

  private EventEnvelope envelope(DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        new Version(1),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now()));
  }

  // -------------------------------------------------------------------------
  // Setup
  // -------------------------------------------------------------------------

  @BeforeEach
  void setUp() {
    bus = CachingQueryBus.builder().delegate(delegate).build();
  }

  // -------------------------------------------------------------------------
  // 1. dispatch_nonCacheable_passesToDelegate
  // -------------------------------------------------------------------------

  @Test
  void dispatch_nonCacheable_passesToDelegate() {
    when(delegate.dispatch(any())).thenReturn("result");

    bus.dispatch(new PlainQuery("a"));
    bus.dispatch(new PlainQuery("a"));

    // No annotation → delegate called both times
    verify(delegate, times(2)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 2. dispatch_cacheable_firstHitsDelegate_subsequentReturnsCached
  // -------------------------------------------------------------------------

  @Test
  void dispatch_cacheable_firstHitsDelegate_subsequentReturnsCached() {
    when(delegate.dispatch(any())).thenReturn("v1", "v2");

    String first = bus.dispatch(new TestQuery("a"));
    String second = bus.dispatch(new TestQuery("a"));

    assertEquals("v1", first);
    assertEquals("v1", second, "second dispatch must return cached value, not v2");
    verify(delegate, times(1)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 3. dispatch_cacheable_differentQueryValues_separateEntries
  // -------------------------------------------------------------------------

  @Test
  void dispatch_cacheable_differentQueryValues_separateEntries() {
    when(delegate.dispatch(new TestQuery("a"))).thenReturn("ra");
    when(delegate.dispatch(new TestQuery("b"))).thenReturn("rb");

    assertEquals("ra", bus.dispatch(new TestQuery("a")));
    assertEquals("rb", bus.dispatch(new TestQuery("b")));
    // Both missed — delegate called for each unique key
    verify(delegate, times(2)).dispatch(any());
    // Repeat — both now cached
    bus.dispatch(new TestQuery("a"));
    bus.dispatch(new TestQuery("b"));
    verify(delegate, times(2)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 4. dispatch_cacheable_equalQueries_sameEntry (record equality)
  // -------------------------------------------------------------------------

  @Test
  void dispatch_cacheable_equalQueries_sameEntry() {
    when(delegate.dispatch(any())).thenReturn("hit");

    // Two distinct instances with same field values — records use structural equality
    TestQuery q1 = new TestQuery("same");
    TestQuery q2 = new TestQuery("same");
    assertNotSame(q1, q2);
    assertEquals(q1, q2);

    bus.dispatch(q1);
    bus.dispatch(q2);

    verify(delegate, times(1)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 5. dispatch_cacheable_ttlExpiry_redispatches
  // -------------------------------------------------------------------------

  @Test
  void dispatch_cacheable_ttlExpiry_redispatches() {
    FakeTicker ticker = new FakeTicker();
    CachingQueryBus timedBus = CachingQueryBus.builder().delegate(delegate).ticker(ticker).build();

    when(delegate.dispatch(any())).thenReturn("first", "second");

    String r1 = timedBus.dispatch(new ShortTtlQuery("x"));
    assertEquals("first", r1);
    verify(delegate, times(1)).dispatch(any());

    // Advance past the 1-second TTL
    ticker.advanceSeconds(2);

    String r2 = timedBus.dispatch(new ShortTtlQuery("x"));
    assertEquals("second", r2);
    verify(delegate, times(2)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 6. dispatch_cacheable_maxEntriesExceeded_evictsLRU
  // -------------------------------------------------------------------------

  @Test
  void dispatch_cacheable_maxEntriesExceeded_evictsLRU() {
    when(delegate.dispatch(any())).thenReturn("r1", "r2", "r1-again");

    // maxEntries=1: after inserting "a", inserting "b" forces "a" to be evicted
    bus.dispatch(new SmallCacheQuery("a")); // miss → r1
    bus.dispatch(new SmallCacheQuery("b")); // miss → r2; "a" scheduled for eviction

    // Force Caffeine to apply pending evictions
    bus.cleanUp(SmallCacheQuery.class);

    // "a" was evicted → dispatching again causes a miss
    bus.dispatch(new SmallCacheQuery("a")); // miss → r1-again
    verify(delegate, times(3)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 7. dispatch_cacheable_invalidTtl_throws
  // -------------------------------------------------------------------------

  @Test
  void dispatch_cacheable_invalidTtl_throws() {
    assertThrows(IllegalStateException.class, () -> bus.dispatch(new BadTtlQuery("x")));
  }

  // -------------------------------------------------------------------------
  // 8. dispatch_cacheable_invalidMaxEntries_throws
  // -------------------------------------------------------------------------

  @Test
  void dispatch_cacheable_invalidMaxEntries_throws() {
    assertThrows(IllegalStateException.class, () -> bus.dispatch(new BadMaxEntriesQuery("x")));
  }

  // -------------------------------------------------------------------------
  // 9. evictAll_clearsAllCaches
  // -------------------------------------------------------------------------

  @Test
  void evictAll_clearsAllCaches() {
    when(delegate.dispatch(any())).thenReturn("v");

    bus.dispatch(new TestQuery("a")); // populate TestQuery cache
    bus.dispatch(new OrderQuery("b")); // populate OrderQuery cache
    verify(delegate, times(2)).dispatch(any());

    bus.evictAll();

    bus.dispatch(new TestQuery("a")); // should miss
    bus.dispatch(new OrderQuery("b")); // should miss
    verify(delegate, times(4)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 10. evict_oneType_keepsOthers
  // -------------------------------------------------------------------------

  @Test
  void evict_oneType_keepsOthers() {
    when(delegate.dispatch(any())).thenReturn("v");

    bus.dispatch(new TestQuery("a")); // populate
    bus.dispatch(new OrderQuery("b")); // populate
    verify(delegate, times(2)).dispatch(any());

    bus.evict(TestQuery.class); // evict only TestQuery

    bus.dispatch(new TestQuery("a")); // miss
    bus.dispatch(new OrderQuery("b")); // still cached → no additional call
    verify(delegate, times(3)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 11. cacheInvalidator_eventMatchesInvalidateOn_evictsCache
  // -------------------------------------------------------------------------

  @Test
  void cacheInvalidator_eventMatchesInvalidateOn_evictsCache() {
    when(delegate.dispatch(any())).thenReturn("v");

    bus.dispatch(new OrderQuery("a")); // populate
    verify(delegate, times(1)).dispatch(any());

    CacheInvalidator inv = bus.cacheInvalidator();
    inv.onEventsProcessed(List.of(envelope(new OrderPlaced("o1"))));

    bus.dispatch(new OrderQuery("a")); // should miss after eviction
    verify(delegate, times(2)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 12. cacheInvalidator_eventDoesNotMatch_keepsCache
  // -------------------------------------------------------------------------

  @Test
  void cacheInvalidator_eventDoesNotMatch_keepsCache() {
    when(delegate.dispatch(any())).thenReturn("v");

    bus.dispatch(new OrderQuery("a")); // populate
    verify(delegate, times(1)).dispatch(any());

    CacheInvalidator inv = bus.cacheInvalidator();
    inv.onEventsProcessed(List.of(envelope(new UnrelatedEvent("x"))));

    bus.dispatch(new OrderQuery("a")); // still cached
    verify(delegate, times(1)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 13. cacheInvalidator_subclassMatches_evictsCache
  // -------------------------------------------------------------------------

  @Test
  void cacheInvalidator_subclassMatches_evictsCache() {
    when(delegate.dispatch(any())).thenReturn("v");

    bus.dispatch(new OrderQuery("a")); // populate
    verify(delegate, times(1)).dispatch(any());

    // OrderCancelled implements OrderEvent — assignableFrom must match
    CacheInvalidator inv = bus.cacheInvalidator();
    inv.onEventsProcessed(List.of(envelope(new OrderCancelled("o1"))));

    bus.dispatch(new OrderQuery("a")); // should miss
    verify(delegate, times(2)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 14. cacheInvalidator_invalidEnvelope_swallowsAndLogs
  // -------------------------------------------------------------------------

  @Test
  void cacheInvalidator_invalidEnvelope_swallowsAndLogs() {
    // A null batch entry will cause NPE inside the invalidator loop
    // — it must be swallowed, not propagated
    CacheInvalidator inv = bus.cacheInvalidator();
    List<EventEnvelope> badBatch = new java.util.ArrayList<>();
    badBatch.add(null); // null envelope → NPE on env.event()

    assertDoesNotThrow(() -> inv.onEventsProcessed(badBatch));
  }

  /**
   * Rule S1181: the invalidator swallows runtime exceptions, as its javadoc promises, but no longer
   * an {@link Error}. Logged at WARN, an Error left the cache stale while the projection runner
   * carried on; propagated, it reaches the runner's stop-and-report path like an Error from the
   * projection itself.
   */
  @Test
  void cacheInvalidator_errorInsideTheLoop_propagates() {
    CacheInvalidator inv = bus.cacheInvalidator();
    var linkageFailure = new NoClassDefFoundError("simulated");
    List<EventEnvelope> batch =
        new java.util.AbstractList<>() {
          @Override
          public EventEnvelope get(int index) {
            throw linkageFailure;
          }

          @Override
          public int size() {
            return 1;
          }
        };

    var thrown = assertThrows(NoClassDefFoundError.class, () -> inv.onEventsProcessed(batch));
    assertSame(linkageFailure, thrown);
  }

  // -------------------------------------------------------------------------
  // User scoping
  // -------------------------------------------------------------------------

  /** Dispatches the query with the given user bound in {@link StreamRuneContext}. */
  private Object dispatchAs(String userId, Query<?> query) {
    return ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of(userId), CorrelationId.of("corr-1"), Instant.now(), null))
        .call(() -> bus.dispatch(query));
  }

  @Test
  void userScoped_sameUser_hitsCache() {
    when(delegate.dispatch(any())).thenReturn("alice-data", "should-not-be-served");

    assertEquals("alice-data", dispatchAs("alice", new UserScopedQuery("q")));
    assertEquals("alice-data", dispatchAs("alice", new UserScopedQuery("q")));

    verify(delegate, times(1)).dispatch(any());
  }

  @Test
  void userScoped_differentUsers_neverShareEntries() {
    when(delegate.dispatch(any())).thenReturn("alice-data", "bob-data");

    assertEquals("alice-data", dispatchAs("alice", new UserScopedQuery("q")));
    // Same query, different user — must MISS, not serve alice's cached result
    assertEquals("bob-data", dispatchAs("bob", new UserScopedQuery("q")));
    // Each user's entry remains cached independently
    assertEquals("alice-data", dispatchAs("alice", new UserScopedQuery("q")));
    assertEquals("bob-data", dispatchAs("bob", new UserScopedQuery("q")));

    verify(delegate, times(2)).dispatch(any());
  }

  @Test
  void userScoped_unboundCallers_bypassTheCacheEntirely() {
    // This used to assert the opposite ("anonymous callers share one partition"),
    // which is the behaviour at issue: with CURRENT unbound — the normal state on
    // every Quarkus resource method and every Micronaut off-request thread — that shared partition
    // holds authenticated users' rows and serves them to whoever asks next. A USER-scoped answer
    // the framework cannot attribute to a caller is now neither served nor stored.
    when(delegate.dispatch(any())).thenReturn("first-caller", "second-caller", "alice-data");

    assertEquals("first-caller", bus.dispatch(new UserScopedQuery("q")));
    assertEquals("second-caller", bus.dispatch(new UserScopedQuery("q")));
    // Nothing was stored under a null user, so a real user still misses.
    assertEquals("alice-data", dispatchAs("alice", new UserScopedQuery("q")));

    verify(delegate, times(3)).dispatch(any());
  }

  @Test
  void globalScope_sharedAcrossUsers() {
    when(delegate.dispatch(any())).thenReturn("shared", "should-not-be-served");

    assertEquals("shared", dispatchAs("alice", new GlobalQuery("q")));
    assertEquals("shared", dispatchAs("bob", new GlobalQuery("q")));
    assertEquals("shared", bus.dispatch(new GlobalQuery("q")));

    verify(delegate, times(1)).dispatch(any());
  }

  @Test
  void userScoped_evictAll_clearsAllUserPartitions() {
    when(delegate.dispatch(any())).thenReturn("v1", "v2");

    assertEquals("v1", dispatchAs("alice", new UserScopedQuery("q")));
    bus.evictAll();
    assertEquals("v2", dispatchAs("alice", new UserScopedQuery("q")));

    verify(delegate, times(2)).dispatch(any());
  }

  // -------------------------------------------------------------------------
  // 15. register_passesToDelegate
  // -------------------------------------------------------------------------

  @Test
  void register_passesToDelegate() {
    QueryHandler<TestQuery, String> handler = q -> "handled";

    bus.register(TestQuery.class, handler);

    verify(delegate, times(1)).register(TestQuery.class, handler);
  }
}
