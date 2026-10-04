package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Cacheable;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * End-to-end test verifying that {@link CacheAwareProjection} integrates with {@link
 * CachingQueryBus} to evict the query cache on projection batch completion.
 */
class CacheAwareProjectionIntegrationTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // -------------------------------------------------------------------------
  // Test-local event and query types
  // -------------------------------------------------------------------------

  record OrderPlacedTestEvent(String orderId) implements DomainEvent {}

  // GLOBAL scope: this test is about projection-driven invalidation, not user partitioning, and a
  // USER-scoped dispatch with no bound context bypasses the cache entirely.
  @Cacheable(
      ttlSeconds = 60,
      scope = Cacheable.Scope.GLOBAL,
      invalidateOn = {OrderPlacedTestEvent.class})
  record GetOrderCountQuery(String userId) implements org.streamrune.core.Query<Integer> {}

  // -------------------------------------------------------------------------
  // Helper: build EventEnvelope for a domain event
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
            CorrelationId.of("corr-e2e"),
            null,
            null,
            Instant.now()));
  }

  // -------------------------------------------------------------------------
  // End-to-end scenario
  // -------------------------------------------------------------------------

  @Test
  void cacheInvalidation_viaProjectionBatch_evictsQueryCache() {
    // --- Arrange ---
    SimpleQueryBus simple = new SimpleQueryBus();
    CachingQueryBus cachingBus = CachingQueryBus.builder().delegate(simple).build();

    AtomicInteger handlerCallCount = new AtomicInteger(0);
    cachingBus.register(
        GetOrderCountQuery.class,
        query -> {
          handlerCallCount.incrementAndGet();
          return handlerCallCount.get();
        });

    // Step a: first dispatch — cache miss, handler called
    int result1 = cachingBus.dispatch(new GetOrderCountQuery("user-1"));
    assertEquals(1, result1, "first dispatch must be a cache miss");
    assertEquals(1, handlerCallCount.get());

    // Step b: second dispatch — cache hit, handler NOT called
    int result2 = cachingBus.dispatch(new GetOrderCountQuery("user-1"));
    assertEquals(1, result2, "second dispatch must return cached value");
    assertEquals(1, handlerCallCount.get(), "handler must not be called on cache hit");

    // Step c: build CacheAwareProjection with no-op delegate + bus invalidator
    Projection noOp = batch -> {}; // no-op projection
    CacheInvalidator invalidator = cachingBus.cacheInvalidator();
    CacheAwareProjection cacheAware = new CacheAwareProjection(noOp, invalidator);

    // Step d + e: build envelope and process batch — cache should be evicted
    EventEnvelope envelope = envelope(new OrderPlacedTestEvent("order-42"));
    cacheAware.process(List.of(envelope));

    // Step f: third dispatch — cache miss again, handler called
    int result3 = cachingBus.dispatch(new GetOrderCountQuery("user-1"));
    assertEquals(2, result3, "dispatch after invalidation must be a cache miss");
    assertEquals(2, handlerCallCount.get(), "handler must be called again after cache eviction");
  }
}
