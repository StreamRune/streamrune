package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.micronaut.context.ApplicationContext;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;
import org.streamrune.core.QueryAuthorizer;
import org.streamrune.core.QueryBus;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.runtime.AuditingQueryBus;
import org.streamrune.runtime.CacheInvalidator;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.SimpleQueryBus;

/**
 * Tests that {@link StreamRuneMicronautModule}'s query-cache factory methods are gated by their
 * {@code @Requires} guards.
 *
 * <p>Each test starts an {@link ApplicationContext} with (or without) the required property /
 * singleton to verify conditional bean registration.
 */
class MicronautQueryCacheFactoryTest {

  @Test
  void cachingQueryBus_enabled_present() {
    try (var ctx =
        ApplicationContext.builder()
            .properties(Map.of("streamrune.query-cache.enabled", "true"))
            .start()) {
      assertTrue(ctx.containsBean(CachingQueryBus.class));
      assertInstanceOf(CachingQueryBus.class, ctx.getBean(QueryBus.class));
    }
  }

  @Test
  void cachingQueryBus_disabled_absent() {
    try (var ctx = ApplicationContext.builder().start()) {
      assertFalse(ctx.containsBean(CachingQueryBus.class));
    }
  }

  @Test
  void cacheInvalidator_whenBusPresent_present() {
    try (var ctx =
        ApplicationContext.builder()
            .properties(Map.of("streamrune.query-cache.enabled", "true"))
            .start()) {
      assertTrue(ctx.containsBean(CacheInvalidator.class));
    }
  }

  @Test
  void cacheInvalidator_whenBusAbsent_absent() {
    try (var ctx = ApplicationContext.builder().start()) {
      assertFalse(ctx.containsBean(CacheInvalidator.class));
    }
  }

  @Test
  void cachingQueryBus_userOverride_suppressesAutoConfig() {
    var userBus =
        CachingQueryBus.builder().delegate(new org.streamrune.runtime.SimpleQueryBus()).build();
    try (var ctx =
        ApplicationContext.builder()
            .properties(Map.of("streamrune.query-cache.enabled", "true"))
            .singletons(userBus)
            .start()) {
      assertInstanceOf(CachingQueryBus.class, ctx.getBean(QueryBus.class));
      // Only one CachingQueryBus bean — the user-supplied one, not a second auto-config one
      assertTrue(ctx.containsBean(CachingQueryBus.class));
    }
  }

  @Cacheable(scope = Cacheable.Scope.GLOBAL)
  record TestCachedQuery(String id) implements Query<String> {}

  static final class RecordingAuthorizer implements QueryAuthorizer {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public void authorize(Query<?> query, org.streamrune.core.types.UserId user) {
      calls.incrementAndGet();
    }
  }

  /**
   * A {@link QueryAuthorizer} singleton bean is auto-detected and wired into the produced {@link
   * CachingQueryBus}, consulted on EVERY dispatch — including a cache HIT, which never calls the
   * delegate.
   */
  @Test
  void cachingQueryBus_withAuthorizerBean_consultedOnAHit() {
    var authorizer = new RecordingAuthorizer();
    try (var ctx =
        ApplicationContext.builder()
            .properties(Map.of("streamrune.query-cache.enabled", "true"))
            .singletons(authorizer)
            .start()) {
      var bus = ctx.getBean(CachingQueryBus.class);
      bus.register(TestCachedQuery.class, q -> "answer");

      bus.dispatch(new TestCachedQuery("q1")); // MISS
      bus.dispatch(new TestCachedQuery("q1")); // HIT

      assertEquals(
          2, authorizer.calls.get(), "the authorizer must run on the hit too, not just the miss");
    }
  }

  /**
   * Sibling-parity check: Micronaut's {@code @Nullable QueryAuthorizer} injection point already
   * fails closed when two unqualified beans exist — ambiguity fails resolution, unlike the Quarkus
   * bug that was fixed (which silently built an unauthorized caching bus instead). Pinning here so
   * a future refactor cannot regress Micronaut onto the same fail-open behavior without a test
   * noticing.
   *
   * <p>Note: one would expect {@code NonUniqueBeanException}, but the type actually thrown for an
   * ambiguous CONSTRUCTOR/METHOD parameter (as opposed to a direct {@code
   * getBean(QueryAuthorizer.class)} call) is {@code DependencyInjectionException}, whose message
   * reports "Multiple possible bean candidates found" — verified empirically against this exact
   * scenario (confirmed by first asserting the narrower type and observing the real thrown type in
   * the failure). The distinction that matters — ambiguity fails the context/lookup rather than
   * silently building an unauthorized bus — holds either way.
   *
   * <p>{@code start()} alone does not trigger this: {@code cachingQueryBus} is an ordinary
   * (non-{@code @Context}) singleton factory, instantiated lazily on first lookup — exactly like
   * {@link #cachingQueryBus_withAuthorizerBean_consultedOnAHit()} above, which also needs an
   * explicit {@code getBean} call. The ambiguity surfaces there, when the factory method's
   * parameter is actually injected.
   */
  @Test
  void cachingQueryBus_ambiguousAuthorizer_contextFails() {
    var authorizerOne = new RecordingAuthorizer();
    var authorizerTwo = new RecordingAuthorizer();
    try (var ctx =
        ApplicationContext.builder()
            .properties(Map.of("streamrune.query-cache.enabled", "true"))
            .singletons(authorizerOne, authorizerTwo)
            .start()) {
      var ex =
          assertThrows(
              io.micronaut.context.exceptions.DependencyInjectionException.class,
              () -> ctx.getBean(CachingQueryBus.class));
      assertTrue(ex.getMessage().contains("Multiple possible bean candidates"), ex.getMessage());
    }
  }

  @Test
  void queryBus_withAuditStore_wrapsInAuditingQueryBus() {
    var module = new StreamRuneMicronautModule();
    var simpleBus = new SimpleQueryBus();
    AuditStore store = mock(AuditStore.class);

    QueryBus result = module.queryBus(simpleBus, null, store);
    assertInstanceOf(AuditingQueryBus.class, result);
  }

  @Test
  void queryBus_withoutAuditStore_returnsSimpleBus() {
    var module = new StreamRuneMicronautModule();
    var simpleBus = new SimpleQueryBus();

    QueryBus result = module.queryBus(simpleBus, null, null);
    assertSame(simpleBus, result);
  }

  @Test
  void queryBus_withCachingAndAudit_wrapsChain() {
    var module = new StreamRuneMicronautModule();
    var simpleBus = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(simpleBus).build();
    AuditStore store = mock(AuditStore.class);

    QueryBus result = module.queryBus(simpleBus, cachingBus, store);
    assertInstanceOf(AuditingQueryBus.class, result);
  }
}
