package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * A {@link Cacheable.Scope#USER} query must never be served across callers when the framework
 * cannot see who the caller is.
 *
 * <p>{@code cacheKey} keyed USER-scoped entries on {@code Authorization.currentUserId()}, which
 * reads {@code StreamRuneContext.CURRENT} and returns {@code null} whenever that ScopedValue is
 * unbound. Every unbound caller therefore produced the byte-identical key {@code
 * UserScopedKey(null, query)} and shared ONE partition. That is not a rare edge case: Spring is the
 * only integration whose filter wraps the handler in a {@code ScopedValue.where(CURRENT, ctx)}. On
 * Quarkus the only binding is the STATIC {@code StreamRuneRequestFilter.callWithContext} helper the
 * application must call by hand (no framework caller exists), and on Micronaut any
 * {@code @ExecuteOn} / reactive route whose body runs off the filter thread leaves it unbound too.
 * So on those runtimes EVERY authenticated user collapses into the shared "anonymous" partition and
 * the first caller's rows are served to the next one.
 */
class CachingQueryBusUnboundUserScopeTest {

  /**
   * USER scope is the default. Resolved from the ambient identity, like a real @Cacheable query.
   */
  @Cacheable(ttlSeconds = 60)
  record MyOrdersQuery() implements Query<String> {}

  @Cacheable(ttlSeconds = 60, scope = Cacheable.Scope.GLOBAL)
  record AllProductsQuery() implements Query<String> {}

  /**
   * Stands in for the application's own resolution of the caller: on Quarkus/Micronaut the identity
   * reaches the handler through the container (a request-scoped bean, a SecurityContext), NOT
   * through {@code StreamRuneContext.CURRENT}, which is exactly why the cache key sees null.
   */
  private final AtomicReference<String> ambientUser = new AtomicReference<>();

  private final AtomicInteger delegateCalls = new AtomicInteger();

  private CachingQueryBus bus;

  @BeforeEach
  void setUp() {
    QueryBus delegate =
        new QueryBus() {
          @Override
          @SuppressWarnings("unchecked")
          public <R> R dispatch(Query<R> query) {
            delegateCalls.incrementAndGet();
            return (R) (ambientUser.get() + "-orders");
          }

          @Override
          public <Q extends Query<R>, R> void register(
              Class<Q> queryType, QueryHandler<Q, R> handler) {}
        };
    bus = CachingQueryBus.builder().delegate(delegate).build();
  }

  private Object dispatchAs(String userId, Query<?> query) {
    return ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of(userId), CorrelationId.of("corr-1"), Instant.now(), null))
        .call(() -> bus.dispatch(query));
  }

  @Test
  void unboundCallers_areNeverServedAnotherCallersUserScopedResult() {
    // Alice's request on Quarkus: the resource method runs with CURRENT unbound.
    ambientUser.set("alice");
    assertEquals("alice-orders", bus.dispatch(new MyOrdersQuery()));

    // Bob's request, inside the TTL. Same query record, same (null) cache key.
    ambientUser.set("bob");
    assertEquals(
        "bob-orders",
        bus.dispatch(new MyOrdersQuery()),
        "an unbound USER-scoped caller must never be served the previous caller's rows");
    assertEquals(
        2,
        delegateCalls.get(),
        "an unbound USER-scoped query must always reach the"
            + " delegate — there is no safe partition to serve it from");
  }

  @Test
  void unboundCallers_neverPopulateThePartitionOfABoundUser() {
    // The reverse direction: an unbound dispatch must not poison a real user's partition either.
    ambientUser.set("mallory");
    bus.dispatch(new MyOrdersQuery());

    ambientUser.set("alice");
    assertEquals("alice-orders", dispatchAs("alice", new MyOrdersQuery()));
    assertEquals(2, delegateCalls.get());
  }

  @Test
  void boundUsers_stillGetTheirOwnCachedPartition() {
    // The regression guard: fail-closed must not disable caching for the callers it can identify.
    ambientUser.set("alice");
    assertEquals("alice-orders", dispatchAs("alice", new MyOrdersQuery()));
    ambientUser.set("still-alice-but-not-consulted");
    assertEquals("alice-orders", dispatchAs("alice", new MyOrdersQuery()));
    assertEquals(1, delegateCalls.get(), "a bound user's second dispatch is a cache HIT");

    ambientUser.set("bob");
    assertEquals("bob-orders", dispatchAs("bob", new MyOrdersQuery()));
    assertEquals(2, delegateCalls.get(), "a different bound user MISSES, never shares");
  }

  @Test
  void globalScope_isStillCachedForUnboundCallers() {
    // GLOBAL means "the answer does not depend on the caller", so an unbound caller is no risk:
    // the fail-closed bypass must not degrade it.
    ambientUser.set("alice");
    assertEquals("alice-orders", bus.dispatch(new AllProductsQuery()));
    ambientUser.set("bob");
    assertEquals("alice-orders", bus.dispatch(new AllProductsQuery()));
    assertEquals(1, delegateCalls.get(), "GLOBAL scope stays cached with no bound context");
  }
}
