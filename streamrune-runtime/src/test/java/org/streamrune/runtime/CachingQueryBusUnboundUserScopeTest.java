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
 * <p>{@code Authorization.currentUserId()} reads {@code StreamRuneContext.CURRENT} and returns
 * {@code null} whenever that ScopedValue is unbound, so a USER-scoped key built from it, {@code
 * UserScopedKey(null, query)}, is byte-identical for every unbound caller: they would share ONE
 * partition and the first caller's rows would be served to the next. Unbound callers are ordinary.
 * The integrations bind the caller for the query paths they own — Spring's filter on the servlet
 * thread, and the Spring, Micronaut and Quarkus {@code QueryBus} beans for a dispatch that starts
 * unbound, through their unbound-request-context source — but every query dispatched outside a
 * request (a scheduled job, a subscription thread, a DLQ replay, a thread of the application's own)
 * is unbound on every runtime, and so is a query a handler sends through a {@code QueryBus} built
 * without that source. Those callers bypass the cache.
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
   * Stands in for the application's own resolution of the caller — a request-scoped bean, a
   * SecurityContext — which a handler can read where {@code StreamRuneContext.CURRENT} is unbound,
   * and where the cache key therefore sees null.
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
    // Alice's query from a thread where nothing is bound, such as a QueryBus built without the
    // integration's unbound-request-context source.
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
