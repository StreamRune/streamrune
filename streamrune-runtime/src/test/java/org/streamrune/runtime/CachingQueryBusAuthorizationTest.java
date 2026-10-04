package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;
import org.streamrune.core.QueryAuthorizer;
import org.streamrune.core.QueryBus;
import org.streamrune.core.QueryHandler;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * {@link CachingQueryBus} served a {@link Cacheable.Scope#USER}-partition cache HIT without ever
 * calling the delegate, so a caller whose access was revoked after the entry was cached kept
 * receiving it until TTL/invalidation — neither of which reacts to a permission change. {@link
 * QueryAuthorizer}, configured via {@link CachingQueryBusBuilder#authorizer}, closes the gap:
 * consulted on every dispatch, hit or miss, before the cache is ever touched.
 *
 * <p>The cache scenario: same userId, warm cache, permission revoked mid-flight → the next dispatch
 * is DENIED, the handler is never called, and the (still-cached) entry is never served.
 */
class CachingQueryBusAuthorizationTest {

  record SecretQuery(String resourceId) implements Query<String> {}

  @Cacheable(ttlSeconds = 300)
  record CacheableSecretQuery(String resourceId) implements Query<String> {}

  private static Object dispatchAs(QueryBus bus, String userId, Query<?> query) {
    return ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of(userId), CorrelationId.of("corr-1"), Instant.now(), null))
        .call(() -> bus.dispatch(query));
  }

  private static QueryBus delegateReturning(AtomicInteger handlerCalls, String value) {
    return new QueryBus() {
      @Override
      @SuppressWarnings("unchecked")
      public <R> R dispatch(Query<R> query) {
        handlerCalls.incrementAndGet();
        return (R) value;
      }

      @Override
      public <Q extends Query<R>, R> void register(
          Class<Q> queryType, QueryHandler<Q, R> handler) {}
    };
  }

  // ---------------------------------------------------------------------------------------------
  // The cache scenario: same userId, warm cache, permission revoked -> next dispatch DENIED,
  // handler not
  // called, entry not served.
  // ---------------------------------------------------------------------------------------------

  @Test
  void warmCache_permissionRevoked_nextDispatchIsDeniedAndHandlerNotCalled() {
    var handlerCalls = new AtomicInteger();
    var allowed = new java.util.concurrent.atomic.AtomicBoolean(true);
    QueryAuthorizer authorizer =
        (query, user) -> {
          if (!allowed.get()) {
            throw new AuthorizationException("revoked");
          }
        };
    QueryBus bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "secret"))
            .authorizer(authorizer)
            .build();

    // First dispatch: authorized, MISS, populates the cache.
    assertEquals("secret", dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    assertEquals(1, handlerCalls.get());

    // Permission revoked. The entry is still warm (well within its 300s TTL) — but the very next
    // dispatch must be denied, never reaching the stale cached value and never calling the
    // handler again.
    allowed.set(false);
    assertThrows(
        AuthorizationException.class,
        () -> dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")),
        "revocation must be honoured on the very next dispatch, not after the TTL");
    assertEquals(
        1, handlerCalls.get(), "the handler must not be called again for a denied dispatch");
  }

  @Test
  void deniedDispatch_doesNotPopulateTheCacheEither() {
    // A denial must not have any lingering side effect: once access is restored, the NEXT
    // dispatch must be a genuine MISS (handler called again), not served from something the
    // denied attempt might have stored.
    var handlerCalls = new AtomicInteger();
    var allowed = new java.util.concurrent.atomic.AtomicBoolean(false);
    QueryAuthorizer authorizer =
        (query, user) -> {
          if (!allowed.get()) {
            throw new AuthorizationException("not yet allowed");
          }
        };
    QueryBus bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "secret"))
            .authorizer(authorizer)
            .build();

    assertThrows(
        AuthorizationException.class,
        () -> dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    assertEquals(0, handlerCalls.get(), "a denial must never reach the delegate");

    allowed.set(true);
    assertEquals("secret", dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    assertEquals(1, handlerCalls.get(), "the first successful dispatch after a denial is a MISS");
  }

  @Test
  void uncachedAfterRevocation_stillDenied_matchesAppendixA() {
    // The review's own probe additionally evicts and re-dispatches; the uncached path must deny
    // identically (the authorizer runs whether or not anything is cached).
    var handlerCalls = new AtomicInteger();
    var allowed = new java.util.concurrent.atomic.AtomicBoolean(true);
    QueryAuthorizer authorizer =
        (query, user) -> {
          if (!allowed.get()) {
            throw new AuthorizationException("revoked");
          }
        };
    var bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "secret"))
            .authorizer(authorizer)
            .build();

    assertEquals("secret", dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    allowed.set(false);
    bus.evictAll();
    assertThrows(
        AuthorizationException.class,
        () -> dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
  }

  // ---------------------------------------------------------------------------------------------
  // Role / ACL / ownership variants — the authorizer must see enough of the query and caller to
  // express object-level checks, not just a role gate.
  // ---------------------------------------------------------------------------------------------

  record Document(String ownerId, String body) {}

  @Cacheable(ttlSeconds = 300)
  record GetDocument(String docId) implements Query<Document> {}

  @Test
  void ownershipRevoked_objectLevelCheck_deniesOnlyTheAffectedResource() {
    // The authorizer sees the QUERY (which carries the target id), so it can express "does this
    // caller own THIS resource" — not just a role/type-level gate.
    var owners = new java.util.concurrent.ConcurrentHashMap<String, String>();
    owners.put("doc-1", "alice");
    owners.put("doc-2", "alice");

    QueryAuthorizer ownershipAuthorizer =
        (query, user) -> {
          if (query instanceof GetDocument q) {
            String owner = owners.get(q.docId());
            if (owner == null || user == null || !owner.equals(user.value())) {
              throw new AuthorizationException("caller does not own " + q.docId());
            }
          }
        };
    var handlerCalls = new AtomicInteger();
    QueryBus delegate =
        new QueryBus() {
          @Override
          @SuppressWarnings("unchecked")
          public <R> R dispatch(Query<R> query) {
            handlerCalls.incrementAndGet();
            return (R) new Document("alice", "contents");
          }

          @Override
          public <Q extends Query<R>, R> void register(
              Class<Q> queryType, QueryHandler<Q, R> handler) {}
        };
    QueryBus bus =
        CachingQueryBus.builder().delegate(delegate).authorizer(ownershipAuthorizer).build();

    assertEquals(
        new Document("alice", "contents"), dispatchAs(bus, "alice", new GetDocument("doc-1")));

    // Ownership of doc-1 transfers away from alice; doc-2 is untouched.
    owners.put("doc-1", "bob");
    assertThrows(
        AuthorizationException.class, () -> dispatchAs(bus, "alice", new GetDocument("doc-1")));
    // A different, still-owned resource is unaffected by doc-1's revocation.
    assertEquals(
        new Document("alice", "contents"), dispatchAs(bus, "alice", new GetDocument("doc-2")));
  }

  @Test
  void aclRevoked_denyForOneUser_stillServesAnotherUsersOwnCachedEntry() {
    // Per-user USER-scope keying plus per-dispatch authorization compose correctly: revoking one
    // user's access must not deny — or evict — a DIFFERENT user's independent cache entry.
    var acl = new java.util.concurrent.ConcurrentHashMap<String, Set<String>>();
    acl.put("alice", new java.util.concurrent.CopyOnWriteArraySet<>(Set.of("r-1")));
    acl.put("bob", new java.util.concurrent.CopyOnWriteArraySet<>(Set.of("r-1")));

    QueryAuthorizer aclAuthorizer =
        (query, user) -> {
          if (query instanceof CacheableSecretQuery q) {
            Set<String> granted = user != null ? acl.get(user.value()) : null;
            if (granted == null || !granted.contains(q.resourceId())) {
              throw new AuthorizationException("no ACL entry for " + q.resourceId());
            }
          }
        };
    var handlerCalls = new AtomicInteger();
    QueryBus bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "secret"))
            .authorizer(aclAuthorizer)
            .build();

    assertEquals("secret", dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    assertEquals("secret", dispatchAs(bus, "bob", new CacheableSecretQuery("r-1")));
    assertEquals(2, handlerCalls.get(), "each user's own USER-scoped entry is a separate MISS");

    acl.get("alice").clear();
    // Alice's ACL entry is gone: denied, even though her own entry is still warm.
    assertThrows(
        AuthorizationException.class,
        () -> dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    // Bob's independent ACL and cache entry are unaffected.
    assertEquals("secret", dispatchAs(bus, "bob", new CacheableSecretQuery("r-1")));
    assertEquals(2, handlerCalls.get(), "bob's dispatch is still served from HIS warm cache entry");
  }

  // ---------------------------------------------------------------------------------------------
  // Consulted on every dispatch (hit AND miss); scope coverage; absence preserves old behavior.
  // ---------------------------------------------------------------------------------------------

  @Test
  void authorizer_consultedOnBothMissAndHit() {
    var authorizeCalls = new AtomicInteger();
    QueryAuthorizer countingAuthorizer = (query, user) -> authorizeCalls.incrementAndGet();
    var handlerCalls = new AtomicInteger();
    QueryBus bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "secret"))
            .authorizer(countingAuthorizer)
            .build();

    dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")); // MISS
    dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")); // HIT
    dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")); // HIT

    assertEquals(1, handlerCalls.get(), "sanity: the last two dispatches really were cache hits");
    assertEquals(
        3, authorizeCalls.get(), "the authorizer must run on every dispatch, hit or miss alike");
  }

  @Cacheable(ttlSeconds = 300, scope = Cacheable.Scope.GLOBAL)
  record CacheableGlobalQuery(String id) implements Query<String> {}

  @Test
  void authorizer_alsoConsultedForGlobalScopeQueries() {
    var allowed = new java.util.concurrent.atomic.AtomicBoolean(true);
    QueryAuthorizer authorizer =
        (query, user) -> {
          if (!allowed.get()) {
            throw new AuthorizationException("globally denied");
          }
        };
    var handlerCalls = new AtomicInteger();
    QueryBus bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "public-data"))
            .authorizer(authorizer)
            .build();

    assertEquals("public-data", bus.dispatch(new CacheableGlobalQuery("g-1")));
    allowed.set(false);
    assertThrows(AuthorizationException.class, () -> bus.dispatch(new CacheableGlobalQuery("g-1")));
  }

  @Test
  void noAuthorizerConfigured_behaviorUnchanged() {
    // Regression guard: absence of an authorizer must not alter existing caching behavior at all.
    var handlerCalls = new AtomicInteger();
    QueryBus bus =
        CachingQueryBus.builder().delegate(delegateReturning(handlerCalls, "secret")).build();

    assertEquals("secret", dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    assertEquals("secret", dispatchAs(bus, "alice", new CacheableSecretQuery("r-1")));
    assertEquals(1, handlerCalls.get(), "still caches normally with no authorizer configured");
  }

  @Test
  void nonCacheableQuery_authorizerNotConsulted() {
    // The authorizer exists to close a gap CACHING creates; a non-@Cacheable query already
    // reaches the delegate on every call, so it is out of scope for this authorizer.
    var authorizeCalls = new AtomicInteger();
    QueryAuthorizer countingAuthorizer = (query, user) -> authorizeCalls.incrementAndGet();
    var handlerCalls = new AtomicInteger();
    QueryBus bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "plain"))
            .authorizer(countingAuthorizer)
            .build();

    assertEquals("plain", dispatchAs(bus, "alice", new SecretQuery("r-1")));
    assertEquals(0, authorizeCalls.get());
    assertEquals(1, handlerCalls.get());
  }

  @Test
  void unidentifiedCaller_authorizerStillConsulted_beforeTheExistingBypass() {
    // The bypass (no bound user -> skip the cache) and the authorizer are independent
    // and compose: the authorizer sees a null user and may deny outright, or may allow — in
    // which case the existing bypass still applies underneath it (never served from, or stored
    // into, an unattributable partition).
    var authorizeCalls = new AtomicInteger();
    var sawNullUser = new java.util.concurrent.atomic.AtomicBoolean(false);
    QueryAuthorizer authorizer =
        (query, user) -> {
          authorizeCalls.incrementAndGet();
          if (user == null) {
            sawNullUser.set(true);
          }
        };
    var handlerCalls = new AtomicInteger();
    QueryBus bus =
        CachingQueryBus.builder()
            .delegate(delegateReturning(handlerCalls, "secret"))
            .authorizer(authorizer)
            .build();

    // No StreamRuneContext bound at all.
    assertEquals("secret", bus.dispatch(new CacheableSecretQuery("r-1")));
    assertTrue(sawNullUser.get(), "the authorizer must be consulted even with no bound identity");
    assertEquals(1, authorizeCalls.get());

    assertEquals("secret", bus.dispatch(new CacheableSecretQuery("r-1")));
    assertEquals(
        2,
        handlerCalls.get(),
        "an unidentified USER-scoped caller still bypasses the cache entirely,"
            + " independent of the authorizer allowing it");
  }
}
