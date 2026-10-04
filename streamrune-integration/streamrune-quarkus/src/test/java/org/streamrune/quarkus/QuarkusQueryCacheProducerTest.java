package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
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
 * Tests for the query cache producer chain in Quarkus.
 *
 * <p>{@code cachingQueryBus} and {@code cacheInvalidator} are gated at build time via
 * {@code @IfBuildProperty(name = "streamrune.query-cache.enabled", stringValue = "true")} — when
 * caching is disabled (the default) those beans do not exist, and the {@code queryBus} producer
 * must still yield a working {@link SimpleQueryBus}-based chain instead of failing on a null
 * product.
 */
class QuarkusQueryCacheProducerTest {

  private final StreamRuneProducers producers = new StreamRuneProducers();

  @SuppressWarnings("unchecked")
  private <T> Instance<T> satisfied(T value) {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(false);
    when(inst.isResolvable()).thenReturn(true);
    when(inst.get()).thenReturn(value);
    return inst;
  }

  @SuppressWarnings("unchecked")
  private <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(true);
    when(inst.isResolvable()).thenReturn(false);
    return inst;
  }

  /**
   * Simulates two unqualified beans of the same type: {@code Instance.isResolvable()} is {@code
   * false} for this case too — indistinguishable from {@link #unsatisfied()} by that method alone,
   * which is exactly the bug: {@code isAmbiguous()} is the only way to tell the two apart.
   */
  @SuppressWarnings("unchecked")
  private <T> Instance<T> ambiguous() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(false);
    when(inst.isResolvable()).thenReturn(false);
    when(inst.isAmbiguous()).thenReturn(true);
    return inst;
  }

  @Test
  void cachingQueryBus_produces() {
    var delegate = new SimpleQueryBus();
    var bus = producers.cachingQueryBus(delegate, unsatisfied(), unsatisfied());
    assertNotNull(bus);
    assertInstanceOf(CachingQueryBus.class, bus);
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
   * A resolvable {@link QueryAuthorizer} bean is wired into the produced {@link CachingQueryBus}
   * and consulted on EVERY dispatch — including a cache HIT, which never calls the delegate.
   */
  @Test
  void cachingQueryBus_withAuthorizerBean_consultedOnAHit() {
    var delegate = new SimpleQueryBus();
    delegate.register(TestCachedQuery.class, q -> "answer");
    var authorizer = new RecordingAuthorizer();

    var bus = producers.cachingQueryBus(delegate, unsatisfied(), satisfied(authorizer));

    bus.dispatch(new TestCachedQuery("q1")); // MISS
    bus.dispatch(new TestCachedQuery("q1")); // HIT — the delegate is never called for this one

    assertEquals(
        2, authorizer.calls.get(), "the authorizer must run on the hit too, not just the miss");
  }

  /**
   * {@code Instance.isResolvable()} is also {@code false} when the bean is AMBIGUOUS (two
   * unqualified {@code QueryAuthorizer} beans), not only when none exists. Before this fix the
   * producer treated ambiguous exactly like absent and silently built the caching bus with NO
   * authorizer — every cache hit then served unauthorized, with nothing logged, reopening the exact
   * bypass the authorizer closes. This module's own convention for a security-relevant ambiguous
   * bean (see {@code SagaStateCryptoValidator.validate}) fails closed; this producer must too.
   */
  @Test
  void cachingQueryBus_ambiguousAuthorizer_throws() {
    var delegate = new SimpleQueryBus();
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> producers.cachingQueryBus(delegate, unsatisfied(), ambiguous()));
    assertTrue(ex.getMessage().contains("QueryAuthorizer"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("@Default") || ex.getMessage().contains("@Priority"),
        ex.getMessage());
  }

  @Test
  void cachingQueryBus_gatedAtBuildTime() throws Exception {
    // The producer must not exist when streamrune.query-cache.enabled != true —
    // gating via @IfBuildProperty instead of returning a null product (which CDI
    // rejects for non-dependent producers).
    var method =
        StreamRuneProducers.class.getMethod(
            "cachingQueryBus", SimpleQueryBus.class, Instance.class, Instance.class);
    var gate = method.getAnnotation(io.quarkus.arc.properties.IfBuildProperty.class);
    assertNotNull(gate);
    assertEquals("streamrune.query-cache.enabled", gate.name());
    assertEquals("true", gate.stringValue());
    assertFalse(gate.enableIfMissing());
  }

  /**
   * Sibling-path pin. The framework composite must be a CDI {@code @DefaultBean} so an
   * application-defined {@code QueryBus} (a tenant/authorization-filtering decorator) removes it
   * instead of losing to it. Micronaut carried no such back-off and silently discarded the user's
   * bus — a read-side authorization bypass. This module has no ArC container harness, so the
   * back-off is pinned on the declaration; the behavioural equivalents are Spring's {@code
   * StreamRuneAutoConfigurationTest.userDefinedQueryBusUnderAnotherName_winsOverFrameworkPrimary}
   * and Micronaut's {@code
   * MicronautBeanWiringTest.userSuppliedQueryBusReplacesTheFrameworkComposite}.
   */
  @Test
  void queryBus_isDefaultBean_soAnApplicationSuppliedBusWins() throws Exception {
    var method =
        StreamRuneProducers.class.getMethod(
            "queryBus", SimpleQueryBus.class, Instance.class, Instance.class);
    assertNotNull(
        method.getAnnotation(io.quarkus.arc.DefaultBean.class),
        "without @DefaultBean the framework composite would win every QueryBus injection point"
            + " even when the application defines its own bus");
  }

  @Test
  void cacheInvalidator_producedFromCachingBus() {
    var delegate = new SimpleQueryBus();
    var bus = CachingQueryBus.builder().delegate(delegate).build();
    var invalidator = producers.cacheInvalidator(bus);
    assertNotNull(invalidator);
    assertInstanceOf(CacheInvalidator.class, invalidator);
  }

  @Test
  void cacheInvalidator_gatedAtBuildTime() throws Exception {
    var method = StreamRuneProducers.class.getMethod("cacheInvalidator", CachingQueryBus.class);
    var gate = method.getAnnotation(io.quarkus.arc.properties.IfBuildProperty.class);
    assertNotNull(gate);
    assertEquals("streamrune.query-cache.enabled", gate.name());
    assertEquals("true", gate.stringValue());
  }

  @Test
  void queryBus_cacheDisabled_returnsWorkingSimpleBus() {
    // The default: no CachingQueryBus bean exists, no AuditStore — the primary
    // QueryBus must be the SimpleQueryBus itself, not null and not an NPE.
    var simpleBus = new SimpleQueryBus();
    Instance<CachingQueryBus> noCaching = unsatisfied();
    Instance<AuditStore> noAudit = unsatisfied();

    QueryBus result = producers.queryBus(simpleBus, noCaching, noAudit);
    assertSame(simpleBus, result);
  }

  @Test
  void queryBus_withAuditStore_wrapsInAuditingQueryBus() {
    var simpleBus = new SimpleQueryBus();
    Instance<CachingQueryBus> noCaching = unsatisfied();
    AuditStore store = mock(AuditStore.class);
    Instance<AuditStore> auditStoreInstance = satisfied(store);

    QueryBus result = producers.queryBus(simpleBus, noCaching, auditStoreInstance);
    assertInstanceOf(AuditingQueryBus.class, result);
  }

  @Test
  void queryBus_withNullAuditStoreProduct_returnsSimpleBus() {
    var simpleBus = new SimpleQueryBus();
    Instance<CachingQueryBus> noCaching = unsatisfied();
    Instance<AuditStore> nullAudit = satisfied(null);

    QueryBus result = producers.queryBus(simpleBus, noCaching, nullAudit);
    assertSame(simpleBus, result);
  }

  @Test
  void queryBus_withCachingAndAudit_wrapsChain() {
    var simpleBus = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(simpleBus).build();
    Instance<CachingQueryBus> cachingInstance = satisfied(cachingBus);
    AuditStore store = mock(AuditStore.class);
    Instance<AuditStore> auditStoreInstance = satisfied(store);

    QueryBus result = producers.queryBus(simpleBus, cachingInstance, auditStoreInstance);
    assertInstanceOf(AuditingQueryBus.class, result);
  }

  @Test
  void queryBus_withCachingOnly_returnsCachingBus() {
    var simpleBus = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(simpleBus).build();
    Instance<CachingQueryBus> cachingInstance = satisfied(cachingBus);
    Instance<AuditStore> noAudit = unsatisfied();

    QueryBus result = producers.queryBus(simpleBus, cachingInstance, noAudit);
    assertSame(cachingBus, result);
  }
}
