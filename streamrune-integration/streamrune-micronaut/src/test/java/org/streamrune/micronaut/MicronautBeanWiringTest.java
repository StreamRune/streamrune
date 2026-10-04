package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.AsyncCommandBus;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.QueryBus;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.postgres.LeaseBasedLeadership;
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresSagaDeadLetterStore;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.AuditCommandInterceptor;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.BeanValidationInterceptor;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;
import org.streamrune.runtime.ProjectionState;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * Container-level DI tests: the context must start, properties must bind, and every framework
 * default must resolve uniquely — and lose to application-defined beans of the same type.
 */
class MicronautBeanWiringTest {

  @Test
  void contextStarts_andPropertiesBeanIsUnique() {
    try (var ctx = ApplicationContext.run()) {
      var beans = ctx.getBeansOfType(StreamRuneMicronautProperties.class);
      assertEquals(1, beans.size(), "exactly one StreamRuneMicronautProperties bean must exist");
      var props = ctx.getBean(StreamRuneMicronautProperties.class);
      assertEquals(100, props.snapshotEveryNEvents());
      assertEquals(1024, props.stripeCount());
      assertEquals(java.time.Duration.ofSeconds(5), props.lockTimeout());
      assertEquals("streamrune", props.metricsPrefix());
      assertTrue(props.sagaEnabled());
      assertFalse(props.outboxEnabled());
      assertTrue(props.autoInitializeSchema(), "schema.auto-initialize must default to true");
      assertEquals(
          java.time.Duration.ofDays(7),
          props.outboxRetentionMaxAge(),
          "outbox.retention-max-age must default to 7 days");
    }
  }

  @Test
  void propertiesBindFromConfiguration() {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "streamrune.snapshot-every-n-events", "42",
                "streamrune.lock-timeout", "11s",
                "streamrune.metrics.prefix", "custom"))) {
      var props = ctx.getBean(StreamRuneMicronautProperties.class);
      assertEquals(42, props.snapshotEveryNEvents());
      assertEquals(java.time.Duration.ofSeconds(11), props.lockTimeout());
      assertEquals("custom", props.metricsPrefix());
      assertEquals(3, props.retryMaxAttempts(), "unset keys must keep defaults");
    }
  }

  @Test
  void nestedGroupKeysBindFromConfiguration_matchingSpringScheme() {
    // The saga/outbox/dead-letter/metrics groups use the same nested keys as the Spring
    // integration (streamrune.saga.enabled, ...) so configuration is portable across runtimes.
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "streamrune.saga.enabled", "false",
                "streamrune.outbox.batch-size", "42",
                "streamrune.outbox.flush-interval-ms", "1234",
                "streamrune.dead-letter.enabled", "false",
                "streamrune.dead-letter.retry-interval-ms", "9876",
                "streamrune.metrics.enabled", "false"))) {
      var props = ctx.getBean(StreamRuneMicronautProperties.class);
      assertFalse(props.sagaEnabled());
      assertEquals(42, props.outboxBatchSize());
      assertEquals(1234L, props.outboxFlushIntervalMs());
      assertFalse(props.deadLetterEnabled());
      assertEquals(9876L, props.deadLetterRetryIntervalMs());
      assertFalse(props.metricsEnabled());
      assertFalse(props.outboxEnabled(), "unset keys must keep defaults");
      assertEquals(5, props.deadLetterMaxRetries(), "unset keys must keep defaults");
    }
  }

  @Test
  void newParityPropertiesBindFromConfiguration() {
    // Verifies the two new properties added for Spring parity bind correctly from config keys
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "streamrune.event-store.schema.auto-initialize", "false",
                "streamrune.outbox.retention-max-age", "14d"))) {
      var props = ctx.getBean(StreamRuneMicronautProperties.class);
      assertFalse(props.autoInitializeSchema(), "schema.auto-initialize should bind to false");
      assertEquals(
          java.time.Duration.ofDays(14),
          props.outboxRetentionMaxAge(),
          "outbox.retention-max-age should bind to 14 days");
    }
  }

  @Test
  void baggageAllowlistDefaultsToEmpty() {
    try (var ctx = ApplicationContext.run()) {
      var props = ctx.getBean(StreamRuneMicronautProperties.class);
      assertTrue(props.metadataBaggageAllowlist().isEmpty());
    }
  }

  @Test
  void baggageAllowlistBindsCommaSeparatedConfiguration() {
    try (var ctx =
        ApplicationContext.run(
            Map.of("streamrune.metadata.baggage-allowlist", "tracking,tenant"))) {
      var props = ctx.getBean(StreamRuneMicronautProperties.class);
      assertEquals(java.util.List.of("tracking", "tenant"), props.metadataBaggageAllowlist());
    }
  }

  @Test
  void cryptoNestedPropertiesBind() {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "streamrune.crypto.cache.maximum-size", "123",
                "streamrune.crypto.cache.expire-after-write", "9m",
                "streamrune.crypto.filesystem.key-directory", "/tmp/sr-keys",
                "streamrune.crypto.vault.address", "http://vault:8200",
                "streamrune.crypto.vault.token", "s.test",
                "streamrune.crypto.aws.region", "eu-central-1"))) {
      var crypto = ctx.getBean(StreamRuneMicronautCryptoProperties.class);
      assertEquals(123, crypto.cache().maximumSize());
      assertEquals(java.time.Duration.ofMinutes(9), crypto.cache().expireAfterWrite());
      assertEquals("/tmp/sr-keys", crypto.filesystem().keyDirectory());
      assertEquals("http://vault:8200", crypto.vault().address());
      assertEquals("s.test", crypto.vault().token());
      assertEquals("eu-central-1", crypto.aws().region());
    }
  }

  @Test
  void fileSystemCryptoEngineBuiltFromNestedProperties() throws Exception {
    var keyDir = Files.createTempDirectory("sr-keys");
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "streamrune.crypto.filesystem.enabled",
                "true",
                "streamrune.crypto.filesystem.key-directory",
                keyDir.toString()))) {
      assertNotNull(ctx.getBean(CryptoEngine.class));
    }
  }

  @Test
  void frameworkDefaultsResolveUniquely_withoutDataSource() {
    try (var ctx = ApplicationContext.run()) {
      assertNotNull(ctx.getBean(EventTypeRegistry.class));
      assertNotNull(ctx.getBean(QueryBus.class), "QueryBus injection must not be ambiguous");
      assertNotNull(ctx.getBean(BeanValidationInterceptor.class));
      assertFalse(ctx.containsBean(EventStore.class), "no EventStore default without a DataSource");
      assertFalse(
          ctx.containsBean(VirtualThreadCommandBus.class),
          "no command bus default without an EventStore");
      assertFalse(
          ctx.containsBean(SubscriptionLeadership.class),
          "no single-active-consumer leadership bean without a DataSource — runners fall back to "
              + "SubscriptionLeadership.NOOP (always leader, single-instance behavior)");
    }
  }

  @Test
  void subscriptionLeadershipBean_isLeaseBased_whenDataSourcePresent() {
    // Multi-replica single-active-consumer safety: with a DataSource the module must produce a real
    // LeaseBasedLeadership so at most one replica is the active consumer and the rest sit in
    // standby. Spring and Quarkus both have this wiring test; Micronaut's declarative @Requires
    // gating (present when a DataSource exists) was previously unguarded at the container level — a
    // regression dropping the bean when a DataSource IS present would make every replica a leader
    // (split-brain: duplicate projection / DLQ-retry / scheduled processing) with no test catching
    // it. LeaseBasedLeadership opens no connection eagerly (its renew heartbeat idles while no
    // lease
    // is held), so the mock DataSource fixture suffices to boot the bean.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "with-datasource"))) {
      assertTrue(
          ctx.containsBean(SubscriptionLeadership.class),
          "a SubscriptionLeadership bean must exist when a DataSource is present");
      assertInstanceOf(
          LeaseBasedLeadership.class,
          ctx.getBean(SubscriptionLeadership.class),
          "the leadership bean must be the DB-clocked lease implementation");
    }
  }

  @Test
  void atomicBatchProcessorBean_exists_whenDataSourcePresent() {
    // The module auto-wires LeaseBasedLeadership the moment a DataSource
    // exists, and the runners thread that leadership's monotonic epoch into every projection
    // commit — but the epoch is only honoured by a TRANSACTIONAL AtomicBatchProcessor. Micronaut
    // produced a PostgresOffsetStore and a leadership bean, and no processor at all, so every
    // projection ran on the nonatomic processor: the takeover stamp was a no-op, the epoch fence
    // and overlap guard never ran, and a superseded leader's batch re-applied to the read model
    // silently. Spring (@ConditionalOnBean(DataSource) jdbcProjectionRepository) and Quarkus
    // (@DefaultBean jdbcProjectionRepository) have always produced one.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "with-datasource"))) {
      assertTrue(
          ctx.containsBean(org.streamrune.core.projection.AtomicBatchProcessor.class),
          "an AtomicBatchProcessor bean must exist when a DataSource is present — leadership is "
              + "auto-wired on the same condition and its epoch fence is inert without one");
      assertInstanceOf(
          org.streamrune.postgres.JdbcProjectionRepository.class,
          ctx.getBean(org.streamrune.core.projection.AtomicBatchProcessor.class),
          "the processor must be the transactional JDBC repository, matching Spring and Quarkus");
    }
  }

  @Test
  void frameworkProjectionRunner_getsAFencingProcessor_whenDataSourcePresent() throws Exception {
    // The bean existing is not the same as the runner USING it: ProjectionFactory gives a runner
    // the processor bean only when a registration declares a transactional mode or the leadership
    // must fence. The fixture's projection is AT_LEAST_ONCE_IDEMPOTENT, so it is the real
    // (lease-based) leadership that selects the bean here. Assert on the assembled runner.
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name",
                "fenced-projection",
                "streamrune.runner-lifecycle-enabled",
                "false"))) {
      var runner =
          ctx.getBean(
              MultiProjectionRunner.class,
              io.micronaut.inject.qualifiers.Qualifiers.byName(
                  ProjectionFactory.FRAMEWORK_MULTI_RUNNER));
      Field field = MultiProjectionRunner.class.getDeclaredField("atomicProcessor");
      field.setAccessible(true);
      Object processor = field.get(runner);
      assertFalse(
          processor instanceof AtomicBatchProcessor.NonAtomicAtLeastOnce,
          "the framework-assembled runner must not fall back to the nonatomic processor while a "
              + "real (lease-based) leadership is threaded into it");
      assertInstanceOf(org.streamrune.postgres.JdbcProjectionRepository.class, processor);
    }
  }

  @Test
  void transactionalProjection_overAProxiedProcessorBean_verifiesAndBoots() throws Exception {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name",
                "proxied-processor",
                "streamrune.runner-lifecycle-enabled",
                "false"))) {
      var runner =
          ctx.getBean(
              MultiProjectionRunner.class,
              io.micronaut.inject.qualifiers.Qualifiers.byName(
                  ProjectionFactory.FRAMEWORK_MULTI_RUNNER));
      Field field = MultiProjectionRunner.class.getDeclaredField("atomicProcessor");
      field.setAccessible(true);
      Object processor = field.get(runner);
      assertTrue(
          Proxy.isProxyClass(processor.getClass()),
          "the runner holds the proxy, not an unwrapped object");
      assertSame(
          ctx.getBean(AtomicBatchProcessor.class),
          processor,
          "the identity check verified the TRANSACTIONAL_LOCAL projection through the JDK proxy on"
              + " both sides");
    }
  }

  @Test
  void subscriptionLeadershipBean_absent_whenSingleActiveConsumerDisabled() {
    // Explicit opt-out: streamrune.subscription.single-active-consumer.enabled=false suppresses the
    // leadership bean even with a DataSource present (the @Requires(notEquals = "false") gate), so
    // runners fall back to SubscriptionLeadership.NOOP.
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name",
                "with-datasource",
                "streamrune.subscription.single-active-consumer.enabled",
                "false"))) {
      assertFalse(
          ctx.containsBean(SubscriptionLeadership.class),
          "no leadership bean when single-active-consumer is disabled, even with a DataSource");
    }
  }

  @Test
  void userDefinedBeansWinOverFrameworkDefaults() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-overrides"))) {
      var registry = ctx.getBean(EventTypeRegistry.class);
      assertSame(
          UserOverrides.REGISTRY, registry, "user-defined EventTypeRegistry must win the default");

      var store = ctx.getBean(EventStore.class);
      assertInstanceOf(InMemoryEventStore.class, store, "user-defined EventStore must be used");

      var bus = ctx.getBean(VirtualThreadCommandBus.class);
      assertNotNull(bus, "framework default bus must build on the user-defined EventStore");
      assertSame(
          bus, ctx.getBean(AsyncCommandBus.class), "the bus must satisfy AsyncCommandBus too");
    }
  }

  // ── a user-defined framework interceptor must REPLACE the framework default, not JOIN it
  // ──
  // Micronaut collection injection (List<CommandInterceptor>) includes @Secondary beans, so without
  // a @Requires(missingBeans = ...) guard a user's override runs ALONGSIDE the framework default —
  // double audit rows, a ghost circuit breaker at the framework threshold, etc.

  @Test
  void userAuditInterceptorReplacesFrameworkDefault_notBoth() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "audit-override"))) {
      var all = ctx.getBeansOfType(AuditCommandInterceptor.class);
      assertEquals(
          1,
          all.size(),
          "a user AuditCommandInterceptor must REPLACE the framework default, not run alongside it"
              + " (double audit rows)");
      assertSame(
          AuditOverrideFixture.USER,
          all.iterator().next(),
          "the single AuditCommandInterceptor must be the user's");
    }
  }

  @Test
  void userCircuitBreakerReplacesFrameworkDefault_notBoth() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "cb-override"))) {
      var all = ctx.getBeansOfType(CircuitBreakerCommandInterceptor.class);
      assertEquals(
          1,
          all.size(),
          "a user CircuitBreakerCommandInterceptor must REPLACE the framework default — otherwise a"
              + " hidden framework breaker keeps rejecting at its own threshold");
      assertSame(CircuitBreakerOverrideFixture.USER, all.iterator().next());
    }
  }

  @Test
  void userInlineProjectionInterceptorReplacesFrameworkDefault_notBoth() {
    // ProjectionFactory.inlineProjectionInterceptor carried @Secondary but no
    // @Requires(missingBeans = InlineProjectionInterceptor.class), so a user override JOINED the
    // framework default in the List<CommandInterceptor> the command bus consumes — after every
    // command commit both after() hooks ran and every inline projection applied each committed
    // event
    // TWICE (double-counted read models, duplicate rows) with no error or log.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "inline-override"))) {
      var all = ctx.getBeansOfType(InlineProjectionInterceptor.class);
      assertEquals(
          1,
          all.size(),
          "a user InlineProjectionInterceptor must REPLACE the framework default, not run alongside"
              + " it — both would apply every inline projection twice per command");
      assertSame(
          InlineProjectionOverrideFixture.USER,
          all.iterator().next(),
          "the single InlineProjectionInterceptor must be the user's");

      // The chain the bus actually receives: collection injection includes @Secondary beans, so
      // this is where a missing missingBeans guard shows up as a duplicated interceptor.
      long inChain =
          ctx.getBeansOfType(org.streamrune.core.CommandInterceptor.class).stream()
              .filter(InlineProjectionInterceptor.class::isInstance)
              .count();
      assertEquals(
          1, inChain, "the command-interceptor chain must contain exactly one inline interceptor");
    }
  }

  @Test
  void everyFrameworkCommandInterceptorFactoryDeclaresMissingBeansGuard() {
    // The complete set of framework CommandInterceptor factories collected into the bus's
    // List<CommandInterceptor>: each must back off (@Requires(missingBeans = <its type>)) when the
    // user defines their own of the same type, matching Spring's @ConditionalOnMissingBean and
    // Quarkus's @DefaultBean.
    assertMissingBeansGuard(
        StreamRuneMicronautModule.class, "auditCommandInterceptor", AuditCommandInterceptor.class);
    assertMissingBeansGuard(
        StreamRuneMicronautModule.class,
        "circuitBreakerCommandInterceptor",
        CircuitBreakerCommandInterceptor.class);
    assertMissingBeansGuard(
        StreamRuneMicronautModule.class,
        "beanValidationInterceptor",
        BeanValidationInterceptor.class);
    assertMissingBeansGuard(
        StreamRuneMicronautModule.class,
        "annotationAuthorizationInterceptor",
        AnnotationAuthorizationInterceptor.class);
    assertMissingBeansGuard(
        StreamRuneMicronautModule.class,
        "openTelemetryCommandInterceptor",
        OpenTelemetryCommandInterceptor.class);
    assertMissingBeansGuard(
        StreamRuneMicronautModule.class,
        "authorizationCommandInterceptor",
        AuthorizationCommandInterceptor.class);
    // The seventh framework CommandInterceptor is produced by the sibling
    // ProjectionFactory and is easy to overlook.
    assertMissingBeansGuard(
        ProjectionFactory.class, "inlineProjectionInterceptor", InlineProjectionInterceptor.class);
  }

  private static void assertMissingBeansGuard(
      Class<?> declaringClass, String methodName, Class<?> interceptorType) {
    Method method = null;
    for (Method m : declaringClass.getDeclaredMethods()) {
      if (m.getName().equals(methodName)) {
        method = m;
        break;
      }
    }
    assertNotNull(method, "no factory method " + methodName);
    boolean guarded = false;
    for (Requires r : method.getAnnotationsByType(Requires.class)) {
      for (Class<?> missing : r.missingBeans()) {
        if (missing.equals(interceptorType)) {
          guarded = true;
        }
      }
    }
    assertTrue(
        guarded,
        methodName
            + " must declare @Requires(missingBeans = "
            + interceptorType.getSimpleName()
            + ".class) so a user override replaces it instead of joining the chain");
  }

  @Test
  void queryBusComposesCachingLayer_whenEnabled() {
    try (var ctx = ApplicationContext.run(Map.of("streamrune.query-cache.enabled", "true"))) {
      var bus = ctx.getBean(QueryBus.class);
      assertInstanceOf(org.streamrune.runtime.CachingQueryBus.class, bus);
      assertNotNull(ctx.getBean(org.streamrune.runtime.CacheInvalidator.class));
    }
  }

  @Test
  void queryBusIsSimpleBus_whenNoCachingOrAudit() {
    try (var ctx = ApplicationContext.run()) {
      assertSame(ctx.getBean(SimpleQueryBus.class), ctx.getBean(QueryBus.class));
    }
  }

  /**
   * A read-side authorization bypass. The framework composite carried neither {@code @Secondary}
   * nor {@code @Requires(missingBeans = ...)}, and its declared return type is the {@code QueryBus}
   * interface itself. Micronaut's {@code DefaultBeanContext.lastChanceResolve} ends in {@code
   * filterExactMatch}, which keeps only definitions whose {@code getBeanType() == QueryBus.class} —
   * so an application's {@code @Singleton class TenantFilteringQueryBus implements QueryBus} (bean
   * type {@code TenantFilteringQueryBus}) was filtered out and the framework's unfiltered chain
   * served every query. No exception, no warning: cross-tenant / unauthorized read-model data.
   * Spring backs off via {@code @ConditionalOnMissingBean(QueryBus)} and Quarkus via
   * {@code @DefaultBean}; Micronaut was the only runtime that silently discarded the override.
   */
  @Test
  void userSuppliedQueryBusReplacesTheFrameworkComposite() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-query-bus"))) {
      assertSame(
          UserQueryBusFixture.BUS,
          ctx.getBean(QueryBus.class),
          "an application-defined QueryBus (the documented tenant/authorization-filtering decorator)"
              + " must win every QueryBus injection point — the framework composite winning is a"
              + " silent read-side authorization bypass");
      assertEquals(
          1,
          ctx.getBeansOfType(QueryBus.class).size(),
          "the framework composite must be gone, not merely out-ranked — otherwise a QueryBus"
              + " injection point elsewhere can still resolve to it");
    }
  }

  /**
   * The interface-typed variant of the same override (a {@code @Factory} method declaring {@code
   * QueryBus} as its return type). Previously this ended in {@code NonUniqueBeanException} — loud
   * rather than silent, but still never the user's bus.
   */
  @Test
  void userSuppliedInterfaceTypedQueryBusReplacesTheFrameworkComposite() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-query-bus-iface"))) {
      assertSame(UserInterfaceTypedQueryBusFixture.BUS, ctx.getBean(QueryBus.class));
    }
  }

  /**
   * The other side of {@code SimpleQueryBus} and {@code CachingQueryBus} both implement {@code
   * QueryBus}, so a naive {@code @Requires(missingBeans = QueryBus.class)} on the composite would
   * be satisfied by the framework's OWN building blocks and self-suppress — leaving an application
   * with no {@code QueryBus} bean at all (or an unaudited one). The building blocks are therefore
   * narrowed with {@code @Bean(typed = ...)}. Pinned with the full chain enabled so a regression
   * cannot hide behind the degenerate "composite == simpleBus" case.
   */
  @Test
  void frameworkQueryBusCompositeStillBuiltWhenTheApplicationDefinesNone() {
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", "framework-query-bus", "streamrune.query-cache.enabled", "true"))) {
      var bus = ctx.getBean(QueryBus.class);
      assertInstanceOf(
          org.streamrune.runtime.AuditingQueryBus.class,
          bus,
          "the composed SimpleQueryBus -> CachingQueryBus -> AuditingQueryBus chain must survive");
      assertEquals(1, ctx.getBeansOfType(QueryBus.class).size());
    }
  }

  // ── the snapshot schema version must be reachable ────────────────────────────────

  /**
   * This module hardcoded {@code SnapshotPolicy.everyNEvents(n)} — the ONE-ARG overload, which pins
   * {@code snapshotVersion} to 1 — so the documented schema-version bump was unreachable and every
   * registered {@code SnapshotMigration} bean was dead code. Observed through the wired bus: the
   * version it passes to {@code EventStore.load(streamId, expectedSnapshotVersion)} IS the
   * effective snapshot schema version.
   */
  @Test
  void applicationSuppliedSnapshotPolicyReachesTheCommandBus() {
    SnapshotProbeFixture.LAST_EXPECTED_SNAPSHOT_VERSION.set(-1);
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", "snapshot-probe", "spec.snapshot-policy", "bumped"))) {
      ctx.getBean(VirtualThreadCommandBus.class).execute(new SnapshotProbeCommand("agg-snap-1"));
      assertEquals(
          2,
          SnapshotProbeFixture.LAST_EXPECTED_SNAPSHOT_VERSION.get(),
          "an application SnapshotPolicy bean must drive the bus's expected snapshot schema"
              + " version — otherwise the bump, and every SnapshotMigration, is unreachable");
    }
  }

  /** The default side: with no application policy bean the shipped behaviour is unchanged. */
  @Test
  void defaultSnapshotPolicyIsEveryNEventsAtSchemaVersionOne() {
    SnapshotProbeFixture.LAST_EXPECTED_SNAPSHOT_VERSION.set(-1);
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "snapshot-probe"))) {
      ctx.getBean(VirtualThreadCommandBus.class).execute(new SnapshotProbeCommand("agg-snap-2"));
      assertEquals(1, SnapshotProbeFixture.LAST_EXPECTED_SNAPSHOT_VERSION.get());
    }
  }

  /**
   * {@code SnapshotPolicy.never()} was equally unreachable — {@code
   * streamrune.snapshot-every-n-events} is validated {@code > 0}, so snapshots could not be
   * switched off either. The bus then passes {@code EventStore.IGNORE_SNAPSHOT}, the store's "never
   * consult a snapshot at all" sentinel — distinct from {@code 0}, "accept whatever is stored,"
   * which would silently rehydrate a snapshot left over from before the policy switch.
   */
  @Test
  void applicationSuppliedNeverPolicyDisablesSnapshotting() {
    // never() must route through EventStore.IGNORE_SNAPSHOT, not the old "skip the
    // version check, use any stored snapshot" sentinel (0) — a snapshot left over from before the
    // policy switched to never() must never be silently rehydrated. -1 here is this fixture's own
    // "not yet observed" marker (unrelated to IGNORE_SNAPSHOT's value) and must differ from it, so
    // a successful set() is unambiguous either way.
    SnapshotProbeFixture.LAST_EXPECTED_SNAPSHOT_VERSION.set(Integer.MIN_VALUE);
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", "snapshot-probe", "spec.snapshot-policy", "never"))) {
      ctx.getBean(VirtualThreadCommandBus.class).execute(new SnapshotProbeCommand("agg-snap-3"));
      assertEquals(
          EventStore.IGNORE_SNAPSHOT, SnapshotProbeFixture.LAST_EXPECTED_SNAPSHOT_VERSION.get());
    }
  }

  record SnapshotProbeCommand(String id) implements org.streamrune.core.Command {}

  record SnapshotProbeState() implements org.streamrune.core.AggregateState {}

  /** Emits nothing, so the probe dispatch resolves entirely inside the load path. */
  static final class SnapshotProbeDecider
      implements org.streamrune.core.Decider<
          SnapshotProbeCommand, SnapshotProbeState, org.streamrune.core.DomainEvent> {
    @Override
    public SnapshotProbeState initialState() {
      return new SnapshotProbeState();
    }

    @Override
    public List<org.streamrune.core.DomainEvent> decide(
        SnapshotProbeCommand command, SnapshotProbeState state) {
      return List.of();
    }

    @Override
    public SnapshotProbeState evolve(
        SnapshotProbeState state, org.streamrune.core.DomainEvent event) {
      return state;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "snapshot-probe")
  static class SnapshotProbeFixture {
    static final java.util.concurrent.atomic.AtomicInteger LAST_EXPECTED_SNAPSHOT_VERSION =
        new java.util.concurrent.atomic.AtomicInteger(-1);

    @Singleton
    EventStore recordingEventStore() {
      EventStore store = mock(EventStore.class);
      when(store.load(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt()))
          .thenAnswer(
              invocation -> {
                LAST_EXPECTED_SNAPSHOT_VERSION.set(invocation.getArgument(1));
                return org.streamrune.core.AggregateHistory.empty();
              });
      return store;
    }

    @Singleton
    org.streamrune.runtime.DeciderRegistration<
            SnapshotProbeCommand, SnapshotProbeState, org.streamrune.core.DomainEvent>
        snapshotProbeRegistration() {
      return new org.streamrune.runtime.DeciderRegistration<>(
          org.streamrune.core.types.AggregateType.of("snapshot_probe"),
          SnapshotProbeCommand.class,
          cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
          new SnapshotProbeDecider());
    }

    @Singleton
    @Requires(property = "spec.snapshot-policy", value = "bumped")
    org.streamrune.core.SnapshotPolicy bumpedSnapshotPolicy() {
      return org.streamrune.core.SnapshotPolicy.everyNEvents(100, 2);
    }

    @Singleton
    @Requires(property = "spec.snapshot-policy", value = "never")
    org.streamrune.core.SnapshotPolicy neverSnapshotPolicy() {
      return org.streamrune.core.SnapshotPolicy.never();
    }
  }

  // ── Decider registrations: the registration rule applies in the real container ─────────────────

  /**
   * Two registration beans for one command type: the bus refuses the second one with the
   * registration rule's message. The startup authorization validator resolves the bus, so the
   * refusal fails startup itself — never the first command.
   */
  @Test
  void twoRegistrationsOfOneCommandType_failStartupInTheRealContainer() {
    var failure =
        assertThrows(
            RuntimeException.class,
            () -> {
              try (var ctx =
                  ApplicationContext.run(
                      Map.of(
                          "spec.name",
                          "decider-registrations",
                          "spec.registrations",
                          "duplicate"))) {
                ctx.getBean(VirtualThreadCommandBus.class);
              }
            });
    Throwable root = failure;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    assertInstanceOf(IllegalArgumentException.class, root, () -> String.valueOf(failure));
    assertTrue(
        root.getMessage().contains("is already registered (aggregate type 'dup')"),
        root.getMessage());
    assertTrue(root.getMessage().contains(PlaceIt.class.getName()), root.getMessage());
  }

  /**
   * Five registrations: three sibling command roots under one aggregate type, two more under two
   * other types. The container builds the bus with every one of them.
   */
  @Test
  void fiveDistinctRegistrations_resolveTheBusInTheRealContainer() {
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", "decider-registrations", "spec.registrations", "five"))) {
      var bus = ctx.getBean(VirtualThreadCommandBus.class);
      assertNotNull(bus);
      assertEquals(
          java.util.Set.of(
              PlaceIt.class, ConfirmIt.class, CancelIt.class, StockIt.class, AuditIt.class),
          bus.registeredCommandTypes());
    }
  }

  record PlaceIt(String id) implements org.streamrune.core.Command {}

  record ConfirmIt(String id) implements org.streamrune.core.Command {}

  record CancelIt(String id) implements org.streamrune.core.Command {}

  record StockIt(String id) implements org.streamrune.core.Command {}

  record AuditIt(String id) implements org.streamrune.core.Command {}

  record RegistrationProbeState() implements org.streamrune.core.AggregateState {}

  /** Decides nothing; one instance per registration, so no instance spans two aggregate types. */
  static final class NoEventDecider<C extends org.streamrune.core.Command>
      implements org.streamrune.core.Decider<
          C, RegistrationProbeState, org.streamrune.core.DomainEvent> {
    @Override
    public RegistrationProbeState initialState() {
      return new RegistrationProbeState();
    }

    @Override
    public List<org.streamrune.core.DomainEvent> decide(C command, RegistrationProbeState state) {
      return List.of();
    }

    @Override
    public RegistrationProbeState evolve(
        RegistrationProbeState state, org.streamrune.core.DomainEvent event) {
      return state;
    }
  }

  private static <C extends org.streamrune.core.Command>
      org.streamrune.runtime.DeciderRegistration<
              C, RegistrationProbeState, org.streamrune.core.DomainEvent>
          registration(
              String aggregateType,
              Class<C> commandType,
              java.util.function.Function<C, String> id) {
    return new org.streamrune.runtime.DeciderRegistration<>(
        org.streamrune.core.types.AggregateType.of(aggregateType),
        commandType,
        command -> org.streamrune.core.types.AggregateId.of(id.apply(command)),
        new NoEventDecider<>());
  }

  @Factory
  @Requires(property = "spec.name", value = "decider-registrations")
  static class DeciderRegistrationsFixture {

    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }

    @Singleton
    @Requires(property = "spec.registrations", value = "duplicate")
    org.streamrune.runtime.DeciderRegistration<
            PlaceIt, RegistrationProbeState, org.streamrune.core.DomainEvent>
        firstPlaceRegistration() {
      return registration("dup", PlaceIt.class, PlaceIt::id);
    }

    @Singleton
    @Requires(property = "spec.registrations", value = "duplicate")
    org.streamrune.runtime.DeciderRegistration<
            PlaceIt, RegistrationProbeState, org.streamrune.core.DomainEvent>
        secondPlaceRegistration() {
      return registration("dup", PlaceIt.class, PlaceIt::id);
    }

    @Singleton
    @Requires(property = "spec.registrations", value = "five")
    org.streamrune.runtime.DeciderRegistration<
            PlaceIt, RegistrationProbeState, org.streamrune.core.DomainEvent>
        placeRegistration() {
      return registration("order", PlaceIt.class, PlaceIt::id);
    }

    @Singleton
    @Requires(property = "spec.registrations", value = "five")
    org.streamrune.runtime.DeciderRegistration<
            ConfirmIt, RegistrationProbeState, org.streamrune.core.DomainEvent>
        confirmRegistration() {
      return registration("order", ConfirmIt.class, ConfirmIt::id);
    }

    @Singleton
    @Requires(property = "spec.registrations", value = "five")
    org.streamrune.runtime.DeciderRegistration<
            CancelIt, RegistrationProbeState, org.streamrune.core.DomainEvent>
        cancelRegistration() {
      return registration("order", CancelIt.class, CancelIt::id);
    }

    @Singleton
    @Requires(property = "spec.registrations", value = "five")
    org.streamrune.runtime.DeciderRegistration<
            StockIt, RegistrationProbeState, org.streamrune.core.DomainEvent>
        stockRegistration() {
      return registration("inventory", StockIt.class, StockIt::id);
    }

    @Singleton
    @Requires(property = "spec.registrations", value = "five")
    org.streamrune.runtime.DeciderRegistration<
            AuditIt, RegistrationProbeState, org.streamrune.core.DomainEvent>
        auditRegistration() {
      return registration("test", AuditIt.class, AuditIt::id);
    }
  }

  /** An application-supplied QueryBus declared (and registered) as its own concrete type. */
  static final class TenantFilteringQueryBus implements QueryBus {
    @Override
    public <R> R dispatch(org.streamrune.core.Query<R> query) {
      throw new UnsupportedOperationException("not dispatched in this test");
    }

    @Override
    public <Q extends org.streamrune.core.Query<R>, R> void register(
        Class<Q> queryType, org.streamrune.core.QueryHandler<Q, R> handler) {
      throw new UnsupportedOperationException("not registered in this test");
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "user-query-bus")
  static class UserQueryBusFixture {
    static final TenantFilteringQueryBus BUS = new TenantFilteringQueryBus();

    // Declared as the CONCRETE type — exactly the shape a `@Singleton class ... implements
    // QueryBus` produces, and the shape filterExactMatch used to discard.
    @Singleton
    TenantFilteringQueryBus tenantFilteringQueryBus() {
      return BUS;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "user-query-bus-iface")
  static class UserInterfaceTypedQueryBusFixture {
    static final TenantFilteringQueryBus BUS = new TenantFilteringQueryBus();

    @Singleton
    QueryBus interfaceTypedQueryBus() {
      return BUS;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "framework-query-bus")
  static class AuditStoreFixture {

    @Singleton
    AuditStore auditStore() {
      return mock(AuditStore.class);
    }
  }

  @Test
  void lifecycleStartsAndStopsProjectionRunner() throws Exception {
    MultiProjectionRunner runner;
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "lifecycle"))) {
      assertNotNull(ctx.getBean(StreamRuneLifecycle.class));
      runner = ctx.getBean(MultiProjectionRunner.class);

      long deadline = System.currentTimeMillis() + 10_000;
      ProjectionState state = runner.status().get("wiring-test-projection").state();
      while (state == ProjectionState.PENDING && System.currentTimeMillis() < deadline) {
        Thread.sleep(50);
        state = runner.status().get("wiring-test-projection").state();
      }
      assertNotEquals(ProjectionState.PENDING, state, "lifecycle must start the projection runner");
    }
    assertEquals(
        ProjectionState.STOPPED,
        runner.status().get("wiring-test-projection").state(),
        "context shutdown must stop the projection runner");
  }

  @Test
  void frameworkDoesNotStartUserSuppliedRunner() {
    // A user-supplied (unqualified) MultiProjectionRunner bean is owned by the user — the
    // framework lifecycle must NOT start it. The lifecycle injects only the framework-assembled
    // runner BY NAME. Before the fix it injected the unqualified runner and started the user's, so
    // a user who self-started their runner double-started it (and it diverged from Spring, where a
    // user runner is never framework-started).
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-runner"))) {
      assertNotNull(
          ctx.getBean(StreamRuneLifecycle.class), "lifecycle must exist so StartupEvent fires");
      verify(UserRunnerFixture.RUNNER, never()).start();
    }
  }

  @Test
  void frameworkDoesNotStartUserSuppliedOutboxPoller() {
    // A user-supplied (unqualified) OutboxPoller bean is owned by the user — the framework
    // lifecycle must NOT start it. The lifecycle injects only the framework-assembled poller BY
    // NAME. Before the fix it injected the unqualified poller and started the user's, which
    // OutboxPoller.start() rejects on a second start (the user starts their own), crashing boot.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-outbox-poller"))) {
      assertNotNull(
          ctx.getBean(StreamRuneLifecycle.class), "lifecycle must exist so StartupEvent fires");
      verify(UserOutboxPollerFixture.POLLER, never()).start();
    }
  }

  @Test
  void lifecycleCanBeDisabled() {
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", "lifecycle", "streamrune.runner-lifecycle-enabled", "false"))) {
      assertFalse(ctx.containsBean(StreamRuneLifecycle.class));
      var runner = ctx.getBean(MultiProjectionRunner.class);
      assertEquals(
          ProjectionState.PENDING,
          runner.status().get("wiring-test-projection").state(),
          "runner must not start when lifecycle is disabled");
    }
  }

  @Test
  void outboxRetentionSweeper_absentWhenOutboxDisabled() {
    // outbox.enabled defaults to false — the sweeper must not be registered
    try (var ctx = ApplicationContext.run()) {
      assertFalse(
          ctx.containsBean(org.streamrune.runtime.OutboxRetentionSweeper.class),
          "OutboxRetentionSweeper must not be present when streamrune.outbox.enabled=false");
    }
  }

  @Test
  void deadLetterRetentionSweeper_presentByDefaultWhenQueuePresent() {
    // streamrune.dead-letter.enabled defaults to true, so with a DeadLetterQueue bean the
    // retention sweeper must be registered.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "with-dlq"))) {
      assertTrue(
          ctx.containsBean(org.streamrune.runtime.DeadLetterRetentionSweeper.class),
          "DeadLetterRetentionSweeper must be present by default when a DeadLetterQueue bean "
              + "exists");
      // The created sweeper is registered for relay liveness health.
      ctx.getBean(org.streamrune.runtime.DeadLetterRetentionSweeper.class);
      assertTrue(
          ctx
              .getBean(org.streamrune.runtime.BackgroundRelayHealthContributor.class)
              .components()
              .stream()
              .anyMatch(c -> c.name().equals("dead-letter-retention-sweeper")),
          "the dead-letter retention sweeper must be registered with the relay-health contributor"
              + " so a dead sweep thread reports DOWN");
    }
  }

  @Test
  void deadLetterRetentionSweeper_presentWhenAutomaticRetryIsDisabled() {
    // streamrune.dead-letter.enabled switches the automatic retry runner only. The command bus
    // dead-letters into the queue bean whatever it says, so the retention sweeper that bounds how
    // long those payloads stay queryable must follow the queue bean, not that switch.
    // runnerGateConfig makes every framework runner eligible (a command bus and a queue bean
    // exist), so the retry runner's absence below is the property alone.
    var config = runnerGateConfig(null);
    config.put("streamrune.dead-letter.enabled", "false");
    try (var ctx = ApplicationContext.run(config)) {
      assertFalse(
          ctx.containsBean(org.streamrune.runtime.DeadLetterRetryRunner.class),
          "the automatic retry runner must be off when streamrune.dead-letter.enabled=false");
      assertTrue(
          ctx.containsBean(org.streamrune.runtime.DeadLetterRetentionSweeper.class),
          "DeadLetterRetentionSweeper must stay present while a DeadLetterQueue bean exists");
      assertTrue(
          ctx
              .getBean(org.streamrune.runtime.BackgroundRelayHealthContributor.class)
              .components()
              .stream()
              .anyMatch(c -> c.name().equals("dead-letter-retention-sweeper")),
          "and its liveness must still be reported through the relay health");
    }
  }

  @Test
  void deadLetterRetentionSweeper_absentWithoutAQueueBean() {
    try (var ctx = ApplicationContext.run()) {
      assertFalse(
          ctx.containsBean(org.streamrune.runtime.DeadLetterRetentionSweeper.class),
          "no DeadLetterQueue bean, nothing to sweep");
    }
  }

  @Test
  void concretePostgresCommandInboxResolvable_whenDataSourcePresent() {
    // Regression test: the commandInbox factory used to declare CommandInbox (the interface) as
    // its return type. Micronaut resolves @Requires(beans = ...) / concrete-typed injection
    // points from the DECLARED factory-method return type, so consumers asking for the concrete
    // PostgresCommandInbox (postgresEventStoreFactory's `@Nullable PostgresCommandInbox`
    // parameter, and inboxRetentionSweeper's `PostgresCommandInbox` parameter) were NEVER
    // satisfied even though a bean existed — the event store silently built without a command
    // inbox and saga command dispatch threw UnsupportedOperationException at runtime. This test
    // fails on the interface-typed factory and passes once it declares PostgresCommandInbox.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "with-datasource"))) {
      assertTrue(
          ctx.containsBean(PostgresCommandInbox.class),
          "PostgresCommandInbox must be resolvable by its concrete type when a DataSource bean "
              + "is present");
      assertTrue(
          ctx.containsBean(CommandInbox.class),
          "PostgresCommandInbox must still satisfy the CommandInbox interface for interface-typed "
              + "consumers");
      assertSame(
          ctx.getBean(PostgresCommandInbox.class),
          ctx.getBean(CommandInbox.class),
          "the concrete and interface lookups must resolve to the same bean instance");

      // The concrete-typed downstream injection points must actually be satisfied.
      assertTrue(
          ctx.containsBean(InboxRetentionSweeper.class),
          "InboxRetentionSweeper requires a concrete PostgresCommandInbox bean and must be "
              + "registered once a DataSource is present");
    }
  }

  /**
   * The bus takes the {@link CommandInbox} INTERFACE while the event-store factory takes the
   * concrete {@link PostgresCommandInbox}, so a user-supplied inbox wins the bus's injection point
   * while the framework's {@code @Secondary} default still satisfies the factory's. The store would
   * keep claiming idempotency keys in the framework's own {@code command_inbox} table while the bus
   * pre-checks the user's inbox — the advertised override silently ignored on the authoritative
   * dedup path, so the user's store never receives a row and {@code execute(command, key)}'s
   * effectively-once guarantee is broken across a resume. It must fail fast at wiring, and only a
   * real container reproduces the split resolution. The startup validators resolve the command bus,
   * which needs the event store and so the factory: the startup itself is refused.
   */
  @Test
  void customCommandInboxWithTheFrameworkEventStoreFactoryFailsFast() {
    var failure =
        org.junit.jupiter.api.Assertions.assertThrows(
            RuntimeException.class,
            () -> ApplicationContext.run(Map.of("spec.name", "user-command-inbox")).close(),
            "a custom CommandInbox without a custom EventStoreFactory must fail at startup");
    var messages = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    assertTrue(
        messages.toString().contains("command-inbox wiring"),
        "the failure must name the inbox wiring invariant, got: " + messages);
    assertTrue(
        messages.toString().contains("appendWithKey"),
        "the failure must explain the appendWithKey contract, got: " + messages);
  }

  @Test
  void defaultCommandInboxWiringBuildsTheEventStoreFactory() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "with-datasource"))) {
      assertNotNull(
          ctx.getBean(org.streamrune.core.EventStoreFactory.class),
          "the default configuration (framework inbox on both sides) must still build");
    }
  }

  /**
   * The natural way to declare the outbox store is through its interface ({@code OutboxStore
   * outboxStore(DataSource ds)}). Micronaut exposes a factory bean only under its declared return
   * type and that type's supertypes, so every outbox consumer must ask for the {@link
   * org.streamrune.core.outbox.OutboxStore} interface: one that asks for the concrete {@code
   * PostgresOutboxStore} type does not see this bean, and the event store then appends every event
   * without its outbox row while the poller relays an always-empty table.
   */
  @Test
  void interfaceTypedOutboxStoreIsBoundIntoTheEventStoreAndSwept() throws Exception {
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", "interface-outbox-store", "streamrune.outbox.enabled", "true"))) {
      var store = ctx.getBean(org.streamrune.core.outbox.OutboxStore.class);
      var factory = ctx.getBean(org.streamrune.core.EventStoreFactory.class);
      assertSame(
          store,
          privateField(factory, "outboxStore"),
          "the framework event store must write its outbox rows into the OutboxStore bean");
      assertSame(
          InterfaceTypedOutboxStoreFixture.MAPPER,
          privateField(factory, "outboxEventMapper"),
          "the framework event store must map its events with the OutboxEventMapper bean");
      assertFrameworkRunnerPresent(
          ctx,
          org.streamrune.runtime.OutboxRetentionSweeper.class,
          StreamRuneMicronautModule.FRAMEWORK_OUTBOX_RETENTION_SWEEPER);
    }
  }

  /**
   * The framework's event store writes outbox rows on the append's own connection, which only a
   * {@code PostgresOutboxStore} can do. Paired with an {@code OutboxEventMapper}, any other {@code
   * OutboxStore} would leave every append without its outbox row, so startup must fail and name the
   * remedy.
   */
  @Test
  void outboxMapperWithAStoreTheEventStoreCannotWriteFailsStartup() {
    var failure =
        assertThrows(
            RuntimeException.class,
            () -> ApplicationContext.run(Map.of("spec.name", "foreign-outbox-store")).close(),
            "an OutboxEventMapper with a non-Postgres OutboxStore must fail startup");
    var messages = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    assertTrue(
        messages.toString().contains("PostgresOutboxStore"),
        "the failure must name the store the event store needs, got: " + messages);
    assertTrue(
        messages.toString().contains("InMemoryOutboxStore"),
        "the failure must name the store it was given, got: " + messages);
    assertTrue(
        messages.toString().contains("EventStoreFactory"),
        "the failure must name the alternative (an own EventStoreFactory), got: " + messages);
  }

  private static Object privateField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  @Factory
  @Requires(property = "spec.name", value = "interface-outbox-store")
  static class InterfaceTypedOutboxStoreFixture {

    static final org.streamrune.core.outbox.OutboxEventMapper MAPPER = event -> List.of();

    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    // See WithDataSourceFixture: the command bus the startup validators resolve needs an event
    // store that does not query the mock DataSource. The event-store factory, whose wiring this
    // test inspects, is still the framework's.
    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }

    // Declared through the interface on purpose. The ordering mode is stubbed because an un-stubbed
    // mock returns null from orderingMode(), which OutboxPoller.build() refuses.
    @Singleton
    org.streamrune.core.outbox.OutboxStore outboxStore() {
      var store = mock(org.streamrune.postgres.PostgresOutboxStore.class);
      when(store.orderingMode())
          .thenReturn(org.streamrune.core.outbox.OutboxOrderingMode.STRICT_PER_AGGREGATE);
      return store;
    }

    @Singleton
    org.streamrune.core.outbox.OutboxEventMapper outboxEventMapper() {
      return MAPPER;
    }

    @Singleton
    org.streamrune.core.outbox.OutboxPublisher outboxPublisher() {
      return mock(org.streamrune.core.outbox.OutboxPublisher.class);
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "foreign-outbox-store")
  static class ForeignOutboxStoreFixture {

    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Singleton
    org.streamrune.core.outbox.OutboxStore outboxStore() {
      return new org.streamrune.test.InMemoryOutboxStore();
    }

    @Singleton
    org.streamrune.core.outbox.OutboxEventMapper outboxEventMapper() {
      return event -> List.of();
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "user-command-inbox")
  static class UserCommandInboxFixture {

    // A DataSource so the framework's own @Secondary PostgresCommandInbox default is eligible too —
    // this reproduces the split resolution, not merely an absent default.
    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Singleton
    CommandInbox userCommandInbox() {
      return new org.streamrune.test.InMemoryCommandInbox();
    }
  }

  @Test
  void sagaDeadLetterRetentionSweeperPresent_whenDataSourcePresent() {
    // The sagaDeadLetterStore factory
    // declared the SagaDeadLetterStore INTERFACE as its return type while
    // sagaDeadLetterRetentionSweeper is gated on @Requires(beans =
    // PostgresSagaDeadLetterStore.class)
    // and takes a concrete PostgresSagaDeadLetterStore parameter. Micronaut derives bean types from
    // the declared factory-method return type, so that gate never matched the framework's own
    // default bean: the sweeper was silently absent in every default deployment and
    // saga_dead_letters grew without bound (a GDPR storage-limitation violation). The
    // boot validator only checks the STORE by interface type, so it passed while the sweeper was
    // missing.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "with-datasource"))) {
      assertTrue(
          ctx.containsBean(PostgresSagaDeadLetterStore.class),
          "PostgresSagaDeadLetterStore must be resolvable by its concrete type when a DataSource "
              + "bean is present");
      assertSame(
          ctx.getBean(PostgresSagaDeadLetterStore.class),
          ctx.getBean(SagaDeadLetterStore.class),
          "the concrete and interface lookups must resolve to the same bean instance");
      assertTrue(
          ctx.containsBean(SagaDeadLetterRetentionSweeper.class),
          "SagaDeadLetterRetentionSweeper requires a concrete PostgresSagaDeadLetterStore bean and "
              + "must be registered once a DataSource is present — without it saga_dead_letters is "
              + "never pruned");
      // StreamRuneLifecycle starts the sweeper through exactly this named lookup; asserting the
      // same qualifier proves the started bean is wired, not merely a type-compatible one.
      assertTrue(
          ctx.findBean(
                  SagaDeadLetterRetentionSweeper.class,
                  io.micronaut.inject.qualifiers.Qualifiers.byName(
                      StreamRuneMicronautModule.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER))
              .isPresent(),
          "the framework sweeper must be resolvable by the name StreamRuneLifecycle injects");
    }
  }

  @Test
  void sagaDeadLetterRetentionSweeper_absentWhenSagasDisabled() {
    try (var ctx =
        ApplicationContext.run(
            Map.of("spec.name", "with-datasource", "streamrune.saga.enabled", "false"))) {
      assertFalse(
          ctx.containsBean(PostgresSagaDeadLetterStore.class),
          "no saga dead-letter store when streamrune.saga.enabled=false");
      assertFalse(
          ctx.containsBean(SagaDeadLetterRetentionSweeper.class),
          "no saga dead-letter retention sweeper when streamrune.saga.enabled=false");
    }
  }

  @Test
  void userSuppliedSagaDeadLetterStoreWinsInterfaceResolution() {
    // Override semantics live on the INTERFACE: the framework default is @Secondary, so a
    // user-supplied SagaDeadLetterStore bean takes precedence for every interface-typed consumer
    // (the saga runner writes to the user's store) even though the default now declares the
    // concrete PostgresSagaDeadLetterStore return type. Note Micronaut's @Secondary deprioritizes
    // rather than removes, so the framework's own Postgres store bean — and the sweeper that prunes
    // it — remain registered; the sweeper only ever touches the table the framework created.
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-saga-dlq-store"))) {
      assertSame(
          UserSagaDeadLetterStoreFixture.USER_STORE,
          ctx.getBean(SagaDeadLetterStore.class),
          "a user-supplied SagaDeadLetterStore must win interface-typed resolution over the "
              + "framework @Secondary default");
    }
  }

  // === user runners/sweepers must SUPPRESS the framework defaults ===

  /**
   * Configuration that makes all six framework runner/sweeper factories eligible at once, so a
   * missing framework bean in the sibling test can only be the {@code missingBeans} guard and never
   * an unsatisfied gate.
   */
  private static java.util.HashMap<String, Object> runnerGateConfig(String specName) {
    var config = new java.util.HashMap<String, Object>();
    config.put("spec.gates", "runners");
    config.put("streamrune.outbox.enabled", "true");
    if (specName != null) {
      config.put("spec.name", specName);
    }
    return config;
  }

  /**
   * Regression pin: with every gate satisfied and NO user beans, all six framework runners/sweepers
   * exist and are resolvable under the name {@link StreamRuneLifecycle} injects. This is the
   * baseline the override test measures against — without it, "framework bean absent" could pass
   * for the wrong reason.
   */
  @Test
  void frameworkRunnersAndSweepersArePresentWhenTheApplicationSuppliesNone() {
    try (var ctx = ApplicationContext.run(runnerGateConfig(null))) {
      assertFrameworkRunnerPresent(
          ctx,
          org.streamrune.runtime.DeadLetterRetryRunner.class,
          StreamRuneMicronautModule.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER);
      assertFrameworkRunnerPresent(
          ctx,
          org.streamrune.runtime.OutboxPoller.class,
          StreamRuneMicronautModule.FRAMEWORK_OUTBOX_POLLER);
      assertFrameworkRunnerPresent(
          ctx,
          org.streamrune.runtime.OutboxRetentionSweeper.class,
          StreamRuneMicronautModule.FRAMEWORK_OUTBOX_RETENTION_SWEEPER);
      assertFrameworkRunnerPresent(
          ctx,
          InboxRetentionSweeper.class,
          StreamRuneMicronautModule.FRAMEWORK_INBOX_RETENTION_SWEEPER);
      assertFrameworkRunnerPresent(
          ctx,
          SagaDeadLetterRetentionSweeper.class,
          StreamRuneMicronautModule.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER);
      assertFrameworkRunnerPresent(
          ctx,
          org.streamrune.runtime.DeadLetterRetentionSweeper.class,
          StreamRuneMicronautModule.FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER);
      // Sibling sweep: the leadership coordinator carries no @Named (nothing injects it by name),
      // so only the one-bean assertion applies.
      assertEquals(
          1,
          ctx.getBeansOfType(SubscriptionLeadership.class).size(),
          "exactly one framework SubscriptionLeadership must exist by default");
    }
  }

  /**
   * On Spring the framework runners back off via {@code @ConditionalOnMissingBean}; on Quarkus via
   * {@code @DefaultBean}. On Micronaut the six factories carried only {@code @Secondary} +
   * {@code @Named}, and {@code @Secondary} merely DEMOTES a definition in ambiguous
   * single-injection-point selection — it never removes it. {@link StreamRuneLifecycle} injects
   * each runner BY NAME, so the framework bean was created and started alongside the user's: two
   * {@code DeadLetterRetryRunner}s polling the same {@code dead_letter_queue} (entries claimed by
   * the framework runner retried under the DEFAULT 5-attempt policy and its decider-only registry,
   * so a command registered via {@code Builder.registerCommand} — the pattern the runner's own
   * javadoc directs apps to — burns attempts unresolvable and is exhausted without ever executing),
   * and two retention sweepers deleting rows under different windows.
   *
   * <p>Each factory now carries {@code @Requires(missingBeans = <type>.class)} — the same
   * self-excluding pattern {@link StreamRuneMicronautModule#virtualThreadCommandBus} already uses —
   * so a user bean cleanly REPLACES the default and exactly one runner of each type exists. Only a
   * real container reproduces this: {@code @Secondary} demotion and named-bean lifecycle ownership
   * are container behaviors.
   */
  @Test
  void userSuppliedRunnersAndSweepersSuppressTheFrameworkDefaults() {
    try (var ctx = ApplicationContext.run(runnerGateConfig("user-runners"))) {
      assertUserRunnerReplacedFramework(
          ctx,
          org.streamrune.runtime.DeadLetterRetryRunner.class,
          UserRunnersFixture.DEAD_LETTER_RETRY_RUNNER,
          StreamRuneMicronautModule.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER);
      assertUserRunnerReplacedFramework(
          ctx,
          org.streamrune.runtime.OutboxPoller.class,
          UserRunnersFixture.OUTBOX_POLLER,
          StreamRuneMicronautModule.FRAMEWORK_OUTBOX_POLLER);
      assertUserRunnerReplacedFramework(
          ctx,
          org.streamrune.runtime.OutboxRetentionSweeper.class,
          UserRunnersFixture.OUTBOX_RETENTION_SWEEPER,
          StreamRuneMicronautModule.FRAMEWORK_OUTBOX_RETENTION_SWEEPER);
      assertUserRunnerReplacedFramework(
          ctx,
          InboxRetentionSweeper.class,
          UserRunnersFixture.INBOX_RETENTION_SWEEPER,
          StreamRuneMicronautModule.FRAMEWORK_INBOX_RETENTION_SWEEPER);
      assertUserRunnerReplacedFramework(
          ctx,
          SagaDeadLetterRetentionSweeper.class,
          UserRunnersFixture.SAGA_DEAD_LETTER_RETENTION_SWEEPER,
          StreamRuneMicronautModule.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER);
      assertUserRunnerReplacedFramework(
          ctx,
          org.streamrune.runtime.DeadLetterRetentionSweeper.class,
          UserRunnersFixture.DEAD_LETTER_RETENTION_SWEEPER,
          StreamRuneMicronautModule.FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER);
      // Sibling sweep: LeaseBasedLeadership starts a renew-heartbeat thread in its CONSTRUCTOR, so
      // a surviving framework definition is a stray heartbeat and a second lease holder id.
      var leaderships = ctx.getBeansOfType(SubscriptionLeadership.class);
      assertEquals(
          1,
          leaderships.size(),
          () ->
              "a user-supplied SubscriptionLeadership must replace the default, found: "
                  + leaderships);
      assertSame(
          UserRunnersFixture.LEADERSHIP,
          leaderships.iterator().next(),
          "the surviving coordinator must be the user's — a framework LeaseBasedLeadership would"
              + " start a stray heartbeat and claim leases under a second holder id");
    }
  }

  /**
   * The sweep that made the other framework runners back off deliberately left {@link
   * ProjectionFactory}'s two runners on the named-bean ownership contract alone. That contract
   * governs who <em>starts </em> a given instance; it does nothing about the framework
   * <em>defining</em> a second runner over the same {@code @ProjectionConfig} projections. Spring
   * ({@code @ConditionalOnMissingBean(MultiProjectionRunner.class)}) and Quarkus
   * ({@code @DefaultBean}) both suppress the framework definition outright, so a user following the
   * documented cross-framework override path — defining their own runner to wire a custom
   * subscription, e.g. a {@code HybridEventSubscription} for LISTEN/NOTIFY, which the default
   * runner cannot build — silently got BOTH runners on Micronaut, processing the same projection
   * concurrently in one process. The shared {@link SubscriptionLeadership} bean cannot fence them
   * apart (one holder id ⇒ both are "leader" for the same subscription name).
   *
   * <p>The existing {@link #frameworkDoesNotStartUserSuppliedRunner} declares no
   * {@code @ProjectionConfig} projection, so the framework runner is never assembled there and the
   * coexistence path went untested. These fixtures pair a user runner WITH an enabled projection of
   * the matching mode.
   */
  @Test
  void userSuppliedProjectionRunnersSuppressTheFrameworkDefaults() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-continuous-runner"))) {
      assertUserRunnerReplacedFramework(
          ctx,
          MultiProjectionRunner.class,
          UserContinuousRunnerFixture.RUNNER,
          ProjectionFactory.FRAMEWORK_MULTI_RUNNER);
      // Named-bean ownership still holds: the lifecycle's @Nullable @Named parameter resolves to
      // null (the user's
      // bean carries a different name), so the framework never starts the runner the user owns.
      assertNotNull(
          ctx.getBean(StreamRuneLifecycle.class), "lifecycle must exist so StartupEvent fires");
      verify(UserContinuousRunnerFixture.RUNNER, never()).start();
    }

    try (var ctx = ApplicationContext.run(Map.of("spec.name", "user-scheduled-runner"))) {
      assertUserRunnerReplacedFramework(
          ctx,
          org.streamrune.runtime.ScheduledProjectionRunner.class,
          UserScheduledRunnerFixture.RUNNER,
          ProjectionFactory.FRAMEWORK_SCHEDULED_RUNNER);
      assertNotNull(
          ctx.getBean(StreamRuneLifecycle.class), "lifecycle must exist so StartupEvent fires");
      verify(UserScheduledRunnerFixture.RUNNER, never()).start();
    }
  }

  /**
   * Regression pin for the other side of with NO user bean the framework runner must still be
   * created and resolvable under the name {@link StreamRuneLifecycle} injects, so adding {@code
   * missingBeans} cannot silently self-exclude the default (the failure mode that would leave
   * projections never running at all). The CONTINUOUS side is additionally covered end to end by
   * {@link #lifecycleStartsAndStopsProjectionRunner}, which asserts the lifecycle actually starts
   * it.
   */
  @Test
  void frameworkProjectionRunnersExistWhenTheApplicationDefinesNone() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "framework-continuous-runner"))) {
      assertFrameworkRunnerPresent(
          ctx, MultiProjectionRunner.class, ProjectionFactory.FRAMEWORK_MULTI_RUNNER);
    }

    try (var ctx = ApplicationContext.run(Map.of("spec.name", "framework-scheduled-runner"))) {
      assertFrameworkRunnerPresent(
          ctx,
          org.streamrune.runtime.ScheduledProjectionRunner.class,
          ProjectionFactory.FRAMEWORK_SCHEDULED_RUNNER);
    }
  }

  private static <T> void assertFrameworkRunnerPresent(
      ApplicationContext ctx, Class<T> type, String frameworkBeanName) {
    assertEquals(
        1,
        ctx.getBeansOfType(type).size(),
        () -> "exactly one framework " + type.getSimpleName() + " must exist by default");
    assertTrue(
        ctx.findBean(type, io.micronaut.inject.qualifiers.Qualifiers.byName(frameworkBeanName))
            .isPresent(),
        () ->
            "the framework "
                + type.getSimpleName()
                + " must be resolvable under the name StreamRuneLifecycle injects ("
                + frameworkBeanName
                + ")");
  }

  private static <T> void assertUserRunnerReplacedFramework(
      ApplicationContext ctx, Class<T> type, T userBean, String frameworkBeanName) {
    var beans = ctx.getBeansOfType(type);
    assertEquals(
        1,
        beans.size(),
        () ->
            "a user-supplied "
                + type.getSimpleName()
                + " must REPLACE the framework default, not coexist with it — both would run"
                + " against the same table. Found: "
                + beans);
    assertSame(userBean, beans.iterator().next(), () -> "the surviving bean must be the user's");
    assertFalse(
        ctx.findBean(type, io.micronaut.inject.qualifiers.Qualifiers.byName(frameworkBeanName))
            .isPresent(),
        () ->
            "the framework-named "
                + frameworkBeanName
                + " must be gone, otherwise StreamRuneLifecycle still starts it");
  }

  // === test fixtures ===

  /**
   * Satisfies every gate the six framework runner/sweeper factories declare — a DataSource (for the
   * Postgres command inbox and saga dead-letter store), a DeadLetterQueue, an EventStore (so the
   * framework command bus exists for {@code @Requires(beans = CommandBus.class)}), and the outbox
   * store/publisher pair. Keyed on its own property so both the default-present and the
   * user-override run share it.
   */
  @Factory
  @Requires(property = "spec.gates", value = "runners")
  static class RunnerGatesFixture {

    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Singleton
    org.streamrune.core.DeadLetterQueue deadLetterQueue() {
      return new org.streamrune.test.InMemoryDeadLetterQueue();
    }

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    // Every outbox consumer injects the OutboxStore interface, so the concrete declaration here
    // satisfies them all (the interface-typed declaration has its own test). The mode is stubbed
    // because an un-stubbed mock returns null from orderingMode(), which OutboxPoller.build()
    // refuses; the lease is stubbed because an un-stubbed mock returns Duration.ZERO from
    // claimLease(), which opts the poller out of its lease guard.
    @Singleton
    org.streamrune.postgres.PostgresOutboxStore outboxStore() {
      var store = mock(org.streamrune.postgres.PostgresOutboxStore.class);
      when(store.orderingMode())
          .thenReturn(org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST);
      when(store.claimLease()).thenReturn(Duration.ofMinutes(4));
      return store;
    }

    @Singleton
    org.streamrune.core.outbox.OutboxPublisher outboxPublisher() {
      return mock(org.streamrune.core.outbox.OutboxPublisher.class);
    }
  }

  /** The six application-supplied runners/sweepers that must replace the framework defaults. */
  @Factory
  @Requires(property = "spec.name", value = "user-runners")
  static class UserRunnersFixture {

    static final org.streamrune.runtime.DeadLetterRetryRunner DEAD_LETTER_RETRY_RUNNER =
        mock(org.streamrune.runtime.DeadLetterRetryRunner.class);
    static final org.streamrune.runtime.OutboxPoller OUTBOX_POLLER =
        mock(org.streamrune.runtime.OutboxPoller.class);
    static final org.streamrune.runtime.OutboxRetentionSweeper OUTBOX_RETENTION_SWEEPER =
        mock(org.streamrune.runtime.OutboxRetentionSweeper.class);
    static final InboxRetentionSweeper INBOX_RETENTION_SWEEPER = mock(InboxRetentionSweeper.class);
    static final SagaDeadLetterRetentionSweeper SAGA_DEAD_LETTER_RETENTION_SWEEPER =
        mock(SagaDeadLetterRetentionSweeper.class);
    static final org.streamrune.runtime.DeadLetterRetentionSweeper DEAD_LETTER_RETENTION_SWEEPER =
        mock(org.streamrune.runtime.DeadLetterRetentionSweeper.class);
    static final SubscriptionLeadership LEADERSHIP = mock(SubscriptionLeadership.class);

    @Singleton
    SubscriptionLeadership userLeadership() {
      return LEADERSHIP;
    }

    @Singleton
    org.streamrune.runtime.DeadLetterRetryRunner userDeadLetterRetryRunner() {
      return DEAD_LETTER_RETRY_RUNNER;
    }

    @Singleton
    org.streamrune.runtime.OutboxPoller userOutboxPoller() {
      return OUTBOX_POLLER;
    }

    @Singleton
    org.streamrune.runtime.OutboxRetentionSweeper userOutboxRetentionSweeper() {
      return OUTBOX_RETENTION_SWEEPER;
    }

    @Singleton
    InboxRetentionSweeper userInboxRetentionSweeper() {
      return INBOX_RETENTION_SWEEPER;
    }

    @Singleton
    SagaDeadLetterRetentionSweeper userSagaDeadLetterRetentionSweeper() {
      return SAGA_DEAD_LETTER_RETENTION_SWEEPER;
    }

    @Singleton
    org.streamrune.runtime.DeadLetterRetentionSweeper userDeadLetterRetentionSweeper() {
      return DEAD_LETTER_RETENTION_SWEEPER;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "with-datasource")
  static class WithDataSourceFixture {

    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    // The startup validators resolve the command bus; an EventStore of its own lets the bus be
    // built over the mock DataSource, which the framework's event store would query for its schema.
    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "user-saga-dlq-store")
  static class UserSagaDeadLetterStoreFixture {

    static final SagaDeadLetterStore USER_STORE = mock(SagaDeadLetterStore.class);

    // A DataSource so the framework's own @Secondary default is eligible too — this proves the
    // user's bean wins on precedence, not because the default was gated out.
    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    // See WithDataSourceFixture: the command bus the startup validators resolve needs an event
    // store that does not query the mock DataSource.
    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }

    @Singleton
    SagaDeadLetterStore userSagaDeadLetterStore() {
      return USER_STORE;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "with-dlq")
  static class WithDeadLetterQueueFixture {

    @Singleton
    org.streamrune.core.DeadLetterQueue deadLetterQueue() {
      return new org.streamrune.test.InMemoryDeadLetterQueue();
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "user-overrides")
  static class UserOverrides {

    static final EventTypeRegistry REGISTRY = SimpleEventTypeRegistry.builder().build();

    @Singleton
    EventTypeRegistry customRegistry() {
      return REGISTRY;
    }

    @Singleton
    EventStore customEventStore() {
      return new InMemoryEventStore();
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "audit-override")
  static class AuditOverrideFixture {

    static final AuditCommandInterceptor USER = new AuditCommandInterceptor(mock(AuditStore.class));

    // The framework auditCommandInterceptor activates on @Requires(beans = AuditStore.class); this
    // AuditStore makes that gate open so the test proves the missingBeans guard (not an absent
    // AuditStore) is what suppresses the framework default.
    @Singleton
    AuditStore auditStore() {
      return mock(AuditStore.class);
    }

    @Singleton
    AuditCommandInterceptor userAudit() {
      return USER;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "cb-override")
  static class CircuitBreakerOverrideFixture {

    static final CircuitBreakerCommandInterceptor USER =
        new CircuitBreakerCommandInterceptor(50, Duration.ofSeconds(30));

    @Singleton
    CircuitBreakerCommandInterceptor userCircuitBreaker() {
      return USER;
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "user-runner")
  static class UserRunnerFixture {
    // A user-supplied MultiProjectionRunner bean (unqualified) — the framework must never start it.
    static final MultiProjectionRunner RUNNER = mock(MultiProjectionRunner.class);

    @Singleton
    MultiProjectionRunner userRunner() {
      return RUNNER;
    }
  }

  /**
   * Projection-runner coexistence fixtures. Unlike {@link UserRunnerFixture} these declare an
   * ENABLED {@code @ProjectionConfig} projection of the matching mode, so the framework's {@code
   * HasContinuousProjections} / {@code HasScheduledProjections} condition opens and its runner
   * WOULD be assembled — proving the {@code missingBeans} guard, not an absent projection, is what
   * suppresses it. The event/offset stores are required by the factories' {@code @Requires(beans =
   * ...)}.
   */
  @Factory
  @Requires(property = "spec.name", value = "user-continuous-runner")
  static class UserContinuousRunnerFixture {

    static final MultiProjectionRunner RUNNER = mock(MultiProjectionRunner.class);

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }

    @Singleton
    MultiProjectionRunner userMultiProjectionRunner() {
      return RUNNER;
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "user-continuous-runner")
  @ProjectionConfig(
      name = "coexist-continuous",
      mode = ProjectionConfig.Mode.CONTINUOUS,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class CoexistContinuousProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @Factory
  @Requires(property = "spec.name", value = "user-scheduled-runner")
  static class UserScheduledRunnerFixture {

    static final org.streamrune.runtime.ScheduledProjectionRunner RUNNER =
        mock(org.streamrune.runtime.ScheduledProjectionRunner.class);

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }

    @Singleton
    org.streamrune.runtime.ScheduledProjectionRunner userScheduledProjectionRunner() {
      return RUNNER;
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "user-scheduled-runner")
  @ProjectionConfig(
      name = "coexist-scheduled",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class CoexistScheduledProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Same shape as the fixtures above, minus the user runner — the no-override regression pin. */
  @Factory
  @Requires(property = "spec.name", value = "framework-continuous-runner")
  static class FrameworkContinuousRunnerFixture {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "framework-continuous-runner")
  @ProjectionConfig(
      name = "default-continuous",
      mode = ProjectionConfig.Mode.CONTINUOUS,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class DefaultContinuousProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @Factory
  @Requires(property = "spec.name", value = "framework-scheduled-runner")
  static class FrameworkScheduledRunnerFixture {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "framework-scheduled-runner")
  @ProjectionConfig(
      name = "default-scheduled",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class DefaultScheduledProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /**
   * Fixtures for the auto-discovery kill switch itself (distinct from {@link
   * FrameworkContinuousRunnerFixture} above only in spec.name, so this group is unaffected by the
   * property this test varies). One always-enabled CONTINUOUS {@code @ProjectionConfig} projection
   * so {@code HasContinuousProjections} would open whenever {@code ProjectionFactory} itself is
   * active — isolating what this test actually varies: whether the class-level {@code @Requires}
   * lets the factory be considered AT ALL.
   */
  @Factory
  @Requires(property = "spec.name", value = "auto-discovery-kill-switch")
  static class AutoDiscoveryKillSwitchFixture {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "auto-discovery-kill-switch")
  @ProjectionConfig(
      name = "kill-switch-continuous",
      mode = ProjectionConfig.Mode.CONTINUOUS,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class KillSwitchContinuousProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /**
   * The auto-discovery KILL SWITCH ({@code @Requires} on {@link ProjectionFactory} itself) must
   * agree with Spring/Quarkus on every canonical-true spelling {@link
   * org.streamrune.integration.RelaxedBoolean} accepts. Before the fix, {@code @Requires(notEquals
   * = "false")} already kept these enabled (Micronaut was the CORRECT sibling for this half), but
   * nothing pinned it — this closes that gap and gives the fix a parametrised parity companion to
   * the Spring/Quarkus tests for the same property.
   */
  @ParameterizedTest
  @ValueSource(strings = {"true", "on", "yes", "y", "t", "1", "TRUE", "On"})
  void autoDiscoveryNonCanonicalTruthyValuesStayEnabled(String value) {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name",
                "auto-discovery-kill-switch",
                "streamrune.projections.auto-discovery.enabled",
                value))) {
      assertTrue(
          ctx.findBean(MultiProjectionRunner.class).isPresent(),
          () -> "auto-discovery.enabled=" + value + " must leave the factory enabled");
    }
  }

  /** Every canonical-false spelling must disable the whole factory. */
  @ParameterizedTest
  @ValueSource(strings = {"false", "off", "no", "n", "f", "0", "FALSE", "Off"})
  void autoDiscoveryNonCanonicalFalsyValuesStayDisabled(String value) {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name",
                "auto-discovery-kill-switch",
                "streamrune.projections.auto-discovery.enabled",
                value))) {
      assertTrue(
          ctx.findBean(MultiProjectionRunner.class).isEmpty(),
          () -> "auto-discovery.enabled=" + value + " must disable the factory");
    }
  }

  /** Absent (the default) must still mean enabled. */
  @Test
  void autoDiscoveryAbsentPropertyStaysEnabled() {
    try (var ctx = ApplicationContext.run(Map.of("spec.name", "auto-discovery-kill-switch"))) {
      assertTrue(ctx.findBean(MultiProjectionRunner.class).isPresent());
    }
  }

  /**
   * Reflective pin on the exact {@code getOrder()} values — the reliable half of this fix's
   * coverage. {@link #aFailingValidatorPreventsAnyRunnerFromStarting()} below proves the
   * CONSEQUENCE end to end through a real context boot, but Micronaut's default (unordered)
   * notification sequence for these specific classes already happens to put the validator first in
   * this build (verified: the container test passes even against the pre-fix sources, which carried
   * no {@code Ordered} at all) — a coincidence of bean-registration order, not a guarantee, which
   * is exactly the defect. This reflective check is what actually fails without the fix.
   */
  @Test
  void everyValidatorOutranksTheLifecycleStarter() {
    int configValidator = new StreamRuneConfigValidator(null, null).getOrder();
    int authValidator = new StreamRuneAuthorizationValidator(null).getOrder();
    int cryptoValidator = new SagaStateCryptoValidator(List.of(), List.of()).getOrder();
    // The request-identity startup refusal is a validator too.
    int identityValidator = new StreamRuneRequestIdentityValidator(null, null, null).getOrder();
    int lifecycleStart =
        new StreamRuneLifecycle(null, null, null, null, null, null, null, null, null).getOrder();
    // SagaCompensationRetryLifecycle is a SECOND runner-starting startup listener — left out
    // of the startup-validator ordering fix. Same tier as StreamRuneLifecycle (1100).
    int sagaSweeperLifecycleStart =
        new org.streamrune.micronaut.SagaCompensationRetryLifecycle(
                null, null, null, null, null, null, null, null, null, false)
            .getOrder();

    assertTrue(
        configValidator < lifecycleStart,
        "StreamRuneConfigValidator must run before StreamRuneLifecycle");
    assertTrue(
        authValidator < lifecycleStart,
        "StreamRuneAuthorizationValidator must run before StreamRuneLifecycle");
    assertTrue(
        cryptoValidator < lifecycleStart,
        "SagaStateCryptoValidator must run before StreamRuneLifecycle");
    assertTrue(
        configValidator < sagaSweeperLifecycleStart,
        "StreamRuneConfigValidator must run before SagaCompensationRetryLifecycle");
    assertTrue(
        authValidator < sagaSweeperLifecycleStart,
        "StreamRuneAuthorizationValidator must run before SagaCompensationRetryLifecycle");
    assertTrue(
        cryptoValidator < sagaSweeperLifecycleStart,
        "SagaStateCryptoValidator must run before SagaCompensationRetryLifecycle");
    assertTrue(
        identityValidator < lifecycleStart,
        "StreamRuneRequestIdentityValidator must run before StreamRuneLifecycle");
    assertTrue(
        identityValidator < sagaSweeperLifecycleStart,
        "StreamRuneRequestIdentityValidator must run before SagaCompensationRetryLifecycle");
  }

  record OrderingEncryptedSagaState(
      String customerId,
      @org.streamrune.core.crypto.Encrypted(subjectId = "customerId") String customerEmail)
      implements org.streamrune.core.saga.SagaState {
    @Override
    public org.streamrune.core.saga.SagaStatus status() {
      return org.streamrune.core.saga.SagaStatus.RUNNING;
    }
  }

  /**
   * Fixture: a {@link org.streamrune.runtime.SagaRunner} whose saga-state type carries an
   * {@code @Encrypted} field, with NO {@code CryptoEngine} bean anywhere in the container (not
   * auto-configured by default, unlike {@code AnnotationAuthorizationInterceptor} — verified: a
   * plain {@code ApplicationContext.run()} resolves zero {@code CryptoEngine} beans) — {@code
   * SagaStateCryptoValidator} (always active, not spec.name-gated) fails fast on exactly this shape
   * . Plus a mock {@link MultiProjectionRunner} under the name {@link StreamRuneLifecycle} injects,
   * replacing {@link ProjectionFactory}'s own default (its producer backs off via {@code
   * missingBeans}), so the test can observe whether it was ever started.
   */
  @Factory
  @Requires(property = "spec.name", value = "startup-ordering")
  static class StartupOrderingFixture {
    static final MultiProjectionRunner RUNNER = mock(MultiProjectionRunner.class);

    // SagaCompensationRetryLifecycle is a second runner-starting startup listener. A mocked
    // SagaStore (so startSweepers() does not bail out on a null store) plus a mocked
    // BackgroundRelayHealthContributor let the test observe whether a sweeper was ever built and
    // started — registerSagaCompensationRetrySweeper(...) is called unconditionally right after
    // sweeper.start() for every runner startSweepers() processes, so "never invoked" is equivalent
    // to "startSweepers() never ran past its guards", the same proof shape as RUNNER.start() above.
    static final org.streamrune.runtime.BackgroundRelayHealthContributor RELAY_HEALTH =
        mock(org.streamrune.runtime.BackgroundRelayHealthContributor.class);

    @Singleton
    @Named(ProjectionFactory.FRAMEWORK_MULTI_RUNNER)
    MultiProjectionRunner runner() {
      return RUNNER;
    }

    @Singleton
    org.streamrune.core.saga.SagaStore sagaStore() {
      return mock(org.streamrune.core.saga.SagaStore.class);
    }

    @Singleton
    org.streamrune.runtime.BackgroundRelayHealthContributor relayHealth() {
      return RELAY_HEALTH;
    }

    @Singleton
    org.streamrune.runtime.SagaRunner<OrderingEncryptedSagaState> encryptedSagaRunner() {
      org.streamrune.core.CommandBus commandBus = mock(org.streamrune.core.CommandBus.class);
      when(commandBus.supportsIdempotentExecution()).thenReturn(true);
      org.streamrune.core.saga.SagaDecider<OrderingEncryptedSagaState> decider =
          new org.streamrune.core.saga.SagaDecider<>() {
            @Override
            public Class<OrderingEncryptedSagaState> stateType() {
              return OrderingEncryptedSagaState.class;
            }

            @Override
            public OrderingEncryptedSagaState initialState(org.streamrune.core.saga.SagaId sagaId) {
              return new OrderingEncryptedSagaState("cust-1", "a@example.com");
            }

            @Override
            public OrderingEncryptedSagaState evolve(
                OrderingEncryptedSagaState state, EventEnvelope event) {
              return state;
            }

            @Override
            public List<org.streamrune.core.saga.SagaCommand> handle(
                OrderingEncryptedSagaState state, EventEnvelope event) {
              return List.of();
            }
          };
      return org.streamrune.runtime.SagaRunner.<OrderingEncryptedSagaState>builder()
          .decider(decider)
          .startWhen(event -> true)
          .extractSagaId(event -> org.streamrune.core.saga.SagaId.of("saga-1"))
          .correlateBy(event -> java.util.Optional.empty())
          .sagaStore(mock(org.streamrune.core.saga.SagaStore.class))
          .commandBus(commandBus)
          .sagaDeadLetterStore(mock(SagaDeadLetterStore.class))
          .build();
    }
  }

  /**
   * {@link StreamRuneLifecycle} and the three startup validators ({@code
   * StreamRuneConfigValidator}, {@code StreamRuneAuthorizationValidator}, {@code
   * SagaStateCryptoValidator}) carried no {@link io.micronaut.core.order.Ordered}, so their
   * relative firing order was unspecified — the lifecycle's runner start (whose first poll is
   * immediate) could run before a fail-closed validator ever did. An {@code @Encrypted} saga-state
   * field with no CryptoEngine must fail context startup, and — because the validator now outranks
   * the lifecycle starter — the runner it would otherwise have started must never see start().
   */
  @Test
  void aFailingValidatorPreventsAnyRunnerFromStarting() {
    assertThrows(
        RuntimeException.class,
        () -> ApplicationContext.run(Map.of("spec.name", "startup-ordering")).close(),
        "an @Encrypted saga-state field with no CryptoEngine must fail context startup");

    verify(StartupOrderingFixture.RUNNER, never()).start();
    // Same proof for the second runner-starting listener.
    verify(StartupOrderingFixture.RELAY_HEALTH, never())
        .registerSagaCompensationRetrySweeper(any());
  }

  @Factory
  @Requires(property = "spec.name", value = "user-outbox-poller")
  static class UserOutboxPollerFixture {
    // A user-supplied OutboxPoller bean (unqualified) — the framework must never start it.
    static final org.streamrune.runtime.OutboxPoller POLLER =
        mock(org.streamrune.runtime.OutboxPoller.class);

    @Singleton
    org.streamrune.runtime.OutboxPoller userOutboxPoller() {
      return POLLER;
    }
  }

  /**
   * An enabled INLINE projection (so the framework's {@code HasInlineProjections} condition opens —
   * proving the {@code missingBeans} guard, not an absent projection, is what suppresses the
   * framework default) plus a user-defined {@link InlineProjectionInterceptor}.
   */
  @Factory
  @Requires(property = "spec.name", value = "inline-override")
  static class InlineProjectionOverrideFixture {

    static final InlineProjectionInterceptor USER =
        InlineProjectionInterceptor.builder().register("user-inline", batch -> {}).build();

    @Singleton
    InlineProjectionInterceptor userInlineProjectionInterceptor() {
      return USER;
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "inline-override")
  @ProjectionConfig(
      name = "override-inline",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class OverrideInlineProjection implements Projection {

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /**
   * A DataSource (so the module's lease leadership AND its JDBC projection defaults are both
   * eligible), an application-supplied EventStore/OffsetStore so the runner assembles without a
   * live database, and one CONTINUOUS projection so {@code ProjectionFactory.multiProjectionRunner}
   * actually produces a runner.
   */
  @Factory
  @Requires(property = "spec.name", value = "fenced-projection")
  static class FencedProjectionFixtures {

    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "fenced-projection")
  @ProjectionConfig(
      name = "fenced_test_projection",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class FencedTestProjection implements Projection {

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /**
   * A forwarding JDK proxy over one {@link InMemoryProjectionRepository}, produced as the only
   * {@link AtomicBatchProcessor} bean and, separately, as a {@link ProjectionRepository} bean, plus
   * one {@code TRANSACTIONAL_LOCAL} projection over that repository bean: the write-target identity
   * check must see through the proxy on both sides.
   */
  @Factory
  @Requires(property = "spec.name", value = "proxied-processor")
  static class ProxiedProcessorFixtures {
    // A field, not a bean: a bean of this class would be exposed under every type it implements,
    // AtomicBatchProcessor included — a second candidate for the factory's
    // List<AtomicBatchProcessor>.
    private final InMemoryProjectionRepository real = new InMemoryProjectionRepository();
    private final Object proxy =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {ProjectionRepository.class, AtomicBatchProcessor.class},
            (p, m, a) -> m.invoke(real, a));

    // No DataSource bean on purpose: the module then produces neither a LeaseBasedLeadership (NOOP)
    // nor its default JdbcProjectionRepository, so the proxy below is the ONLY processor candidate.
    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new org.streamrune.test.InMemoryOffsetStore(); // the runner is never started here
    }

    @Singleton
    AtomicBatchProcessor proxiedProcessor() {
      return (AtomicBatchProcessor) proxy;
    }

    // Declared as ProjectionRepository: Micronaut exposes a @Factory bean only under its declared
    // return type, so this is NOT a second AtomicBatchProcessor bean.
    @Singleton
    ProjectionRepository proxiedRepository() {
      return (ProjectionRepository) proxy;
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "proxied-processor")
  @ProjectionConfig(
      name = "proxied_orders",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class ProxiedOrders extends BaseProjection {
    ProxiedOrders(ProjectionRepository proxiedRepository) {
      super(proxiedRepository, "proxied_orders");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      save("row", "v");
    }
  }

  @Factory
  @Requires(property = "spec.name", value = "lifecycle")
  static class LifecycleFixtures {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }
  }

  @Singleton
  @Requires(property = "spec.name", value = "lifecycle")
  @ProjectionConfig(
      name = "wiring-test-projection",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class WiringTestProjection implements Projection {

    final List<EventEnvelope> seen = new CopyOnWriteArrayList<>();

    @Override
    public void process(List<EventEnvelope> batch) {
      seen.addAll(batch);
    }
  }

  static class InMemoryOffsetStore implements OffsetStore {

    private final Map<String, GlobalOffset> offsets = new ConcurrentHashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      offsets.put(projectionName.value(), offset);
    }
  }
}
