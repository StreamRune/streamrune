package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.integration.MicrometerStreamRuneMetrics;
import org.streamrune.runtime.SimpleQueryBus;

/**
 * The framework {@link StreamRuneMetrics} bean must be ONE instance.
 *
 * <p>{@link StreamRuneProducers#micrometerStreamRuneMetrics} had no scope annotation, so it was a
 * CDI {@code @Dependent} producer: every one of the {@code Instance<StreamRuneMetrics>} injection
 * points in the framework producers (the command bus, the query buses, the DLQ retry runner, the
 * outbox poller, the projection producers, the health contributor, the retention sweepers, the saga
 * lifecycle, …) received its <em>own</em> {@link MicrometerStreamRuneMetrics}. Each instance
 * registers the same gauge ids against the shared {@code MeterRegistry}, and Micrometer's {@code
 * register()} dedups by id, so only the FIRST instance's {@code AtomicLong}s back the gauges —
 * every later consumer wrote an orphaned field and its backlog / degradation samples never reached
 * a scrape. Spring ({@code @Bean}) and Micronaut ({@code @Singleton}) were already single-instance;
 * the same file's earlier fix did the same for {@code SubscriptionHealthContributor}.
 *
 * <p>Boots a REAL Arc container (only real CDI scoping can show the defect — a Mockito {@code
 * Instance} hands back whatever it is told) with the two beans a Quarkus application supplies that
 * raw Arc cannot (Agroal {@code DataSource}, the SmallRye-bound {@code @ConfigMapping}) plus the
 * {@code MeterRegistry} the {@code quarkus-micrometer} extension would contribute.
 */
class QuarkusMetricsSingletonWiringTest {

  /** Application infrastructure with a Micrometer registry present (metrics on, the default). */
  @ApplicationScoped
  public static class MetricsInfrastructure {

    static final SimpleMeterRegistry REGISTRY = new SimpleMeterRegistry();

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of());
    }

    @Produces
    @Singleton
    public MeterRegistry meterRegistry() {
      return REGISTRY;
    }
  }

  /** A registry is present but the operator switched StreamRune metrics off. */
  @ApplicationScoped
  public static class MetricsDisabledInfrastructure {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of("streamrune.metrics.enabled", "false"));
    }

    @Produces
    @Singleton
    public MeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
    }
  }

  /** No Micrometer registry at all (an application without the quarkus-micrometer extension). */
  @ApplicationScoped
  public static class NoRegistryInfrastructure {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of());
    }
  }

  /**
   * Application infrastructure that ALSO supplies its own {@code @Singleton StreamRuneMetrics} bean
   * — the shape an application takes to plug in a custom metrics backend instead of the framework's
   * Micrometer adapter. {@code micrometerStreamRuneMetrics} stays {@code @DefaultBean} specifically
   * so this bean wins every injection point; that claim was previously asserted only by reflection
   * (StreamRuneProducersTest#micrometerStreamRuneMetricsIsSingletonScoped checks the annotations
   * are present) and never proven against a real Arc container.
   */
  @ApplicationScoped
  public static class AppSuppliedMetricsInfrastructure {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of());
    }

    @Produces
    @Singleton
    public MeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
    }

    @Produces
    @Singleton
    public StreamRuneMetrics appMetrics() {
      return new AppSuppliedMetrics();
    }
  }

  /**
   * A distinguishable application-supplied {@link StreamRuneMetrics}, so an {@code
   * assertInstanceOf} / {@code assertSame} proves Arc resolved THIS bean rather than the
   * framework's {@code @DefaultBean} {@link MicrometerStreamRuneMetrics}.
   */
  private static final class AppSuppliedMetrics implements StreamRuneMetrics {}

  @Test
  void everyInjectionPointSharesTheOneMicrometerMetricsInstance() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, MetricsInfrastructure.class))) {
      StreamRuneMetrics first = arc.container().instance(StreamRuneMetrics.class).get();
      StreamRuneMetrics second = arc.container().instance(StreamRuneMetrics.class).get();
      assertInstanceOf(MicrometerStreamRuneMetrics.class, first);
      assertSame(
          first,
          second,
          "two resolutions of the framework StreamRuneMetrics bean must be the SAME instance — a"
              + " @Dependent producer hands every injection point its own MicrometerStreamRuneMetrics");

      // A consumer assembled by the REAL framework producers receives that same instance.
      SimpleQueryBus queryBus = arc.container().instance(SimpleQueryBus.class).get();
      assertSame(
          first,
          metricsOf(queryBus),
          "the query bus produced by the framework must carry the shared metrics instance");

      // The failure chain, end to end: Micrometer's register() dedups by id,
      // so the gauges are backed by the AtomicLongs of whichever instance registered first. A
      // sample recorded through ANY resolution of the bean must therefore reach the scrape.
      second.recordDlqBacklog(7);
      assertEquals(
          7.0,
          MetricsInfrastructure.REGISTRY.find(MetricNames.DLQ_PENDING).gauge().value(),
          "a backlog sample recorded through the second resolution must back the registered gauge"
              + " — with per-injection-point instances it lands in an orphaned AtomicLong and the"
              + " gauge reads 0 forever");
    }
  }

  @Test
  void metricsDisabled_yieldsTheNoopSingleton_neverNull() throws Exception {
    // CDI forbids a null @Singleton product, so the switched-off path must yield the inert
    // StreamRuneMetrics.NOOP (the subscriptionLeadership pattern), which every consumer
    // already treats exactly like an absent bean.
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, MetricsDisabledInfrastructure.class))) {
      var instance = arc.container().instance(StreamRuneMetrics.class);
      assertTrue(instance.isAvailable(), "the @DefaultBean still resolves with metrics off");
      assertSame(StreamRuneMetrics.NOOP, instance.get());
      assertSame(
          StreamRuneMetrics.NOOP,
          metricsOf(arc.container().instance(SimpleQueryBus.class).get()),
          "consumers receive the inert NOOP, exactly as they did for an absent bean");
    }
  }

  @Test
  void noMeterRegistry_yieldsTheNoopSingleton_neverNull() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, NoRegistryInfrastructure.class))) {
      var instance = arc.container().instance(StreamRuneMetrics.class);
      assertTrue(instance.isAvailable(), "the @DefaultBean still resolves without a registry");
      assertSame(StreamRuneMetrics.NOOP, instance.get());
    }
  }

  @Test
  void applicationSuppliedStreamRuneMetrics_overridesTheDefaultBean() throws Exception {
    // micrometerStreamRuneMetrics is @Singleton @DefaultBean specifically so an
    // application-supplied StreamRuneMetrics still wins every injection point — a claim the
    // sibling StreamRuneProducersTest.micrometerStreamRuneMetricsIsSingletonScoped only verifies
    // by reflection (the annotations are present). This boots a REAL Arc container with an
    // application @Singleton StreamRuneMetrics bean alongside the framework producers and proves
    // the override actually happens at CDI resolution time, not just structurally.
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, AppSuppliedMetricsInfrastructure.class))) {
      StreamRuneMetrics resolved = arc.container().instance(StreamRuneMetrics.class).get();
      assertInstanceOf(
          AppSuppliedMetrics.class,
          resolved,
          "an application @Singleton StreamRuneMetrics must win over the framework's @DefaultBean"
              + " MicrometerStreamRuneMetrics producer");

      SimpleQueryBus queryBus = arc.container().instance(SimpleQueryBus.class).get();
      assertSame(
          resolved,
          metricsOf(queryBus),
          "the query bus produced by the framework must carry the application-supplied metrics"
              + " instance, not the framework's own default");
    }
  }

  private static StreamRuneMetrics metricsOf(SimpleQueryBus queryBus) throws Exception {
    Field field = SimpleQueryBus.class.getDeclaredField("metrics");
    field.setAccessible(true);
    return (StreamRuneMetrics) field.get(queryBus);
  }

  // ── @ConfigMapping binding (the real StreamRuneQuarkusProperties interface) ──────────────────

  private static StreamRuneQuarkusProperties bind(Map<String, String> overrides) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            // Register the same Quarkus Duration converter the runtime uses, so short forms like
            // "11s" bind exactly as they do in a Quarkus app (raw SmallRye only accepts ISO-8601).
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withDefaultValues(overrides)
            .build();
    return config.getConfigMapping(StreamRuneQuarkusProperties.class);
  }
}
