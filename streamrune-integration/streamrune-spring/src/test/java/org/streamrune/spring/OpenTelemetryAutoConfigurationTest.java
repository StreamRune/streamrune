package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.postgres.PgAdvisoryLocker;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.OpenTelemetryTracing;
import org.streamrune.runtime.TracingCommandInterceptor;
import org.streamrune.runtime.TracingSettings;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStoreFactory;

/**
 * The {@link StreamRuneTracing} bean of the Spring integration and what it reaches. The rules the
 * three integrations keep alike (the switches, the global OpenTelemetry, the application's beans)
 * are in {@code SpringIntegrationWiringContractTest}; these are the Spring-specific ones.
 */
class OpenTelemetryAutoConfigurationTest {

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubDataSourceConfig.class, InMemoryEventStoreConfig.class);

  /**
   * The extension's SDK as a bean the context cannot close: Spring closes an {@code AutoCloseable}
   * bean with its context, and the SDK serves every test of the class.
   */
  private static OpenTelemetry openTelemetry() {
    OpenTelemetry sdk = otelTesting.getOpenTelemetry();
    return new OpenTelemetry() {
      @Override
      public io.opentelemetry.api.trace.TracerProvider getTracerProvider() {
        return sdk.getTracerProvider();
      }

      @Override
      public io.opentelemetry.context.propagation.ContextPropagators getPropagators() {
        return sdk.getPropagators();
      }
    };
  }

  @Test
  void anOpenTelemetryBeanGivesATracingThatTracesEveryArea() {
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        .run(
            ctx -> {
              StreamRuneTracing tracing = ctx.getBean(StreamRuneTracing.class);
              assertThat(tracing).isNotSameAs(StreamRuneTracing.NOOP);
              for (TracingArea area : TracingArea.values()) {
                assertThat(tracing.enabled(area)).as(area.name()).isTrue();
              }
            });
  }

  @Test
  void eachAreaKeyBindsToItsArea() {
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        .withPropertyValues(
            "streamrune.tracing.subscription.enabled=false",
            "streamrune.tracing.event-store.enabled=false",
            "streamrune.tracing.saga.enabled=false")
        .run(
            ctx -> {
              StreamRuneTracing tracing = ctx.getBean(StreamRuneTracing.class);
              assertThat(tracing.enabled(TracingArea.SUBSCRIPTION)).isFalse();
              assertThat(tracing.enabled(TracingArea.EVENT_STORE)).isFalse();
              assertThat(tracing.enabled(TracingArea.SAGA)).isFalse();
              assertThat(tracing.enabled(TracingArea.COMMAND)).isTrue();
              assertThat(tracing.enabled(TracingArea.PROJECTION)).isTrue();
              assertThat(tracing.enabled(TracingArea.LOCK)).isTrue();
              assertThat(tracing.enabled(TracingArea.OUTBOX)).isTrue();
            });
  }

  @Test
  void theMasterSwitchOffGivesTheNoopTracing() {
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        .withPropertyValues("streamrune.tracing.enabled=false")
        .run(
            ctx ->
                assertThat(ctx.getBean(StreamRuneTracing.class)).isSameAs(StreamRuneTracing.NOOP));
  }

  @Test
  void anOpenTelemetrySwitchedOffByItsOwnSettingGivesTheNoopTracing() {
    for (String off :
        java.util.List.of("management.opentelemetry.enabled=false", "otel.sdk.disabled=true")) {
      runner
          .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
          .withPropertyValues(off)
          .run(
              ctx ->
                  assertThat(ctx.getBean(StreamRuneTracing.class))
                      .as(off)
                      .isSameAs(StreamRuneTracing.NOOP));
    }
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        // Without an exporter the SDK still runs, and a span processor still receives the spans.
        .withPropertyValues("management.tracing.export.enabled=false", "otel.traces.exporter=none")
        .run(
            ctx ->
                assertThat(ctx.getBean(StreamRuneTracing.class))
                    .isNotSameAs(StreamRuneTracing.NOOP));
  }

  /**
   * Spring Boot's own OpenTelemetry SDK auto-configuration, not a stub bean: with {@code
   * management.opentelemetry.enabled=false}, or any other value Spring Boot reads as not {@code
   * true}, it builds an SDK with no span processor, and StreamRune's tracing is {@code NOOP}; by
   * default, and with Spring Boot 3's export switch {@code management.tracing.enabled=false}, which
   * Spring Boot 4 does not read, StreamRune traces through it.
   */
  @Test
  void springBootsOwnOpenTelemetrySwitch_turnsStreamRunesTracingOff() {
    ApplicationContextRunner withBootsSdk =
        runner.withConfiguration(
            AutoConfigurations.of(
                org.springframework.boot.opentelemetry.autoconfigure
                    .OpenTelemetrySdkAutoConfiguration.class));
    for (String off :
        java.util.List.of(
            "management.opentelemetry.enabled=false", "management.opentelemetry.enabled=off")) {
      withBootsSdk
          .withPropertyValues(off)
          .run(
              ctx -> {
                assertThat(ctx).hasSingleBean(OpenTelemetry.class);
                assertThat(ctx.getBean(StreamRuneTracing.class))
                    .as(off)
                    .isSameAs(StreamRuneTracing.NOOP);
              });
    }
    for (String[] on :
        java.util.List.of(
            new String[0],
            new String[] {"management.opentelemetry.enabled=true"},
            new String[] {"management.tracing.enabled=false"})) {
      withBootsSdk
          .withPropertyValues(on)
          .run(
              ctx -> {
                assertThat(ctx).hasSingleBean(OpenTelemetry.class);
                assertThat(ctx.getBean(StreamRuneTracing.class))
                    .as(java.util.Arrays.toString(on))
                    .isNotSameAs(StreamRuneTracing.NOOP);
              });
    }
  }

  @Test
  void anOpenTelemetryWithoutATracerProviderGivesTheNoopTracing() {
    runner
        .withBean(OpenTelemetry.class, OpenTelemetry::noop)
        .run(
            ctx ->
                assertThat(ctx.getBean(StreamRuneTracing.class)).isSameAs(StreamRuneTracing.NOOP));
  }

  @Test
  void theApplicationsTracingBeanWins() {
    StreamRuneTracing own = OpenTelemetryTracing.of(openTelemetry(), TracingSettings.disabled());
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        .withBean(StreamRuneTracing.class, () -> own)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(StreamRuneTracing.class);
              assertThat(ctx.getBean(StreamRuneTracing.class)).isSameAs(own);
            });
  }

  @Test
  void theCommandBusRunsTheApplicationsInterceptorAlone() {
    var own =
        new TracingCommandInterceptor(
            OpenTelemetryTracing.of(openTelemetry(), TracingSettings.allEnabled()));
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        .withBean(TracingCommandInterceptor.class, () -> own)
        .run(
            ctx -> {
              List<CommandInterceptor> chain =
                  ctx.getBean(VirtualThreadCommandBus.class).interceptors();
              assertThat(chain.stream().filter(TracingCommandInterceptor.class::isInstance))
                  .containsExactly(own);
            });
  }

  @Test
  void anApplicationBusWithAnExplicitInterceptorListIsTracedThroughTheTracingBean() {
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        .withUserConfiguration(ApplicationBusConfig.class)
        .run(
            ctx -> {
              VirtualThreadCommandBus bus = ctx.getBean(VirtualThreadCommandBus.class);
              assertThat(bus.interceptors().getFirst())
                  .isInstanceOf(TracingCommandInterceptor.class);
              bus.execute(new Probe("bus-1"));
              assertThat(otelTesting.getSpans())
                  .extracting(SpanData::getName)
                  .anyMatch(name -> name.startsWith("command/"));
            });
  }

  @Test
  void anApplicationAdvisoryLockerTracesItsAcquisitions() throws SQLException {
    DataSource unreachable = mock(DataSource.class);
    when(unreachable.getConnection()).thenThrow(new SQLException("no database"));
    runner
        .withBean(OpenTelemetry.class, OpenTelemetryAutoConfigurationTest::openTelemetry)
        .withBean(PgAdvisoryLocker.class, () -> new PgAdvisoryLocker(unreachable))
        .withUserConfiguration(ProbeRegistrationConfig.class)
        .run(
            ctx -> {
              VirtualThreadCommandBus bus = ctx.getBean(VirtualThreadCommandBus.class);
              assertThatThrownBy(() -> bus.execute(new Probe("locked-1")))
                  .isInstanceOf(RuntimeException.class);
              assertThat(otelTesting.getSpans())
                  .extracting(SpanData::getName)
                  .contains(TraceAttributes.LOCK_ACQUIRE);
            });
  }

  @Test
  void withoutOpenTelemetryOnTheClasspathTheTracingIsNoop() {
    // opentelemetry-api is compileOnly (classpath-optional). The OTel @Bean lives in a nested
    // @Configuration gated by @ConditionalOnClass, so Spring never introspects its OTel-typed
    // method signature when the OTel API is absent; the fallback bean is NOOP.
    runner
        .withClassLoader(new FilteredClassLoader(OpenTelemetry.class))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(StreamRuneTracing.class)).isSameAs(StreamRuneTracing.NOOP);
            });
  }

  record Probe(String id) implements Command {}

  record ProbeState() implements AggregateState {}

  record Probed(String id) implements DomainEvent {}

  static final class ProbeDecider implements Decider<Probe, ProbeState, Probed> {
    @Override
    public ProbeState initialState() {
      return new ProbeState();
    }

    @Override
    public List<Probed> decide(Probe command, ProbeState state) {
      return List.of(new Probed(command.id()));
    }

    @Override
    public ProbeState evolve(ProbeState state, Probed event) {
      return state;
    }
  }

  @Configuration
  static class ProbeRegistrationConfig {
    @Bean
    DeciderRegistration<Probe, ProbeState, Probed> probeRegistration() {
      return new DeciderRegistration<>(
          org.streamrune.core.types.AggregateType.of("probe"),
          Probe.class,
          probe -> org.streamrune.core.types.AggregateId.of(probe.id()),
          new ProbeDecider());
    }
  }

  /** An application's own command bus, with an explicit interceptor list and the tracing bean. */
  @Configuration
  static class ApplicationBusConfig {
    @Bean
    VirtualThreadCommandBus applicationCommandBus(
        EventStore eventStore, StreamRuneTracing tracing) {
      return VirtualThreadCommandBus.builder()
          .eventStore(eventStore)
          .interceptors(new CommandInterceptor() {})
          .tracing(tracing)
          .register(
              org.streamrune.core.types.AggregateType.of("probe"),
              Probe.class,
              probe -> org.streamrune.core.types.AggregateId.of(probe.id()),
              new ProbeDecider())
          .build();
    }
  }

  @Configuration
  static class StubDataSourceConfig {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }
  }

  @Configuration
  static class InMemoryEventStoreConfig {
    @Bean
    EventStoreFactory eventStoreFactory(EventTypeRegistry typeRegistry) {
      return InMemoryEventStoreFactory.builder(typeRegistry).build();
    }
  }
}
