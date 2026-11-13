package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.context.BeanContext;
import io.micronaut.context.env.Environment;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TracingArea;

/**
 * The {@link StreamRuneTracing} bean of the Micronaut integration. The rules the three integrations
 * keep alike are in {@code MicronautIntegrationWiringContractTest}; these are the factory's own.
 */
class OpenTelemetryModuleTest {

  private static final OpenTelemetry SDK =
      OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder().build()).build();

  @BeforeEach
  @AfterEach
  void resetTheGlobalOpenTelemetry() {
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  void anOpenTelemetryBeanGivesATracingThatTracesEveryArea() {
    StreamRuneTracing tracing =
        new StreamRuneMicronautModule()
            .streamRuneTracing(
                contextWith(SDK),
                StreamRuneMicronautProperties.withDefaults(),
                environment(Map.of()));

    assertNotSame(StreamRuneTracing.NOOP, tracing);
    for (TracingArea area : TracingArea.values()) {
      assertTrue(tracing.enabled(area), area.name());
    }
  }

  @Test
  void eachAreaComponentReachesItsArea() {
    var defaults = StreamRuneMicronautProperties.withDefaults();
    var settings = StreamRuneMicronautModule.tracingSettings(defaults);
    for (TracingArea area : TracingArea.values()) {
      assertTrue(settings.enabled(area), area.name());
    }
    assertTrue(settings.enabled());
  }

  @Test
  void aNoopOpenTelemetryBeanGivesTheNoopTracing() {
    StreamRuneTracing tracing =
        new StreamRuneMicronautModule()
            .streamRuneTracing(
                contextWith(OpenTelemetry.noop()),
                StreamRuneMicronautProperties.withDefaults(),
                environment(Map.of()));

    assertSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void withoutABeanTheGlobalOpenTelemetryIsTaken() {
    GlobalOpenTelemetry.set(SDK);

    StreamRuneTracing tracing =
        new StreamRuneMicronautModule()
            .streamRuneTracing(
                contextWith(null),
                StreamRuneMicronautProperties.withDefaults(),
                environment(Map.of()));

    assertNotSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void withoutABeanAndWithoutAGlobalOpenTelemetryTheTracingIsNoop() {
    StreamRuneTracing tracing =
        new StreamRuneMicronautModule()
            .streamRuneTracing(
                contextWith(null),
                StreamRuneMicronautProperties.withDefaults(),
                environment(Map.of()));

    assertSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void openTelemetryIsOnThisClasspath() {
    assertTrue(StreamRuneMicronautModule.openTelemetryOnTheClasspath());
    assertFalse(
        StreamRuneMicronautModule.tracingSettings(StreamRuneMicronautProperties.withDefaults())
            .equals(org.streamrune.runtime.TracingSettings.disabled()));
  }

  @Test
  void anOpenTelemetrySwitchedOffByItsOwnSettingGivesTheNoopTracing_andLeavesTheGlobalUnread() {
    for (Map<String, String> off :
        List.of(Map.of("otel.sdk.disabled", "true"), Map.of("otel.sdk.disabled", " True "))) {
      assertSame(
          StreamRuneTracing.NOOP,
          new StreamRuneMicronautModule()
              .streamRuneTracing(
                  contextWith(SDK), StreamRuneMicronautProperties.withDefaults(), environment(off)),
          off::toString);
      assertSame(
          StreamRuneTracing.NOOP,
          new StreamRuneMicronautModule()
              .streamRuneTracing(
                  contextWith(null),
                  StreamRuneMicronautProperties.withDefaults(),
                  environment(off)),
          off::toString);
    }
    assertNotSame(
        StreamRuneTracing.NOOP,
        new StreamRuneMicronautModule()
            .streamRuneTracing(
                contextWith(SDK),
                StreamRuneMicronautProperties.withDefaults(),
                environment(Map.of("otel.sdk.disabled", "false", "otel.traces.exporter", "none"))));

    GlobalOpenTelemetry.set(SDK);
    assertSame(SDK.getTracerProvider(), GlobalOpenTelemetry.get().getTracerProvider());
  }

  private static Environment environment(Map<String, String> values) {
    Environment environment = mock(Environment.class);
    when(environment.getProperty(org.mockito.ArgumentMatchers.anyString(), eq(String.class)))
        .thenAnswer(call -> Optional.ofNullable(values.get(call.<String>getArgument(0))));
    return environment;
  }

  private static BeanContext contextWith(OpenTelemetry openTelemetry) {
    BeanContext context = mock(BeanContext.class);
    when(context.findBean(OpenTelemetry.class)).thenReturn(Optional.ofNullable(openTelemetry));
    return context;
  }
}
