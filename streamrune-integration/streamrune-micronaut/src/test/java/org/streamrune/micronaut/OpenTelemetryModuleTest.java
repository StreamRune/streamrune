package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.context.BeanContext;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
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
            .streamRuneTracing(contextWith(SDK), StreamRuneMicronautProperties.withDefaults());

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
                contextWith(OpenTelemetry.noop()), StreamRuneMicronautProperties.withDefaults());

    assertSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void withoutABeanTheGlobalOpenTelemetryIsTaken() {
    GlobalOpenTelemetry.set(SDK);

    StreamRuneTracing tracing =
        new StreamRuneMicronautModule()
            .streamRuneTracing(contextWith(null), StreamRuneMicronautProperties.withDefaults());

    assertNotSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void withoutABeanAndWithoutAGlobalOpenTelemetryTheTracingIsNoop() {
    StreamRuneTracing tracing =
        new StreamRuneMicronautModule()
            .streamRuneTracing(contextWith(null), StreamRuneMicronautProperties.withDefaults());

    assertSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void openTelemetryIsOnThisClasspath() {
    assertTrue(StreamRuneMicronautModule.openTelemetryOnTheClasspath());
    assertFalse(
        StreamRuneMicronautModule.tracingSettings(StreamRuneMicronautProperties.withDefaults())
            .equals(org.streamrune.runtime.TracingSettings.disabled()));
  }

  private static BeanContext contextWith(OpenTelemetry openTelemetry) {
    BeanContext context = mock(BeanContext.class);
    when(context.findBean(OpenTelemetry.class)).thenReturn(Optional.ofNullable(openTelemetry));
    return context;
  }
}
