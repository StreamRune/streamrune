package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TracingArea;

/**
 * The {@link StreamRuneTracing} producer of the Quarkus integration. The rules the three
 * integrations keep alike are in {@code QuarkusIntegrationWiringContractTest}; these are the
 * producer's own.
 */
class OpenTelemetryProducersTest {

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
        new StreamRuneProducers()
            .streamRuneTracing(
                satisfied(SDK), TestProperties.defaults(), TestProperties.config(Map.of()));

    assertNotSame(StreamRuneTracing.NOOP, tracing);
    for (TracingArea area : TracingArea.values()) {
      assertTrue(tracing.enabled(area), area.name());
    }
  }

  @Test
  void eachAreaKeyBindsToItsArea() {
    StreamRuneTracing tracing =
        new StreamRuneProducers()
            .streamRuneTracing(
                satisfied(SDK),
                TestProperties.of(
                    Map.of(
                        "streamrune.tracing.subscription.enabled", "false",
                        "streamrune.tracing.event-store.enabled", "false",
                        "streamrune.tracing.saga.enabled", "false")),
                TestProperties.config(Map.of()));

    assertFalse(tracing.enabled(TracingArea.SUBSCRIPTION));
    assertFalse(tracing.enabled(TracingArea.EVENT_STORE));
    assertFalse(tracing.enabled(TracingArea.SAGA));
    assertTrue(tracing.enabled(TracingArea.COMMAND));
    assertTrue(tracing.enabled(TracingArea.PROJECTION));
    assertTrue(tracing.enabled(TracingArea.LOCK));
    assertTrue(tracing.enabled(TracingArea.OUTBOX));
  }

  @Test
  void theMasterSwitchOffGivesTheNoopTracing() {
    StreamRuneTracing tracing =
        new StreamRuneProducers()
            .streamRuneTracing(
                satisfied(SDK),
                TestProperties.of(Map.of("streamrune.tracing.enabled", "false")),
                TestProperties.config(Map.of()));

    assertSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void withoutABeanTheGlobalOpenTelemetryIsTaken() {
    GlobalOpenTelemetry.set(SDK);

    StreamRuneTracing tracing =
        new StreamRuneProducers()
            .streamRuneTracing(
                unsatisfied(), TestProperties.defaults(), TestProperties.config(Map.of()));

    assertNotSame(StreamRuneTracing.NOOP, tracing);
    assertTrue(tracing.enabled(TracingArea.COMMAND));
  }

  @Test
  void withoutABeanAndWithoutAGlobalOpenTelemetryTheTracingIsNoop() {
    StreamRuneTracing tracing =
        new StreamRuneProducers()
            .streamRuneTracing(
                unsatisfied(), TestProperties.defaults(), TestProperties.config(Map.of()));

    assertSame(StreamRuneTracing.NOOP, tracing);
  }

  @Test
  void theMasterSwitchOffLeavesTheGlobalOpenTelemetryUnread() {
    new StreamRuneProducers()
        .streamRuneTracing(
            unsatisfied(),
            TestProperties.of(Map.of("streamrune.tracing.enabled", "false")),
            TestProperties.config(Map.of()));

    // OpenTelemetry fixes the global instance on its first read; unread, it can still be set.
    GlobalOpenTelemetry.set(SDK);
    assertEquals(
        SDK.getTracerProvider().getClass(),
        GlobalOpenTelemetry.get().getTracerProvider().getClass());
  }

  @Test
  void anOpenTelemetrySwitchedOffByItsOwnSettingGivesTheNoopTracing_andLeavesTheGlobalUnread() {
    for (Map<String, String> off :
        List.of(
            Map.of("quarkus.otel.sdk.disabled", "true"),
            Map.of("quarkus.otel.traces.enabled", "false"),
            Map.of("quarkus.otel.traces.exporter", "none"),
            Map.of("otel.sdk.disabled", " TRUE "),
            Map.of("otel.traces.exporter", "none"))) {
      assertSame(
          StreamRuneTracing.NOOP,
          new StreamRuneProducers()
              .streamRuneTracing(
                  satisfied(SDK), TestProperties.defaults(), TestProperties.config(off)),
          off::toString);
      assertSame(
          StreamRuneTracing.NOOP,
          new StreamRuneProducers()
              .streamRuneTracing(
                  unsatisfied(), TestProperties.defaults(), TestProperties.config(off)),
          off::toString);
    }
    assertNotSame(
        StreamRuneTracing.NOOP,
        new StreamRuneProducers()
            .streamRuneTracing(
                satisfied(SDK),
                TestProperties.defaults(),
                TestProperties.config(
                    Map.of("quarkus.otel.sdk.disabled", "false", "otel.traces.exporter", "otlp"))));

    GlobalOpenTelemetry.set(SDK);
    assertEquals(
        SDK.getTracerProvider().getClass(),
        GlobalOpenTelemetry.get().getTracerProvider().getClass());
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> satisfied(T value) {
    Instance<T> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(value);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfied() {
    Instance<T> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    when(instance.isResolvable()).thenReturn(false);
    return instance;
  }
}
