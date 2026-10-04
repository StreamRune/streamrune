package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.opentelemetry.api.OpenTelemetry;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;

class OpenTelemetryProducersTest {

  @Test
  @SuppressWarnings("unchecked")
  void shouldProduceOpenTelemetryCommandInterceptor_whenOtelPresent() {
    OpenTelemetry otel = mock(OpenTelemetry.class);
    Instance<OpenTelemetry> otelInstance = mock(Instance.class);
    when(otelInstance.isUnsatisfied()).thenReturn(false);
    when(otelInstance.get()).thenReturn(otel);
    StreamRuneProducers producers = new StreamRuneProducers();

    OpenTelemetryCommandInterceptor interceptor =
        producers.openTelemetryCommandInterceptor(otelInstance);

    assertNotNull(interceptor, "Should produce a non-null interceptor when OTel is present");
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldReturnNull_whenOtelAbsent() {
    Instance<OpenTelemetry> otelInstance = mock(Instance.class);
    when(otelInstance.isUnsatisfied()).thenReturn(true);
    StreamRuneProducers producers = new StreamRuneProducers();

    OpenTelemetryCommandInterceptor interceptor =
        producers.openTelemetryCommandInterceptor(otelInstance);

    assertNull(interceptor, "Should return null when OTel is not available");
  }
}
