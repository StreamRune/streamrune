package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import io.opentelemetry.api.OpenTelemetry;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;

class OpenTelemetryModuleTest {

  @Test
  void shouldCreateOpenTelemetryCommandInterceptor() {
    OpenTelemetry otel = mock(OpenTelemetry.class);
    StreamRuneMicronautModule module = new StreamRuneMicronautModule();

    OpenTelemetryCommandInterceptor interceptor = module.openTelemetryCommandInterceptor(otel);

    assertNotNull(interceptor, "Should create a non-null interceptor");
  }
}
