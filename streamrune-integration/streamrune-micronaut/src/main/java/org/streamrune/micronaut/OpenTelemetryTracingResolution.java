package org.streamrune.micronaut;

import io.micronaut.context.BeanContext;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.runtime.OpenTelemetryTracing;
import org.streamrune.runtime.TracingSettings;

/**
 * The OpenTelemetry side of {@link StreamRuneMicronautModule#streamRuneTracing}: the one class of
 * the module that names OpenTelemetry types, loaded only once the module has found {@code
 * opentelemetry-api} on its classpath, so an application without it never loads it.
 */
final class OpenTelemetryTracingResolution {

  private OpenTelemetryTracingResolution() {}

  /**
   * The tracing over the context's {@code OpenTelemetry} bean, or over the global OpenTelemetry
   * without one, under {@code settings}; {@link StreamRuneTracing#NOOP} when that OpenTelemetry has
   * no tracer provider of its own. The global OpenTelemetry is read once, here: OpenTelemetry fixes
   * it on its first read, so an SDK the application registers globally is registered before the
   * context starts, or is an {@code OpenTelemetry} bean.
   */
  static StreamRuneTracing of(BeanContext beanContext, TracingSettings settings) {
    OpenTelemetry source =
        beanContext.findBean(OpenTelemetry.class).orElseGet(GlobalOpenTelemetry::get);
    if (source.getTracerProvider() == TracerProvider.noop()) {
      return StreamRuneTracing.NOOP;
    }
    return OpenTelemetryTracing.of(source, settings);
  }
}
