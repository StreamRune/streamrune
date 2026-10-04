package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;

/**
 * Decorates a {@link Projection} to create an OpenTelemetry span for each batch processed.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * Projection tracedProjection = new TracingProjectionDecorator(
 *     ProjectionName.of("OrdersProjection"), rawProjection, openTelemetry);
 * projectionRunner.run(
 *     ProjectionName.of("orders"), tracedProjection, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
 * }</pre>
 *
 * <p>This class is opt-in — the {@code opentelemetry-api} dependency must be on the classpath.
 */
public final class TracingProjectionDecorator implements Projection {

  private static final String INSTRUMENTATION_SCOPE = "org.streamrune";

  private final ProjectionName projectionName;
  private final Projection delegate;
  private final Tracer tracer;

  /**
   * Creates a tracing decorator.
   *
   * @param projectionName the projection name, used in the span name
   * @param delegate the real projection to delegate to
   * @param openTelemetry the OTel SDK instance
   */
  public TracingProjectionDecorator(
      ProjectionName projectionName, Projection delegate, OpenTelemetry openTelemetry) {
    this.projectionName = projectionName;
    this.delegate = delegate;
    this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
  }

  @Override
  public void process(List<EventEnvelope> batch) {
    traced(
        batch,
        () -> {
          delegate.process(batch);
          return null;
        });
  }

  @Override
  public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
    traced(
        batch,
        () -> {
          delegate.process(batch, repository);
          return null;
        });
  }

  /**
   * Forwards to the delegate's OWN {@link Projection#processDeadLetterReplay}, inside the span, and
   * reports the delegate's answer. The inherited default would call THIS decorator's {@link
   * #process(List)} — reaching the delegate's {@code process}, never its override — and report
   * {@code true}, so a decorated self-fencing projection ({@code WindowedProjection}) read as
   * "applied" on a wholly fenced replay and the replayer discarded the range's only record (back
   * for every traced projection).
   */
  @Override
  public boolean processDeadLetterReplay(List<EventEnvelope> batch) {
    return traced(batch, () -> delegate.processDeadLetterReplay(batch));
  }

  /**
   * Forwards to the delegate's OWN answer — this decorator overrides {@code process(List,
   * ProjectionRepository)} only to wrap it in a span, so the reflective default (see {@link
   * Projection#writesThroughRepository()}) would read {@code true} on this class regardless of
   * whether the WRAPPED projection actually writes through the repository.
   */
  @Override
  public boolean writesThroughRepository() {
    return delegate.writesThroughRepository();
  }

  /** Forwards the delegate's write target so the delivery policy can verify the pairing. */
  @Override
  public Optional<ProjectionRepository> writeTarget() {
    return delegate.writeTarget();
  }

  private <T> T traced(List<EventEnvelope> batch, Supplier<T> call) {
    Span span =
        tracer
            .spanBuilder(TraceAttributes.projectionProcessSpanName(projectionName))
            .setSpanKind(SpanKind.INTERNAL)
            .setAttribute(TraceAttributes.PROJECTION_NAME, projectionName.value())
            .setAttribute(TraceAttributes.BATCH_SIZE, (long) batch.size())
            .startSpan();
    try (var _ = span.makeCurrent()) {
      T result = call.get();
      span.setStatus(StatusCode.OK);
      return result;
    } catch (Exception e) {
      span.recordException(e, Attributes.empty());
      span.setStatus(StatusCode.ERROR, e.getMessage());
      throw e;
    } finally {
      span.end();
    }
  }
}
