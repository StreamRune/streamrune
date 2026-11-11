package org.streamrune.runtime;

import io.opentelemetry.api.OpenTelemetry;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.ProjectionName;

/**
 * Decorates a {@link Projection} to start the span {@code projection/{name}/process} for each batch
 * processed, through a {@link StreamRuneTracing} (area {@link TracingArea#PROJECTION}).
 *
 * <p>The projection runner builders decorate every projection they run through {@link #traced} when
 * they are given a tracing. An application can also decorate a projection itself:
 *
 * <pre>{@code
 * Projection tracedProjection = new TracingProjectionDecorator(
 *     ProjectionName.of("OrdersProjection"), rawProjection, openTelemetry);
 * projectionRunner.run(
 *     ProjectionName.of("orders"), tracedProjection, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
 * }</pre>
 *
 * <p>The {@link OpenTelemetry} constructor needs the {@code opentelemetry-api} dependency on the
 * classpath and traces with every area on.
 */
public final class TracingProjectionDecorator implements Projection {

  private final ProjectionName projectionName;
  private final Projection delegate;
  private final StreamRuneTracing tracing;
  private final boolean traced;

  /**
   * Creates a tracing decorator that traces through {@code openTelemetry}, as {@code
   * OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled())} does.
   *
   * @param projectionName the projection name, used in the span name
   * @param delegate the real projection to delegate to
   * @param openTelemetry the OTel SDK instance
   */
  public TracingProjectionDecorator(
      ProjectionName projectionName, Projection delegate, OpenTelemetry openTelemetry) {
    this(
        projectionName,
        delegate,
        OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled()));
  }

  /**
   * Creates a tracing decorator that traces through {@code tracing}, when its {@link
   * TracingArea#PROJECTION} area is on; with the area off it only delegates.
   *
   * @param projectionName the projection name, used in the span name
   * @param delegate the real projection to delegate to
   * @param tracing the tracing
   * @throws NullPointerException if an argument is {@code null}
   */
  public TracingProjectionDecorator(
      ProjectionName projectionName, Projection delegate, StreamRuneTracing tracing) {
    this.projectionName = Objects.requireNonNull(projectionName, "projectionName is required");
    this.delegate = Objects.requireNonNull(delegate, "delegate is required");
    this.tracing = Objects.requireNonNull(tracing, "tracing is required");
    this.traced = tracing.enabled(TracingArea.PROJECTION);
  }

  /**
   * The projection a runner processes under {@code tracing}: {@code projection} decorated with a
   * {@code TracingProjectionDecorator} when the {@link TracingArea#PROJECTION} area is on, else
   * {@code projection} itself. A projection that already is a {@code TracingProjectionDecorator},
   * or a {@code null} projection or name, is returned as it is, so a batch is never traced twice
   * and the runner's own checks still report a missing argument.
   *
   * @param projectionName the projection name, or {@code null}
   * @param projection the projection, or {@code null}
   * @param tracing the tracing; {@code null} traces nothing
   * @return the projection to process batches with
   */
  public static Projection traced(
      ProjectionName projectionName, Projection projection, StreamRuneTracing tracing) {
    if (projection == null
        || projectionName == null
        || tracing == null
        || !tracing.enabled(TracingArea.PROJECTION)
        || projection instanceof TracingProjectionDecorator) {
      return projection;
    }
    return new TracingProjectionDecorator(projectionName, projection, tracing);
  }

  @Override
  public void process(List<EventEnvelope> batch) {
    if (!traced) {
      delegate.process(batch);
      return;
    }
    inSpan(
        batch,
        () -> {
          delegate.process(batch);
          return null;
        });
  }

  @Override
  public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
    if (!traced) {
      delegate.process(batch, repository);
      return;
    }
    inSpan(
        batch,
        () -> {
          delegate.process(batch, repository);
          return null;
        });
  }

  /**
   * Forwards to the delegate's OWN {@link Projection#processDeadLetterReplay}, inside the span,
   * with the repository it was handed, and reports the delegate's answer. The inherited default
   * would call THIS decorator's {@code process} — reaching the delegate's {@code process}, never
   * its override — and report {@code true}, so a decorated self-fencing projection ({@code
   * WindowedProjection}) would read as "applied" on a wholly fenced replay and the replayer would
   * discard the range's only record.
   */
  @Override
  public boolean processDeadLetterReplay(
      List<EventEnvelope> batch, ProjectionRepository repository) {
    if (!traced) {
      return delegate.processDeadLetterReplay(batch, repository);
    }
    return inSpan(batch, () -> delegate.processDeadLetterReplay(batch, repository));
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

  private <T> T inSpan(List<EventEnvelope> batch, Supplier<T> call) {
    SpanAttributes attributes =
        SpanAttributes.builder()
            .put(TraceAttributes.PROJECTION_NAME, projectionName.value())
            .put(TraceAttributes.BATCH_SIZE, (long) batch.size())
            .build();
    TraceScope scope =
        tracing.start(
            TracingArea.PROJECTION,
            TraceAttributes.projectionProcessSpanName(projectionName),
            SpanKind.INTERNAL,
            attributes);
    try {
      T result = call.get();
      scope.recordSuccess();
      return result;
    } catch (Exception e) {
      scope.recordError(e);
      throw e;
    } finally {
      scope.close();
    }
  }
}
