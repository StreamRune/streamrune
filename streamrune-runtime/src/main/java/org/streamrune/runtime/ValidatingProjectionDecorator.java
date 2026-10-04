package org.streamrune.runtime;

import jakarta.validation.Validator;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.LogSanitizer;

/**
 * Decorates a {@link Projection} to validate event payloads before processing.
 *
 * <p>Invalid events are logged and skipped — they do not halt the projection. This is intentional:
 * projections must be resilient, and a single bad event should not block processing of the entire
 * batch.
 *
 * <p>Validates the domain event ({@link EventEnvelope#event()}) using the {@code Default} group.
 * Metadata is framework-controlled and always valid.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * Projection validated = new ValidatingProjectionDecorator(myProjection, validator);
 * projectionRunner.run(
 *     ProjectionName.of("orders"), validated, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
 * }</pre>
 */
public final class ValidatingProjectionDecorator implements Projection {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatingProjectionDecorator.class);

  private final Projection delegate;
  private final Validator validator;

  /**
   * Creates a validating decorator.
   *
   * @param delegate the real projection to delegate to
   * @param validator the JSR-380 Validator used to validate event payloads
   */
  public ValidatingProjectionDecorator(Projection delegate, Validator validator) {
    this.delegate = delegate;
    this.validator = validator;
  }

  @Override
  public void process(List<EventEnvelope> batch) {
    List<EventEnvelope> valid = batch.stream().filter(this::isValid).toList();
    if (!valid.isEmpty()) {
      delegate.process(valid);
    }
  }

  @Override
  public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
    List<EventEnvelope> valid = batch.stream().filter(this::isValid).toList();
    if (!valid.isEmpty()) {
      delegate.process(valid, repository);
    }
  }

  /**
   * Validates, then forwards the valid events to the delegate's OWN {@link
   * Projection#processDeadLetterReplay} and reports the delegate's answer. The inherited default
   * would call THIS decorator's {@link #process(List)} — reaching the delegate's {@code process},
   * never its override — and report {@code true}, so a decorated self-fencing projection ({@code
   * WindowedProjection}) read as "applied" on a wholly fenced replay and the replayer discarded the
   * range's only record (back for every validated projection).
   *
   * <p>A batch with NO valid event reports {@code true}: every event was rejected by validation
   * (each one logged, exactly as on the live path), which is the handled outcome for invalid
   * events, not a fence — reporting {@code false} would keep the entry forever with a "replay from
   * a fresh process" warning that can never come true.
   */
  @Override
  public boolean processDeadLetterReplay(List<EventEnvelope> batch) {
    List<EventEnvelope> valid = batch.stream().filter(this::isValid).toList();
    if (valid.isEmpty()) {
      return true;
    }
    return delegate.processDeadLetterReplay(valid);
  }

  /**
   * Forwards to the delegate's OWN answer — this decorator overrides {@code process(List,
   * ProjectionRepository)} only to filter the batch before forwarding, so the reflective default
   * (see {@link Projection#writesThroughRepository()}) would read {@code true} on this class
   * regardless of whether the WRAPPED projection actually writes through the repository.
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

  private boolean isValid(EventEnvelope envelope) {
    var violations = validator.validate(envelope.event());
    if (!violations.isEmpty()) {
      LOG.warn(
          "Skipping invalid event {} in stream {}: {}",
          LogSanitizer.sanitizeForLog(envelope.metadata().eventId().value()),
          LogSanitizer.sanitizeForLog(envelope.streamId().value()),
          violations);
      return false;
    }
    return true;
  }
}
