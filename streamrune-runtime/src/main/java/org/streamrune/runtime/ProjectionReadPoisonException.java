package org.streamrune.runtime;

/**
 * Terminal signal that a projection was halted because a <b>read/deserialization-level poison
 * event</b> could not be read from the event store and never will be on retry — for example an
 * event whose type is no longer registered in the {@code EventTypeRegistry}, or whose payload is
 * corrupt / schema-incompatible.
 *
 * <p>Such a failure occurs <em>before</em> the projection's {@code process()} runs, so it can reach
 * neither the {@code ProjectionErrorStrategy} (SKIP/DLQ/HALT) nor the projection dead-letter store.
 * Left unbounded it would be retried as a "transient" store error forever, silently wedging the
 * read model with no progress. The runners therefore bound such reads and, once the bound is
 * exhausted, stop with this distinct exception instead of an endless backoff — so the stall
 * surfaces as a terminal ERROR / health-DOWN with an actionable message rather than being mistaken
 * for a recoverable blip.
 *
 * <p>Recovery is operational: register the missing event type (or repair / crypto-erase the
 * offending event) and restart the projection, which resumes from the last committed checkpoint.
 */
public final class ProjectionReadPoisonException extends RuntimeException {

  /**
   * Public so a {@code ContinuousProjectionRunner.SubscriptionFactory} subscription outside this
   * package can satisfy the {@link ReadPoisonAware} contract — see {@link ReadPoisonClassifier}.
   *
   * @param projectionName the projection (or subscription) whose read is poisoned
   * @param attempts consecutive deterministic poison reads observed at the same checkpoint
   * @param cause the last read failure
   */
  public ProjectionReadPoisonException(String projectionName, int attempts, Throwable cause) {
    super(
        "Projection '"
            + projectionName
            + "' halted: a read/deserialization-level poison event (e.g. an unregistered event type"
            + " or a corrupt/schema-incompatible payload) failed deterministically "
            + attempts
            + " time(s) and can never be read on retry. The projection is stopped rather than"
            + " retried forever — register the missing event type or repair/erase the offending"
            + " event, then restart the projection.",
        cause);
  }
}
