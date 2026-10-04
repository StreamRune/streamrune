package org.streamrune.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.LogSanitizer;

/**
 * How the three framework integrations choose a runner's {@link AtomicBatchProcessor} — by what the
 * registrations DECLARE and by whether leadership needs the epoch honoured, never by the presence
 * of a {@code DataSource} or of the bean. One rule, three call sites (Spring {@code
 * ProjectionAutoConfig}, Quarkus {@code ProjectionProducer}, Micronaut {@code ProjectionFactory}),
 * applied once per runner.
 *
 * <p>The INLINE rule lives here too: an INLINE projection runs post-commit on the command thread
 * with no checkpoint and no transaction of its own, so it can only keep the {@link
 * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} promise.
 */
public final class ProjectionProcessorResolution {

  private ProjectionProcessorResolution() {}

  /** One discovered registration: its name and the mode its {@code @ProjectionConfig} declares. */
  public record Registration(String projectionName, ProjectionDeliveryMode mode) {}

  /** The one INFO sentence the integrations log per INLINE registration. */
  public static final String INLINE_INFO =
      "Projection '{}' delivery mode AT_LEAST_ONCE_IDEMPOTENT inline — post-commit on the command"
          + " thread, no checkpoint; a failure is logged and the batch is not retried";

  /**
   * Selects the processor for one runner.
   *
   * @param registrations the runner's discovered registrations
   * @param leadership the configured leadership bean, or {@code null} when none exists
   * @param candidates every {@code AtomicBatchProcessor} bean in the context (0, 1 or more)
   * @param runnerName for the message
   * @return the single bean when a registration or the leadership needs it; otherwise {@link
   *     AtomicBatchProcessor#nonAtomicAtLeastOnce()} (every registration is at-least-once and there
   *     is no epoch to fence)
   * @throws IllegalStateException no bean while one is needed, or more than one bean
   */
  public static AtomicBatchProcessor select(
      List<Registration> registrations,
      SubscriptionLeadership leadership,
      List<AtomicBatchProcessor> candidates,
      String runnerName) {
    List<Registration> transactional =
        registrations.stream().filter(r -> r.mode().writesInCheckpointTransaction()).toList();
    boolean needsFencing = leadership != null && leadership != SubscriptionLeadership.NOOP;
    if (transactional.isEmpty() && !needsFencing) {
      return AtomicBatchProcessor.nonAtomicAtLeastOnce();
    }
    if (candidates.isEmpty()) {
      throw new IllegalStateException(
          noProcessorMessage(transactional, needsFencing, leadership, runnerName));
    }
    if (candidates.size() > 1) {
      throw new IllegalStateException(
          runnerName
              + ": "
              + candidates.size()
              + " AtomicBatchProcessor beans are available ("
              + candidates.stream()
                  .map(c -> c.getClass().getName())
                  .collect(Collectors.joining(", "))
              + ") but a runner has exactly one processor. Keep one bean, or move the second"
              + " repository's TRANSACTIONAL_LOCAL projections to a runner of their own.");
    }
    return candidates.get(0);
  }

  /**
   * The refusal when a registration or the leadership needs a processor and none is available.
   *
   * @param transactional the registrations whose mode writes in the checkpoint transaction
   * @param needsFencing whether the leadership's epoch must be honoured
   * @param leadership the configured leadership (named in the message when {@code needsFencing})
   * @param runnerName for the message
   */
  private static String noProcessorMessage(
      List<Registration> transactional,
      boolean needsFencing,
      SubscriptionLeadership leadership,
      String runnerName) {
    var reasons = new ArrayList<String>();
    for (var r : transactional) {
      reasons.add(
          "projection '"
              + LogSanitizer.sanitizeForLog(r.projectionName())
              + "' declares "
              + r.mode());
    }
    if (needsFencing) {
      reasons.add(
          "single-active-consumer leadership (" + leadership.getClass().getName() + ") is on");
    }
    var sb = new StringBuilder(runnerName).append(": ");
    for (int i = 0; i < reasons.size(); i++) {
      if (i > 0) {
        sb.append(i == reasons.size() - 1 ? " and " : ", ");
      }
      sb.append(reasons.get(i));
    }
    sb.append(", but no AtomicBatchProcessor bean is available");
    if (needsFencing) {
      sb.append(" to honour its epoch");
    }
    sb.append(
        ". Define one — org.streamrune.postgres.JdbcProjectionRepository over the DataSource"
            + " whose projection_offset table the runner's OffsetStore reads, declared with its"
            + " CONCRETE type — or ");
    if (!transactional.isEmpty()) {
      sb.append(
          transactional.size() == 1
              ? "declare the projection AT_LEAST_ONCE_IDEMPOTENT"
              : "declare these projections AT_LEAST_ONCE_IDEMPOTENT");
    }
    if (!transactional.isEmpty() && needsFencing) {
      sb.append(", and ");
    }
    if (needsFencing) {
      sb.append(
          "disable leadership for a single instance"
              + " (streamrune.subscription.single-active-consumer.enabled=false)");
    }
    return sb.append('.').toString();
  }

  /**
   * Refuses an INLINE registration that declares anything but {@link
   * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT}: INLINE runs post-commit on the command thread
   * with no checkpoint and no transaction of its own, so it cannot keep a transactional promise.
   *
   * @param projectionName the INLINE projection's name (sanitized into the message)
   * @param mode the mode its {@code @ProjectionConfig} declares
   * @throws IllegalStateException when {@code mode} is not {@code AT_LEAST_ONCE_IDEMPOTENT}
   */
  public static void requireInlineMode(String projectionName, ProjectionDeliveryMode mode) {
    if (mode == ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT) {
      return;
    }
    throw new IllegalStateException(
        "projection '"
            + LogSanitizer.sanitizeForLog(projectionName)
            + "' is INLINE and declares "
            + mode
            + ": INLINE runs post-commit on the command thread with no checkpoint and no transaction"
            + " of its own. Declare AT_LEAST_ONCE_IDEMPOTENT, or run it CONTINUOUS or SCHEDULED.");
  }
}
