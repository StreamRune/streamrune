package org.streamrune.runtime;

import java.util.Optional;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.LogSanitizer;

/**
 * The one place where every projection runner checks a registration's declared {@link
 * ProjectionDeliveryMode} against the runner's {@link AtomicBatchProcessor} and the projection
 * object, before any event is read. Pure — no I/O, no state. A refusal is an {@link
 * IllegalArgumentException} naming the runner, the projection, the mode, the processor, the write
 * target when present, and the two ways out; it is never a log line.
 */
final class ProjectionDeliveryPolicy {

  private ProjectionDeliveryPolicy() {}

  /** What the policy concluded, for the one INFO line per registration. */
  enum Verdict {
    VERIFIED("transactional, write target verified"),
    DECLARED("transactional, write-through declared (write target not exposed)"),
    AT_LEAST_ONCE(
        "at-least-once: no transaction-scoped repository is handed; read-model writes are outside"
            + " the checkpoint transaction"),
    AT_LEAST_ONCE_TRANSACTIONAL_AVAILABLE(
        "at-least-once: this projection writes to the processor's own store, so TRANSACTIONAL_LOCAL"
            + " is available at no cost");

    private final String text;

    Verdict(String text) {
      this.text = text;
    }

    String describe() {
      return text;
    }
  }

  /** The processor's name for messages: {@code nonAtomicAtLeastOnce} or the class name. */
  static String processorLabel(AtomicBatchProcessor processor) {
    return processor instanceof AtomicBatchProcessor.NonAtomicAtLeastOnce
        ? "nonAtomicAtLeastOnce"
        : processor.getClass().getName();
  }

  /**
   * Decides whether {@code projection} may run under {@code mode} on {@code processor}.
   *
   * @param projection the projection about to be registered or run
   * @param mode the delivery mode the registration declares
   * @param processor the runner's batch processor
   * @param runnerName the runner type, for the message
   * @param projectionName the projection's name, for the message (sanitized here)
   * @return the verdict the runner logs once per registration
   * @throws IllegalArgumentException when the pairing cannot honour the declared mode
   */
  static Verdict require(
      Projection projection,
      ProjectionDeliveryMode mode,
      AtomicBatchProcessor processor,
      String runnerName,
      String projectionName) {
    String name = LogSanitizer.sanitizeForLog(projectionName);
    if (processor == null) {
      throw new IllegalArgumentException(
          runnerName
              + ": atomicProcessor is required: pass the JdbcProjectionRepository your"
              + " TRANSACTIONAL_LOCAL projections write to, or"
              + " AtomicBatchProcessor.nonAtomicAtLeastOnce() for a runner of"
              + " AT_LEAST_ONCE_IDEMPOTENT projections");
    }
    if (projection == null) {
      throw new IllegalArgumentException(
          runnerName + " projection '" + name + "': projection is required");
    }
    if (mode == null) {
      throw new IllegalArgumentException(
          runnerName
              + " projection '"
              + name
              + "': deliveryMode is required — declare TRANSACTIONAL_LOCAL,"
              + " AT_LEAST_ONCE_IDEMPOTENT or EXTERNAL_EFFECT (there is no default)");
    }
    if (!mode.writesInCheckpointTransaction()) {
      // The write target first: a projection without one never reaches the reflective
      // writesThroughRepository(), which this mode needs only for the INFO line's verdict.
      boolean upgradeAvailable =
          processor.supportsFencing()
              && projection.writeTarget().map(processor::writesTo).orElse(false)
              && projection.writesThroughRepository();
      return upgradeAvailable
          ? Verdict.AT_LEAST_ONCE_TRANSACTIONAL_AVAILABLE
          : Verdict.AT_LEAST_ONCE;
    }
    if (!processor.supportsFencing()) {
      throw new IllegalArgumentException(
          runnerName
              + " projection '"
              + name
              + "' declares "
              + mode
              + " but the runner's AtomicBatchProcessor ("
              + processorLabel(processor)
              + ") needs a transactional processor: it cannot run the read-model write and the"
              + " checkpoint in one transaction. Pass the JdbcProjectionRepository the projection"
              + " writes to as the processor, or declare AT_LEAST_ONCE_IDEMPOTENT and make the"
              + " projection idempotent.");
    }
    if (!projection.writesThroughRepository()) {
      throw new IllegalArgumentException(
          runnerName
              + " projection '"
              + name
              + "' declares "
              + mode
              + " under a transactional AtomicBatchProcessor ("
              + processorLabel(processor)
              + "), but it does not write through the handed repository: its process(List,"
              + " ProjectionRepository) is the INHERITED DEFAULT, which ignores the repository and"
              + " falls back to process(List), so its read-model writes would commit outside the"
              + " checkpoint's transaction. Extend BaseProjection or override process(List,"
              + " ProjectionRepository) and write through the supplied repository, or declare"
              + " AT_LEAST_ONCE_IDEMPOTENT.");
    }
    Optional<ProjectionRepository> target = projection.writeTarget();
    if (target.isEmpty()) {
      return Verdict.DECLARED;
    }
    if (!processor.writesTo(target.get())) {
      throw new IllegalArgumentException(
          runnerName
              + " projection '"
              + name
              + "' declares "
              + mode
              + " and writes to "
              + target.get().getClass().getName()
              + " (identity "
              + target.get().writeTargetIdentity()
              + "), but "
              + runnerName
              + " is configured with "
              + processorLabel(processor)
              + " (identity "
              + identityOf(processor)
              + "): its writes inside process() would go into the processor's transaction and"
              + " store while its reads outside process() go elsewhere. Pass the repository it"
              + " writes to as the processor, or declare AT_LEAST_ONCE_IDEMPOTENT (the runner then"
              + " hands it no transaction-scoped repository and it writes to its own store). A JDBC"
              + " repository's identity is (DataSource, ObjectMapper): same DataSource, different"
              + " ObjectMapper — the processor's mapper would serialise this projection's rows"
              + " (encryption at rest may differ).");
    }
    return Verdict.VERIFIED;
  }

  private static Object identityOf(AtomicBatchProcessor processor) {
    return processor instanceof ProjectionRepository repo ? repo.writeTargetIdentity() : processor;
  }
}
