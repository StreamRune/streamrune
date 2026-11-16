package org.streamrune.core.projection;

import java.util.Objects;

/**
 * Where one registration's checkpoint lives and what commits it: the {@link OffsetStore} the runner
 * reads the registration's position from, and the {@link AtomicBatchProcessor} that advances that
 * same position when a batch commits. The two always come as a pair — a runner never reads a
 * checkpoint from one store and commits it through a processor of another, because the processor's
 * guards (epoch fence, overlap and monotonic checks) only protect the checkpoint it writes.
 *
 * <p>A {@link ReadModelStore} supplies one through {@link ReadModelStore#checkpointTarget()}; the
 * event store's subscription checkpoint is another, with no read models of its own.
 *
 * @param name a stable, user-visible name for logs, health and errors; never blank
 * @param checkpoints the checkpoints the runner reads, in the same store the processor commits to
 * @param processor commits a batch and advances {@code checkpoints}
 */
public record CheckpointTarget(
    String name, OffsetStore checkpoints, AtomicBatchProcessor processor) {

  /**
   * Validates the pair.
   *
   * @throws NullPointerException if any component is {@code null}
   * @throws IllegalArgumentException if {@code name} is blank
   */
  public CheckpointTarget {
    Objects.requireNonNull(name, "name is required");
    Objects.requireNonNull(checkpoints, "checkpoints is required");
    Objects.requireNonNull(processor, "processor is required");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
  }
}
