package org.streamrune.core.projection;

/**
 * One read-model backend: where read models live, where their checkpoints live, and how a batch
 * commits.
 *
 * <p>The three parts belong together. A projection routed to this store writes its read models to
 * {@link #repository()}, the runner reads the projection's position from {@link #checkpoints()},
 * and {@link #processor()} commits each batch and advances that same position. The checkpoints live
 * in the same store as the read models, so the processor's guards protect exactly the position the
 * runner reads, and — for a {@link AtomicBatchProcessor#transactional() transactional} processor —
 * the read-model writes and the checkpoint commit together.
 *
 * <p>The processor's capabilities decide which registrations the store may take: a {@link
 * ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} or {@link ProjectionDeliveryMode#EXTERNAL_EFFECT}
 * registration needs {@link AtomicBatchProcessor#transactional()}, and any registration under real
 * (multi-replica) leadership needs {@link AtomicBatchProcessor#fencesCheckpoint()}.
 */
public interface ReadModelStore {

  /**
   * Stable, user-visible name ({@code "postgres"}, {@code "search"}, ...) for logs, health, errors
   * and explicit routing ({@link org.streamrune.core.ProjectionConfig#readModelStore()}).
   *
   * @return the store's name; never blank
   */
  String name();

  /**
   * The repository projections routed here write to; its {@link
   * ProjectionRepository#writeTargetIdentity()} identifies this store.
   *
   * @return the store's repository
   */
  ProjectionRepository repository();

  /**
   * Checkpoints of the projections routed here — read by the runner, kept in the same store as the
   * read models.
   *
   * @return the store's checkpoints
   */
  OffsetStore checkpoints();

  /**
   * Commits a batch and advances {@link #checkpoints()}; its capabilities ({@link
   * AtomicBatchProcessor#transactional()}, {@link AtomicBatchProcessor#fencesCheckpoint()}) decide
   * which registrations it may take.
   *
   * @return the store's batch processor
   */
  AtomicBatchProcessor processor();

  /**
   * This store's checkpoints paired with the processor that commits them, under the store's name.
   *
   * @return the pair a runner reads and commits a registration's position through
   */
  default CheckpointTarget checkpointTarget() {
    return new CheckpointTarget(name(), checkpoints(), processor());
  }

  /**
   * A store from its parts.
   *
   * @param name the store's name; never blank
   * @param repository the repository projections routed here write to
   * @param checkpoints the checkpoints of projections routed here, in the same store
   * @param processor commits a batch and advances {@code checkpoints}
   * @return the store
   * @throws NullPointerException if any argument is {@code null}
   * @throws IllegalArgumentException if {@code name} is blank
   */
  static ReadModelStore of(
      String name,
      ProjectionRepository repository,
      OffsetStore checkpoints,
      AtomicBatchProcessor processor) {
    return new SimpleReadModelStore(name, repository, checkpoints, processor);
  }
}
