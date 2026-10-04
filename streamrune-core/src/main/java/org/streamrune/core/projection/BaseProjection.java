package org.streamrune.core.projection;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.types.ProjectionName;

/**
 * Base projection class that receives a repository for storing read models. Subclasses can use the
 * repository to persist read models in PostgreSQL.
 *
 * <p>The projection name namespaces this projection's read models in the {@link
 * ProjectionRepository}. Two projections with the same name silently share (and overwrite) each
 * other's read models, so names must be unique across the application — including across packages,
 * since the derived name uses the simple class name only. All read models saved under one name must
 * also be of a single Java type (see the one-type-per-projection-name contract on {@link
 * ProjectionRepository}). The name should also match the name the projection is registered under
 * for offset tracking (e.g. {@code @ProjectionConfig.name()} or the name passed to a projection
 * runner); otherwise offsets and read models are tracked under different keys.
 *
 * <p><b>Atomic offset checkpointing.</b> When a runner drives this projection through an {@link
 * AtomicBatchProcessor} it invokes {@link #process(List, ProjectionRepository)} with a
 * transaction-scoped repository. {@code BaseProjection} routes every inherited {@code
 * save()/findById()/delete()} call through that repository for the duration of the batch, so the
 * read-model write and the offset checkpoint commit — or roll back — together under the monotonic
 * guard. A re-delivered/already-applied batch (or a superseded split-brain leader) therefore rolls
 * the read-model write back instead of committing it a second time. Subclasses only implement
 * {@link #process(List)} and use the {@code save()/findById()/delete()} helpers; the routing is
 * transparent. When no transaction-scoped repository is supplied — an {@link
 * ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} registration, the nonatomic processor, or a
 * direct call to {@code process(batch)} — the helpers write through the captured {@link
 * #repository}, autocommit, outside any checkpoint transaction.
 *
 * <p><b>Inside {@code process}, write only through the helpers.</b> The {@link #repository} field
 * is the captured repository, never the batch's transaction: a {@code repository.save(...)} or
 * {@code repository.delete(...)} call from {@code process} under a {@link
 * ProjectionDeliveryMode#TRANSACTIONAL_LOCAL} registration runs outside the transaction. It cannot
 * see the rows the batch has written but not yet committed, so a delete misses a row the same batch
 * inserted, and on JDBC it opens a second connection that can wait forever on a row lock the
 * batch's own transaction holds. Use the field outside a batch, for example for the query methods a
 * projection offers its read side.
 */
public abstract class BaseProjection implements Projection {

  private static final String PROJECTION_SUFFIX = "Projection";

  /** The read-model name rule of JdbcProjectionRepository, the strictest built-in repository. */
  private static final Pattern DERIVED_NAME = Pattern.compile("[a-z_][a-z0-9_]{0,57}");

  /**
   * The repository this projection was constructed with. Outside a batch the helpers write through
   * it; inside {@code process} write through the helpers only (see the class javadoc).
   */
  protected final ProjectionRepository repository;

  private final ProjectionName projectionName;

  /**
   * The transaction-scoped repository supplied by an {@link AtomicBatchProcessor} for the duration
   * of the current {@link #process(List, ProjectionRepository)} call, or {@code null} outside one.
   *
   * <p><b>Thread-confined</b> via a {@link ThreadLocal}. A plain field is unsafe because the SAME
   * projection instance can be driven by more than one thread concurrently through
   * framework-sanctioned flows — a {@code ProjectionDeadLetterReplayer.replay(...)} running while
   * the owning runner is LIVE, or the javadoc-recommended inline+runner dual registration — and an
   * unsynchronized field read could observe the OTHER thread's binding, routing a {@code save()}
   * into the wrong open transaction (a silent lost write or JDBC-connection corruption). Confining
   * the binding to a ThreadLocal gives each thread its own transaction-scoped repository; the
   * per-call save/restore below still supports re-entrancy on a single thread.
   */
  private final ThreadLocal<ProjectionRepository> transactionalRepository = new ThreadLocal<>();

  /**
   * Derives the projection name from the simple class name: the {@code Projection} suffix is
   * removed and the rest is converted to snake_case (e.g. {@code CartViewProjection} becomes {@code
   * cart_view}, {@code OrderProjection} becomes {@code order}). The derived name always matches
   * {@code [a-z_][a-z0-9_]{0,57}}, so every repository, JdbcProjectionRepository included, stores
   * it as given.
   *
   * @throws IllegalStateException if no valid name can be derived (anonymous class, a class named
   *     exactly {@code Projection}, or a class name with characters other than ASCII letters,
   *     digits and underscores, or too long) — use {@link #BaseProjection(ProjectionRepository,
   *     String)} instead
   */
  protected BaseProjection(ProjectionRepository repository) {
    this.repository = repository;
    this.projectionName = ProjectionName.of(deriveProjectionName(getClass().getSimpleName()));
  }

  /**
   * Uses the given explicit projection name instead of deriving one from the class name.
   *
   * @throws IllegalArgumentException if {@code projectionName} is null or blank
   */
  protected BaseProjection(ProjectionRepository repository, String projectionName) {
    this.repository = repository;
    this.projectionName = ProjectionName.of(projectionName); // validates non-blank
  }

  /**
   * Processes a batch through the transaction-scoped repository supplied by an {@link
   * AtomicBatchProcessor}, so this base class's {@code save()/delete()} writes commit — or roll
   * back — together with the offset checkpoint. Binds {@code repository} for the duration of the
   * {@link #process(List)} call, then restores the previous binding; a {@code null} repository (an
   * at-least-once registration or the nonatomic processor) leaves the helpers writing through the
   * captured {@link #repository}.
   */
  @Override
  public final void process(List<EventEnvelope> batch, ProjectionRepository repository) {
    ProjectionRepository previous = this.transactionalRepository.get();
    this.transactionalRepository.set(repository);
    try {
      process(batch);
    } finally {
      // Restore the previous binding (re-entrancy); remove when there was none so the ThreadLocal
      // does not leak a stale reference on this (possibly pooled) thread.
      if (previous != null) {
        this.transactionalRepository.set(previous);
      } else {
        this.transactionalRepository.remove();
      }
    }
  }

  /**
   * The repository the {@code save()/findById()/delete()} helpers use: the transaction-scoped one
   * bound by {@link #process(List, ProjectionRepository)} when present, else the captured {@link
   * #repository}.
   */
  private ProjectionRepository activeRepository() {
    ProjectionRepository bound = transactionalRepository.get();
    return bound != null ? bound : repository;
  }

  /**
   * The captured repository: what the {@code save()/findById()/delete()} helpers use outside a
   * batch.
   */
  @Override
  public Optional<ProjectionRepository> writeTarget() {
    return Optional.ofNullable(repository);
  }

  /** Saves a read model with upsert semantics (idempotent). */
  protected <T> void save(String id, T readModel) {
    activeRepository().save(projectionName(), id, readModel);
  }

  /** Finds a read model by ID. */
  protected <T> Optional<T> findById(String id, Class<T> type) {
    return activeRepository().findById(projectionName(), id, type);
  }

  /**
   * Deletes a read model by ID, through the batch's transaction-scoped repository when one is bound
   * (like {@link #save(String, Object)}). Deleting a missing ID is a no-op.
   */
  protected void delete(String id) {
    activeRepository().delete(projectionName(), id);
  }

  /** Returns the projection name used to namespace read models in the repository. */
  protected ProjectionName projectionName() {
    return projectionName;
  }

  /**
   * The snake_case projection name for a simple class name: the {@code Projection} suffix removed,
   * an underscore before each upper-case letter that starts a word, everything lower-cased ({@code
   * HTTPRequestLog} becomes {@code http_request_log}).
   *
   * @throws IllegalStateException if the result does not match {@code [a-z_][a-z0-9_]{0,57}}
   */
  static String deriveProjectionName(String simpleName) {
    String stem =
        simpleName.endsWith(PROJECTION_SUFFIX)
            ? simpleName.substring(0, simpleName.length() - PROJECTION_SUFFIX.length())
            : simpleName;
    StringBuilder snake = new StringBuilder(stem.length() + 8);
    for (int i = 0; i < stem.length(); i++) {
      char c = stem.charAt(i);
      if (Character.isUpperCase(c) && i > 0 && startsWord(stem, i)) {
        snake.append('_');
      }
      snake.append(Character.toLowerCase(c));
    }
    String name = snake.toString();
    if (!DERIVED_NAME.matcher(name).matches()) {
      throw new IllegalStateException(
          "Cannot derive a projection name from "
              + (simpleName.isEmpty()
                  ? "an anonymous class"
                  : "the class name '" + simpleName + "'")
              + ": the derived name '"
              + name
              + "' must match [a-z_][a-z0-9_]{0,57}; pass an explicit name via"
              + " BaseProjection(repository, projectionName)");
    }
    return name;
  }

  /**
   * Whether the upper-case letter at {@code i} starts a new word: {@code cartView}, {@code
   * SKUList}.
   */
  private static boolean startsWord(String stem, int i) {
    char previous = stem.charAt(i - 1);
    if (previous == '_') {
      return false;
    }
    if (Character.isLowerCase(previous) || Character.isDigit(previous)) {
      return true;
    }
    return Character.isUpperCase(previous)
        && i + 1 < stem.length()
        && Character.isLowerCase(stem.charAt(i + 1));
  }
}
