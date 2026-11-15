package org.streamrune.core.projection;

import java.util.List;
import java.util.Optional;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Versioned;
import org.streamrune.core.types.ProjectionName;

/**
 * Repository for storing and retrieving projection read models in PostgreSQL. Tables are
 * automatically created based on the projection schema.
 *
 * <p><b>One read-model type per projection name.</b> Rows are keyed by {@code (projectionName, id)}
 * only — the stored payload carries no type discriminator, and the {@code Class<T>} token passed to
 * the find methods drives deserialization, not filtering. Saving values of different Java types
 * under the same {@code projectionName} is therefore unsupported: a colliding id either fails at
 * deserialization/cast time or, with structurally compatible payloads, silently maps one type onto
 * another. Use a distinct projection name per read-model type.
 */
public interface ProjectionRepository {

  /**
   * Saves a read model (upsert). Implementation must handle idempotency.
   *
   * @param projectionName the name of the projection
   * @param id the unique identifier of the read model
   * @param readModel the read model to save
   */
  <T> void save(ProjectionName projectionName, String id, T readModel);

  /**
   * Finds a read model by ID.
   *
   * @param projectionName the name of the projection
   * @param id the unique identifier of the read model
   * @param type the class type of the read model
   * @return an Optional containing the read model if found, empty otherwise
   */
  <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type);

  /**
   * Finds all read models for a projection.
   *
   * @param projectionName the name of the projection
   * @param type the class type of the read model
   * @return a list of all read models for the projection, empty if none
   */
  <T> List<T> findAll(ProjectionName projectionName, Class<T> type);

  /**
   * Deletes a read model by ID. Silently succeeds if the ID does not exist.
   *
   * @param projectionName the name of the projection
   * @param id the unique identifier of the read model to delete
   */
  void delete(ProjectionName projectionName, String id);

  /**
   * Finds one page of a projection's read models, in the order of {@code pageRequest.sort()}.
   *
   * <p><b>Order.</b> A sort order names a top-level field of the read model, or one of the
   * repository's columns {@code id}, {@code version} and {@code updated_at}; a field outside {@code
   * [A-Za-z0-9_]+} (nested paths included) is refused with an {@link IllegalArgumentException}. A
   * field sorts by its text, as the stored JSON renders it ({@code data->>'field'} on PostgreSQL),
   * compared by Unicode code point — byte order, PostgreSQL's {@code COLLATE "C"} — not by the
   * database's linguistic collation and not by value: {@code "B"} sorts before {@code "a"}, the
   * number {@code 10} before {@code 9}, and an ISO-8601 instant with fewer fraction digits after
   * one with more within the same second ({@code "…00.5Z"} after {@code "…00.123Z"}). A missing or
   * JSON-{@code null} field sorts after every value ascending and before every value descending.
   * The id breaks every tie: a sort that does not order by {@code id} is followed by {@code id}
   * ascending, so the order is total and a row appears on exactly one page of a stable table. An
   * unsorted request orders by id.
   *
   * <p><b>Consistency.</b> {@link Page#content()} and {@link Page#totalElements()} come from one
   * view of the table: a write committed while the page is read changes both or neither.
   *
   * <p>A page beyond the last is empty and still carries the total.
   *
   * @param projectionName the name of the projection
   * @param type the class type of the read model
   * @param pageRequest pagination and sort parameters
   * @return a page of read models
   */
  <T> Page<T> findAll(ProjectionName projectionName, Class<T> type, PageRequest pageRequest);

  /**
   * Finds one page of the read models whose top-level field holds a value, in the order of {@code
   * pageRequest.sort()}. Order, consistency and the page beyond the last are as for {@link
   * #findAll(ProjectionName, Class, PageRequest)}; the total counts the matching read models. The
   * filter compares text (see {@link FieldEquals}).
   *
   * <p>On PostgreSQL the read is fast only with an index that starts with the filtered field and
   * continues with the sort: declare it with {@link ProjectionIndex#forFilter}. Without one, every
   * page and its count read the whole table.
   *
   * @param projectionName the name of the projection
   * @param type the class type of the read model
   * @param filter the field and the value it must hold
   * @param pageRequest pagination and sort parameters
   * @return a page of the matching read models
   * @throws IllegalArgumentException if an argument is {@code null}, or the filter or a sort order
   *     names a field the repository refuses
   */
  <T> Page<T> findAllBy(
      ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest);

  /**
   * Finds a read model by ID with the specified lock mode. Returns a {@link Versioned} wrapper
   * including the current version.
   *
   * <p>{@link LockMode#PESSIMISTIC} acquires a row-level exclusive lock (SELECT FOR UPDATE). The
   * lock is released when the enclosing transaction commits or rolls back. If there is no active
   * transaction, the lock releases immediately after the statement.
   *
   * @param projectionName the name of the projection
   * @param id the unique identifier of the read model
   * @param type the class type of the read model
   * @param lockMode the lock mode to use
   * @return an Optional containing the versioned read model if found, empty otherwise
   */
  <T> Optional<Versioned<T>> findById(
      ProjectionName projectionName, String id, Class<T> type, LockMode lockMode);

  /**
   * Saves a read model with optimistic version check. The save succeeds only if the current stored
   * version matches {@code expectedVersion}. On success, the version is incremented.
   *
   * @param projectionName the name of the projection
   * @param id the unique identifier of the read model
   * @param readModel the read model to save
   * @param expectedVersion the version expected to be in the store
   * @throws OptimisticLockException if the current version does not match expectedVersion
   */
  <T> void save(ProjectionName projectionName, String id, T readModel, long expectedVersion);

  /**
   * An object that identifies the store this repository writes to. Two repositories with equal
   * identities write to the same place; {@link AtomicBatchProcessor#writesTo} compares them so the
   * delivery policy can refuse a {@code TRANSACTIONAL_LOCAL} projection whose store is not the
   * processor's. The default is this repository object (reference identity); a repository bound to
   * a store that several instances share (a JDBC repository over one {@code DataSource}) returns a
   * value that names the store instead, and a transaction-scoped view returns its owner's identity.
   *
   * @return the write-target identity; never {@code null}
   */
  default Object writeTargetIdentity() {
    return this;
  }

  /**
   * Runs {@code action} once the writes made through this repository are committed, so that other
   * readers can see them.
   *
   * <p>The default runs {@code action} at once: a repository that is not bound to an open
   * transaction commits each write as it makes it, so by the time a caller registers an action its
   * writes are already visible.
   *
   * <p>A repository bound to an open transaction (the one a transactional {@link
   * AtomicBatchProcessor} hands to its {@link AtomicBatchProcessor.ProjectionUpdater}) must
   * override this: it queues {@code action}, runs it after that transaction commits, and discards
   * it if the transaction rolls back. Running it earlier lets a concurrent reader act on the read
   * model as it was before the transaction: a query cache evicted before the commit is refilled
   * from the old rows by a query that lands before the commit, and serves them until its TTL runs
   * out. A decorator of a {@code ProjectionRepository} must forward this method to the repository
   * it wraps; inheriting the default would run the action before the wrapped transaction commits.
   *
   * <p>The action runs on the thread that committed. Keep it short and non-blocking, and do not
   * write through this repository from it: the transaction it was registered in is over.
   *
   * @param action what to run after the commit; must not be {@code null}
   */
  default void afterCommit(Runnable action) {
    action.run();
  }
}
