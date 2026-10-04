package org.streamrune.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Versioned;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * In-memory {@link ProjectionRepository} that is also a fencing-capable {@link
 * AtomicBatchProcessor} and the {@link OffsetStore} of its own checkpoint — the Docker-free way to
 * run a {@code TRANSACTIONAL_LOCAL} projection, including rollback. {@link #executeAtomically}
 * mirrors {@code JdbcProjectionRepository}: one lock per projection name, the epoch fence and the
 * first-offset overlap guard BEFORE the updater, the writes staged in a transaction-scoped view and
 * applied together with the checkpoint, the monotonic guard on the checkpoint move, after-commit
 * actions run after the apply and dropped on rollback. Pass the same instance as {@code
 * .offsetStore(...)} and {@code .atomicProcessor(...)} of a runner, exactly as {@code
 * PostgresOffsetStore} and {@code JdbcProjectionRepository} share {@code projection_offset}. Passes
 * {@link AtomicBatchProcessorContract}.
 *
 * <p>Same rules as the JDBC repository: an inserted row starts at version 0 and every save adds
 * one; a versioned save only updates an existing row at the expected version, so a missing row is
 * an {@link OptimisticLockException} at every expected version; the repository methods refuse a
 * {@code null} projection name, a {@code null} or blank id, a {@code null} read model and a
 * negative expected version with an {@link IllegalArgumentException} at the call. The
 * transaction-scoped view refuses every read and write once its batch has committed or rolled back;
 * an after-commit action registered on it after the commit runs at once, one registered after a
 * rollback is discarded.
 *
 * <p>One deliberate difference: an after-commit action that throws a {@link RuntimeException}. Both
 * repositories still run the remaining actions and keep the batch and its checkpoint applied, but
 * where the JDBC repository logs the failure and returns normally, this one rethrows the first
 * failure from {@link #executeAtomically} once every action has run, so a test sees it. A runner
 * then reports that batch as failed although it is applied, which production never does.
 *
 * <p>A reader on another thread sees a batch's rows and its checkpoint together, never one without
 * the other; it is not blocked while the updater runs, only while the batch is applied.
 *
 * <p>Read models are stored as the objects themselves, not serialized: a projection that mutates a
 * read model after saving it mutates the stored row too. Not for production.
 */
public final class InMemoryProjectionRepository
    implements ProjectionRepository, AtomicBatchProcessor, OffsetStore {

  private record Stored(Object value, long version) {}

  private record Checkpoint(long offset, long epoch) {
    static final Checkpoint NONE = new Checkpoint(0L, 0L);
  }

  private final ConcurrentHashMap<ProjectionName, ConcurrentSkipListMap<String, Stored>> tables =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ProjectionName, Checkpoint> checkpoints =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ProjectionName, Object> locks = new ConcurrentHashMap<>();

  // Publication lock: every change to the committed rows or checkpoints happens under the write
  // lock and every read of them under the read lock, so a batch's rows and its checkpoint become
  // visible to other threads together. Separate from the per-name monitor so readers never wait for
  // an updater.
  private final ReentrantReadWriteLock publication = new ReentrantReadWriteLock();

  /** Creates an empty store: no read models, every checkpoint at offset 0 and epoch 0. */
  public InMemoryProjectionRepository() {
    // Nothing to initialise: tables, checkpoints and locks are created on first use.
  }

  private ConcurrentSkipListMap<String, Stored> table(ProjectionName name) {
    return tables.computeIfAbsent(name, n -> new ConcurrentSkipListMap<>());
  }

  private Object lockFor(ProjectionName name) {
    return locks.computeIfAbsent(name, n -> new Object());
  }

  private <R> R read(Supplier<R> body) {
    return locked(publication.readLock(), body);
  }

  private void write(Runnable body) {
    locked(
        publication.writeLock(),
        () -> {
          body.run();
          return null;
        });
  }

  private static <R> R locked(Lock lock, Supplier<R> body) {
    lock.lock();
    try {
      return body.get();
    } finally {
      lock.unlock();
    }
  }

  private Stored committedRow(ProjectionName name, String id) {
    var t = tables.get(name);
    return t == null ? null : t.get(id);
  }

  private static void validateName(ProjectionName name) {
    if (name == null) {
      throw new IllegalArgumentException("projectionName cannot be null");
    }
  }

  private static void validateInputs(ProjectionName name, String id) {
    validateName(name);
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("id cannot be null or blank");
    }
  }

  private static void validateWrite(ProjectionName name, String id, Object readModel) {
    validateInputs(name, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
  }

  private static void validateVersionedWrite(
      ProjectionName name, String id, Object readModel, long expectedVersion) {
    validateWrite(name, id, readModel);
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must be >= 0");
    }
  }

  private static String quoted(ProjectionName name) {
    return "'" + LogSanitizer.sanitizeForLog(name.value()) + "'";
  }

  /** The versioned-save outcome of the JDBC repository's {@code UPDATE ... AND version = ?}. */
  private static Stored versionedUpdate(
      ProjectionName name, String id, Object readModel, long expectedVersion, Stored current) {
    if (current == null || current.version() != expectedVersion) {
      throw new OptimisticLockException(
          "Optimistic lock conflict on projection '%s/%s': expected version %d but %s"
              .formatted(
                  LogSanitizer.sanitizeForLog(name.value()),
                  LogSanitizer.sanitizeForLog(id),
                  expectedVersion,
                  current == null ? "no such row exists" : "was " + current.version()));
    }
    return new Stored(readModel, current.version() + 1);
  }

  private static <T> Page<T> page(List<T> all, PageRequest pageRequest) {
    int from = (int) Math.min((long) pageRequest.page() * pageRequest.size(), all.size());
    int to = Math.min(from + pageRequest.size(), all.size());
    return new Page<>(all.subList(from, to), all.size(), pageRequest.page(), pageRequest.size());
  }

  // ---- ProjectionRepository (autocommit face: what a BaseProjection uses outside a batch) ----

  @Override
  public <T> void save(ProjectionName projectionName, String id, T readModel) {
    validateWrite(projectionName, id, readModel);
    write(
        () ->
            table(projectionName)
                .compute(
                    id, (k, old) -> new Stored(readModel, old == null ? 0L : old.version() + 1)));
  }

  @Override
  public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
    validateInputs(projectionName, id);
    return read(
        () -> Optional.ofNullable(committedRow(projectionName, id)).map(s -> type.cast(s.value())));
  }

  /** Every row of {@code projectionName}, sorted by id. */
  @Override
  public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
    validateName(projectionName);
    return read(
        () -> {
          var t = tables.get(projectionName);
          return t == null
              ? List.<T>of()
              : t.values().stream().map(s -> type.cast(s.value())).toList();
        });
  }

  @Override
  public void delete(ProjectionName projectionName, String id) {
    validateInputs(projectionName, id);
    write(
        () -> {
          var t = tables.get(projectionName);
          if (t != null) {
            t.remove(id);
          }
        });
  }

  /** Pages the id-sorted rows; {@code pageRequest.sort()} is accepted and ignored (ids order). */
  @Override
  public <T> Page<T> findAll(
      ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
    return page(findAll(projectionName, type), pageRequest);
  }

  /** The lock mode is accepted and ignored: a single JVM map needs no row lock. */
  @Override
  public <T> Optional<Versioned<T>> findById(
      ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
    validateInputs(projectionName, id);
    return read(
        () ->
            Optional.ofNullable(committedRow(projectionName, id))
                .map(s -> new Versioned<>(type.cast(s.value()), s.version())));
  }

  /**
   * Updates an existing row at {@code expectedVersion}; a missing row is a conflict at every
   * expected version, as with the JDBC repository's {@code UPDATE}.
   */
  @Override
  public <T> void save(
      ProjectionName projectionName, String id, T readModel, long expectedVersion) {
    validateVersionedWrite(projectionName, id, readModel, expectedVersion);
    write(
        () -> {
          Stored next =
              versionedUpdate(
                  projectionName, id, readModel, expectedVersion, committedRow(projectionName, id));
          table(projectionName).put(id, next);
        });
  }

  // ---- extra accessors for assertions ----

  /**
   * The committed checkpoint of {@code name}, as any reader outside a batch sees it.
   *
   * @param name the projection
   * @return the committed checkpoint; {@link GlobalOffset#initial()} if none
   */
  public GlobalOffset committedOffset(ProjectionName name) {
    return read(() -> GlobalOffset.of(checkpoints.getOrDefault(name, Checkpoint.NONE).offset()));
  }

  /**
   * The leadership epoch stamped on the checkpoint of {@code name}.
   *
   * @param name the projection
   * @return the stamped epoch; {@code 0} if none
   */
  public long stampedEpoch(ProjectionName name) {
    return read(() -> checkpoints.getOrDefault(name, Checkpoint.NONE).epoch());
  }

  /**
   * {@code true} once any row was ever written under {@code name} (the in-memory "table exists").
   *
   * @param name the projection
   * @return whether {@code name} has a table
   */
  public boolean hasReadModels(ProjectionName name) {
    return read(() -> tables.containsKey(name));
  }

  /**
   * The number of committed rows of {@code name}.
   *
   * @param name the projection
   * @return the row count; {@code 0} if {@code name} has no table
   */
  public int rowCount(ProjectionName name) {
    return read(
        () -> {
          var t = tables.get(name);
          return t == null ? 0 : t.size();
        });
  }

  // ---- OffsetStore (the same checkpoint the processor advances) ----

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return committedOffset(projectionName);
  }

  /**
   * Monotonic: a backward or sideways move is a silent no-op, as in {@code PostgresOffsetStore}.
   */
  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    synchronized (lockFor(projectionName)) {
      Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
      if (offset.value() > cp.offset()) {
        write(() -> checkpoints.put(projectionName, new Checkpoint(offset.value(), cp.epoch())));
      }
    }
  }

  /** Unguarded rewind to offset 0; the stamped epoch is kept. */
  @Override
  public void reset(ProjectionName projectionName) {
    synchronized (lockFor(projectionName)) {
      Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
      write(() -> checkpoints.put(projectionName, new Checkpoint(0L, cp.epoch())));
    }
  }

  // ---- AtomicBatchProcessor ----

  @Override
  public boolean supportsFencing() {
    return true;
  }

  /** Monotonic: an older epoch is a no-op, and the offset never moves. */
  @Override
  public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
    if (fencingEpoch == 0L) {
      return;
    }
    synchronized (lockFor(projectionName)) {
      Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
      write(
          () ->
              checkpoints.put(
                  projectionName, new Checkpoint(cp.offset(), Math.max(cp.epoch(), fencingEpoch))));
    }
  }

  /**
   * Runs the updater against a transaction-scoped view under the projection's lock, then applies
   * its writes and moves the checkpoint together. {@code offsetStore} is ignored: the checkpoint
   * this processor advances is its own. The view refuses every read and write once this method has
   * committed or rolled back.
   */
  @Override
  public void executeAtomically(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      OffsetStore offsetStore) {
    var tx = new Transaction();
    try {
      synchronized (lockFor(projectionName)) {
        commit(projectionName, batch, newOffset, fencingEpoch, projectionUpdater, tx);
      }
    } finally {
      tx.close();
    }
    tx.runAfterCommit();
  }

  private void commit(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      Transaction tx) {
    Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
    if (fencingEpoch != 0L && fencingEpoch < cp.epoch()) {
      throw new ProjectionCommitFencedException(
          "Projection "
              + quoted(projectionName)
              + " commit fenced out: caller epoch "
              + fencingEpoch
              + " is below the stored epoch "
              + cp.epoch()
              + " — a newer leader has taken over. Rolling back the stale leader's write.");
    }
    if (!batch.isEmpty() && batch.getFirst().globalOffset().value() <= cp.offset()) {
      throw new ProjectionCommitFencedException(
          "Projection "
              + quoted(projectionName)
              + " batch starting at offset "
              + batch.getFirst().globalOffset().value()
              + " overlaps the committed checkpoint ("
              + cp.offset()
              + "). Rolling back so the read-model write is not applied a second time.");
    }
    projectionUpdater.update(tx); // throws → nothing below runs, the staged writes are dropped
    // Re-read like the SQL guard does at UPDATE time: the lock is reentrant, so the updater's own
    // thread could have moved the checkpoint or stamped an epoch through this object.
    Checkpoint current = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
    if (newOffset.value() <= current.offset()) {
      throw new ProjectionCommitFencedException(
          "Projection "
              + quoted(projectionName)
              + " checkpoint advance to "
              + newOffset.value()
              + " was rejected by the monotonic guard (stored offset "
              + current.offset()
              + "). Rolling back so the read-model write is not committed a second time.");
    }
    write(
        () -> {
          tx.apply();
          checkpoints.put(
              projectionName,
              new Checkpoint(newOffset.value(), Math.max(current.epoch(), fencingEpoch)));
        });
  }

  /** The transaction-scoped view handed to the updater: staged writes, read-your-writes. */
  private final class Transaction implements ProjectionRepository {
    private final Map<ProjectionName, Map<String, Stored>> staged = new HashMap<>();
    private final Map<ProjectionName, Set<String>> deleted = new HashMap<>();
    private final List<Runnable> afterCommit = new ArrayList<>();
    private volatile boolean closed;
    private volatile boolean ran;

    private void ensureOpen() {
      if (closed) {
        throw new IllegalStateException(
            "The transaction is over: a transaction-scoped repository can be used only inside"
                + " the updater it was handed to");
      }
    }

    private Optional<Stored> lookup(ProjectionName name, String id) {
      if (deleted.getOrDefault(name, Set.of()).contains(id)) {
        return Optional.empty();
      }
      var s = staged.getOrDefault(name, Map.of()).get(id);
      if (s != null) {
        return Optional.of(s);
      }
      return read(() -> Optional.ofNullable(committedRow(name, id)));
    }

    private void stage(ProjectionName name, String id, Stored row) {
      staged.computeIfAbsent(name, n -> new HashMap<>()).put(id, row);
      var gone = deleted.get(name);
      if (gone != null) {
        gone.remove(id);
      }
    }

    @Override
    public <T> void save(ProjectionName name, String id, T readModel) {
      ensureOpen();
      validateWrite(name, id, readModel);
      long version = lookup(name, id).map(s -> s.version() + 1).orElse(0L);
      stage(name, id, new Stored(readModel, version));
    }

    @Override
    public <T> Optional<T> findById(ProjectionName name, String id, Class<T> type) {
      ensureOpen();
      validateInputs(name, id);
      return lookup(name, id).map(s -> type.cast(s.value()));
    }

    @Override
    public <T> List<T> findAll(ProjectionName name, Class<T> type) {
      ensureOpen();
      validateName(name);
      var merged = new TreeMap<String, Stored>();
      read(
          () -> {
            var t = tables.get(name);
            if (t != null) {
              merged.putAll(t);
            }
            return null;
          });
      merged.putAll(staged.getOrDefault(name, Map.of()));
      deleted.getOrDefault(name, Set.of()).forEach(merged::remove);
      return merged.values().stream().map(s -> type.cast(s.value())).toList();
    }

    @Override
    public void delete(ProjectionName name, String id) {
      ensureOpen();
      validateInputs(name, id);
      var rows = staged.get(name);
      if (rows != null) {
        rows.remove(id);
      }
      deleted.computeIfAbsent(name, n -> new HashSet<>()).add(id);
    }

    @Override
    public <T> Page<T> findAll(ProjectionName name, Class<T> type, PageRequest pageRequest) {
      return page(findAll(name, type), pageRequest);
    }

    @Override
    public <T> Optional<Versioned<T>> findById(
        ProjectionName name, String id, Class<T> type, LockMode lockMode) {
      ensureOpen();
      validateInputs(name, id);
      return lookup(name, id).map(s -> new Versioned<>(type.cast(s.value()), s.version()));
    }

    @Override
    public <T> void save(ProjectionName name, String id, T readModel, long expectedVersion) {
      ensureOpen();
      validateVersionedWrite(name, id, readModel, expectedVersion);
      stage(
          name,
          id,
          versionedUpdate(name, id, readModel, expectedVersion, lookup(name, id).orElse(null)));
    }

    @Override
    public Object writeTargetIdentity() {
      return InMemoryProjectionRepository.this.writeTargetIdentity();
    }

    /**
     * Queued until the commit and dropped on rollback; registered after the commit (from an action,
     * or through a view kept past the batch) it runs at once, as with the JDBC repository.
     */
    @Override
    public void afterCommit(Runnable action) {
      if (action == null) {
        throw new IllegalArgumentException("action cannot be null");
      }
      if (ran) {
        action.run();
      } else {
        afterCommit.add(action);
      }
    }

    void apply() {
      deleted.forEach(
          (name, ids) -> {
            var t = tables.get(name);
            if (t != null) {
              ids.forEach(t::remove);
            }
          });
      staged.forEach((name, rows) -> table(name).putAll(rows));
    }

    void close() {
      closed = true;
    }

    /**
     * Runs every queued action. A {@link RuntimeException} does not stop the rest; the first one is
     * rethrown once all have run, the later ones suppressed on it. An {@link Error} ends the run,
     * as with the JDBC repository.
     */
    void runAfterCommit() {
      ran = true;
      RuntimeException first = null;
      for (Runnable action : afterCommit) {
        try {
          action.run();
        } catch (RuntimeException e) {
          if (first == null) {
            first = e;
          } else if (e != first) {
            first.addSuppressed(e);
          }
        }
      }
      if (first != null) {
        throw first;
      }
    }
  }
}
