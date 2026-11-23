package org.streamrune.test;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * In-memory {@link ProjectionDeadLetterStore} for unit tests.
 *
 * <p>Thread-safe via {@link CopyOnWriteArrayList}. Not suitable for production use.
 *
 * <p>Use {@link #all()} in assertions to inspect all stored entries directly.
 */
public final class InMemoryProjectionDeadLetterStore implements ProjectionDeadLetterStore {

  private final CopyOnWriteArrayList<ProjectionDeadLetterEntry> entries =
      new CopyOnWriteArrayList<>();

  @Override
  public synchronized void save(ProjectionDeadLetterEntry entry) {
    requireArgument(entry, "entry");
    for (int i = 0; i < entries.size(); i++) {
      ProjectionDeadLetterEntry stored = entries.get(i);
      if (stored.projectionName().equals(entry.projectionName())
          && stored.fromOffset().equals(entry.fromOffset())) {
        // The same batch failed again: refresh it, keeping attempts monotonic like the upsert of
        // the PostgreSQL store.
        entries.set(
            i,
            new ProjectionDeadLetterEntry(
                entry.projectionName(),
                entry.fromOffset(),
                entry.toOffset(),
                entry.batchSize(),
                entry.errorType(),
                entry.errorMessage(),
                Math.max(stored.attempts() + 1, entry.attempts()),
                entry.failedAt()));
        return;
      }
    }
    entries.add(entry);
  }

  private static void requireArgument(Object value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " is required");
    }
  }

  private static void requirePositiveLimit(int limit) {
    if (limit <= 0) {
      throw new IllegalArgumentException("limit must be > 0");
    }
  }

  private static final Comparator<ProjectionDeadLetterEntry> OLDEST_FIRST =
      Comparator.comparing(ProjectionDeadLetterEntry::failedAt)
          .thenComparingLong(e -> e.fromOffset().value());

  @Override
  public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
    requireArgument(projectionName, "projectionName");
    requirePositiveLimit(limit);
    return entries.stream()
        .filter(e -> e.projectionName().equals(projectionName))
        .sorted(OLDEST_FIRST)
        .limit(limit)
        .toList();
  }

  @Override
  public List<ProjectionDeadLetterEntry> readAll(int limit) {
    requirePositiveLimit(limit);
    return entries.stream().sorted(OLDEST_FIRST).limit(limit).toList();
  }

  @Override
  public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
    requireArgument(projectionName, "projectionName");
    requireArgument(fromOffset, "fromOffset");
    entries.removeIf(
        e -> e.projectionName().equals(projectionName) && e.fromOffset().equals(fromOffset));
  }

  /** {@inheritDoc} Total entries across all projections. */
  @Override
  public long countPending() {
    return entries.size();
  }

  /** Returns all entries regardless of projection. Use in test assertions. */
  public List<ProjectionDeadLetterEntry> all() {
    return List.copyOf(entries);
  }
}
