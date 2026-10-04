package org.streamrune.test;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
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
  public void save(ProjectionDeadLetterEntry entry) {
    Objects.requireNonNull(entry, "entry is required");
    entries.add(entry);
  }

  private static final Comparator<ProjectionDeadLetterEntry> OLDEST_FIRST =
      Comparator.comparing(ProjectionDeadLetterEntry::failedAt)
          .thenComparingLong(e -> e.fromOffset().value());

  @Override
  public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
    Objects.requireNonNull(projectionName, "projectionName is required");
    return entries.stream()
        .filter(e -> e.projectionName().equals(projectionName))
        .sorted(OLDEST_FIRST)
        .limit(limit)
        .toList();
  }

  @Override
  public List<ProjectionDeadLetterEntry> readAll(int limit) {
    return entries.stream().sorted(OLDEST_FIRST).limit(limit).toList();
  }

  @Override
  public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
    Objects.requireNonNull(projectionName, "projectionName is required");
    Objects.requireNonNull(fromOffset, "fromOffset is required");
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
