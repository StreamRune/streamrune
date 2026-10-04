package org.streamrune.test;

import java.util.concurrent.ConcurrentHashMap;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Thread-safe in-memory {@link OffsetStore} for unit tests. Returns {@link GlobalOffset#initial()}
 * for any projection that has not yet had an offset stored.
 *
 * <p>This is the offset store for a runner on {@link
 * org.streamrune.core.projection.AtomicBatchProcessor#nonAtomicAtLeastOnce()}. For a {@code
 * TRANSACTIONAL_LOCAL} runner pass the {@link InMemoryProjectionRepository} itself as the offset
 * store: its checkpoint is the one its processor advances.
 *
 * <p>Mirrors the production monotonic guard: {@link #saveOffset} rejects a backward/sideways move
 * as a silent no-op, so tests exercise the same semantics as the PostgreSQL/JDBC stores. A genuine
 * rewind must go through {@link #reset}, which bypasses the guard.
 */
public final class InMemoryOffsetStore implements OffsetStore {

  private final ConcurrentHashMap<String, GlobalOffset> offsets = new ConcurrentHashMap<>();

  /** Creates an empty store: every projection starts at {@link GlobalOffset#initial()}. */
  public InMemoryOffsetStore() {
    // Nothing to initialise: an absent name reads as the initial offset.
  }

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
  }

  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    // Monotonic guard parity with PostgresOffsetStore: only a strictly-forward move is stored.
    offsets.merge(
        projectionName.value(),
        offset,
        (existing, candidate) -> candidate.value() > existing.value() ? candidate : existing);
  }

  @Override
  public void reset(ProjectionName projectionName) {
    // Unguarded rewind — bypasses the monotonic guard so a reset actually rewinds to 0.
    offsets.put(projectionName.value(), GlobalOffset.initial());
  }
}
