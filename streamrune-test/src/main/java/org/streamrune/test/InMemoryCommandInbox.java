package org.streamrune.test;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.types.IdempotencyKey;

/**
 * In-memory {@link CommandInbox} for tests. Atomicity of claim+append is provided by the caller
 * (the in-memory event store appends under its global append lock).
 */
public final class InMemoryCommandInbox implements CommandInbox {

  private final ConcurrentHashMap<IdempotencyKey, InboxResult> rows = new ConcurrentHashMap<>();

  @Override
  public Optional<InboxResult> find(IdempotencyKey key) {
    return Optional.ofNullable(rows.get(key));
  }

  /**
   * Records a processed command. Called by {@link InMemoryEventStore#appendWithKey} under its lock.
   */
  public void record(InboxResult result) {
    rows.put(result.key(), result);
  }

  /**
   * Replaces the processed time of the row recorded under {@code key}, so a test can age it without
   * waiting.
   *
   * @throws IllegalStateException if no row is recorded under {@code key}
   */
  void setProcessedAt(IdempotencyKey key, Instant processedAt) {
    Objects.requireNonNull(processedAt, "processedAt is required");
    InboxResult aged =
        rows.computeIfPresent(
            key,
            (k, row) ->
                new InboxResult(
                    row.key(),
                    row.commandType(),
                    row.streamId(),
                    row.finalVersion(),
                    row.globalOffsets(),
                    processedAt));
    if (aged == null) {
      throw new IllegalStateException("no row is recorded under " + key);
    }
  }

  @Override
  public int deleteProcessedBefore(Instant cutoff) {
    int[] deleted = {0};
    rows.values()
        .removeIf(
            r -> {
              boolean old = r.processedAt().isBefore(cutoff);
              if (old) {
                deleted[0]++;
              }
              return old;
            });
    return deleted[0];
  }
}
