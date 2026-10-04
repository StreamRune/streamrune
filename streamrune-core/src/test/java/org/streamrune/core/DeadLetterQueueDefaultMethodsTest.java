package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.CommandId;

/**
 * Verifies the {@link DeadLetterQueue} default methods fail fast: an implementation that does not
 * support retry tracking must error loudly when scheduled DLQ retry is enabled, never silently
 * no-op.
 */
class DeadLetterQueueDefaultMethodsTest {

  /** Minimal implementation that overrides only the abstract methods. */
  private static final class MinimalDeadLetterQueue implements DeadLetterQueue {

    @Override
    public void publish(DeadLetterPublishRequest request) {
      // no-op
    }

    @Override
    public List<DeadLetterEntry> read(int limit) {
      return List.of();
    }

    @Override
    public void discard(CommandId commandId) {
      // no-op
    }
  }

  @Test
  void updateAttemptsDefault_throwsUnsupportedOperation() {
    var dlq = new MinimalDeadLetterQueue();
    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> dlq.updateAttempts(CommandId.of("cmd_1"), 1, Instant.now()));
    assertTrue(ex.getMessage().contains("updateAttempts"), ex.getMessage());
    assertTrue(ex.getMessage().contains(MinimalDeadLetterQueue.class.getName()), ex.getMessage());
  }

  @Test
  void readRetryableDefault_throwsUnsupportedOperation() {
    var dlq = new MinimalDeadLetterQueue();
    var ex = assertThrows(UnsupportedOperationException.class, () -> dlq.readRetryable(3, 10));
    assertTrue(ex.getMessage().contains("readRetryable"), ex.getMessage());
    assertTrue(ex.getMessage().contains(MinimalDeadLetterQueue.class.getName()), ex.getMessage());
  }

  @Test
  void deleteOlderThanDefault_throwsUnsupportedOperation() {
    var dlq = new MinimalDeadLetterQueue();
    var ex =
        assertThrows(UnsupportedOperationException.class, () -> dlq.deleteOlderThan(Instant.now()));
    assertTrue(ex.getMessage().contains("deleteOlderThan"), ex.getMessage());
    assertTrue(ex.getMessage().contains(MinimalDeadLetterQueue.class.getName()), ex.getMessage());
  }

  @Test
  void countPendingDefault_throwsUnsupportedOperation() {
    var dlq = new MinimalDeadLetterQueue();
    var ex = assertThrows(UnsupportedOperationException.class, dlq::countPending);
    assertTrue(ex.getMessage().contains("countPending"), ex.getMessage());
    assertTrue(ex.getMessage().contains(MinimalDeadLetterQueue.class.getName()), ex.getMessage());
  }

  // ========== find(CommandId) default method ==========

  private static DeadLetterQueue.DeadLetterEntry entry(String id) {
    return new DeadLetterQueue.DeadLetterEntry(
        CommandId.of(id),
        "TestCmd",
        "{}",
        null,
        "Err",
        "boom",
        1,
        Instant.parse("2026-06-14T12:00:00Z"),
        Instant.parse("2026-06-14T12:00:01Z"),
        0,
        null,
        null,
        null,
        null,
        null);
  }

  /** Returns a fixed three-entry list from {@link #read(int)}; uses the default {@code find}. */
  private static final class FixedReadDeadLetterQueue implements DeadLetterQueue {
    @Override
    public void publish(DeadLetterPublishRequest request) {
      // no-op
    }

    @Override
    public List<DeadLetterEntry> read(int limit) {
      return List.of(entry("cmd-a"), entry("cmd-b"), entry("cmd-c"));
    }

    @Override
    public void discard(CommandId commandId) {
      // no-op
    }
  }

  @Test
  void findDefault_returnsMatchingEntry() {
    var dlq = new FixedReadDeadLetterQueue();

    Optional<DeadLetterQueue.DeadLetterEntry> found = dlq.find(CommandId.of("cmd-b"));

    assertTrue(found.isPresent(), "find must return the entry whose commandId matches");
    assertEquals(CommandId.of("cmd-b"), found.get().commandId());
  }

  @Test
  void findDefault_returnsEmptyWhenNoEntryMatches() {
    var dlq = new FixedReadDeadLetterQueue();

    Optional<DeadLetterQueue.DeadLetterEntry> found = dlq.find(CommandId.of("cmd-missing"));

    assertTrue(found.isEmpty(), "find must return empty when no entry has the given commandId");
  }

  @Test
  void findDefault_returnsEmptyForEmptyQueue() {
    // MinimalDeadLetterQueue.read(...) returns an empty list, so find never matches.
    var dlq = new MinimalDeadLetterQueue();

    assertTrue(dlq.find(CommandId.of("cmd-a")).isEmpty());
  }

  @Test
  void overridingFindImplementationIsUsedInsteadOfDefault() {
    var fixed = entry("override-1");
    var dlq =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            // no-op
          }

          @Override
          public List<DeadLetterEntry> read(int limit) {
            // The default find would scan this and never match "anything"; the override must win.
            throw new AssertionError("read must not be consulted when find is overridden");
          }

          @Override
          public void discard(CommandId commandId) {
            // no-op
          }

          @Override
          public Optional<DeadLetterEntry> find(CommandId commandId) {
            // An overriding impl (e.g. an indexed DB lookup) must win over the scanning default —
            // here it ignores read() entirely and always returns the same fixed entry.
            return Optional.of(fixed);
          }
        };

    Optional<DeadLetterQueue.DeadLetterEntry> found = dlq.find(CommandId.of("anything"));

    assertTrue(found.isPresent());
    assertEquals(CommandId.of("override-1"), found.get().commandId());
  }
}
