package org.streamrune.integration;

import java.time.Duration;

/**
 * Refuses a configuration in which a command dead-letter entry can outlive the command-inbox row
 * that deduplicates its replay.
 *
 * <p><b>Why it matters.</b> A dead-letter entry of a command that ran under an idempotency key is
 * replayed under that key, and the replay is a no-op exactly when the command inbox still holds the
 * key's row — the row the caller's own successful retry wrote. The inbox retention sweeper deletes
 * rows older than {@code streamrune.inbox.retention-max-age}; the dead-letter retention sweeper
 * deletes entries older than {@code streamrune.dead-letter.retention-max-age}. If the dead-letter
 * window is the longer one, an entry is still there, and can be retried by an operator, after its
 * row is gone: the replay misses the inbox and executes the command a second time (a second stock
 * reservation, a second payment capture). The retry runner refuses such a replay at run time
 * ({@code STALE_REPLAY_BLOCKED}); this check makes the configuration that produces it impossible to
 * start with, so the refusal stays the exception rather than every operator's daily routine.
 *
 * <p>The integrations run it whenever a {@code CommandInbox} and a {@code DeadLetterQueue} are both
 * configured — independent of {@code streamrune.dead-letter.enabled}, which switches only the
 * automatic retry, not the operator's. The defaults (both 7 days) satisfy it.
 *
 * <p>Zero or negative disables the corresponding sweeper (unbounded retention): a never-pruned
 * inbox can never be outlived (always valid); a never-pruned dead-letter queue outlives any finite
 * inbox window (refused). Mirrors {@link SagaRetentionValidator#validateDeadLetterRetention} for
 * the saga dead-letter store.
 */
public final class CommandDeadLetterRetentionValidator {

  private CommandDeadLetterRetentionValidator() {}

  /**
   * Fails when {@code deadLetterRetentionMaxAge} can outlive {@code inboxRetentionMaxAge}.
   *
   * @param deadLetterRetentionMaxAge {@code streamrune.dead-letter.retention-max-age}
   * @param inboxRetentionMaxAge {@code streamrune.inbox.retention-max-age}
   * @throws IllegalStateException naming both properties and values when the dead-letter window is
   *     longer than the inbox window, or unbounded while the inbox window is finite
   */
  public static void validate(Duration deadLetterRetentionMaxAge, Duration inboxRetentionMaxAge) {
    if (deadLetterRetentionMaxAge == null || inboxRetentionMaxAge == null) {
      return;
    }
    if (inboxRetentionMaxAge.isZero() || inboxRetentionMaxAge.isNegative()) {
      return;
    }
    boolean deadLetterUnbounded =
        deadLetterRetentionMaxAge.isZero() || deadLetterRetentionMaxAge.isNegative();
    if (deadLetterUnbounded || deadLetterRetentionMaxAge.compareTo(inboxRetentionMaxAge) > 0) {
      String configured =
          deadLetterUnbounded
              ? deadLetterRetentionMaxAge + " (pruning disabled)"
              : deadLetterRetentionMaxAge.toString();
      throw new IllegalStateException(
          "StreamRune dead-letter config: streamrune.dead-letter.retention-max-age ("
              + configured
              + ") must not exceed streamrune.inbox.retention-max-age ("
              + inboxRetentionMaxAge
              + "). Otherwise a dead-letter entry of a keyed command outlives the command-inbox row"
              + " that deduplicates its replay: once the inbox sweeper has pruned the row, a retry of"
              + " the entry executes the command a second time even though the caller's own retry"
              + " already completed it. Either shorten streamrune.dead-letter.retention-max-age or"
              + " lengthen streamrune.inbox.retention-max-age so the inbox window is at least as"
              + " long.");
    }
  }
}
