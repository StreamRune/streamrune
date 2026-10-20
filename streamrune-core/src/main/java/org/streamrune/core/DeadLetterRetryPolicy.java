package org.streamrune.core;

import java.time.Duration;

/**
 * Configurable retry policy for dead letter queue entries. Controls how many times a DLQ entry is
 * retried, the backoff between retries, whether exhausted entries are auto-discarded, and whether
 * entries of commands that ran without an idempotency key are retried automatically at all.
 *
 * <p><b>Unkeyed entries.</b> A command that ran unkeyed ({@code execute(command)}) has nothing the
 * command inbox can recognise it by. When its first attempt fails on infrastructure, the caller
 * sees the failure and may retry on its own; if that retry succeeds, an automatic replay of the
 * dead-letter entry is a second execution of the same business operation — for a relative command
 * ("add 50 to the stock") a second, silent application of the delta. The runner cannot tell the two
 * cases apart, so {@link #DEFAULT} leaves unkeyed entries to an operator ({@link
 * UnkeyedReplay#OPERATOR_ONLY}) and replays only keyed entries automatically: a keyed replay runs
 * under the caller's original key, which the inbox deduplicates against the caller's own retry.
 * Give commands an idempotency key ({@code execute(command, key)}) to have them retried
 * automatically.
 *
 * @param maxRetries maximum number of DLQ retry attempts (>= 1)
 * @param initialDelay delay before the first DLQ retry (required, must not be negative)
 * @param backoffMultiplier multiplier applied to delay on each subsequent retry (>= 1.0)
 * @param discardAfterMaxRetries whether to auto-discard entries that exhaust all retries
 * @param unkeyedReplay whether the scheduled retry replays entries of commands that ran without an
 *     idempotency key (required); see {@link UnkeyedReplay}
 */
public record DeadLetterRetryPolicy(
    int maxRetries,
    Duration initialDelay,
    double backoffMultiplier,
    boolean discardAfterMaxRetries,
    UnkeyedReplay unkeyedReplay) {

  /**
   * Five retries, 30 seconds initial delay doubling per retry, exhausted entries kept for
   * inspection, unkeyed entries left to an operator.
   */
  public static final DeadLetterRetryPolicy DEFAULT =
      new DeadLetterRetryPolicy(5, Duration.ofSeconds(30), 2.0, false, UnkeyedReplay.OPERATOR_ONLY);

  /**
   * What the scheduled retry does with an entry whose command ran without an idempotency key. An
   * operator's on-demand retry of a single entry replays it either way: that is the operator's
   * decision, made after checking whether the caller already retried.
   */
  public enum UnkeyedReplay {
    /**
     * The scheduled retry never replays an unkeyed entry; it stays in the queue until an operator
     * retries or discards it (or the retention sweeper prunes it). The safe choice: a caller that
     * already retried successfully is never followed by a second execution.
     */
    OPERATOR_ONLY,
    /**
     * The scheduled retry replays unkeyed entries like keyed ones. Safe only when every command
     * that can be dead-lettered is idempotent by construction — re-running it after a successful
     * retry by its caller changes nothing (an absolute "set", a create whose aggregate id the
     * caller chose). A relative command ("adjust by", "add", "capture") is applied twice.
     */
    AUTOMATIC
  }

  public DeadLetterRetryPolicy {
    if (maxRetries < 1) {
      throw new IllegalArgumentException("maxRetries must be >= 1");
    }
    if (initialDelay == null) {
      throw new IllegalArgumentException("initialDelay is required");
    }
    if (initialDelay.isNegative()) {
      throw new IllegalArgumentException("initialDelay must not be negative: " + initialDelay);
    }
    if (backoffMultiplier < 1.0) {
      throw new IllegalArgumentException("backoffMultiplier must be >= 1.0");
    }
    if (unkeyedReplay == null) {
      throw new IllegalArgumentException("unkeyedReplay is required (OPERATOR_ONLY or AUTOMATIC)");
    }
  }

  /**
   * Computes the delay for a given DLQ retry attempt (1-based). Jitter is always applied: the base
   * delay {@code initialDelay * backoffMultiplier^(attempt-1)} is multiplied by a random factor
   * uniform in {@code [0.5, 1.5)} ("equal jitter"). See {@link RetryPolicy#backoffDelay}.
   */
  public Duration delayForAttempt(int attempt) {
    return RetryPolicy.backoffDelay(initialDelay, backoffMultiplier, true, attempt);
  }
}
