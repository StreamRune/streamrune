package org.streamrune.integration;

import java.time.Duration;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates the effectively-once-compensation invariant across the framework auto-configs: the saga
 * compensation give-up horizon must be strictly less than the command-inbox retention window.
 *
 * <p><b>Why it matters.</b> Effectively-once compensation across a resume relies on a succeeded
 * compensation's command-inbox row surviving until the episode terminalizes. If a {@code
 * COMPENSATING} episode can dwell longer than the inbox retention window, the inbox-retention
 * sweeper prunes that succeeded-compensation key first; the next resume then re-dispatches it, the
 * inbox misses, and the compensation <b>re-executes</b> — a double refund / double stock release.
 * The bound this validator enforces is {@code streamrune.saga.compensation-retry-give-up-after}
 * (the compensation-retry <em>sweeper's</em> give-up horizon); the pruning horizon is {@code
 * streamrune.inbox.retention-max-age}. Keeping give-up strictly below retention guarantees a
 * <em>sweeper-driven</em> episode is always terminalized before its dedup keys can be swept.
 *
 * <p><b>The timeout-runner path is NOT auto-bounded.</b> The framework never builds a {@code
 * SagaTimeoutRunner} — those are application-declared beans the auto-configs only register for
 * health — so no auto-config sets {@code SagaTimeoutRunner.Builder.maxCompensatingDwell} (it
 * defaults to {@code null}, i.e. unbounded). An operator who switches the sweeper's re-drive off
 * ({@code streamrune.saga.compensation-retry-enabled=false}, sampling-only mode) and relies on a
 * {@code SagaTimeoutRunner} to drive compensation must set that runner's {@code
 * maxCompensatingDwell} strictly below the inbox retention window themselves; this validator only
 * bounds the sweeper. This matches {@code docs/guide/advanced/saga.md} and {@code
 * docs/guide/production.md}.
 *
 * <p>Emits a loud {@code WARNING} (with the exact configured values and the remediation) rather
 * than failing startup: the default configuration (give-up 1h, retention 7d) is safe, so this only
 * fires on an operator override, and on the sweeper path the misconfiguration cannot re-execute a
 * compensation — the {@code SagaCompensationRetrySweeper}'s key-age guard, wired with {@code
 * streamrune.inbox.retention-max-age}, faults an episode that outlives the inbox retention window
 * instead of re-driving it. The WARN makes the misconfiguration impossible to miss in the logs.
 */
public final class SagaRetentionValidator {

  private static final Logger LOG = LoggerFactory.getLogger(SagaRetentionValidator.class);

  private SagaRetentionValidator() {}

  /**
   * Validates {@code compensationRetryGiveUpAfter < inboxRetentionMaxAge}, logging a {@code
   * WARNING} with the exact values and remediation when the invariant is violated.
   *
   * @param compensationRetryGiveUpAfter {@code streamrune.saga.compensation-retry-give-up-after}
   * @param inboxRetentionMaxAge {@code streamrune.inbox.retention-max-age}
   * @return the warning message when the invariant is violated (also logged), otherwise {@link
   *     Optional#empty()}
   */
  public static Optional<String> validate(
      Duration compensationRetryGiveUpAfter, Duration inboxRetentionMaxAge) {
    if (compensationRetryGiveUpAfter == null || inboxRetentionMaxAge == null) {
      return Optional.empty();
    }
    // Zero/negative == inbox pruning disabled == dedup keys kept forever. A
    // COMPENSATING episode can then never outlive its keys, so NO give-up horizon is unsafe —
    // comparing against the raw value would emit a spurious, unsatisfiable WARN that giveUpAfter
    // "must be strictly less than" PT0S. Mirrors validateDeadLetterRetention's zero-handling.
    if (inboxRetentionMaxAge.isZero() || inboxRetentionMaxAge.isNegative()) {
      return Optional.empty();
    }
    if (compensationRetryGiveUpAfter.compareTo(inboxRetentionMaxAge) >= 0) {
      String message =
          "StreamRune saga config: streamrune.saga.compensation-retry-give-up-after ("
              + compensationRetryGiveUpAfter
              + ") must be strictly less than streamrune.inbox.retention-max-age ("
              + inboxRetentionMaxAge
              + "). Otherwise a stuck COMPENSATING saga can outlive its succeeded-compensation"
              + " inbox keys; the compensation-retry sweeper then FAULTs it without re-dispatch"
              + " (partial compensation to reconcile by hand), and any unguarded resume path (e.g."
              + " a SagaTimeoutRunner without maxCompensatingDwell / inboxRetentionMaxAge) could"
              + " re-execute an already-succeeded compensation (double refund / double stock"
              + " release). Lower compensation-retry-give-up-after (and any SagaTimeoutRunner"
              + " maxCompensatingDwell) below the inbox retention window.";
      LOG.warn(message);
      return Optional.of(message);
    }
    return Optional.empty();
  }

  /**
   * Fails fast (throws {@link IllegalStateException}) when the saga dead-letter retention window
   * can outlive the command-inbox retention window — a configuration in which a quarantined
   * dead-letter can survive longer than the compensation dedup keys its replay re-derives.
   *
   * <p><b>Why fail fast rather than warn.</b> A dead-letter that faulted mid-compensation is
   * replayable throughout the saga dead-letter retention window, and a replay re-derives the
   * <em>original</em> command-inbox dedup keys (durable {@code episode_version}). If those keys are
   * pruned first (inbox retention shorter than dead-letter retention), the replayed resume MISSES
   * the inbox and re-executes an already-succeeded compensation — a double refund / double stock
   * release. Unlike the compensation-retry give-up horizon (bounded by the always-on sweeper),
   * nothing at runtime bounds how long a FAULTED, quarantined saga waits before an operator replays
   * it, so a permissive config is a standing hazard rather than a transient one: the framework
   * refuses to start on it.
   *
   * <p>The framework's own defaults are safe ({@code saga.dead-letter-retention-max-age} defaults
   * to the inbox retention window), so this only fires on an operator override that raises
   * dead-letter retention above inbox retention (or disables inbox pruning below the dead-letter
   * window). The runtime {@code SagaDeadLetterReplayer}'s own {@code inboxRetentionMaxAge} guard is
   * the defense-in-depth runtime companion to this boot-time check.
   *
   * <p>A zero or negative duration disables the corresponding retention sweeper (unbounded
   * retention): a never-pruned inbox can never be outlived (always safe); a never-pruned saga
   * dead-letter store outlives any finite inbox window (fails fast).
   *
   * @param sagaDeadLetterRetentionMaxAge {@code streamrune.saga.dead-letter-retention-max-age}
   * @param commandInboxRetentionMaxAge {@code streamrune.inbox.retention-max-age}
   * @throws IllegalStateException when the dead-letter window can outlive the inbox window
   */
  public static void validateDeadLetterRetention(
      Duration sagaDeadLetterRetentionMaxAge, Duration commandInboxRetentionMaxAge) {
    if (sagaDeadLetterRetentionMaxAge == null || commandInboxRetentionMaxAge == null) {
      return;
    }
    // Zero/negative == pruning disabled == unbounded retention. A never-pruned inbox keeps its
    // dedup keys forever, so no dead-letter can ever outlive them — always safe.
    if (commandInboxRetentionMaxAge.isZero() || commandInboxRetentionMaxAge.isNegative()) {
      return;
    }
    boolean deadLetterUnbounded =
        sagaDeadLetterRetentionMaxAge.isZero() || sagaDeadLetterRetentionMaxAge.isNegative();
    if (deadLetterUnbounded
        || sagaDeadLetterRetentionMaxAge.compareTo(commandInboxRetentionMaxAge) > 0) {
      String configured =
          deadLetterUnbounded
              ? (sagaDeadLetterRetentionMaxAge + " (pruning disabled)")
              : "" + sagaDeadLetterRetentionMaxAge;
      throw new IllegalStateException(
          "StreamRune saga config: streamrune.saga.dead-letter-retention-max-age ("
              + configured
              + ") must not exceed streamrune.inbox.retention-max-age ("
              + commandInboxRetentionMaxAge
              + "). Otherwise a quarantined saga dead-letter can outlive the command-inbox dedup"
              + " keys its replay re-derives, so replaying a mid-compensation entry after the inbox"
              + " sweeper has pruned those keys RE-EXECUTES an already-succeeded compensation (double"
              + " refund / double stock release). Either shorten"
              + " streamrune.saga.dead-letter-retention-max-age or lengthen"
              + " streamrune.inbox.retention-max-age so the inbox window is at least as long.");
    }
  }
}
