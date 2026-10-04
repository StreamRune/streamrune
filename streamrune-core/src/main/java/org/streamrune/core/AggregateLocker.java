package org.streamrune.core;

import java.time.Duration;
import org.streamrune.core.types.StreamId;

/**
 * Acquires a lock for one stream (an aggregate instance of one aggregate type) to prevent
 * concurrent modification. Two aggregate types sharing an id value have distinct lock keys; a
 * striped implementation may still map distinct keys onto one stripe. Implementations may use local
 * striped locks, PostgreSQL advisory locks, or a composite.
 */
public interface AggregateLocker {

  /**
   * Acquires a lock for the given stream, blocking up to {@code timeout}. The returned {@link
   * AutoCloseable} releases the lock when closed.
   *
   * <p><b>Timeout contract (frozen for 1.0.0).</b> {@code timeout} is the configured {@code
   * streamrune.lock-timeout} passed through verbatim, so every implementation must read it the same
   * way — otherwise moving from the in-process default locker to a distributed one silently changes
   * the behaviour of an unchanged configuration:
   *
   * <ul>
   *   <li>a <b>positive</b> duration is the upper bound on how long acquisition may wait, and it
   *       must be at least {@value #MIN_TIMEOUT_MILLIS} ms (see the granularity arm below);
   *   <li>{@link Duration#ZERO} means <b>do not wait</b>: make a single attempt and, if the lock is
   *       held elsewhere, fail immediately with {@link LockException};
   *   <li>a <b>negative</b> duration is never a valid StreamRune value and MUST be rejected with
   *       {@link IllegalArgumentException} — not clamped to zero, not substituted with a default.
   *       This is a configuration error, not a transient acquisition failure, so it deliberately
   *       does NOT go through the retryable {@link LockException} channel. All three framework
   *       integrations reject a negative {@code streamrune.lock-timeout} at startup, so this arm
   *       only fires for programmatic wiring.
   *   <li><b>The (0 ms, 1 ms) band.</b> A positive duration BELOW one millisecond is rejected on
   *       the same terms as a negative one. Lock timeouts are expressed in whole milliseconds end
   *       to end — PostgreSQL's {@code lock_timeout} takes an integer count of them, {@code
   *       Lock#tryLock} is called with them — so a smaller value cannot be honoured by any
   *       implementation, and every implementation was free to invent its own meaning for what it
   *       truncated to. They did: {@code PgAdvisoryLocker} produced {@code lock_timeout = '0ms'},
   *       which PostgreSQL reads as DISABLED and turned a 0.5 ms bound into an unbounded
   *       server-side wait, while {@code LocalStripedLocker} on the identical value made a single
   *       attempt. Rounding UP to 1 ms would have restored agreement, but only by silently
   *       substituting a number the operator did not write and only for as long as every
   *       implementation remembered to do it — the shape the negative arm above already refuses.
   * </ul>
   *
   * @param streamId the stream to lock
   * @param timeout how long acquisition may wait; see the timeout contract above
   * @throws IllegalArgumentException if {@code timeout} is negative, or positive and shorter than
   *     {@value #MIN_TIMEOUT_MILLIS} ms
   * @throws LockException if the lock cannot be acquired (e.g. the timeout elapses while another
   *     holder keeps it, or the acquisition is interrupted). Implementations throw this type or a
   *     subtype of it; the command bus classifies it as a transient, retryable failure.
   */
  AutoCloseable acquireLock(StreamId streamId, Duration timeout);

  /**
   * The finest lock timeout the framework can express. Every wait bound in the lock path is a whole
   * number of milliseconds, so this is a property of the contract rather than of any one backend.
   */
  int MIN_TIMEOUT_MILLIS = 1;

  /**
   * Enforces the timeout contract above. One rule in one place, called by every implementation at
   * acquire time and by all three integrations' {@code StreamRuneConfigValidator} at boot, so the
   * five enforcement points cannot drift into the divergence that two earlier defects both came
   * from — a per-implementation obligation to remember a rule is exactly what produced two
   * behaviours for one knob, twice.
   *
   * <p>{@code null} is deliberately NOT decided here: each caller owns its own null policy (the
   * validators see {@code null} when the property is unset, {@code PgAdvisoryLocker} substitutes
   * its documented default before calling this), and folding that decision in would hide the
   * difference rather than resolve it.
   *
   * @param timeout the timeout to validate; {@code null} is accepted and validates nothing
   * @param name how to name the offending value in the failure message — the property name ({@code
   *     "streamrune.lock-timeout"}) at boot, {@code "lock timeout"} at acquire time
   * @throws IllegalArgumentException if {@code timeout} is negative, or positive and shorter than
   *     {@value #MIN_TIMEOUT_MILLIS} ms
   */
  static void requireValidTimeout(Duration timeout, String name) {
    if (timeout == null) {
      return;
    }
    if (timeout.isNegative()) {
      throw new IllegalArgumentException(
          name + " must not be negative (use 0 for \"do not wait\"), got: " + timeout);
    }
    if (!timeout.isZero() && timeout.toMillis() < MIN_TIMEOUT_MILLIS) {
      throw new IllegalArgumentException(
          name
              + " must be either exactly 0 (\"do not wait\") or at least "
              + MIN_TIMEOUT_MILLIS
              + "ms — lock timeouts are expressed in whole milliseconds, and a shorter value"
              + " truncates to 0, which PostgreSQL reads as \"no timeout at all\"; got: "
              + timeout);
    }
  }
}
