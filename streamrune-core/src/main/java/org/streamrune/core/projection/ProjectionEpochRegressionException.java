package org.streamrune.core.projection;

import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * Signals that {@link AtomicBatchProcessor#stampFencingEpoch} was asked to arm the fence at an
 * epoch that is <b>strictly below</b> the epoch already stored on the projection's checkpoint row.
 * The stamp is refused and nothing is written.
 *
 * <p><b>Why this needs its own signal.</b> The stamp is a monotonic upsert guarded by {@code WHERE
 * EXCLUDED.epoch > projection_offset.epoch}, so three quite different situations all produced the
 * same 0-row outcome: a same-epoch re-stamp (every scheduled tick — benign), a late stamp from a
 * leader that has since been superseded (benign), and "the epoch I was handed is BELOW the stored
 * one" (never benign). Discarding the row count made them indistinguishable, so {@code
 * stampTakeoverEpoch} reported success and the runner proceeded to CATCHING_UP — where every {@code
 * executeAtomically} is rejected by {@code rejectStaleEpoch}, and the runners classify that
 * rejection as a benign LEADERSHIP/CAS signal: abandon the batch without advancing, re-check
 * leadership (still legitimately held), go LIVE, convert every live rejection to {@code
 * LeadershipLostException} so the subscription does not advance either. Net effect: the read model
 * froze permanently, on every replica, at INFO log level, with the state reported LIVE and health
 * UP. The guard "did the row exist / did the stamp succeed" simply cannot answer "was this
 * observation produced by a HIGHER epoch".
 *
 * <p><b>How it is reached.</b> An operator clears the lease table to unstick leadership ({@code
 * DELETE FROM subscription_leases;} — a natural reflex, and what the framework's own
 * TwoRunnerLeadershipFailoverIT does) while {@code projection_offset.epoch} still carries the last
 * leader's epoch. The next acquire is a fresh INSERT, so the lease hands out epoch 1 against a
 * stored epoch of 57. Restoring a database snapshot of {@code subscription_leases} alone does the
 * same thing.
 *
 * <p><b>How to recover.</b> The two tables must be made consistent again: either raise the lease
 * epoch above the stored one, or reset the checkpoint's epoch to 0 ({@code UPDATE projection_offset
 * SET epoch = 0 WHERE projection_name = ...}) so the next takeover re-arms it — <em>only</em> with
 * every replica of that projection stopped, since a zeroed fence protects nothing while it is
 * zeroed.
 *
 * <p>Deliberately <b>not</b> a {@link ProjectionCommitFencedException}: that type means "a newer
 * leader legitimately superseded me, stand by and re-read", which is the response that turns this
 * condition into an invisible infinite loop.
 */
public class ProjectionEpochRegressionException extends RuntimeException {

  private final transient ProjectionName projectionName;
  private final long acquiredEpoch;
  private final long storedEpoch;

  /**
   * Creates an epoch-regression signal.
   *
   * @param projectionName the projection whose checkpoint row was to be stamped
   * @param acquiredEpoch the epoch the leadership lease handed out
   * @param storedEpoch the (strictly greater) epoch already on the checkpoint row
   */
  public ProjectionEpochRegressionException(
      ProjectionName projectionName, long acquiredEpoch, long storedEpoch) {
    super(
        "Projection '"
            + (projectionName != null
                ? LogSanitizer.sanitizeForLog(projectionName.value())
                : "null")
            + "' was handed leadership epoch "
            + acquiredEpoch
            + " but its checkpoint row already stores epoch "
            + storedEpoch
            + ". The lease table and the projection checkpoint have diverged — typically because"
            + " subscription_leases was cleared or restored independently of projection_offset."
            + " Every commit under epoch "
            + acquiredEpoch
            + " would be rejected by the epoch fence, so the projection cannot make progress."
            + " Recover by raising the lease epoch above "
            + storedEpoch
            + ", or (with every replica of this projection stopped) by resetting the checkpoint"
            + " epoch to 0 so the next takeover re-arms the fence.");
    this.projectionName = projectionName;
    this.acquiredEpoch = acquiredEpoch;
    this.storedEpoch = storedEpoch;
  }

  /**
   * The projection whose checkpoint row was to be stamped.
   *
   * @return the projection name; may be {@code null} if none was supplied
   */
  public ProjectionName projectionName() {
    return projectionName;
  }

  /**
   * The epoch the leadership lease handed out.
   *
   * @return the acquired (too-low) epoch
   */
  public long acquiredEpoch() {
    return acquiredEpoch;
  }

  /**
   * The epoch already stored on the checkpoint row.
   *
   * @return the stored epoch, strictly greater than {@link #acquiredEpoch()}
   */
  public long storedEpoch() {
    return storedEpoch;
  }
}
