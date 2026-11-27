package org.streamrune.runtime.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.streamrune.core.Internal;
import org.streamrune.core.types.ProjectionName;

/**
 * The actions a projection's updater registers through the transaction-scoped repository's {@code
 * afterCommit} during one checkpoint transaction (a batch or a replay). They run, in registration
 * order, once the transaction's COMMIT has been issued, and are discarded if the transaction rolls
 * back before that.
 *
 * <p>{@code CacheAwareProjection} registers its query-cache eviction here. Run before the commit,
 * the eviction would let a query landing between the projection's writes and the commit read the
 * pre-batch rows (the batch's writes are invisible to other connections until the commit) and cache
 * them, and the cache would then serve the pre-batch read model until the entry's TTL ran out.
 *
 * <p>Crash and failure points:
 *
 * <ul>
 *   <li>Failure before the COMMIT is issued (an updater throw, a fence or overlap rejection, the
 *       monotonic offset guard): the transaction rolls back and the actions are discarded. The read
 *       model did not change, so whatever the cache holds still matches it.
 *   <li>The COMMIT throws: the outcome is unknown (the connection can drop after the database
 *       committed), so the actions run anyway. An unneeded eviction costs one cache miss; a missing
 *       one serves the pre-batch read model until the TTL.
 *   <li>An action throws a {@link RuntimeException}: logged at WARN on the repository's logger, and
 *       the remaining actions still run. It never fails the batch: the batch is committed, and
 *       reporting it as failed would make the runner retry it, have the overlap guard reject the
 *       retry, and drop the projection to standby.
 *   <li>An action throws an {@link Error}: the actions after it do not run. After a clean commit
 *       the Error propagates; the batch and its checkpoint stay committed. When the COMMIT or
 *       closing the connection threw, that exception is already in flight and stays the one thrown,
 *       with the Error added as suppressed: it reports the commit's unknown outcome, which the
 *       runner's retry resolves (the overlap guard rejects it if the database committed), and an
 *       Error from cache-eviction code must not hide it.
 *   <li>The process dies between the commit and the actions: the batch is committed and the
 *       checkpoint advanced, and the actions are lost. For the query cache that is safe: the cache
 *       lives in the same process's memory and dies with it, so the restarted process reads the
 *       committed read model on its first miss. An action whose effect outlives the process is not
 *       covered and must not rely on running.
 * </ul>
 *
 * <p>An action registered after the actions ran (from an action, or through a repository kept past
 * the transaction) runs at once: the writes are committed. One registered after a rollback is
 * discarded. Not thread-safe: the transaction-scoped repository, like the connection it is bound
 * to, belongs to the thread running the transaction.
 *
 * <p><b>Helper rules.</b> It touches no connection: the repository runs the transaction and calls
 * {@link #commitIssued} once its body is done, just before the COMMIT. It logs through the
 * repository's {@link Logger}, so the WARN keeps the repository's category. One instance serves one
 * transaction; it keeps no static mutable state.
 *
 * @hidden
 */
@Internal
public final class AfterCommitActions {

  private final ProjectionName projectionName;
  private final Logger log;
  private final List<Runnable> queued = new ArrayList<>();
  private boolean commitIssued;
  private boolean ran;

  /**
   * The actions of one transaction of {@code projectionName}.
   *
   * @param projectionName the projection, named in the WARN of a failing action
   * @param log the repository's logger
   */
  public AfterCommitActions(ProjectionName projectionName, Logger log) {
    this.projectionName = projectionName;
    this.log = log;
  }

  /**
   * Runs {@code commitUnderLock} with a new instance and then the after-commit actions it
   * registered: also when the COMMIT or closing the connection failed after the COMMIT was issued.
   * A transaction that rolled back discards them. The actions run with the transaction's exception
   * in flight, and an Error from one rides on it as suppressed instead of replacing it.
   *
   * @param projectionName the projection
   * @param log the repository's logger
   * @param commitUnderLock the whole transaction, which calls {@link #commitIssued} on the instance
   *     it is handed before its COMMIT
   */
  public static void runCheckpointTransaction(
      ProjectionName projectionName, Logger log, Consumer<AfterCommitActions> commitUnderLock) {
    var afterCommitActions = new AfterCommitActions(projectionName, log);
    try {
      commitUnderLock.accept(afterCommitActions);
    } catch (Throwable inFlight) {
      afterCommitActions.runIfCommitIssued(inFlight);
      throw inFlight;
    }
    afterCommitActions.runIfCommitIssued(null);
  }

  /**
   * Queues {@code action}, or runs it at once if the actions already ran.
   *
   * @param action the action
   */
  public void add(Runnable action) {
    if (ran) {
      runGuarded(action);
    } else {
      queued.add(action);
    }
  }

  /** Records that the COMMIT is being issued: from here on the actions run, whatever it reports. */
  public void commitIssued() {
    commitIssued = true;
  }

  /**
   * Runs the queued actions, in registration order, if the COMMIT was issued.
   *
   * @param inFlight the exception the transaction is throwing (the COMMIT or closing the connection
   *     failed after the COMMIT was issued), or {@code null} after a clean commit
   */
  public void runIfCommitIssued(Throwable inFlight) {
    if (!commitIssued) {
      return;
    }
    ran = true;
    try {
      for (Runnable action : queued) {
        runGuarded(action);
      }
    } catch (Error actionError) {
      // An Error ends the run. With nothing in flight it propagates, as it always has.
      // With the batch's exception in flight it must not replace it: that exception reports the
      // commit's unknown outcome, so the Error rides on it as suppressed. The JVM can throw one
      // preallocated Error instance more than once, and suppressing an exception on itself throws
      // an IllegalArgumentException that would replace the one in flight.
      if (inFlight == null) {
        throw actionError;
      }
      if (actionError != inFlight) {
        inFlight.addSuppressed(actionError);
      }
    }
    queued.clear();
  }

  private void runGuarded(Runnable action) {
    try {
      action.run();
    } catch (RuntimeException e) {
      log.warn(
          "Projection '{}' after-commit action failed; the batch stays committed and the"
              + " remaining after-commit actions still run",
          projectionName.value(),
          e);
    }
  }
}
