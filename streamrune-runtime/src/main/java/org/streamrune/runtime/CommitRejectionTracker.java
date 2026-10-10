package org.streamrune.runtime;

/**
 * Tells the commit rejection a re-read resolves from the one that repeats forever, for one
 * projection on one runner.
 *
 * <p>A runner reads its checkpoint from its {@code OffsetStore}, reads the batch after it and
 * commits through its {@code AtomicBatchProcessor}. When the processor rejects the commit through
 * the overlap or the monotonic guard, its own checkpoint is ahead of the one the runner read. If
 * both are the same row, the runner's next read returns the moved checkpoint and the next batch
 * commits. If they are not — a processor over one {@code DataSource}, an offset store over another
 * — the runner reads the same checkpoint again and is rejected again, without end.
 *
 * <p>So: the first rejection at a checkpoint is tolerated, and a second consecutive one at the same
 * checkpoint, with no commit in between, is reported as repeating. A rejection by the epoch fence
 * is never counted — the runner calls {@link #reset()} for it: it names a newer leader, and
 * standing by is the answer to it for as long as it lasts.
 *
 * <p>Thread-safe: a continuous runner's catch-up thread and its live poll thread both report here.
 */
final class CommitRejectionTracker {

  private boolean rejected;
  private long rejectedAtCheckpoint;

  /**
   * Forgets the last rejection: a batch or a checkpoint advance committed, or the processor's epoch
   * fence named a newer leader. The next rejection is a first one again.
   */
  synchronized void reset() {
    rejected = false;
  }

  /**
   * Records an overlap or monotonic rejection.
   *
   * @param checkpoint the checkpoint the runner's {@code OffsetStore} returns now
   * @return {@code true} when this is the second consecutive rejection at {@code checkpoint}
   */
  synchronized boolean repeatsAt(long checkpoint) {
    if (rejected && rejectedAtCheckpoint == checkpoint) {
      return true;
    }
    rejected = true;
    rejectedAtCheckpoint = checkpoint;
    return false;
  }
}
