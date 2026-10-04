package org.streamrune.runtime;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Test stand-ins for a <em>transactional</em> {@link AtomicBatchProcessor}.
 *
 * <p>The leadership-aware runner builders refuse to assemble a real (non-NOOP) {@link
 * org.streamrune.core.subscription.SubscriptionLeadership} against a processor that cannot honour
 * the fencing epoch. Production never has that pairing — every integration wires {@code
 * JdbcProjectionRepository}, which does fence — but tests that only exercise standby / resign /
 * health semantics used to lean on {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()}, which
 * cannot.
 *
 * <p>These helpers keep the behaviour those tests were written against and only make the double
 * honest about representing a fencing-capable processor.
 */
final class TestFencingProcessor {

  private TestFencingProcessor() {}

  /**
   * Claims fencing and hands {@code null} — it violates {@code AtomicBatchProcessorContract} (i) on
   * purpose. It exists to test the fencing guard and the leadership protocol alone; never register
   * a {@code TRANSACTIONAL_LOCAL} projection on a runner that uses it (the delivery policy would
   * accept a write-through projection as DECLARED and the test would run at-least-once without
   * saying so). For a transactional double use {@code
   * org.streamrune.test.InMemoryProjectionRepository}. Behaviourally it is {@link
   * AtomicBatchProcessor#nonAtomicAtLeastOnce()}'s process-then-save for any epoch — it just no
   * longer misrepresents a multi-replica deployment as running on a processor that ignores the
   * epoch.
   */
  static AtomicBatchProcessor fencingClaimOnly() {
    return new AtomicBatchProcessor() {
      @Override
      public boolean supportsFencing() {
        return true;
      }

      @Override
      public void executeAtomically(
          ProjectionName projectionName,
          List<EventEnvelope> batch,
          GlobalOffset newOffset,
          long fencingEpoch,
          ProjectionUpdater projectionUpdater,
          OffsetStore offsetStore) {
        projectionUpdater.update(null);
        offsetStore.saveOffset(projectionName, newOffset);
      }

      @Override
      public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
        // The in-memory test path has no checkpoint row to stamp.
      }
    };
  }

  /**
   * Wraps a processor written as a lambda (which cannot override a default method) so it declares
   * fencing support. {@code executeAtomically} delegates; the takeover stamp is a no-op — these
   * doubles have no checkpoint row, and the interface default now throws on a non-zero epoch, which
   * a lambda cannot override. Tests that assert ON the stamp use an anonymous class and override it
   * themselves.
   */
  static AtomicBatchProcessor fencing(AtomicBatchProcessor delegate) {
    return new AtomicBatchProcessor() {
      @Override
      public boolean supportsFencing() {
        return true;
      }

      @Override
      public void executeAtomically(
          ProjectionName projectionName,
          List<EventEnvelope> batch,
          GlobalOffset newOffset,
          long fencingEpoch,
          ProjectionUpdater projectionUpdater,
          OffsetStore offsetStore) {
        delegate.executeAtomically(
            projectionName, batch, newOffset, fencingEpoch, projectionUpdater, offsetStore);
      }

      @Override
      public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
        // No checkpoint row to stamp on the in-memory path.
      }
    };
  }
}
