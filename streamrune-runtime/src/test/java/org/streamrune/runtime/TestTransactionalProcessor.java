package org.streamrune.runtime;

import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Test stand-in for an {@link AtomicBatchProcessor} that claims {@link
 * AtomicBatchProcessor#transactional()} without {@link AtomicBatchProcessor#fencesCheckpoint()}. It
 * exists to test what the delivery policy and the runners do with a {@code TRANSACTIONAL_LOCAL}
 * registration; it does not prove the claim (that is {@code AtomicBatchProcessorContract}'s job),
 * and the runners refuse it under real leadership. For a double that honours the whole contract use
 * {@code org.streamrune.test.InMemoryProjectionRepository}; for a leadership double use {@link
 * TestFencingProcessor}.
 */
final class TestTransactionalProcessor {

  private TestTransactionalProcessor() {}

  /**
   * Wraps a processor written as a lambda (which cannot override a default method) so it declares
   * {@code transactional()}. {@code executeAtomically} delegates.
   *
   * @param delegate the processor whose {@code executeAtomically} runs
   * @return the wrapping processor
   */
  static AtomicBatchProcessor transactional(AtomicBatchProcessor delegate) {
    return new AtomicBatchProcessor() {
      @Override
      public boolean transactional() {
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
    };
  }
}
