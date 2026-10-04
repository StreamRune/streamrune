package org.streamrune.runtime;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

/**
 * Forwards every {@link SagaDeadLetterStore} call to a delegate; crash injection for the replayer's
 * durable writes. {@link #crashOnceOnDiscard} throws BEFORE the next typed discard delegates — the
 * "feed committed, discard not yet executed" point; {@link #crashOnceAfter} throws once right AFTER
 * the named write has committed (the mirror of {@link ForwardingSagaStore#crashOnceAfter}). {@link
 * #resolvedLookupUnsupported} makes {@link #findNullSagaEntriesByResolvedTarget} throw the SPI
 * default's {@link UnsupportedOperationException} (a third-party store that never overrode it).
 */
class ForwardingSagaDeadLetterStore implements SagaDeadLetterStore {
  private final SagaDeadLetterStore delegate;
  private boolean crashNextDiscard;
  private Consumer<String> afterWrite = method -> {};

  /**
   * When set, the resolved-target lookup AND the conditional shield clear throw like their SPI
   * defaults — a third-party store that overrode neither (fold impossible, shield never cleared).
   */
  volatile boolean resolvedLookupUnsupported;

  /** When set, every {@code findBySaga} throws it — a database blip on the backlog read. */
  volatile RuntimeException findBySagaFailure;

  ForwardingSagaDeadLetterStore(SagaDeadLetterStore delegate) {
    this.delegate = delegate;
  }

  /** The next typed {@code discard} throws {@link ForwardingSagaStore.SimulatedCrash} first. */
  void crashOnceOnDiscard() {
    crashNextDiscard = true;
  }

  /** Throws {@link ForwardingSagaStore.SimulatedCrash} once, right after the named write. */
  void crashOnceAfter(String method) {
    afterWrite =
        m -> {
          if (m.equals(method)) {
            afterWrite = x -> {};
            throw new ForwardingSagaStore.SimulatedCrash(method);
          }
        };
  }

  /**
   * The next {@code establishFirstReplayAnchor} delegates, then throws {@link
   * ForwardingSagaStore.SimulatedCrash} (after the anchor is established, before the feed).
   * Delegates to {@link #crashOnceAfter} rather than duplicating it — the thrown message becomes
   * {@code "simulated crash after establishFirstReplayAnchor"}, same as any other {@code
   * crashOnceAfter} call.
   */
  void crashOnceAfterAnchor() {
    crashOnceAfter("establishFirstReplayAnchor");
  }

  private void wrote(String method) {
    afterWrite.accept(method);
  }

  @Override
  public void publish(SagaDeadLetterEntry entry) {
    delegate.publish(entry);
    wrote("publish");
  }

  /**
   * Delegates; reports the seam as {@code "publish"} too — the seam means "the entry publish
   * committed", whichever variant the runner used (a named hold uses this one).
   */
  @Override
  public void publishShielded(SagaDeadLetterEntry entry) {
    delegate.publishShielded(entry);
    wrote("publish");
  }

  @Override
  public List<SagaDeadLetterEntry> findAll(int limit) {
    return delegate.findAll(limit);
  }

  @Override
  public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
    RuntimeException failure = findBySagaFailure;
    if (failure != null) {
      throw failure;
    }
    return delegate.findBySaga(sagaId);
  }

  @Override
  public Optional<SagaDeadLetterEntry> findNullSagaEntry(SagaType sagaType, GlobalOffset offset) {
    return delegate.findNullSagaEntry(sagaType, offset);
  }

  @Override
  public List<SagaDeadLetterEntry> findNullSagaEntriesByResolvedTarget(
      SagaType sagaType, SagaId targetSagaId) {
    if (resolvedLookupUnsupported) {
      throw new UnsupportedOperationException(
          "findNullSagaEntriesByResolvedTarget is not implemented by this SagaDeadLetterStore");
    }
    return delegate.findNullSagaEntriesByResolvedTarget(sagaType, targetSagaId);
  }

  @Override
  public boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset) {
    if (crashNextDiscard) {
      crashNextDiscard = false;
      throw new ForwardingSagaStore.SimulatedCrash("the feed, before discard");
    }
    boolean removed = delegate.discard(sagaId, sagaType, eventOffset);
    wrote("discard");
    return removed;
  }

  @Override
  public void establishFirstReplayAnchor(
      SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent) {
    delegate.establishFirstReplayAnchor(sagaId, sagaType, eventOffset, anchorIfAbsent);
    wrote("establishFirstReplayAnchor");
  }

  @Override
  public void setResolvedTarget(SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId) {
    delegate.setResolvedTarget(sagaType, eventOffset, targetSagaId);
    wrote("setResolvedTarget");
  }

  @Override
  public boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType) {
    if (resolvedLookupUnsupported) {
      throw new UnsupportedOperationException(
          "clearShieldIfDrained is not implemented by this SagaDeadLetterStore");
    }
    boolean cleared = delegate.clearShieldIfDrained(sagaId, sagaType);
    wrote("clearShieldIfDrained");
    return cleared;
  }

  @Override
  public int deleteOlderThan(Instant cutoff) {
    return delegate.deleteOlderThan(cutoff);
  }

  @Override
  public long countFaultedBacklog() {
    return delegate.countFaultedBacklog();
  }
}
