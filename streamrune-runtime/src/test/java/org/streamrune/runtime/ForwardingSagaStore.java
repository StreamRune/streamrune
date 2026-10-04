package org.streamrune.runtime;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;

/**
 * Forwards every call; {@link #afterWrite} runs after the named write completes (crash injection).
 */
class ForwardingSagaStore implements SagaStore {
  static final class SimulatedCrash extends RuntimeException {
    SimulatedCrash(String where) {
      super("simulated crash after " + where);
    }
  }

  private final SagaStore delegate;
  private Consumer<String> afterWrite = method -> {};

  ForwardingSagaStore(SagaStore delegate) {
    this.delegate = delegate;
  }

  /** Throws {@link SimulatedCrash} once, right after the named write has committed. */
  void crashOnceAfter(String method) {
    afterWrite =
        m -> {
          if (m.equals(method)) {
            afterWrite = x -> {};
            throw new SimulatedCrash(method);
          }
        };
  }

  private void wrote(String method) {
    afterWrite.accept(method);
  }

  @Override
  public void create(SagaId id, SagaType t, SagaState s, SagaStatus st, boolean deadLetterPending) {
    delegate.create(id, t, s, st, deadLetterPending);
    wrote("create");
  }

  @Override
  public void createGenesisPending(
      SagaId id, SagaType t, SagaState s, SagaStatus st, boolean deadLetterPending) {
    delegate.createGenesisPending(id, t, s, st, deadLetterPending);
    wrote("createGenesisPending");
  }

  @Override
  public void createFaulted(SagaId id, SagaType t, SagaState s) {
    delegate.createFaulted(id, t, s);
    wrote("createFaulted");
  }

  @Override
  public void update(SagaId id, SagaType t, SagaState s, SagaStatus st, long v) {
    delegate.update(id, t, s, st, v);
    wrote("update");
  }

  @Override
  public void applyEvent(
      SagaId id, SagaType t, SagaState s, SagaStatus st, long v, AppliedEvent a) {
    delegate.applyEvent(id, t, s, st, v, a);
    wrote("applyEvent");
  }

  @Override
  public void claimCompensating(SagaId id, SagaType t, SagaState s, long v) {
    delegate.claimCompensating(id, t, s, v);
    wrote("claimCompensating");
  }

  @Override
  public boolean markFaulted(SagaId id, SagaType t, long v) {
    boolean r = delegate.markFaulted(id, t, v);
    wrote("markFaulted");
    return r;
  }

  @Override
  public void setDeadLetterPending(SagaId id, SagaType t, boolean p) {
    delegate.setDeadLetterPending(id, t, p);
    wrote("setDeadLetterPending");
  }

  @Override
  public <S extends SagaState> Optional<LoadedSaga<S>> load(SagaId id, SagaType t, Class<S> type) {
    return delegate.load(id, t, type);
  }

  @Override
  public void delete(SagaId id, SagaType t) {
    delegate.delete(id, t);
  }

  @Override
  public List<SagaId> findTimedOut(SagaType t, Instant cutoff, int limit) {
    return delegate.findTimedOut(t, cutoff, limit);
  }

  @Override
  public List<SagaId> findByStatus(SagaType t, SagaStatus st, int limit) {
    return delegate.findByStatus(t, st, limit);
  }

  @Override
  public long countByStatus(SagaType t, SagaStatus st) {
    return delegate.countByStatus(t, st);
  }

  @Override
  public List<CompensatingSaga> findCompensating(SagaType t, Instant before, int limit) {
    return delegate.findCompensating(t, before, limit);
  }
}
