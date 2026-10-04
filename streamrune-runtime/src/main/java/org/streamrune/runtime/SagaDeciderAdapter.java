package org.streamrune.runtime;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;

/**
 * Adapts a {@link SagaDecider} with externally-configured routing into a {@link SagaOrchestrator}.
 * Package-private — users interact via {@link SagaRunner.Builder}.
 */
final class SagaDeciderAdapter<S extends SagaState> implements SagaOrchestrator<S> {

  private final SagaDecider<S> decider;
  private final Predicate<EventEnvelope> startPredicate;
  private final Function<EventEnvelope, SagaId> idExtractor;
  private final Function<EventEnvelope, Optional<SagaId>> correlator;

  SagaDeciderAdapter(
      SagaDecider<S> decider,
      Predicate<EventEnvelope> startPredicate,
      Function<EventEnvelope, SagaId> idExtractor,
      Function<EventEnvelope, Optional<SagaId>> correlator) {
    this.decider = Objects.requireNonNull(decider, "decider");
    this.startPredicate = Objects.requireNonNull(startPredicate, "startPredicate");
    this.idExtractor = Objects.requireNonNull(idExtractor, "idExtractor");
    this.correlator = Objects.requireNonNull(correlator, "correlator");
  }

  /**
   * A compensation-only orchestrator: forward routing is never used on the timeout/sweeper path, so
   * the routers throw. Lets {@code SagaTimeoutRunner}'s bare {@link SagaDecider} back a {@code
   * SagaStepExecutor} without an SPI change (discrepancy: the executor needs a {@link
   * SagaOrchestrator}; the timeout runner holds a {@link SagaDecider}). Only {@link #stateType},
   * {@link #initialState}, {@link #evolve}, {@link #handle} and {@link #compensate} are reachable
   * on the timeout path — {@link #isStartEvent}, {@link #extractSagaId} and {@link #correlate}
   * throw {@link UnsupportedOperationException} because that path never routes a triggering event.
   */
  static <S extends SagaState> SagaOrchestrator<S> compensationOnly(SagaDecider<S> decider) {
    return new SagaDeciderAdapter<>(
        decider,
        e -> {
          throw new UnsupportedOperationException("timeout-path executor: no start routing");
        },
        e -> {
          throw new UnsupportedOperationException("timeout-path executor: no id extraction");
        },
        e -> {
          throw new UnsupportedOperationException("timeout-path executor: no correlation");
        });
  }

  @Override
  public Class<S> stateType() {
    return decider.stateType();
  }

  @Override
  public S initialState(SagaId sagaId) {
    return decider.initialState(sagaId);
  }

  @Override
  public boolean isStartEvent(EventEnvelope event) {
    return startPredicate.test(event);
  }

  @Override
  public SagaId extractSagaId(EventEnvelope event) {
    return idExtractor.apply(event);
  }

  @Override
  public Optional<SagaId> correlate(EventEnvelope event) {
    return correlator.apply(event);
  }

  @Override
  public S evolve(S state, EventEnvelope event) {
    return decider.evolve(state, event);
  }

  @Override
  public List<SagaCommand> handle(S state, EventEnvelope event) {
    return decider.handle(state, event);
  }

  @Override
  public List<SagaCommand> compensate(S state, Throwable failure, SagaCommand failedCommand) {
    return decider.compensate(state, failure, failedCommand);
  }
}
