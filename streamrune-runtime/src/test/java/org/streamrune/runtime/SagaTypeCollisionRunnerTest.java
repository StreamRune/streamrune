package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaTypeCollisionException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Two distinct saga types that derive the SAME {@link SagaId} must not let the second type silently
 * swallow its own start (nor read/overwrite the first type's state). The type-scoped {@code
 * SagaStore.load} makes the foreign row invisible to the second type, so its start takes the create
 * path and the store surfaces a loud {@link SagaTypeCollisionException} that {@code SagaRunner}
 * does NOT mis-swallow as a benign concurrent-create dedup.
 */
class SagaTypeCollisionRunnerTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  record Start(String id) implements DomainEvent {}

  record FooState(SagaStatus status, String id) implements SagaState {}

  record BarState(SagaStatus status, String id) implements SagaState {}

  /** Orchestrator whose start emits no forward commands, so the start goes straight to persist. */
  abstract static class NoCommandOrchestrator<S extends SagaState> implements SagaOrchestrator<S> {
    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Start;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      // Both types derive the SAME id from the business key — the collision.
      return SagaId.of("shared-" + ((Start) event.event()).id());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public List<SagaCommand> handle(S state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(S state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final class FooOrchestrator extends NoCommandOrchestrator<FooState> {
    @Override
    public Class<FooState> stateType() {
      return FooState.class;
    }

    @Override
    public FooState initialState(SagaId sagaId) {
      return new FooState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public FooState evolve(FooState state, EventEnvelope event) {
      return new FooState(SagaStatus.RUNNING, state.id());
    }
  }

  static final class BarOrchestrator extends NoCommandOrchestrator<BarState> {
    @Override
    public Class<BarState> stateType() {
      return BarState.class;
    }

    @Override
    public BarState initialState(SagaId sagaId) {
      return new BarState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public BarState evolve(BarState state, EventEnvelope event) {
      return new BarState(SagaStatus.RUNNING, state.id());
    }
  }

  static final class NoopBus implements CommandBus {
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return ok();
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  private EventEnvelope startEnvelope(String id, long offset) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType("Start"),
        new Start(id),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of("unrelated"),
            null,
            null,
            Instant.now()));
  }

  InMemorySagaStore sagaStore;
  SagaRunner<FooState> fooRunner;
  SagaRunner<BarState> barRunner;

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    var bus = new NoopBus();
    fooRunner =
        SagaRunner.<FooState>builder()
            .orchestrator(new FooOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();
    barRunner =
        SagaRunner.<BarState>builder()
            .orchestrator(new BarOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();
  }

  @Test
  void secondTypeSharingSagaId_failsLoudly_insteadOfSilentSwallow() {
    // Foo starts and creates the shared saga id.
    fooRunner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));
    assertThat(
            sagaStore.load(
                SagaId.of("shared-x"), SagaType.fromClass(FooState.class), FooState.class))
        .isPresent();

    // Bar's start on the SAME id must NOT be silently swallowed (pre-fix: load returned Foo's row,
    // processStartEvent dedup-returned, Bar never started) — it must fail loudly, naming both
    // types.
    assertThatThrownBy(() -> barRunner.asEventListener().onEvents(List.of(startEnvelope("x", 1L))))
        .isInstanceOf(SagaTypeCollisionException.class)
        .hasMessageContaining("FooState")
        .hasMessageContaining("BarState");
  }
}
