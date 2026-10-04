package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Regression test: a start-event compensation that already SUCCEEDED must not execute again when
 * the (now {@code COMPENSATING}) saga is resumed.
 *
 * <p>The start-event path persists {@code COMPENSATING} (via {@code create()} at version 1) on a
 * transient compensation failure, making the saga resumable. But the start-event path dispatched
 * its compensations under the OFFSET-scoped {@code compensationKey}, while the resume paths ({@link
 * SagaStepExecutor}'s {@code ResumeCompensation} trigger and its {@code ForwardStep}
 * COMPENSATING-resume branch, and {@code SagaTimeoutRunner}) re-drive under the EPISODE-scoped
 * {@code episodeCompensationKey(corr, version=1, i)}. Those keyspaces are disjoint, so the command
 * inbox does NOT dedup: an already-succeeded start-event compensation (e.g. a refund) would EXECUTE
 * AGAIN on resume → double refund / double stock release.
 *
 * <p>The fix dispatches start-event compensations under the same episode-scoped key ({@code
 * createdVersion}), so a later resume reuses the SAME key and the inbox dedups.
 */
class SagaStartEventCompensationResumeTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ---- domain ----
  record Start(String id) implements DomainEvent {}

  /** A non-start correlated event used to trigger resume of the COMPENSATING saga. */
  record Poke(String id) implements DomainEvent {}

  interface TestCommand extends Command {
    String cmdId();
  }

  record DoWork(String cmdId) implements TestCommand {}

  record Undo(String cmdId) implements TestCommand {}

  record FakeSagaState(SagaStatus status, String id) implements SagaState {}

  /**
   * handle() emits two forward commands (do-1, do-2); do-2 fails transiently → compensate() runs.
   * compensate() deterministically returns [undo-1, undo-2] regardless of the failure (it is a pure
   * function of state, as both resume paths require).
   */
  static final class FakeOrchestrator implements SagaOrchestrator<FakeSagaState> {
    @Override
    public Class<FakeSagaState> stateType() {
      return FakeSagaState.class;
    }

    @Override
    public FakeSagaState initialState(SagaId sagaId) {
      return new FakeSagaState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Start;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((Start) event.event()).id());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corr = event.metadata().correlationId().value();
      return corr.startsWith("saga-") ? Optional.of(SagaId.of(corr)) : Optional.empty();
    }

    @Override
    public FakeSagaState evolve(FakeSagaState state, EventEnvelope event) {
      if (event.event() instanceof Start s) {
        return new FakeSagaState(SagaStatus.RUNNING, "saga-" + s.id());
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(FakeSagaState state, EventEnvelope event) {
      if (state.status() == SagaStatus.RUNNING && event.event() instanceof Start) {
        return List.of(
            SagaCommand.of(new DoWork("do-1"), AggregateId.of(state.id())),
            SagaCommand.of(new DoWork("do-2"), AggregateId.of(state.id())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        FakeSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(new Undo("undo-1"), AggregateId.of(state.id())),
          SagaCommand.of(new Undo("undo-2"), AggregateId.of(state.id())));
    }
  }

  /**
   * A CommandBus that models the real command inbox: a committed idempotency key short-circuits
   * WITHOUT re-executing the handler; a handler that throws does NOT commit its key (so a retry
   * re-runs it). Configurable transient failures via a per-command remaining-failure count.
   */
  static final class DedupingCommandBus implements CommandBus {
    private final java.util.Set<IdempotencyKey> committed = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> execCounts = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> failuresRemaining = new ConcurrentHashMap<>();

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    void failTransientlyNTimes(String cmdId, int n) {
      failuresRemaining.put(cmdId, new AtomicInteger(n));
    }

    int execCount(String cmdId) {
      return execCounts.getOrDefault(cmdId, new AtomicInteger(0)).get();
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (committed.contains(key)) {
        // Inbox dedup: previously-committed key returns the recorded result, no re-execution.
        return ok();
      }
      String id = ((TestCommand) command).cmdId();
      AtomicInteger remaining = failuresRemaining.get(id);
      if (remaining != null && remaining.get() > 0) {
        remaining.decrementAndGet();
        // Transient (non-DLQ-eligible) failure: key is NOT committed, so a resume re-runs it.
        throw new RuntimeException("transient failure for " + id);
      }
      execCounts.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
      committed.add(key);
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  private EventEnvelope startEnvelope(String id, long offset) {
    Start event = new Start(id);
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType("Start"),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of("unrelated-correlation"),
            null,
            null,
            Instant.now()));
  }

  private EventEnvelope pokeEnvelope(String sagaCorrelation, long offset) {
    Poke event = new Poke("poke");
    return new EventEnvelope(
        GlobalOffset.of(offset),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType("Poke"),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-2"),
            null,
            null,
            CorrelationId.of(sagaCorrelation),
            null,
            null,
            Instant.now()));
  }

  InMemorySagaStore sagaStore;
  DedupingCommandBus bus;
  SagaRunner<FakeSagaState> runner;
  SagaId sagaId;

  @BeforeEach
  void setUp() {
    sagaStore = new InMemorySagaStore();
    bus = new DedupingCommandBus();
    runner =
        SagaRunner.<FakeSagaState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();
    sagaId = SagaId.of("saga-x");
  }

  @Test
  void startEventCompensation_thatSucceeded_isNotReExecutedOnResume() {
    // do-2 (forward) fails once → triggers compensation on the start-event path.
    bus.failTransientlyNTimes("do-2", 1);
    // undo-2 (compensation) fails once → the start-event compensation classifies RETRY, so the saga
    // is persisted COMPENSATING (create-first genesis v1 + the compensation claim v2). undo-1
    // SUCCEEDS on the start-event path.
    bus.failTransientlyNTimes("undo-2", 1);

    // 1) Start event: forward do-2 fails → compensate([undo-1, undo-2]); undo-1 ok, undo-2 fails →
    // COMPENSATING persisted at version 2 (create-first genesis v1 + claim v2).
    runner.asEventListener().onEvents(List.of(startEnvelope("x", 0L)));

    var afterStart =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(afterStart.status())
        .as("transient start-event compensation failure leaves the saga COMPENSATING")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(afterStart.version()).as("create-first v1 + claim v2").isEqualTo(2L);
    assertThat(bus.execCount("undo-1"))
        .as("undo-1 ran exactly once on the start path")
        .isEqualTo(1);

    // 2) A redelivered correlated event drives the executor's ForwardStep COMPENSATING-resume
    // branch: undo-2 now succeeds. undo-1 must DEDUP against the start-event dispatch and NOT
    // execute a second time (the double-refund bug).
    runner.asEventListener().onEvents(List.of(pokeEnvelope("saga-x", 1L)));

    assertThat(bus.execCount("undo-1"))
        .as("the already-succeeded start-event compensation must NOT re-execute on resume")
        .isEqualTo(1);
    assertThat(bus.execCount("undo-2"))
        .as("undo-2 runs once (failed on start, succeeded on resume)")
        .isEqualTo(1);

    var afterResume =
        sagaStore
            .load(sagaId, SagaType.fromClass(FakeSagaState.class), FakeSagaState.class)
            .orElseThrow();
    assertThat(afterResume.status())
        .as("resume drives the episode to a terminal COMPENSATED")
        .isEqualTo(SagaStatus.COMPENSATED);
  }
}
