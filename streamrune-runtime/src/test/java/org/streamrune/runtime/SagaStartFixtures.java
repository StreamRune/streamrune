package org.streamrune.runtime;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
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
import org.streamrune.test.InMemoryEventStore;

/**
 * Start-path saga fixtures shared by the executor's genesis / idempotency / durability tests a
 * two-command start orchestrator, its correlating variant, an inbox-modelling command bus, and
 * envelope builders. Extracted verbatim from {@code SagaStartEventRetryLaterDurabilityTest} so the
 * crash-point tests and the durability pins share one orchestrator shape (keys, offsets and command
 * ids line up across the classes).
 */
final class SagaStartFixtures {

  private static final AggregateType STREAM_TYPE = AggregateType.of("test");

  static final SagaType TYPE = SagaType.fromClass(StartState.class);

  private SagaStartFixtures() {}

  record StartEvent(String orderId) implements DomainEvent {}

  /** A correlated event of the same saga. */
  record Advance(String orderId) implements DomainEvent {}

  record Step(String stepId) implements Command {}

  record Undo(String stepId) implements Command {}

  record StartState(SagaStatus status, String orderId) implements SagaState {}

  /** Start orchestrator: two forward commands ("step-1", "step-2") and one undo ("undo-1"). */
  static final class StartOrchestrator implements SagaOrchestrator<StartState> {
    @Override
    public Class<StartState> stateType() {
      return StartState.class;
    }

    @Override
    public StartState initialState(SagaId sagaId) {
      return new StartState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof StartEvent;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-1");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public StartState evolve(StartState state, EventEnvelope event) {
      return new StartState(SagaStatus.RUNNING, "order-1");
    }

    @Override
    public List<SagaCommand> handle(StartState state, EventEnvelope event) {
      return List.of(
          SagaCommand.of(new Step("step-1"), AggregateId.of("a")),
          SagaCommand.of(new Step("step-2"), AggregateId.of("b")));
    }

    @Override
    public List<SagaCommand> compensate(
        StartState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(SagaCommand.of(new Undo("undo-1"), AggregateId.of("a")));
    }
  }

  /**
   * {@link StartOrchestrator} that additionally correlates {@link Advance} events (no forward
   * commands) and records the offsets of the correlated events it genuinely consumed.
   */
  static final class CorrelatingStartOrchestrator implements SagaOrchestrator<StartState> {
    final List<Long> consumedCorrelatedOffsets =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    private final StartOrchestrator start = new StartOrchestrator();

    /** {@code evolve} throws for {@link Advance} events while set. */
    volatile boolean poisonCorrelated;

    /** {@code evolve} throws for {@link StartEvent} events while set. */
    volatile boolean poisonStart;

    /**
     * {@code handle} throws for {@link StartEvent} events while set — a poison AFTER the
     * genesis-pending row exists and BEFORE any dispatch.
     */
    volatile boolean poisonStartHandle;

    @Override
    public Class<StartState> stateType() {
      return StartState.class;
    }

    @Override
    public StartState initialState(SagaId sagaId) {
      return start.initialState(sagaId);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return start.isStartEvent(event);
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return start.extractSagaId(event);
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return event.event() instanceof Advance ? Optional.of(SagaId.of("saga-1")) : Optional.empty();
    }

    @Override
    public StartState evolve(StartState state, EventEnvelope event) {
      if (event.event() instanceof Advance) {
        if (poisonCorrelated) {
          throw new RuntimeException("simulated poison");
        }
        consumedCorrelatedOffsets.add(event.globalOffset().value());
        return state;
      }
      if (poisonStart && event.event() instanceof StartEvent) {
        throw new RuntimeException("simulated poison");
      }
      return start.evolve(state, event);
    }

    @Override
    public List<SagaCommand> handle(StartState state, EventEnvelope event) {
      if (poisonStartHandle && event.event() instanceof StartEvent) {
        throw new RuntimeException("simulated poison");
      }
      return event.event() instanceof Advance ? List.of() : start.handle(state, event);
    }

    @Override
    public List<SagaCommand> compensate(
        StartState state, Throwable failure, SagaCommand failedCommand) {
      return start.compensate(state, failure, failedCommand);
    }
  }

  /**
   * A process death, not a failure: an {@link Error} rather than an exception, so no {@code catch
   * (Exception)} on the way out — in particular the per-command catch in {@code
   * SagaCommandDispatch.compensateAndClassify} — can classify it and carry on. Everything the
   * process committed before it stays committed; nothing after it runs.
   */
  static final class SimulatedProcessCrash extends Error {
    SimulatedProcessCrash(String where) {
      super("simulated process crash after " + where);
    }
  }

  /**
   * Models the real command inbox: a committed key short-circuits; a thrown command is never keyed,
   * so it re-runs on redelivery. (Same shape as the {@code SagaForwardDispatchClassificationTest}
   * bus.)
   */
  static final class ScriptedBus implements CommandBus {
    private final java.util.Set<IdempotencyKey> committed = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
    private final Map<String, Supplier<RuntimeException>> always = new ConcurrentHashMap<>();
    private final Map<String, Supplier<RuntimeException>> once = new ConcurrentHashMap<>();
    private final java.util.Set<String> crashAfterCommit = ConcurrentHashMap.newKeySet();

    /**
     * Models {@code InboxRetentionSweeper} pruning every dedup key recorded so far — what an outage
     * longer than {@code streamrune.inbox.retention-max-age} does to an episode's keys. A later
     * dispatch under a pruned key MISSES the inbox and executes again.
     */
    void pruneCommittedKeys() {
      committed.clear();
    }

    /**
     * The process dies once, right AFTER {@code cmdId} executed and its key was committed ({@link
     * SimulatedProcessCrash}).
     */
    void crashOnceAfterCommit(String cmdId) {
      crashAfterCommit.add(cmdId);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return true; // lets SagaRunner.Builder accept this bus for the live schedule
    }

    void failAlways(String cmdId, Supplier<RuntimeException> failure) {
      always.put(cmdId, failure);
    }

    void failOnce(String cmdId, Supplier<RuntimeException> failure) {
      once.put(cmdId, failure);
    }

    int count(String cmdId) {
      return counts.getOrDefault(cmdId, new AtomicInteger()).get();
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (committed.contains(key)) {
        return ok();
      }
      String id =
          switch (command) {
            case Step s -> s.stepId();
            case Undo u -> u.stepId();
            default -> command.getClass().getSimpleName();
          };
      Supplier<RuntimeException> single = once.remove(id);
      if (single != null) {
        throw single.get();
      }
      Supplier<RuntimeException> repeated = always.get(id);
      if (repeated != null) {
        throw repeated.get();
      }
      counts.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet();
      committed.add(key);
      if (crashAfterCommit.remove(id)) {
        throw new SimulatedProcessCrash(id);
      }
      return ok();
    }

    private static CommandResult ok() {
      return new CommandResult(
          List.of(),
          StreamId.of(STREAM_TYPE, AggregateId.of("test")),
          Version.initial(),
          List.of());
    }
  }

  /** Appends {@code event} to {@code eventStore} and returns it as the store re-reads it. */
  static EventEnvelope appended(
      org.streamrune.test.InMemoryEventStore eventStore, EventEnvelope event) {
    var result = eventStore.append(event.streamId(), List.of(event), Version.initial());
    var offset = result.globalOffsets().get(0);
    return eventStore.readGlobalStream(GlobalOffset.of(offset.value() - 1), 1).get(0);
  }

  /** Appends {@code event} to a fresh stream and returns the stored envelope (real offset). */
  static EventEnvelope append(InMemoryEventStore eventStore, DomainEvent event) {
    var streamId = StreamId.of(STREAM_TYPE, AggregateId.of("stream-" + UUID.randomUUID()));
    var appended =
        eventStore.append(
            streamId,
            List.of(
                new EventEnvelope(
                    GlobalOffset.initial(),
                    streamId,
                    Version.initial(),
                    EventType.fromClass(event.getClass()),
                    event,
                    new EventMetadata(
                        EventId.of("evt-" + UUID.randomUUID()),
                        CommandId.of("cmd-" + UUID.randomUUID()),
                        null,
                        null,
                        CorrelationId.of("saga-1"),
                        null,
                        null,
                        Instant.now()))),
            Version.initial());
    return eventStore
        .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
        .get(0);
  }

  static EventEnvelope startEvent(long globalOffset) {
    var event = new StartEvent("order-1");
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(STREAM_TYPE, AggregateId.of("order-stream")),
        Version.initial(),
        new EventType("StartEvent"),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("saga-1"),
            null,
            null,
            Instant.now()));
  }

  /** A correlated {@link Advance} event at {@code globalOffset}. */
  static EventEnvelope correlatedEvent(long globalOffset) {
    var event = new Advance("order-1");
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        StreamId.of(STREAM_TYPE, AggregateId.of("advance-stream")),
        Version.initial(),
        new EventType("Advance"),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("saga-1"),
            null,
            null,
            Instant.now()));
  }
}
