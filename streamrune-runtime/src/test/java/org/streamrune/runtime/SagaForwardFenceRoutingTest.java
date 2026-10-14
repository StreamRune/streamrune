package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
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
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * What a step does when the event store refuses one of its forward commands at the commit: the saga
 * left its forward phase while the step was in flight. The step goes on from the row it reads again
 * — it resumes the open episode under the episode's stamp, stops at a saga that has ended, reports
 * a stale view for a row that is gone, and quarantines the event when the row is still in its
 * forward phase (a store that broke the fence contract). It claims nothing of its own and is not
 * retried.
 *
 * <p>The executor runs over a bus that refuses one chosen command the way the event store does,
 * after a hook has changed the row as the path that opened the episode or ended the saga would.
 */
class SagaForwardFenceRoutingTest {

  record Advanced(String orderId) implements DomainEvent {}

  record Reserve(String orderId) implements Command {}

  record Ship(String orderId) implements Command {}

  record Release(String orderId) implements Command {}

  record Refund(String orderId) implements Command {}

  record State(SagaStatus status, String orderId) implements SagaState {}

  private static final String ORDER = "7";
  private static final SagaId SAGA = SagaId.of("order-saga-7");
  private static final SagaType TYPE = SagaType.fromClass(State.class);
  private static final long OFFSET = 41L;

  /** Every event issues a reservation and a shipment; compensation releases and refunds. */
  static final class OrderSaga implements SagaOrchestrator<State> {
    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, ORDER);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SAGA;
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return new State(SagaStatus.RUNNING, ORDER);
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return List.of(
          SagaCommand.of(new Reserve(ORDER), AggregateId.of(ORDER)),
          SagaCommand.of(new Ship(ORDER), AggregateId.of(ORDER)));
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(new Release(ORDER), AggregateId.of(ORDER)),
          SagaCommand.of(new Refund(ORDER), AggregateId.of(ORDER)));
    }
  }

  /** A dispatch the bus received: the command's class and its key. */
  record Dispatch(Class<?> command, IdempotencyKey key) {}

  /**
   * Executes every command (as a command that stored an event), except the chosen one: before it, a
   * hook changes the saga row, and then the command is refused as the event store refuses a fenced
   * forward command — thrown as it is, or wrapped.
   */
  static final class RefusingBus implements CommandBus {
    final List<Dispatch> dispatches = new CopyOnWriteArrayList<>();
    private Class<? extends Command> refused;
    private Runnable beforeTheRefusal = () -> {};
    private boolean wrapped;

    void refuse(Class<? extends Command> command, Runnable rowChange, boolean wrapIt) {
      this.refused = command;
      this.beforeTheRefusal = rowChange;
      this.wrapped = wrapIt;
    }

    List<Dispatch> of(Class<?> command) {
      return dispatches.stream().filter(d -> d.command() == command).toList();
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("saga dispatch is keyed");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      dispatches.add(new Dispatch(command.getClass(), key));
      if (command.getClass() == refused) {
        refused = null;
        beforeTheRefusal.run();
        var refusal = new SagaForwardFencedException(new SagaOrigin(TYPE, SAGA, 0));
        throw wrapped ? new RuntimeException("an interceptor wrapped it", refusal) : refusal;
      }
      return null;
    }
  }

  /** Counts the claims the executor writes through it. */
  static final class CountingStore extends ForwardingSagaStore {
    final AtomicInteger claims = new AtomicInteger();

    CountingStore(InMemorySagaStore rows) {
      super(rows);
    }

    @Override
    public void claimCompensating(
        SagaId id, SagaType type, SagaState state, long version, EventId episodeCause) {
      claims.incrementAndGet();
      super.claimCompensating(id, type, state, version, episodeCause);
    }
  }

  final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
  final InMemorySagaStore rows = new InMemorySagaStore(clock);
  final CountingStore store = new CountingStore(rows);
  final RefusingBus bus = new RefusingBus();
  final List<String> fencedSamples = new CopyOnWriteArrayList<>();
  final List<String> casConflictSamples = new CopyOnWriteArrayList<>();
  final StreamRuneMetrics metrics =
      new StreamRuneMetrics() {
        @Override
        public void recordSagaForwardCommandFenced(String sagaType) {
          fencedSamples.add(sagaType);
        }

        @Override
        public void recordSagaCasConflict(String sagaType) {
          casConflictSamples.add(sagaType);
        }
      };
  final SagaStepExecutor<State> executor =
      new SagaStepExecutor<>(new OrderSaga(), store, bus, metrics, null, clock);

  SagaForwardFenceRoutingTest() {
    executor.forwardFence(true);
  }

  private static EventEnvelope advanced() {
    StreamId streamId = StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + ORDER));
    return new EventEnvelope(
        GlobalOffset.of(OFFSET),
        streamId,
        Version.initial(),
        EventType.fromClass(Advanced.class),
        new Advanced(ORDER),
        new EventMetadata(
            EventId.of("evt-advanced"),
            CommandId.of("cmd-advanced"),
            null,
            null,
            CorrelationId.of("request-1"),
            null,
            null,
            Instant.now()));
  }

  /** A RUNNING saga row at {@code version}, read as the step reads it. */
  private Optional<LoadedSaga<State>> runningRowAt(long version) {
    rows.create(SAGA, TYPE, new State(SagaStatus.RUNNING, ORDER), SagaStatus.RUNNING);
    for (long v = 1; v < version; v++) {
      rows.update(SAGA, TYPE, new State(SagaStatus.RUNNING, ORDER), SagaStatus.RUNNING, v);
    }
    return rows.load(SAGA, TYPE, State.class);
  }

  private StepOutcome deliverAdvanced(Optional<LoadedSaga<State>> loaded) {
    return executor.execute(
        SAGA, loaded, new SagaTrigger.ForwardStep(advanced(), false, false, false, false));
  }

  private LoadedSaga<State> row() {
    return rows.load(SAGA, TYPE, State.class).orElseThrow();
  }

  /**
   * What a timeout claim of another process writes: the episode, stamped at the claim's version.
   */
  private Runnable anotherProcessClaims(long version) {
    return () ->
        rows.claimCompensating(SAGA, TYPE, new State(SagaStatus.RUNNING, ORDER), version, null);
  }

  @Test
  void aRefusalOverAStampedRow_resumesTheEpisodeUnderItsStamp() {
    var loaded = runningRowAt(1);
    bus.refuse(Ship.class, anotherProcessClaims(1), false);

    assertThat(deliverAdvanced(loaded)).isEqualTo(StepOutcome.COMPENSATED);

    assertThat(row().episodeVersion()).isEqualTo(2L);
    assertThat(bus.of(Release.class))
        .extracting(Dispatch::key)
        .containsExactly(new IdempotencyKey("saga:order-saga-7:episode:2:comp:0"));
    assertThat(bus.of(Refund.class))
        .extracting(Dispatch::key)
        .containsExactly(new IdempotencyKey("saga:order-saga-7:episode:2:comp:1"));
    assertThat(row().status()).isEqualTo(SagaStatus.COMPENSATED);
  }

  @Test
  void aRefusalOverATerminalRow_isSkippedHalted_andWritesNothing() {
    var loaded = runningRowAt(1);
    bus.refuse(
        Ship.class,
        () ->
            rows.update(
                SAGA, TYPE, new State(SagaStatus.COMPLETED, ORDER), SagaStatus.COMPLETED, 1),
        false);
    int callsBefore = store.calls().size();

    assertThat(deliverAdvanced(loaded)).isEqualTo(StepOutcome.SKIPPED_HALTED);

    assertThat(store.calls().subList(callsBefore, store.calls().size())).containsExactly("load");
    assertThat(row().status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(row().version()).isEqualTo(2L);
    assertThat(bus.of(Release.class)).isEmpty();
    assertThat(bus.of(Refund.class)).isEmpty();
  }

  @Test
  void aRefusalOverNoRow_reportsAStaleView() {
    var loaded = runningRowAt(1);
    bus.refuse(Ship.class, () -> rows.delete(SAGA, TYPE), false);

    assertThatThrownBy(() -> deliverAdvanced(loaded))
        .isInstanceOf(OptimisticLockException.class)
        .hasMessage(
            "Saga order-saga-7 was removed while its forward step was in flight; nothing more was"
                + " dispatched");
    assertThat(bus.of(Release.class)).isEmpty();
    assertThat(store.claims).hasValue(0);
    assertThat(casConflictSamples).as("a stale view is a conflict").containsExactly(TYPE.value());
  }

  @Test
  void aRefusalOverARecreatedRow_reportsAStaleView() {
    var loaded = runningRowAt(3);
    bus.refuse(
        Ship.class,
        () -> {
          rows.delete(SAGA, TYPE);
          rows.create(SAGA, TYPE, new State(SagaStatus.RUNNING, ORDER), SagaStatus.RUNNING);
        },
        false);

    assertThatThrownBy(() -> deliverAdvanced(loaded))
        .isInstanceOf(OptimisticLockException.class)
        .hasMessageContaining("was removed while its forward step was in flight");
    assertThat(row().version())
        .as("the other row is neither read for a stamp nor written")
        .isEqualTo(1L);
    assertThat(bus.of(Release.class)).isEmpty();
  }

  @Test
  void aRefusalOverAForwardRow_isPoison() {
    var loaded = runningRowAt(1);
    bus.refuse(Ship.class, () -> {}, false);

    assertThatThrownBy(() -> deliverAdvanced(loaded))
        .isInstanceOf(SagaPoisonException.class)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The event store refused a forward command of saga order-saga-7 as fenced, and the"
                + " saga row is in its forward phase (status RUNNING, no episode stamp): the store"
                + " does not keep the fence contract");
    assertThat(bus.of(Release.class)).as("never compensated").isEmpty();
    assertThat(store.claims).hasValue(0);
    assertThat(row().version()).isEqualTo(1L);
  }

  /**
   * A stamp on a row in a forward status (a row written outside the saga store's contract, by a
   * tool or by SQL of an application's own). The store refuses every forward command of it, so the
   * step must not run its forward loop again: it quarantines the event after one refusal.
   */
  private Runnable aToolStampsTheRowAndPutsItBackForward(long version, SagaStatus forward) {
    return () -> {
      rows.claimCompensating(SAGA, TYPE, new State(SagaStatus.RUNNING, ORDER), version, null);
      rows.update(SAGA, TYPE, new State(forward, ORDER), forward, version + 1);
    };
  }

  private void assertQuarantinedAfterOneRefusal(Runnable step, String status) {
    assertThatThrownBy(step::run)
        .isInstanceOf(SagaPoisonException.class)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "The event store refused a forward command of saga order-saga-7 as fenced, and the"
                + " saga row is neither in a compensation episode the step can resume nor ended"
                + " (status "
                + status
                + ", episode stamp 2): the row was written outside the saga store's contract");
    assertThat(bus.of(Ship.class)).as("the forward loop is not run again").hasSize(1);
    assertThat(bus.of(Reserve.class)).hasSize(1);
    assertThat(bus.of(Release.class)).as("never compensated").isEmpty();
    assertThat(fencedSamples).containsExactly(TYPE.value());
    assertThat(store.claims).hasValue(0);
  }

  @Test
  void aRefusalOverAStampedRunningRow_isPoison_andDispatchesNothingMore() {
    var loaded = runningRowAt(1);
    bus.refuse(Ship.class, aToolStampsTheRowAndPutsItBackForward(1, SagaStatus.RUNNING), false);

    assertQuarantinedAfterOneRefusal(() -> deliverAdvanced(loaded), "RUNNING");
    assertThat(row().version()).as("nothing written").isEqualTo(3L);
  }

  @Test
  void aRefusalOverAStampedStartedRow_isPoison_andDispatchesNothingMore() {
    rows.createGenesisPending(
        SAGA, TYPE, new State(SagaStatus.STARTED, ORDER), SagaStatus.STARTED, false);
    var loaded = rows.load(SAGA, TYPE, State.class);
    bus.refuse(Ship.class, aToolStampsTheRowAndPutsItBackForward(1, SagaStatus.STARTED), false);

    assertQuarantinedAfterOneRefusal(
        () ->
            executor.execute(
                SAGA, loaded, new SagaTrigger.ForwardStep(advanced(), true, false, false, false)),
        "STARTED");
  }

  @Test
  void aRefusalOverARowFaultedInAForwardStatusWithAStamp_isPoisonOnAReplayFeed() {
    var loaded = runningRowAt(1);
    bus.refuse(
        Ship.class,
        () -> {
          aToolStampsTheRowAndPutsItBackForward(1, SagaStatus.RUNNING).run();
          rows.markFaulted(SAGA, TYPE, 3);
        },
        false);

    assertQuarantinedAfterOneRefusal(
        () ->
            executor.execute(
                SAGA, loaded, new SagaTrigger.ForwardStep(advanced(), false, true, true, false)),
        "FAULTED");
    assertThat(row().preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void aWrappedRefusal_isRoutedTheSame() {
    var loaded = runningRowAt(1);
    bus.refuse(Ship.class, anotherProcessClaims(1), true);

    assertThat(deliverAdvanced(loaded)).isEqualTo(StepOutcome.COMPENSATED);

    assertThat(bus.of(Release.class))
        .extracting(Dispatch::key)
        .containsExactly(new IdempotencyKey("saga:order-saga-7:episode:2:comp:0"));
    assertThat(store.claims).hasValue(0);
  }

  @Test
  void aRefusal_claimsNothingOfItsOwn_andIsNotRetryLater() {
    var loaded = runningRowAt(1);
    bus.refuse(Ship.class, anotherProcessClaims(1), false);

    StepOutcome outcome = deliverAdvanced(loaded);

    assertThat(outcome).isEqualTo(StepOutcome.COMPENSATED);
    assertThat(store.claims).as("no claim of the step's own").hasValue(0);
    assertThat(bus.of(Ship.class)).as("the refused command is not dispatched again").hasSize(1);
    assertThat(bus.of(Reserve.class)).hasSize(1);
    assertThat(store.calls()).doesNotContain("applyEvent");
  }

  @Test
  void theRefusalIsFoundAnywhereInABoundedCauseChain() {
    var refusal = new SagaForwardFencedException(new SagaOrigin(TYPE, SAGA, 0));
    Throwable at50 = refusal;
    for (int i = 1; i < 50; i++) {
      at50 = new RuntimeException("frame " + i, at50);
    }
    Throwable at51 = new RuntimeException("frame 50", at50);
    var cyclic = new RuntimeException("a cause chain that loops");
    var loop = new RuntimeException("back", cyclic);
    cyclic.initCause(loop);

    assertThat(SagaCommandDispatch.isForwardFenced(refusal)).isTrue();
    assertThat(SagaCommandDispatch.isForwardFenced(at50)).as("50 frames deep").isTrue();
    assertThat(SagaCommandDispatch.isForwardFenced(at51)).as("past the depth cap").isFalse();
    assertThat(SagaCommandDispatch.isForwardFenced(cyclic)).isFalse();
    assertThat(SagaCommandDispatch.isForwardFenced(new IllegalStateException("other"))).isFalse();
  }

  @Test
  void aRefusal_countsOneSample_andLogsOneInfoLine() {
    var loaded = runningRowAt(1);
    bus.refuse(Ship.class, anotherProcessClaims(1), false);
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(SagaStepExecutor.class);
    var appender = new ListAppender<ILoggingEvent>();
    Level configured = logger.getLevel();
    logger.setLevel(Level.INFO); // the suite's root logs WARN and above
    appender.start();
    logger.addAppender(appender);
    try {
      deliverAdvanced(loaded);
    } finally {
      logger.detachAppender(appender);
      appender.stop();
      logger.setLevel(configured);
    }

    assertThat(fencedSamples).containsExactly(TYPE.value());
    List<ILoggingEvent> refusals =
        appender.list.stream()
            .filter(e -> e.getFormattedMessage().contains("was refused at its commit"))
            .toList();
    assertThat(refusals).hasSize(1);
    assertThat(refusals.getFirst().getLevel()).isEqualTo(Level.INFO);
    assertThat(refusals.getFirst().getFormattedMessage())
        .isEqualTo(
            "Saga "
                + TYPE.value()
                + " order-saga-7: forward command Ship (index 1) of the event at offset 41 was"
                + " refused at its commit: the saga left its forward phase while this step was in"
                + " flight. 1 earlier command(s) of the step were stored before that. The step goes"
                + " on from the saga row: it resumes the open compensation episode, or stops at a"
                + " saga that has ended.");
  }
}
