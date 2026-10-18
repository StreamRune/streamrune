package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Advanced;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.KeyRecordingBus;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Nudged;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.OneEventDecider;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Opened;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.OrderSaga;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Refund;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Refunded;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Release;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Released;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Reserve;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Reserved;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Ship;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Shipped;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.State;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * The executor reports a compensation episode opened underneath a forward step that executed a
 * command: one WARN and one {@code streamrune.saga.compensation_under_forward_step} sample per
 * step. It reports that the episode was opened in the step's window, not the order in which the
 * step's command and the compensation were stored. The report changes no outcome: the step that
 * lost its write is run again in place on the row that won, so each delivery returns; each case
 * asserts the stored order and the row beside the report.
 *
 * <p>A second path — the timeout runner, or another node's delivery (a second {@link SagaRunner}
 * over the same stores) — runs inside a hook, on the delivery's thread, right before a chosen
 * forward command of the delivery under test. Nothing waits on a clock.
 */
class SagaCompensationUnderForwardStepSignalTest {

  private static final String ORDER = "42";
  private static final SagaId SAGA = SagaId.of("order-saga-" + ORDER);
  private static final SagaType TYPE = SagaType.fromClass(State.class);
  private static final Duration TIMEOUT = Duration.ofMinutes(5);

  /** The death of the process: an {@link Error}, which no saga code catches. */
  static final class ProcessDeath extends Error {
    ProcessDeath() {
      super("the process dies here", null, false, false);
    }
  }

  /** Counts the reports per saga type; {@code failing} makes the recording itself throw. */
  static final class ReportCountingMetrics implements StreamRuneMetrics {
    final List<String> reportedSagaTypes = new CopyOnWriteArrayList<>();
    volatile boolean failing;

    @Override
    public void recordSagaCompensationUnderForwardStep(String sagaType) {
      if (failing) {
        throw new IllegalStateException("the metrics backend is down");
      }
      reportedSagaTypes.add(sagaType);
    }
  }

  /**
   * A forwarding store with a hook before the event write and a switch that fails the next load.
   */
  static final class HookedSagaStore extends ForwardingSagaStore {
    private Runnable beforeEventWrite;
    volatile RuntimeException loadFailure;

    HookedSagaStore(InMemorySagaStore delegate) {
      super(delegate);
    }

    /** Runs once, right before the next event write reaches the store. */
    void runOnceBeforeTheEventWrite(Runnable action) {
      beforeEventWrite = action;
    }

    @Override
    public void applyEvent(
        SagaId id,
        SagaType type,
        SagaState state,
        SagaStatus status,
        long version,
        AppliedEvent applied) {
      if (beforeEventWrite != null) {
        Runnable once = beforeEventWrite;
        beforeEventWrite = null;
        once.run();
      }
      super.applyEvent(id, type, state, status, version, applied);
    }

    @Override
    public <S extends SagaState> Optional<LoadedSaga<S>> load(
        SagaId id, SagaType type, Class<S> stateType) {
      RuntimeException failure = loadFailure;
      if (failure != null) {
        loadFailure = null;
        throw failure;
      }
      return super.load(id, type, stateType);
    }
  }

  static final class Fixture implements AutoCloseable {
    final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemorySagaStore rows = new InMemorySagaStore(clock);
    final InMemoryEventStore eventStore =
        new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(rows);
    final HookedSagaStore sagaStore = new HookedSagaStore(rows);
    final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);
    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
    final OrderSaga saga;
    final ReportCountingMetrics metrics = new ReportCountingMetrics();
    private final AtomicInteger triggerIds = new AtomicInteger();
    final VirtualThreadCommandBus realBus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .register(
                AggregateType.of("reservation"),
                Reserve.class,
                c -> AggregateId.of(c.orderId()),
                reserve)
            .register(
                AggregateType.of("shipment"), Ship.class, c -> AggregateId.of(c.orderId()), ship)
            .register(
                AggregateType.of("release"),
                Release.class,
                c -> AggregateId.of(c.orderId()),
                release)
            .register(
                AggregateType.of("refund"), Refund.class, c -> AggregateId.of(c.orderId()), refund)
            .build();
    final KeyRecordingBus bus = new KeyRecordingBus(realBus);
    final SagaRunner<State> runner;

    /** A second listener on the same stores and bus: another node's delivery. */
    final SagaRunner<State> otherNode;

    final SagaTimeoutRunner<State> timeoutRunner;

    /** Whether the runners fence their forward commands at their commit. */
    private final boolean forwardFence;

    Fixture(boolean bornCompensating) {
      this(bornCompensating, false);
    }

    Fixture(boolean bornCompensating, boolean forwardFence) {
      this.forwardFence = forwardFence;
      saga = new OrderSaga(bornCompensating);
      runner = newRunner(SAGA);
      otherNode = newRunner(SAGA);
      timeoutRunner =
          SagaTimeoutRunner.<State>builder()
              .decider(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(TYPE)
              .clock(clock)
              .metrics(metrics)
              // A lock of its own: this timeout runner stands for one on another node, whose
              // claim the step lock of this node's runner does not hold back.
              .stepLock(new SagaStepLock())
              .build();
    }

    SagaRunner<State> newRunner(SagaId sagaId) {
      return SagaRunner.<State>builder()
          .decider(saga)
          .startWhen(e -> e.event() instanceof Opened)
          .extractSagaId(e -> sagaId)
          .correlateByEventData(
              (streamId, e) ->
                  e instanceof Advanced || e instanceof Nudged
                      ? Optional.of(sagaId)
                      : Optional.empty())
          .sagaStore(sagaStore)
          .commandBus(bus)
          .sagaDeadLetterStore(deadLetters)
          .clock(clock)
          .metrics(metrics)
          // Off unless a case asks for it: the report's WARN is the behaviour of a runner with the
          // fence off.
          .forwardFence(forwardFence)
          .build();
    }

    /** Appends a trigger event as a request stores it. */
    EventEnvelope append(DomainEvent event) {
      StreamId streamId =
          StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + UUID.randomUUID()));
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
                          EventId.of("evt-" + triggerIds.incrementAndGet()),
                          CommandId.of("cmd-" + UUID.randomUUID()),
                          null,
                          null,
                          CorrelationId.of("request-" + UUID.randomUUID()),
                          null,
                          null,
                          Instant.now()))),
              Version.initial());
      return eventStore
          .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
          .get(0);
    }

    void deliver(EventEnvelope event) {
      runner.asEventListener().onEvents(List.of(event));
    }

    void deliverOnTheOtherNode(EventEnvelope event) {
      otherNode.asEventListener().onEvents(List.of(event));
    }

    /** The saga passes its deadline and the timeout runner polls once. */
    void timeOut() {
      clock.advance(TIMEOUT.plusSeconds(1));
      timeoutRunner.pollOnce();
    }

    LoadedSaga<State> row() {
      return rows.load(SAGA, TYPE, State.class).orElseThrow();
    }

    /** The simple names of the stored forward and compensation events, in global order. */
    List<String> storedOrder() {
      return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
          .map(e -> (DomainEvent) e.event())
          .filter(
              e ->
                  e instanceof Reserved
                      || e instanceof Shipped
                      || e instanceof Released
                      || e instanceof Refunded)
          .map(e -> e.getClass().getSimpleName())
          .toList();
    }

    @Override
    public void close() {
      realBus.close();
    }
  }

  /** The executor's log events of {@code level} written while {@code body} runs. */
  private static List<String> executorLinesDuring(Level level, Runnable body) {
    var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SagaStepExecutor.class);
    Level before = logger.getLevel();
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    logger.setLevel(Level.DEBUG);
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(before);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  private static List<String> reportLines(List<String> warnings) {
    return warnings.stream().filter(line -> line.contains("underneath the forward step")).toList();
  }

  private static void assertReportedOnce(
      Fixture h, List<String> warnings, EventEnvelope event, int executedCommands) {
    assertThat(h.metrics.reportedSagaTypes)
        .as("one sample, tagged with the saga type and nothing else")
        .containsExactly(TYPE.value());
    assertThat(reportLines(warnings))
        .as("one WARN per occurrence")
        .singleElement()
        .satisfies(
            line -> {
              assertThat(line).contains(TYPE.value());
              assertThat(line).contains(SAGA.value());
              assertThat(line).contains("offset " + event.globalOffset().value());
              assertThat(line).contains("executed " + executedCommands + " forward command");
            });
  }

  private static void assertSilent(Fixture h, List<String> warnings) {
    assertThat(h.metrics.reportedSagaTypes).as("no sample").isEmpty();
    assertThat(reportLines(warnings)).as("no report line").isEmpty();
  }

  // ---- reported: a compensation claim under a forward step that executed a command ----

  /**
   * With the fence on, the step's command was stored before the claim (a later one would have been
   * refused at its commit), so the compensation covers it: the report is an INFO line that says so,
   * and the sample is counted as with the fence off.
   */
  @Test
  void withTheFenceOn_theReportIsAnInfoLine_andTheSampleIsCounted() {
    try (var h = new Fixture(false, true)) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.sagaStore.runOnceBeforeTheEventWrite(h::timeOut);

      var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SagaStepExecutor.class);
      Level before = logger.getLevel();
      var appender = new ListAppender<ILoggingEvent>();
      appender.start();
      logger.setLevel(Level.DEBUG);
      logger.addAppender(appender);
      try {
        h.deliver(opened);
      } finally {
        logger.detachAppender(appender);
        logger.setLevel(before);
        appender.stop();
      }

      assertThat(h.storedOrder()).containsExactly("Reserved", "Released", "Refunded");
      assertThat(h.metrics.reportedSagaTypes).containsExactly(TYPE.value());
      List<ILoggingEvent> report =
          appender.list.stream()
              .filter(e -> e.getFormattedMessage().contains("underneath the forward step"))
              .toList();
      assertThat(report)
          .singleElement()
          .satisfies(
              e -> {
                assertThat(e.getLevel()).isEqualTo(Level.INFO);
                assertThat(e.getFormattedMessage())
                    .contains(TYPE.value())
                    .contains(SAGA.value())
                    .contains("offset " + opened.globalOffset().value())
                    .contains("executed 1 forward command")
                    .contains("With the forward fence on")
                    .contains("compensate with cancel/void commands");
              });
    }
  }

  @Test
  void theTimeoutRunnerClaimsUnderAStartStep_theStepsForwardCommandIsReported() {
    try (var h = new Fixture(false)) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Reserve.class, h::timeOut);

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Released", "Refunded", "Reserved");
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertReportedOnce(h, warnings, opened, 1);
    }
  }

  @Test
  void theTimeoutRunnerClaimsUnderAStartStepWhoseEvolveYieldsCompensating_itIsReported() {
    try (var h = new Fixture(true)) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Reserve.class, h::timeOut);

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Released", "Refunded", "Reserved");
      assertReportedOnce(h, warnings, opened, 1);
    }
  }

  @Test
  void theTimeoutRunnerClaimsUnderACorrelatedStep_theStepsForwardCommandIsReported() {
    try (var h = new Fixture(false)) {
      h.deliver(h.append(new Opened(ORDER)));
      EventEnvelope advanced = h.append(new Advanced(ORDER));
      h.bus.runOnceBefore(Ship.class, h::timeOut);

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(advanced));

      assertThat(h.storedOrder()).containsExactly("Reserved", "Released", "Refunded", "Shipped");
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertReportedOnce(h, warnings, advanced, 1);
    }
  }

  @Test
  void theOutageEndsInTheRedeliveryThatStraddlesTheTimeoutClaim_thatRedeliveryIsReported() {
    try (var h = new Fixture(false)) {
      h.reserve.failure = () -> new IllegalStateException(new SQLException("connection reset"));
      EventEnvelope opened = h.append(new Opened(ORDER));

      List<String> duringTheOutage =
          executorLinesDuring(
              Level.WARN,
              () ->
                  assertThatThrownBy(() -> h.deliver(opened))
                      .isInstanceOf(IllegalStateException.class));
      assertSilent(h, duringTheOutage);

      h.reserve.failure = null;
      h.bus.runOnceBefore(Reserve.class, h::timeOut);
      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Released", "Refunded", "Reserved");
      assertReportedOnce(h, warnings, opened, 1);
    }
  }

  @Test
  void anotherNodesDeliveryClaimsUnderAStartStep_theStepsForwardCommandIsReported() {
    try (var h = new Fixture(false)) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      // The other node's command is rejected and it claims; this node's command then succeeds.
      h.bus.runOnceBefore(
          Reserve.class,
          () -> {
            h.reserve.failure = () -> new IllegalArgumentException("out of stock");
            h.deliverOnTheOtherNode(opened);
            h.reserve.failure = null;
          });

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Released", "Refunded", "Reserved");
      assertThat(h.row().genesisApplied()).isFalse();
      assertReportedOnce(h, warnings, opened, 1);
    }
  }

  @Test
  void theStepsOwnClaimIsLostToATimeoutClaim_theCommandsItExecutedAreReported() {
    try (var h = new Fixture(false)) {
      h.saga.startAlsoShips = true;
      h.ship.failure = () -> new IllegalArgumentException("address rejected");
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Ship.class, h::timeOut);

      // A start event's lost claim is not an error of the delivery.
      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Reserved", "Released", "Refunded");
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertReportedOnce(h, warnings, opened, 1);
    }
  }

  // ---- silent ----

  @Test
  void aTwinDeliveryWinsTheEventWrite_theRowRecordsTheEvent_nothingIsReported() {
    try (var h = new Fixture(false)) {
      h.saga.startAlsoShips = true;
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Ship.class, () -> h.deliverOnTheOtherNode(opened));

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Reserved", "Shipped");
      assertThat(h.row().genesisApplied()).isTrue();
      assertSilent(h, warnings);
    }
  }

  @Test
  void aTwinDeliveryWinsTheEventWriteThatStampsTheEpisode_nothingIsReported() {
    try (var h = new Fixture(true)) {
      h.saga.startAlsoShips = true;
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Ship.class, () -> h.deliverOnTheOtherNode(opened));

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      // The row carries a stamp, written by the twin's event write, and records the event; the
      // run again resumes the episode the twin opened and ends it.
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.row().episodeVersion()).isNotNull();
      assertThat(h.row().genesisApplied()).isTrue();
      assertSilent(h, warnings);
    }
  }

  /**
   * Silent because the row records an offset at or past this event's: the later event's write
   * raised {@code last_applied_offset}. The stamp is not what decides here; {@link
   * #aFaultWriteWinsTheWrite_theRowCarriesNoStamp_nothingIsReported} is the case it decides.
   */
  @Test
  void aLaterEventThatOpensNoEpisodeWinsTheWrite_theRowRecordsItsOffset_nothingIsReported() {
    try (var h = new Fixture(false)) {
      h.deliver(h.append(new Opened(ORDER)));
      EventEnvelope advanced = h.append(new Advanced(ORDER));
      EventEnvelope nudged = h.append(new Nudged(ORDER));
      h.bus.runOnceBefore(Ship.class, () -> h.deliverOnTheOtherNode(nudged));

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(advanced));

      assertThat(h.storedOrder()).containsExactly("Reserved", "Shipped");
      assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
      assertThat(h.row().episodeVersion()).isNull();
      assertSilent(h, warnings);
    }
  }

  /**
   * The step loses its write to a fault write — the poison handling of another delivery marks the
   * row {@code FAULTED} — and the row does not record the step's event. Nothing compensates, so
   * nothing is reported: the row carries no episode stamp.
   */
  @Test
  void aFaultWriteWinsTheWrite_theRowCarriesNoStamp_nothingIsReported() {
    try (var h = new Fixture(false)) {
      h.deliver(h.append(new Opened(ORDER)));
      EventEnvelope advanced = h.append(new Advanced(ORDER));
      h.bus.runOnceBefore(
          Ship.class, () -> assertThat(h.rows.markFaulted(SAGA, TYPE, h.row().version())).isTrue());

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(advanced));

      assertThat(h.storedOrder()).containsExactly("Reserved", "Shipped");
      assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(h.row().episodeVersion()).as("no episode was opened").isNull();
      assertThat(h.row().lastAppliedOffset())
          .as("the row does not record the step's event")
          .isLessThan(advanced.globalOffset().value());
      assertSilent(h, warnings);
    }
  }

  @Test
  void theCommandIsAnsweredFromTheInbox_theTimeoutClaimsUnderTheRedelivery_nothingIsReported() {
    try (var h = new Fixture(false)) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.sagaStore.runOnceBeforeTheEventWrite(
          () -> {
            throw new ProcessDeath();
          });
      assertThatThrownBy(() -> h.deliver(opened)).isInstanceOf(ProcessDeath.class);
      assertThat(h.storedOrder()).containsExactly("Reserved");

      // The redelivery executes nothing: the inbox answers for the command of the dead attempt.
      h.bus.runOnceBefore(Reserve.class, h::timeOut);
      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Reserved", "Released", "Refunded");
      assertSilent(h, warnings);
    }
  }

  @Test
  void theStepsOwnClaimIsLostToATwinsClaimForTheSameEvent_nothingIsReported() {
    try (var h = new Fixture(false)) {
      h.saga.startAlsoShips = true;
      h.ship.failure = () -> new IllegalArgumentException("address rejected");
      EventEnvelope opened = h.append(new Opened(ORDER));
      // The twin dispatches the same commands under the same keys, is rejected at the same command
      // and claims first: the reservation was stored before its claim.
      h.bus.runOnceBefore(Ship.class, () -> h.deliverOnTheOtherNode(opened));

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Reserved", "Released", "Refunded");
      assertThat(h.row().episodeCauseEventId()).isEqualTo(opened.metadata().eventId());
      assertSilent(h, warnings);
    }
  }

  @Test
  void theStepsOwnClaimIsLostBeforeItExecutedAnyCommand_nothingIsReportedAndNothingIsReloaded() {
    try (var h = new Fixture(false)) {
      h.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Reserve.class, h::timeOut);

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Released", "Refunded");
      assertSilent(h, warnings);
      List<String> calls = h.sagaStore.calls();
      assertThat(calls.subList(calls.lastIndexOf("claimCompensating") + 1, calls.size()))
          .as("the store is not read after the step's lost claim")
          .doesNotContain("load");
    }
  }

  // ---- the report never changes what the runner does with the lost write ----

  @Test
  void theReloadFails_theLostWriteIsRunAgainRegardless_andTheFailureIsLoggedAtDebug() {
    try (var h = new Fixture(false)) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(
          Reserve.class,
          () -> {
            h.timeOut();
            h.sagaStore.loadFailure = new IllegalStateException("the saga store is unreachable");
          });

      List<String> debug = executorLinesDuring(Level.DEBUG, () -> h.deliver(opened));

      assertThat(h.storedOrder()).containsExactly("Released", "Refunded", "Reserved");
      assertThat(h.metrics.reportedSagaTypes).isEmpty();
      assertThat(debug)
          .as("the failed reload is a DEBUG line naming the saga")
          .anySatisfy(
              line -> {
                assertThat(line).contains(SAGA.value());
                assertThat(line).contains("could not be read");
              });
    }
  }

  @Test
  void theMetricsBackendThrows_theWarnIsWrittenAndTheLostWriteIsRunAgainRegardless() {
    try (var h = new Fixture(false)) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.metrics.failing = true;
      h.bus.runOnceBefore(Reserve.class, h::timeOut);

      List<String> warnings = executorLinesDuring(Level.WARN, () -> h.deliver(opened));

      assertThat(reportLines(warnings)).hasSize(1);
    }
  }

  @Test
  void theSagaIdIsSanitisedInTheReport() {
    // A saga id is a business id: bounded in length, not in charset.
    SagaId forged = SagaId.of("order-1\nWARN forged line");
    try (var h = new Fixture(false)) {
      SagaRunner<State> runner = h.newRunner(forged);
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Reserve.class, h::timeOut);

      List<String> warnings =
          executorLinesDuring(Level.WARN, () -> runner.asEventListener().onEvents(List.of(opened)));

      assertThat(reportLines(warnings))
          .singleElement()
          .satisfies(line -> assertThat(line.lines().count()).isEqualTo(1));
    }
  }
}
