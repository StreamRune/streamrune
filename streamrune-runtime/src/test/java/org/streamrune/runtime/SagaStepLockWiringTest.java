package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.SagaStepLockFixture.TYPE;
import static org.streamrune.runtime.SagaStepLockFixture.sagaOf;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.State;
import org.streamrune.test.InMemorySagaStore;

/**
 * Which pieces share a {@link SagaStepLock}: the builders of {@link SagaRunner} and {@link
 * SagaTimeoutRunner}, and what a timeout runner that shares its lock with nothing does.
 */
class SagaStepLockWiringTest {

  private static final String ORDER = "1";

  record OtherState(SagaStatus status) implements SagaState {}

  /** A decider of another saga type, with a timeout. */
  static final class OtherSaga implements SagaDecider<OtherState> {
    @Override
    public Class<OtherState> stateType() {
      return OtherState.class;
    }

    @Override
    public OtherState initialState(SagaId sagaId) {
      return new OtherState(SagaStatus.STARTED);
    }

    @Override
    public OtherState evolve(OtherState state, EventEnvelope event) {
      return new OtherState(SagaStatus.RUNNING);
    }

    @Override
    public List<SagaCommand> handle(OtherState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OtherState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  private static SagaTimeoutRunner.Builder<State> timeoutRunnerOf(SagaStepLockFixture p) {
    return SagaTimeoutRunner.<State>builder()
        .decider(p.saga)
        .sagaStore(p.sagaStore)
        .commandBus(p.bus)
        .sagaType(TYPE)
        .clock(p.clock);
  }

  @Test
  void aTimeoutRunnerBuiltFromTheSagaRunner_takesTheRunnersLock() {
    try (var p = new SagaStepLockFixture()) {
      assertThat(p.runner.stepLock()).isSameAs(p.lock);
      EventEnvelope opened = p.append(new Opened(ORDER));
      SagaTimeoutRunner<State> timeouts = timeoutRunnerOf(p).sagaRunner(p.runner).build();
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            p.clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));
            timeouts.pollOnce();
          });

      p.deliver(opened);

      assertThat(timeouts.deferralsOf(sagaOf(ORDER))).isEqualTo(1);
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
    }
  }

  @Test
  void aSagaRunnerWithoutANamedLock_createsOneOfItsOwn() {
    try (var p = new SagaStepLockFixture()) {
      SagaRunner<State> first = newRunner(p);
      SagaRunner<State> second = newRunner(p);

      assertThat(first.stepLock()).isNotNull().isNotSameAs(second.stepLock());
    }
  }

  @Test
  void theSameLockNamedOnBothBuilders_isShared() {
    try (var p = new SagaStepLockFixture()) {
      // The fixture's runner is built with stepLock(p.lock).
      EventEnvelope opened = p.append(new Opened(ORDER));
      SagaTimeoutRunner<State> timeouts = timeoutRunnerOf(p).stepLock(p.lock).build();
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            p.clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));
            timeouts.pollOnce();
          });

      p.deliver(opened);

      assertThat(timeouts.deferralsOf(sagaOf(ORDER))).isEqualTo(1);
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
    }
  }

  /**
   * What a timeout runner on another node does, and what an application asks for when it gives the
   * timeout runner a lock of its own: the claim lands under the step. With the fence off, the
   * step's forward command is then stored after its compensation; {@code
   * SagaForwardFenceScenarioTest} has the same schedule with the fence on.
   */
  @Test
  void aTimeoutRunnerWithALockOfItsOwn_claimsUnderAStepInFlight() {
    // Pins the behaviour of a runner with the fence off.
    try (var p = new SagaStepLockFixture(false)) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      SagaTimeoutRunner<State> timeouts = timeoutRunnerOf(p).stepLock(new SagaStepLock()).build();
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            p.clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));
            timeouts.pollOnce();
          });

      p.deliver(opened);

      assertThat(timeouts.deferralsOf(sagaOf(ORDER))).isZero();
      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded", "Reserved");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void aTimeoutRunnerThatNamesNeitherARunnerNorALock_isRefused_withWhatToPassAndWhy() {
    try (var p = new SagaStepLockFixture()) {
      assertThatThrownBy(() -> timeoutRunnerOf(p).build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(TYPE.value())
          .hasMessageContaining("sagaRunner(theSagaRunner)")
          .hasMessageContaining("stepLock(new SagaStepLock())")
          .hasMessageContaining("claims underneath")
          .hasMessageContaining("stored after its compensation");
    }
  }

  @Test
  void theRefusalComesAfterEveryOtherCheckOfTheBuilder() {
    try (var p = new SagaStepLockFixture()) {
      // A builder with another fault is told that fault: the lock is asked for last.
      assertThatThrownBy(() -> timeoutRunnerOf(p).batchSize(0).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("batchSize");
    }
  }

  @Test
  void aTimeoutRunnerThatNamesARunnerOrALock_isBuiltAndStartsWithoutAWarning() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner<State> fromTheRunner =
          timeoutRunnerOf(p).sagaRunner(p.runner).pollInterval(Duration.ofHours(1)).build();
      SagaTimeoutRunner<State> withALockOfItsOwn =
          timeoutRunnerOf(p).stepLock(new SagaStepLock()).pollInterval(Duration.ofHours(1)).build();

      List<String> warnings =
          warningsDuring(
              () -> {
                startAndStop(fromTheRunner);
                startAndStop(withALockOfItsOwn);
              });

      assertThat(warnings).isEmpty();
    }
  }

  @Test
  void aRunnerOfAnotherSagaType_isRefused() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner.Builder<OtherState> builder =
          SagaTimeoutRunner.<OtherState>builder()
              .decider(new OtherSaga())
              .sagaStore(p.sagaStore)
              .commandBus(p.bus)
              .sagaType(SagaType.fromClass(OtherState.class));
      // An application often holds its runner as a SagaRunner<?> bean: the saga type is checked
      // when the timeout runner is built.
      SagaRunner<?> runnerOfAnotherSagaType = p.runner;

      assertThatThrownBy(() -> builder.sagaRunner(runnerOfAnotherSagaType).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(TYPE.value())
          .hasMessageContaining(OtherState.class.getName())
          .hasMessageContaining("same saga type");
    }
  }

  @Test
  void aRunnerOverAnotherStore_isRefused() {
    try (var p = new SagaStepLockFixture()) {
      assertThatThrownBy(
              () ->
                  timeoutRunnerOf(p)
                      .sagaStore(new InMemorySagaStore(p.clock))
                      .sagaRunner(p.runner)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("the same store")
          .hasMessageContaining("stepLock(");
    }
  }

  @Test
  void aRunnerAndALockTogether_areRefused_andSoIsNull() {
    try (var p = new SagaStepLockFixture()) {
      assertThatThrownBy(
              () -> timeoutRunnerOf(p).sagaRunner(p.runner).stepLock(new SagaStepLock()).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("name one");
      assertThatThrownBy(() -> timeoutRunnerOf(p).sagaRunner(null))
          .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> timeoutRunnerOf(p).stepLock(null))
          .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> timeoutRunnerOf(p).claimWait(null))
          .isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> SagaRunner.<State>builder().stepLock(null))
          .isInstanceOf(NullPointerException.class);
    }
  }

  private static SagaRunner<State> newRunner(SagaStepLockFixture p) {
    return SagaRunner.<State>builder()
        .decider(p.saga)
        .startWhen(e -> e.event() instanceof Opened)
        .extractSagaId(e -> sagaOf(((Opened) e.event()).orderId()))
        .sagaStore(p.sagaStore)
        .commandBus(p.bus)
        .sagaDeadLetterStore(p.deadLetters)
        .build();
  }

  private static void startAndStop(SagaTimeoutRunner<?> runner) {
    runner.start();
    runner.stop();
  }

  /** The timeout runner's WARN lines written while {@code body} runs. */
  private static List<String> warningsDuring(Runnable body) {
    var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SagaTimeoutRunner.class);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }
}
