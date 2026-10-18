package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/** The startup report of a saga runner that carries no lease while a real leadership exists. */
class SagaLeaseWiringTest {

  record Alpha(SagaStatus status) implements SagaState {}

  record Beta(SagaStatus status) implements SagaState {}

  record Gamma(SagaStatus status) implements SagaState {}

  private final Logger log = (Logger) LoggerFactory.getLogger(SagaLeaseWiring.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level levelBefore;

  @BeforeEach
  void captureTheWiringLog() {
    levelBefore = log.getLevel();
    log.setLevel(Level.INFO);
    appender.start();
    log.addAppender(appender);
  }

  @AfterEach
  void releaseTheWiringLog() {
    log.detachAppender(appender);
    log.setLevel(levelBefore);
  }

  @Test
  void warnsOncePerUngatedRunnerUnderRealLeadership() {
    SubscriptionLeadership leadership = new RecordingLeadership();
    SagaRunner<Alpha> ungatedAlpha = runner(Alpha.class, Alpha::new, null);
    SagaRunner<Beta> ungatedBeta = runner(Beta.class, Beta::new, SubscriptionLeadership.NOOP);
    SagaRunner<Gamma> gatedGamma = runner(Gamma.class, Gamma::new, leadership);

    int reported =
        SagaLeaseWiring.warnUngatedUnderLeadership(
            List.of(ungatedAlpha, ungatedBeta, gatedGamma), leadership);

    assertThat(reported).isEqualTo(2);
    assertThat(warnings()).hasSize(2);
    assertThat(warnings().get(0))
        .contains("Saga type " + ungatedAlpha.sagaType().value() + " is delivered on every replica")
        .contains("SagaRunner.Builder.leadership(...)")
        .contains(".sagaRunner(runner)");
    assertThat(warnings().get(1)).contains(ungatedBeta.sagaType().value());
  }

  @Test
  void saysNothingUnderNoop() {
    SagaRunner<Alpha> ungated = runner(Alpha.class, Alpha::new, null);

    int reported =
        SagaLeaseWiring.warnUngatedUnderLeadership(List.of(ungated), SubscriptionLeadership.NOOP);

    assertThat(reported).isZero();
    assertThat(appender.list).isEmpty();
  }

  @Test
  void saysNothingWithoutLeadership() {
    SagaRunner<Alpha> ungated = runner(Alpha.class, Alpha::new, null);

    int reported = SagaLeaseWiring.warnUngatedUnderLeadership(List.of(ungated), null);

    assertThat(reported).isZero();
    assertThat(appender.list).isEmpty();
  }

  @Test
  void nothingToSayWithoutRunners() {
    SubscriptionLeadership leadership = new RecordingLeadership();

    assertThat(SagaLeaseWiring.warnUngatedUnderLeadership(null, leadership)).isZero();
    assertThat(SagaLeaseWiring.warnUngatedUnderLeadership(List.of(), leadership)).isZero();
    assertThat(appender.list).isEmpty();
  }

  private List<String> warnings() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  /** A runner of the saga type {@code type} that starts on every event and issues no command. */
  private static <S extends SagaState> SagaRunner<S> runner(
      Class<S> type, Function<SagaStatus, S> state, SubscriptionLeadership leadership) {
    InMemorySagaStore rows = new InMemorySagaStore();
    InMemoryEventStore eventStore =
        new InMemoryEventStore().withCommandInbox(new InMemoryCommandInbox()).withSagaStore(rows);
    VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .commandInbox(new InMemoryCommandInbox())
            .build();
    return SagaRunner.<S>builder()
        .decider(
            new SagaDecider<S>() {
              @Override
              public Class<S> stateType() {
                return type;
              }

              @Override
              public S initialState(SagaId sagaId) {
                return state.apply(SagaStatus.STARTED);
              }

              @Override
              public S evolve(S current, EventEnvelope event) {
                return state.apply(SagaStatus.RUNNING);
              }

              @Override
              public List<SagaCommand> handle(S current, EventEnvelope event) {
                return List.of();
              }
            })
        .startWhen(e -> true)
        .extractSagaId(e -> SagaId.of("saga-1"))
        .correlateByEventData((streamId, e) -> Optional.empty())
        .sagaStore(rows)
        .commandBus(bus)
        .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(rows))
        .leadership(leadership)
        .build();
  }
}
