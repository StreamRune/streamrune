package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.State;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

class SagaRunnerLeaseTest {

  private static final String LEASE_NAME = "saga:" + SagaStepLockFixture.TYPE.value();

  private final Logger runnerLog = (Logger) LoggerFactory.getLogger(SagaRunner.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();

  @BeforeEach
  void captureTheRunnersLog() {
    appender.start();
    runnerLog.addAppender(appender);
  }

  @AfterEach
  void releaseTheRunnersLog() {
    runnerLog.detachAppender(appender);
  }

  @Test
  void aRunnerWithoutLeadershipIsUngated() {
    SagaRunner<State> runner = runner(b -> b);

    assertThat(runner.lease().isGated()).isFalse();
    assertThat(runner.lease().role()).isEqualTo(LeaseRole.UNGATED);
    assertThat(runner.lease().name()).isEqualTo(LEASE_NAME);
  }

  @Test
  void aRunnerGivenNoopIsUngated() {
    SagaRunner<State> runner = runner(b -> b.leadership(SubscriptionLeadership.NOOP));

    assertThat(runner.lease().isGated()).isFalse();
    assertThat(runner.lease().name()).isEqualTo(LEASE_NAME);
  }

  @Test
  void aRunnerGivenNullIsUngated() {
    SagaRunner<State> runner = runner(b -> b.leadership(null));

    assertThat(runner.lease().isGated()).isFalse();
    assertThat(runner.lease().name()).isEqualTo(LEASE_NAME);
  }

  @Test
  void aRunnerGivenLeadershipCarriesTheLeaseOfItsSagaType() {
    SagaRunner<State> runner = runner(b -> b.leadership(new RecordingLeadership()));

    assertThat(runner.lease().isGated()).isTrue();
    assertThat(runner.lease().name()).isEqualTo(LEASE_NAME);
    assertThat(SagaRunner.LEASE_NAME_PREFIX).isEqualTo("saga:");
  }

  @Test
  void aGatedRunnerDeliveringWithoutAnAnchor_warnsOnce() {
    try (var process = new SagaStepLockFixture(new RecordingLeadership(), Duration.ofHours(1))) {
      process.deliver(process.append(new Opened("1")));
      process.deliver(process.append(new Opened("2")));

      assertThat(process.row("1").status()).isEqualTo(SagaStatus.RUNNING);
      assertThat(process.row("2").status()).isEqualTo(SagaStatus.RUNNING);
    }

    assertThat(warnings())
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains("delivers without the anchor of its lease")
                    .contains(LEASE_NAME));
  }

  @Test
  void anUngatedRunnerDeliveringWithoutAnAnchor_doesNotWarn() {
    try (var process = new SagaStepLockFixture()) {
      process.deliver(process.append(new Opened("1")));
    }

    assertThat(warnings()).isEmpty();
  }

  @Test
  void aGatedRunnerDeliveringThroughItsSubscription_doesNotWarn() {
    RecordingLeadership leadership = new RecordingLeadership();
    leadership.grant(LEASE_NAME);
    try (var process = new SagaStepLockFixture(leadership, Duration.ofHours(1))) {
      PollingEventSubscription subscription =
          PollingEventSubscription.builder()
              .subscriptionName("orders-saga")
              .eventStore(process.eventStore)
              .offsetStore(new InMemoryOffsetStore())
              .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
              .sagaRunner(process.runner)
              .build();
      subscription.start();
      try {
        process.append(new Opened("1"));
        process.awaitRow("1");
      } finally {
        subscription.close();
      }
    }

    assertThat(warnings()).isEmpty();
  }

  private List<String> warnings() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(m -> m.contains("anchor"))
        .toList();
  }

  private static SagaRunner<State> runner(
      UnaryOperator<SagaRunner.Builder<State>> leadershipSetting) {
    InMemorySagaStore rows = new InMemorySagaStore();
    InMemoryEventStore eventStore =
        new InMemoryEventStore().withCommandInbox(new InMemoryCommandInbox()).withSagaStore(rows);
    VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .commandInbox(new InMemoryCommandInbox())
            .build();
    return leadershipSetting
        .apply(
            SagaRunner.<State>builder()
                .decider(new SagaStepLockFixture.OrderSaga())
                .startWhen(e -> e.event() instanceof Opened)
                .extractSagaId(e -> SagaStepLockFixture.sagaOf(((Opened) e.event()).orderId()))
                .correlateByEventData((streamId, e) -> Optional.empty())
                .sagaStore(rows)
                .commandBus(bus)
                .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(rows)))
        .build();
  }
}
