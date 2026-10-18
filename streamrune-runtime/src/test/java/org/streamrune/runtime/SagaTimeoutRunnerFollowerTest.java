package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;
import static org.streamrune.runtime.SagaStepLockFixture.sagaOf;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.BackgroundRelayHealthContributor.Status;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.State;

/**
 * A timeout runner built from a saga runner that carries a lease follows that lease: it claims
 * deadlines only while a subscription of this process anchors the lease and leads, never acquires
 * or resigns it, and says so — at INFO within the anchor grace, at WARN with {@code DEGRADED}
 * health past it — when no subscription of this process ever anchored it.
 */
class SagaTimeoutRunnerFollowerTest {

  private static final String LEASE = "saga:" + SagaStepLockFixture.TYPE.value();

  private final RecordingLeadership leadership = new RecordingLeadership();
  private final Logger log = (Logger) LoggerFactory.getLogger(SagaTimeoutRunner.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level levelBefore;

  @BeforeEach
  void captureTheTimeoutRunnersLog() {
    levelBefore = log.getLevel();
    log.setLevel(Level.INFO);
    appender.start();
    log.addAppender(appender);
  }

  @AfterEach
  void releaseTheTimeoutRunnersLog() {
    log.detachAppender(appender);
    log.setLevel(levelBefore);
  }

  @Test
  void aFollowerWithoutAnAnchorReadsNothing_andSaysSoOnceAtInfo() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ofHours(1))) {
      p.deliver(p.append(new Opened("1")));

      p.timeOut();
      p.timeOut();

      assertThat(p.sagaStore.timeoutQueryLimits).isEmpty();
      assertThat(p.row("1").status()).isEqualTo(SagaStatus.RUNNING);
      assertThat(lines(Level.INFO))
          .singleElement()
          .satisfies(
              line ->
                  assertThat(line)
                      .contains("deadlines are claimed by the process that delivers its events")
                      .contains(SagaStepLockFixture.TYPE.value()));
      assertThat(lines(Level.WARN)).isEmpty();
      assertThat(p.timeoutRunner.leaseUnanchored()).isFalse();
    }
  }

  @Test
  void pastTheGraceWithNoAnchorEver_itWarnsOnce_andItsHealthIsDegraded() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      var relay = new BackgroundRelayHealthContributor();
      relay.registerSagaTimeoutRunner(p.timeoutRunner);
      p.timeoutRunner.start();
      try {
        p.timeOut();
        p.timeOut();

        assertThat(p.sagaStore.timeoutQueryLimits).isEmpty();
        assertThat(lines(Level.WARN))
            .singleElement()
            .satisfies(line -> assertThat(line).contains("has anchored its lease").contains(LEASE));
        assertThat(p.timeoutRunner.leaseUnanchored()).isTrue();
        assertThat(statusOf(relay, "saga-timeout:" + SagaStepLockFixture.TYPE.value()))
            .isEqualTo(Status.DEGRADED);
      } finally {
        p.timeoutRunner.stop();
      }
    }
  }

  @Test
  void aStandbyFollowerReadsNothing_andStaysUp() {
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      var relay = new BackgroundRelayHealthContributor();
      relay.registerSagaTimeoutRunner(p.timeoutRunner);
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isFalse();
      p.timeoutRunner.start();
      try {
        p.timeOut();
        p.timeOut();

        assertThat(p.sagaStore.timeoutQueryLimits).isEmpty();
        assertThat(p.timeoutRunner.leaseUnanchored()).isFalse();
        assertThat(statusOf(relay, "saga-timeout:" + SagaStepLockFixture.TYPE.value()))
            .isEqualTo(Status.UP);
        assertThat(lines(Level.INFO)).isEmpty();
        assertThat(lines(Level.WARN)).isEmpty();
      } finally {
        p.timeoutRunner.stop();
        anchor.close();
      }
    }
  }

  @Test
  void aLeadingFollowerClaims() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isTrue();
      p.deliver(p.append(new Opened("1")));

      p.timeOut();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.storedOrder("1")).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.timeoutRunner.leaseUnanchored()).isFalse();
      anchor.close();
    }
  }

  @Test
  void aPollThatDoesNotLead_forgetsTheDeferredClaims() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isTrue();
      EventEnvelope opened = p.append(new Opened("1"));
      var atTheCommand = new CountDownLatch(1);
      var letItGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            atTheCommand.countDown();
            park(letItGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(atTheCommand);

      p.timeOut();
      assertThat(p.timeoutRunner.deferralsOf(sagaOf("1"))).isEqualTo(1);

      leadership.revoke(LEASE);
      p.timeoutRunner.pollOnce();

      assertThat(p.timeoutRunner.deferralsOf(sagaOf("1"))).isZero();
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isZero();
      letItGo.countDown();
      assertThat(outcomeOf(step)).isNull();
      anchor.close();
    }
  }

  @Test
  void startAndStopNeitherAcquireNorResign() {
    try (var p = new SagaStepLockFixture(leadership, Duration.ofHours(1))) {
      p.timeoutRunner.start();
      p.timeoutRunner.stop();

      assertThat(leadership.tryAcquires(LEASE)).isZero();
      assertThat(leadership.resigns(LEASE)).isZero();
    }
  }

  @Test
  void aRunnerBuiltWithAStepLockIsUngated() {
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SagaTimeoutRunner<State> lockBound =
          SagaTimeoutRunner.<State>builder()
              .decider(p.saga)
              .sagaStore(p.sagaStore)
              .commandBus(p.bus)
              .sagaType(SagaStepLockFixture.TYPE)
              .clock(p.clock)
              .stepLock(new SagaStepLock())
              .build();
      p.deliver(p.append(new Opened("1")));
      p.clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));

      lockBound.pollOnce();

      assertThat(p.sagaStore.timeoutQueryLimits).isNotEmpty();
      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(lockBound.leaseUnanchored()).isFalse();
      assertThat(p.runner.lease().everAnchored()).isFalse();
      assertThat(lines(Level.INFO)).noneMatch(line -> line.contains("anchored"));
    }
  }

  @Test
  void anAnchorGraceIsRequiredAndNotNegative() {
    try (var p = new SagaStepLockFixture()) {
      SagaTimeoutRunner.Builder<State> builder =
          SagaTimeoutRunner.<State>builder()
              .decider(p.saga)
              .sagaStore(p.sagaStore)
              .commandBus(p.bus)
              .sagaType(SagaStepLockFixture.TYPE)
              .sagaRunner(p.runner);
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> builder.anchorGrace(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessage("anchorGrace is required");
      org.assertj.core.api.Assertions.assertThatThrownBy(
              () -> builder.anchorGrace(Duration.ofSeconds(-1)).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("anchorGrace must not be negative");
    }
  }

  @Test
  void stoppingALeadingRunner_warnsNamingTheConsequence() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isTrue();
      p.timeoutRunner.start();

      p.timeoutRunner.stop();

      assertThat(stopLines(Level.WARN))
          .singleElement()
          .satisfies(
              line ->
                  assertThat(line)
                      .contains(SagaStepLockFixture.TYPE.value())
                      .contains("stops while this process leads that saga type")
                      .contains("no replica claims its deadlines"));
      anchor.close();
    }
  }

  @Test
  void stoppingAStandbyRunner_doesNotWarn() {
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isFalse();
      p.timeoutRunner.start();

      p.timeoutRunner.stop();

      assertThat(stopLines(Level.WARN)).isEmpty();
      anchor.close();
    }
  }

  @Test
  void stoppingANeverStartedRunner_doesNotWarn() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isTrue();

      p.timeoutRunner.stop();

      assertThat(stopLines(Level.WARN)).isEmpty();
      anchor.close();
    }
  }

  private List<String> stopLines(Level level) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(m -> m.contains("stops while this process leads"))
        .toList();
  }

  private List<String> lines(Level level) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(m -> m.contains("anchored"))
        .toList();
  }

  static Status statusOf(BackgroundRelayHealthContributor relay, String component) {
    return relay.components().stream()
        .filter(c -> c.name().equals(component))
        .findFirst()
        .orElseThrow()
        .status();
  }
}
