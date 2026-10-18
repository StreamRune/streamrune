package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.types.GlobalOffset;

/**
 * The overlap after a leader is cut off its leadership while a step of its is in flight: the new
 * leader redelivers the same event from the shared checkpoint while the deposed leader's step is
 * still running. The forward fence refuses a forward command held across a claim the new leader
 * made at the saga's deadline, and a compare-and-swap the new leader loses to the deposed leader is
 * run again in place instead of failing its subscription.
 */
class SagaLeadershipOverlapIT extends SagaLeadershipNodes {

  private static final String ORDER = "o";

  private final Logger pollLoopLog =
      (Logger) LoggerFactory.getLogger("org.streamrune.runtime.ResilientPollLoop");
  private final ListAppender<ILoggingEvent> pollLoopLines = new ListAppender<>();

  private final CountDownLatch aParked = new CountDownLatch(1);
  private final CountDownLatch releaseA = new CountDownLatch(1);
  private final CountDownLatch bParked = new CountDownLatch(1);
  private final CountDownLatch releaseB = new CountDownLatch(1);

  @BeforeEach
  void capturePollLoopLines() {
    pollLoopLines.start();
    pollLoopLog.addAppender(pollLoopLines);
  }

  @AfterEach
  void releaseEverything() {
    releaseA.countDown();
    releaseB.countDown();
    pollLoopLog.detachAppender(pollLoopLines);
  }

  @Test
  void aDeposedLeadersHeldCommandIsRefused_afterTheNewLeaderClaimedTheSagaAtItsDeadline()
      throws Exception {
    LeasedNode a = leased("node-a");
    LeasedNode b = leased("node-b");
    bothParkedOnTheSameEvent(a, b);
    passTheDeadlineOfEverySagaSoFar();

    // The new leader's timeout runner finds its own step in flight under the step lock, waits its
    // bounded wait and claims underneath it.
    await().atMost(Duration.ofSeconds(20)).until(() -> isRow(ORDER, SagaStatus.COMPENSATED));
    releaseA.countDown();
    await().atMost(Duration.ofSeconds(20)).until(() -> a.node.fencedSamples.size() == 1);
    releaseB.countDown();
    await().atMost(Duration.ofSeconds(20)).until(() -> b.node.fencedSamples.size() == 1);

    assertThat(a.node.fencedSamples).hasSize(1);
    assertThat(b.node.fencedSamples).hasSize(1);
    assertThat(storedOrder(ORDER)).containsExactly("Reserved", "Shipped", "Released", "Refunded");
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
    assertNoPollLoopError();
  }

  @Test
  void theNewLeadersLostCompareAndSwapAgainstTheDeposedLeader_isRunAgainInPlace() throws Exception {
    LeasedNode a = leased("node-a");
    LeasedNode b = leased("node-b");
    bothParkedOnTheSameEvent(a, b);
    long versionBefore = row(ORDER).version();

    // The deposed leader's step goes first: its Ship is stored and its row write lands.
    releaseA.countDown();
    await().atMost(Duration.ofSeconds(20)).until(() -> row(ORDER).version() == versionBefore + 1);
    // The new leader's Ship is answered from the inbox; its row write loses and is run again.
    releaseB.countDown();
    await().atMost(Duration.ofSeconds(20)).until(() -> b.node.deliveryRetriedSamples.size() == 1);
    await().atMost(Duration.ofSeconds(20)).until(() -> b.offsets.saves.get() > 0);
    // The new leader goes on to deliver the Shipped event the deposed leader's step stored.
    long head =
        probe.readGlobalStream(GlobalOffset.initial(), 1_000).getLast().globalOffset().value();
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> Long.valueOf(head).equals(row(ORDER).lastAppliedOffset()));

    assertThat(b.node.deliveryRetriedSamples).hasSize(1);
    assertThat(b.node.casConflictSamples).isNotEmpty();
    assertThat(row(ORDER).version())
        .as(
            "the deposed leader's write of Advanced and the new leader's of the Shipped event that"
                + " step stored; the run again writes nothing")
        .isEqualTo(versionBefore + 2);
    assertThat(row(ORDER).status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(storedOrder(ORDER)).containsExactly("Reserved", "Shipped", "Shipped");
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
    assertNoPollLoopError();
  }

  /**
   * Node A leads and applies {@code Opened}; its delivery of {@code Advanced} parks before {@code
   * Ship}. A is then cut off its leadership; B takes the lease, redelivers {@code Advanced} from
   * the checkpoint A never moved and parks before {@code Ship} too; A stands down within one lease
   * time to live of the cut.
   */
  private void bothParkedOnTheSameEvent(LeasedNode a, LeasedNode b) {
    a.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> a.role() == LeaseRole.LEADING);
    b.start();
    append(new Opened(ORDER));
    await().atMost(Duration.ofSeconds(20)).until(() -> isRow(ORDER, SagaStatus.RUNNING));
    await().atMost(Duration.ofSeconds(20)).until(() -> a.offsets.saves.get() > 0);

    a.node.bus.runOnceBefore(Ship.class, () -> park(aParked, releaseA));
    append(new Advanced(ORDER));
    awaitLatch(aParked);
    b.node.bus.runOnceBefore(Ship.class, () -> park(bParked, releaseB));

    long cut = System.nanoTime();
    a.leadershipProxy.silenceOpenConnections();
    a.leadershipProxy.refuseNewConnections(true);
    await().atMost(LEASE_TTL.plusSeconds(2)).until(() -> b.role() == LeaseRole.LEADING);
    awaitLatch(bParked);
    await()
        .pollInterval(Duration.ofMillis(10))
        .atMost(LEASE_TTL.plusSeconds(2))
        .until(() -> a.role() == LeaseRole.STANDBY);
    assertThat(Duration.ofNanos(System.nanoTime() - cut))
        .as("the deposed leader stands down within one lease time to live of the cut")
        .isLessThanOrEqualTo(LEASE_TTL.plusSeconds(1));
  }

  private void assertNoPollLoopError() {
    assertThat(pollLoopLines.list)
        .filteredOn(e -> e.getLevel() == Level.ERROR)
        .noneMatch(e -> e.getFormattedMessage().contains("subscription '" + SUBSCRIPTION + "'"));
  }

  private static void park(CountDownLatch parked, CountDownLatch release) {
    parked.countDown();
    awaitLatch(release);
  }

  private static void awaitLatch(CountDownLatch latch) {
    try {
      assertThat(latch.await(PATIENCE_SECONDS, TimeUnit.SECONDS)).as("the latch opened").isTrue();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }
}
