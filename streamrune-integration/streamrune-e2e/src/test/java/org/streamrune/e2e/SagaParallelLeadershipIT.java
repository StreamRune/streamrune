package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.postgres.PostgresOffsetStore;

/**
 * Two processes of one saga type under its lease, each stepping the sagas of a page eight at a
 * time: the leader loses the lease while a page is in flight, or a worker of the leader's page
 * throws an {@link Error}; the other process delivers the page again from the shared checkpoint,
 * and every step is applied once.
 */
class SagaParallelLeadershipIT extends SagaLeadershipNodes {

  private static final List<String> ORDERS =
      IntStream.rangeClosed(1, 6).mapToObj(n -> "o" + n).toList();

  private final CountDownLatch aParked = new CountDownLatch(1);
  private final CountDownLatch releaseA = new CountDownLatch(1);

  @AfterEach
  void releaseEverything() {
    releaseA.countDown();
  }

  /**
   * The leader's page of six {@code Advanced} events is in flight — one step held before its {@code
   * Ship}, the others written — when it is cut off its lease. The new leader delivers the page
   * again in parallel from the checkpoint the deposed leader never moved: the written steps are
   * dropped on their rows, the held one runs and stores its {@code Shipped}. The held step then
   * goes on: its {@code Ship} is answered from the inbox, its row write loses and is run again in
   * place. Every saga has its events once and a row that applied each of them once.
   */
  @Test
  void aLeaseLostMidPage_theNewLeaderDeliversThePageAgain_andEachStepIsAppliedOnce()
      throws Exception {
    parallelism = 8;
    LeasedNode a = leased("node-a");
    a.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> a.role() == LeaseRole.LEADING);
    LeasedNode b = leased("node-b");
    b.start();
    ORDERS.forEach(o -> append(new Opened(o)));
    awaitCheckpointAtTheHead();
    assertThat(ORDERS).allMatch(o -> isRow(o, SagaStatus.RUNNING));

    a.node.bus.runOnceBefore(Ship.class, () -> park(aParked, releaseA));
    ORDERS.forEach(o -> append(new Advanced(o)));
    awaitLatch(aParked);
    a.leadershipProxy.silenceOpenConnections();
    a.leadershipProxy.refuseNewConnections(true);
    await().atMost(LEASE_TTL.plusSeconds(3)).until(() -> b.role() == LeaseRole.LEADING);
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> ORDERS.stream().allMatch(o -> shipped(o) == 2));

    releaseA.countDown();
    await().atMost(LEASE_TTL.plusSeconds(3)).until(() -> a.role() == LeaseRole.STANDBY);
    awaitCheckpointAtTheHead();

    for (String order : ORDERS) {
      assertThat(storedOrder(order))
          .as(order + ": one Reserve and two Ships, each stored once")
          .containsExactly("Reserved", "Shipped", "Shipped");
      assertAppliedOnce(order, /* triggers= */ 2);
    }
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
  }

  /**
   * A worker of the leader's page throws an {@link Error} in its step. The page is not
   * acknowledged, the leader's subscription stops terminally and resigns the lease, and the standby
   * delivers the page again: the steps the leader's other workers wrote are dropped on their rows,
   * the failed one runs, and every saga has its events once.
   */
  @Test
  void anErrorOnAWorker_leavesThePageUnacknowledged_andTheStandbyFinishesIt() throws Exception {
    parallelism = 8;
    LeasedNode a = leased("node-a");
    a.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> a.role() == LeaseRole.LEADING);
    LeasedNode b = leased("node-b");
    b.start();

    a.node.bus.runOnceBefore(
        Ship.class,
        () -> {
          throw new AssertionError("a worker's step threw an Error");
        });
    ORDERS.forEach(o -> append(new Opened(o)));

    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> a.subscription.state() == SubscriptionLifecycleState.STOPPED);
    assertThat(a.offsets.saves.get()).as("the leader acknowledged no page").isZero();
    await().atMost(LEASE_TTL.plusSeconds(3)).until(() -> b.role() == LeaseRole.LEADING);
    awaitCheckpointAtTheHead();

    for (String order : ORDERS) {
      assertThat(isRow(order, SagaStatus.RUNNING)).as(order).isTrue();
      assertThat(storedOrder(order))
          .as(order + ": its Reserve and its Ship, each stored once")
          .containsExactly("Reserved", "Shipped");
      assertAppliedOnce(order, /* triggers= */ 1);
    }
    assertThat(b.offsets.saves.get()).as("the standby acknowledged the page").isPositive();
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
  }

  /**
   * The saga's row applied each of its events once: one version for its genesis-pending insert and
   * one per event — its {@code triggers} trigger events and the replies of its stored commands.
   */
  private void assertAppliedOnce(String order, int triggers) {
    long events = triggers + storedOrder(order).size();
    assertThat(row(order).version()).as(order + " version").isEqualTo(1 + events);
  }

  private long shipped(String order) {
    return storedOrder(order).stream().filter("Shipped"::equals).count();
  }

  /** Waits until the subscription's checkpoint is at the last stored event. */
  private void awaitCheckpointAtTheHead() {
    PostgresOffsetStore offsets = new PostgresOffsetStore(dataSource);
    await()
        .atMost(Duration.ofSeconds(30))
        .until(
            () -> {
              long checkpoint = offsets.getLastOffset(ProjectionName.of(SUBSCRIPTION)).value();
              long head =
                  probe
                      .readGlobalStream(GlobalOffset.initial(), 10_000)
                      .getLast()
                      .globalOffset()
                      .value();
              return checkpoint == head;
            });
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
