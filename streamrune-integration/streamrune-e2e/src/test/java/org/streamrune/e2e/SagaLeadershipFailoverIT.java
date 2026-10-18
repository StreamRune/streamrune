package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.LeaseRole;

/**
 * Two processes of one saga type over one database, each with the saga's subscription, timeout
 * runner and compensation-retry sweeper, the runner carrying the saga type's lease. One delivers;
 * the other reads nothing and takes over within one attempt interval of a clean stop, and within
 * the lease's time to live of a crash — deadlines included.
 */
class SagaLeadershipFailoverIT extends SagaLeadershipNodes {

  /**
   * The slack of the clean stop's bound, wide for a loaded runner: the lease row's expiry, read
   * before the stop, is what proves the handover did not wait out the lease.
   */
  private static final Duration CLEAN_STOP_SLACK = Duration.ofSeconds(3);

  @Test
  void exactlyOneNodeDeliversTheSagaType() throws Exception {
    LeasedNode a = leased("node-a");
    a.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> a.role() == LeaseRole.LEADING);
    LeasedNode b = leased("node-b");
    b.start();
    assertThat(b.role()).isEqualTo(LeaseRole.STANDBY);

    List<String> orders = IntStream.rangeClosed(1, 20).mapToObj(n -> "o" + n).toList();
    for (String order : orders) {
      append(new Opened(order));
      append(new Advanced(order));
      append(new Completed(order));
    }

    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> orders.stream().allMatch(o -> isRow(o, SagaStatus.COMPLETED)));
    assertThat(b.role()).isEqualTo(LeaseRole.STANDBY);
    assertThat(b.offsets.reads.get()).as("the standby reads no checkpoint").isZero();
    assertThat(b.offsets.saves.get()).as("the standby saves no checkpoint").isZero();
    assertThat(b.node.deliveryRetriedSamples).isEmpty();
    assertThat(b.node.casConflictSamples).isEmpty();
    assertThat(b.node.dispatched).isEmpty();
    assertThat(a.node.dispatched).isNotEmpty();
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
  }

  @Test
  void aCleanStopHandsTheSagaTypeOverWithinOneAttemptInterval() throws Exception {
    LeasedNode a = leased("node-a");
    a.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> a.role() == LeaseRole.LEADING);
    LeasedNode b = leased("node-b");
    b.start();
    List<String> orders = IntStream.rangeClosed(1, 5).mapToObj(n -> "o" + n).toList();
    orders.forEach(o -> append(new Opened(o)));
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> orders.stream().allMatch(o -> isRow(o, SagaStatus.RUNNING)));

    String lease = a.node.runner.lease().name();
    java.sql.Timestamp heldUntil = leaseUntil(lease);
    long t0 = System.nanoTime();
    // The stop runs beside the watch: the handover is measured from the stop's start to the
    // standby leading, not to the end of the stopping process's own teardown.
    var stopping = onAnotherThread(a::close);
    await()
        .pollInterval(Duration.ofMillis(10))
        .atMost(Duration.ofSeconds(10))
        .until(() -> b.role() == LeaseRole.LEADING);
    Duration handover = Duration.ofNanos(System.nanoTime() - t0);
    assertThat(outcomeOf(stopping)).isNull();

    // The standby took the lease while the stopped leader's last renewal was still live: the stop
    // resigned it, and the handover did not wait for it to expire.
    assertThat(leaseRowStillLive(lease, heldUntil))
        .as("the standby leads before the stopped leader's lease would have expired")
        .isTrue();
    assertThat(handover)
        .as("one standby attempt interval, its jitter and the slack")
        .isLessThanOrEqualTo(POLLING.plus(POLLING.dividedBy(5)).plus(CLEAN_STOP_SLACK));
    for (String order : orders) {
      append(new Advanced(order));
      append(new Completed(order));
    }
    await()
        .atMost(Duration.ofSeconds(30))
        .until(() -> orders.stream().allMatch(o -> isRow(o, SagaStatus.COMPLETED)));
    assertThat(b.offsets.reads.get()).isPositive();
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
  }

  @Test
  void aCrashedLeaderIsReplacedWithinTheLeaseTtl_andItsDeadlinesAreClaimedByTheNewLeader()
      throws Exception {
    LeasedNode a = leased("node-a");
    a.start();
    await().atMost(Duration.ofSeconds(10)).until(() -> a.role() == LeaseRole.LEADING);
    LeasedNode b = leased("node-b");
    b.start();
    List<String> orders = IntStream.rangeClosed(1, 10).mapToObj(n -> "o" + n).toList();
    List<String> completed = orders.subList(0, 5);
    List<String> open = orders.subList(5, 10);
    orders.forEach(o -> append(new Opened(o)));
    completed.forEach(o -> append(new Completed(o)));
    await()
        .atMost(Duration.ofSeconds(20))
        .until(
            () ->
                completed.stream().allMatch(o -> isRow(o, SagaStatus.COMPLETED))
                    && open.stream().allMatch(o -> isRow(o, SagaStatus.RUNNING)));

    long t0 = System.nanoTime();
    a.crash();
    passTheDeadlineOfEverySagaSoFar();
    await()
        .pollInterval(Duration.ofMillis(10))
        .atMost(Duration.ofSeconds(15))
        .until(() -> b.role() == LeaseRole.LEADING);
    Duration takeover = Duration.ofNanos(System.nanoTime() - t0);

    assertThat(takeover)
        .as("the lease's time to live, one standby attempt interval, its jitter and the slack")
        .isLessThanOrEqualTo(
            LEASE_TTL.plus(POLLING).plus(POLLING.dividedBy(5)).plus(Duration.ofMillis(1_500)));
    await()
        .atMost(Duration.ofSeconds(20))
        .until(() -> open.stream().allMatch(o -> isRow(o, SagaStatus.COMPENSATED)));
    for (String order : open) {
      assertThat(episodeCauseOf(order)).as("claimed at its deadline").isNull();
    }
    assertThat(b.node.dispatched).contains(Release.class, Refund.class);
    assertThat(SagaForwardFenceInvariant.lateForwardCommands(dataSource)).isEmpty();
  }
}
