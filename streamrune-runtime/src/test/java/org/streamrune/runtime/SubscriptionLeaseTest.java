package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionLeadership;

class SubscriptionLeaseTest {

  private static final String NAME = "saga:orders";

  private final RecordingLeadership leadership = new RecordingLeadership();

  @Test
  void anUngatedLeaseLeadsEverywhereAndCallsNothing() {
    for (SubscriptionLease lease :
        new SubscriptionLease[] {
          SubscriptionLease.ungated(), SubscriptionLease.of(SubscriptionLeadership.NOOP, "x")
        }) {
      assertThat(lease.isGated()).isFalse();
      assertThat(lease.isLeadingHere()).isTrue();
      assertThat(lease.role()).isEqualTo(LeaseRole.UNGATED);
      assertThatThrownBy(lease::anchor)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("ungated");
    }
    assertThat(SubscriptionLease.of(SubscriptionLeadership.NOOP, "x").name()).isEqualTo("x");
  }

  @Test
  void aLeaseNeedsALeadershipAndANonBlankName() {
    assertThatThrownBy(() -> SubscriptionLease.of(null, NAME))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("leadership is required");
    assertThatThrownBy(() -> SubscriptionLease.of(leadership, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("name is required");
    assertThatThrownBy(() -> SubscriptionLease.of(leadership, " "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("name must not be blank");
    assertThatThrownBy(() -> SubscriptionLease.of(leadership, NAME, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("nanoTime is required");
  }

  @Test
  void aGatedLeaseWithoutAnAnchorDoesNotLeadHere() {
    leadership.grant(NAME);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);

    assertThat(lease.isGated()).isTrue();
    assertThat(lease.isLeadingHere()).isFalse();
    assertThat(lease.role()).isEqualTo(LeaseRole.STANDBY);
    assertThat(lease.everAnchored()).isFalse();
    assertThat(leadership.tryAcquires(NAME)).isZero();
  }

  @Test
  void anAnchorThatLeads_makesTheLeaseLeadHere() {
    leadership.grant(NAME);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);

    SubscriptionLease.Anchor anchor = lease.anchor();

    assertThat(lease.everAnchored()).isTrue();
    assertThat(lease.role()).isEqualTo(LeaseRole.STANDBY);
    assertThat(anchor.lead(Duration.ofHours(1))).isTrue();
    assertThat(lease.isLeadingHere()).isTrue();
    assertThat(lease.role()).isEqualTo(LeaseRole.LEADING);
    assertThat(anchor.stillLeads()).isTrue();
  }

  @Test
  void oneAnchorAtATime() {
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);
    SubscriptionLease.Anchor first = lease.anchor();

    assertThatThrownBy(lease::anchor)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already has a delivering subscription in this process");

    first.close();
    SubscriptionLease.Anchor third = lease.anchor();
    assertThat(third.isClosed()).isFalse();
    // Closing the first anchor again leaves the slot of the third one alone.
    first.close();
    assertThatThrownBy(lease::anchor).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void followersNeverAcquireOrResign() {
    leadership.grant(NAME);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);
    SubscriptionLease.Anchor anchor = lease.anchor();
    anchor.lead(Duration.ofHours(1));

    for (int i = 0; i < 100; i++) {
      assertThat(lease.isLeadingHere()).isTrue();
      assertThat(lease.role()).isEqualTo(LeaseRole.LEADING);
    }

    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);
    assertThat(leadership.resigns(NAME)).isZero();
  }

  @Test
  void leadAsksTheLeadershipOncePerInterval() {
    AtomicLong now = new AtomicLong(1_000_000_000L);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME, now::get);
    SubscriptionLease.Anchor anchor = lease.anchor();

    assertThat(anchor.lead(Duration.ofSeconds(5))).isFalse();
    assertThat(anchor.lead(Duration.ofSeconds(5))).isFalse();
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);

    now.addAndGet(Duration.ofSeconds(5).toNanos() - 1);
    assertThat(anchor.lead(Duration.ofSeconds(5))).isFalse();
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);

    now.addAndGet(1);
    leadership.grant(NAME);
    assertThat(anchor.lead(Duration.ofSeconds(5))).isTrue();
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(2);
  }

  @Test
  void leadIsALocalReadWhileTheLeaseIsHeld() {
    leadership.grant(NAME);
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();

    for (int i = 0; i < 10; i++) {
      assertThat(anchor.lead(Duration.ZERO)).isTrue();
    }

    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);
    assertThat(leadership.currents(NAME)).isGreaterThanOrEqualTo(10);
    assertThatThrownBy(() -> anchor.lead(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("minAttemptInterval is required");
  }

  @Test
  void closeResignsOnce_andIsIdempotent() {
    leadership.grant(NAME);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);
    SubscriptionLease.Anchor anchor = lease.anchor();
    assertThat(anchor.lead(Duration.ZERO)).isTrue();

    anchor.close();
    anchor.close();

    assertThat(leadership.resigns(NAME)).isEqualTo(1);
    assertThat(anchor.isClosed()).isTrue();
    assertThat(anchor.lead(Duration.ZERO)).isFalse();
    assertThat(anchor.stillLeads()).isFalse();
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);
    assertThat(lease.isLeadingHere()).isFalse();
    assertThat(lease.everAnchored()).isTrue();
  }

  @Test
  void closeDuringAnAcquire_givesTheGrantBack() throws Exception {
    leadership.grant(NAME);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);
    SubscriptionLease.Anchor anchor = lease.anchor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    leadership.blockInsideTryAcquire(entered, release);

    CompletableFuture<Boolean> led =
        CompletableFuture.supplyAsync(() -> anchor.lead(Duration.ZERO), Thread::startVirtualThread);
    assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue();
    anchor.close();
    release.countDown();

    assertThat(led.get(20, TimeUnit.SECONDS)).isFalse();
    assertThat(leadership.resigns(NAME)).isEqualTo(2);
    assertThat(leadership.current(NAME)).isEmpty();
  }

  @Test
  void aGrantThatArrivesAfterANewerAnchorOpened_isLeftToTheNewerAnchor() throws Exception {
    leadership.grant(NAME);
    SubscriptionLease lease = SubscriptionLease.of(leadership, NAME);
    SubscriptionLease.Anchor first = lease.anchor();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    leadership.blockInsideTryAcquire(entered, release);

    CompletableFuture<Boolean> led =
        CompletableFuture.supplyAsync(() -> first.lead(Duration.ZERO), Thread::startVirtualThread);
    assertThat(entered.await(20, TimeUnit.SECONDS)).isTrue();
    first.close();
    SubscriptionLease.Anchor second = lease.anchor();
    assertThat(second.lead(Duration.ZERO)).isTrue();
    release.countDown();

    assertThat(led.get(20, TimeUnit.SECONDS)).isFalse();
    assertThat(leadership.resigns(NAME)).isEqualTo(1);
    assertThat(second.stillLeads()).isTrue();
    assertThat(lease.isLeadingHere()).isTrue();
  }

  @Test
  void stillLeadsFollowsTheLeadership() {
    leadership.grant(NAME);
    SubscriptionLease.Anchor anchor = SubscriptionLease.of(leadership, NAME).anchor();
    assertThat(anchor.lead(Duration.ZERO)).isTrue();

    leadership.revoke(NAME);

    assertThat(anchor.stillLeads()).isFalse();
    assertThat(leadership.tryAcquires(NAME)).isEqualTo(1);
  }
}
