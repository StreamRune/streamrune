package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLeadership.Lease;
import org.streamrune.core.types.SubscriptionName;

class ScriptedSubscriptionLeadershipTest {

  @Test
  void everyMethodOfTheInterfaceIsOverridden() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        SubscriptionLeadership.class, ScriptedSubscriptionLeadership.class, 4);
  }

  @Test
  void itDeniesUntilGranted() {
    var leadership = new ScriptedSubscriptionLeadership();

    assertEquals(Optional.empty(), leadership.tryAcquire(SubscriptionName.of("orders")));
    assertEquals(Optional.empty(), leadership.current(SubscriptionName.of("orders")));
    assertEquals(1, leadership.attempts());
    assertEquals(0, leadership.acquisitions());
  }

  @Test
  void granting_handsOutALeaseFromTheStart() {
    var leadership = ScriptedSubscriptionLeadership.granting();

    Lease lease = leadership.tryAcquire(SubscriptionName.of("orders")).orElseThrow();

    assertEquals(Optional.of(lease), leadership.current(SubscriptionName.of("orders")));
    assertEquals(1, leadership.acquisitions());
  }

  @Test
  void grant_handsOutALease_andATryAcquireOfAHeldNameRenewsIt() {
    var leadership = new ScriptedSubscriptionLeadership().grant();

    Lease lease = leadership.tryAcquire(SubscriptionName.of("orders")).orElseThrow();

    assertEquals(Optional.of(lease), leadership.current(SubscriptionName.of("orders")));
    assertEquals(
        Optional.of(lease),
        leadership.tryAcquire(SubscriptionName.of("orders")),
        "a renewal keeps the lease");
    assertTrue(lease.epoch() > 0, "an epoch above the unfenced 0: " + lease);
    assertEquals(2, leadership.acquisitions());
  }

  @Test
  void eachNewAcquisition_hasAGreaterEpoch() {
    var leadership = new ScriptedSubscriptionLeadership().grant();

    long orders = leadership.tryAcquire(SubscriptionName.of("orders")).orElseThrow().epoch();
    long payments = leadership.tryAcquire(SubscriptionName.of("payments")).orElseThrow().epoch();
    leadership.revoke();
    leadership.grant();
    long ordersAgain = leadership.tryAcquire(SubscriptionName.of("orders")).orElseThrow().epoch();

    assertTrue(payments > orders, orders + " then " + payments);
    assertTrue(ordersAgain > payments, payments + " then " + ordersAgain);
  }

  @Test
  void revoke_makesTheNextCurrentReportTheLoss_andDeniesUntilGrantedAgain() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    leadership.tryAcquire(SubscriptionName.of("orders"));

    leadership.revoke();

    assertEquals(Optional.empty(), leadership.current(SubscriptionName.of("orders")));
    assertEquals(Optional.empty(), leadership.tryAcquire(SubscriptionName.of("orders")));
    leadership.grant();
    assertTrue(leadership.tryAcquire(SubscriptionName.of("orders")).isPresent());
  }

  @Test
  void deny_handsOutNoNewLease_andKeepsTheHeldOnes() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    Lease held = leadership.tryAcquire(SubscriptionName.of("orders")).orElseThrow();

    leadership.deny();

    assertEquals(Optional.empty(), leadership.tryAcquire(SubscriptionName.of("payments")));
    assertEquals(Optional.of(held), leadership.current(SubscriptionName.of("orders")));
    assertEquals(Optional.of(held), leadership.tryAcquire(SubscriptionName.of("orders")));
  }

  @Test
  void resign_releasesTheName_andIsCounted() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    leadership.tryAcquire(SubscriptionName.of("orders"));

    leadership.resign(SubscriptionName.of("orders"));
    leadership.resign(SubscriptionName.of("never-held"));

    assertEquals(Optional.empty(), leadership.current(SubscriptionName.of("orders")));
    assertEquals(2, leadership.resignations());
  }

  @Test
  void close_releasesEveryHeldLease_andKeepsTheScript() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    leadership.tryAcquire(SubscriptionName.of("orders"));
    leadership.tryAcquire(SubscriptionName.of("payments"));

    leadership.close();

    assertEquals(Optional.empty(), leadership.current(SubscriptionName.of("orders")));
    assertEquals(Optional.empty(), leadership.current(SubscriptionName.of("payments")));
    assertTrue(leadership.tryAcquire(SubscriptionName.of("orders")).isPresent(), "still granting");
  }
}
