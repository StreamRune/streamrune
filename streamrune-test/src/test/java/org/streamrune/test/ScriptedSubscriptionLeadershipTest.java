package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLeadership.Lease;

class ScriptedSubscriptionLeadershipTest {

  @Test
  void everyMethodOfTheInterfaceIsOverridden() {
    ForwardingDecoratorChecks.assertEveryMethodOverridden(
        SubscriptionLeadership.class, ScriptedSubscriptionLeadership.class, 4);
  }

  @Test
  void itDeniesUntilGranted() {
    var leadership = new ScriptedSubscriptionLeadership();

    assertEquals(Optional.empty(), leadership.tryAcquire("orders"));
    assertEquals(Optional.empty(), leadership.current("orders"));
    assertEquals(1, leadership.attempts());
    assertEquals(0, leadership.acquisitions());
  }

  @Test
  void grant_handsOutALease_andATryAcquireOfAHeldNameRenewsIt() {
    var leadership = new ScriptedSubscriptionLeadership().grant();

    Lease lease = leadership.tryAcquire("orders").orElseThrow();

    assertEquals(Optional.of(lease), leadership.current("orders"));
    assertEquals(Optional.of(lease), leadership.tryAcquire("orders"), "a renewal keeps the lease");
    assertTrue(lease.epoch() > 0, "an epoch above the unfenced 0: " + lease);
    assertEquals(2, leadership.acquisitions());
  }

  @Test
  void eachNewAcquisition_hasAGreaterEpoch() {
    var leadership = new ScriptedSubscriptionLeadership().grant();

    long orders = leadership.tryAcquire("orders").orElseThrow().epoch();
    long payments = leadership.tryAcquire("payments").orElseThrow().epoch();
    leadership.revoke();
    leadership.grant();
    long ordersAgain = leadership.tryAcquire("orders").orElseThrow().epoch();

    assertTrue(payments > orders, orders + " then " + payments);
    assertTrue(ordersAgain > payments, payments + " then " + ordersAgain);
  }

  @Test
  void revoke_makesTheNextCurrentReportTheLoss_andDeniesUntilGrantedAgain() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    leadership.tryAcquire("orders");

    leadership.revoke();

    assertEquals(Optional.empty(), leadership.current("orders"));
    assertEquals(Optional.empty(), leadership.tryAcquire("orders"));
    leadership.grant();
    assertTrue(leadership.tryAcquire("orders").isPresent());
  }

  @Test
  void deny_handsOutNoNewLease_andKeepsTheHeldOnes() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    Lease held = leadership.tryAcquire("orders").orElseThrow();

    leadership.deny();

    assertEquals(Optional.empty(), leadership.tryAcquire("payments"));
    assertEquals(Optional.of(held), leadership.current("orders"));
    assertEquals(Optional.of(held), leadership.tryAcquire("orders"));
  }

  @Test
  void resign_releasesTheName_andIsCounted() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    leadership.tryAcquire("orders");

    leadership.resign("orders");
    leadership.resign("never-held");

    assertEquals(Optional.empty(), leadership.current("orders"));
    assertEquals(2, leadership.resignations());
  }

  @Test
  void close_releasesEveryHeldLease_andKeepsTheScript() {
    var leadership = new ScriptedSubscriptionLeadership().grant();
    leadership.tryAcquire("orders");
    leadership.tryAcquire("payments");

    leadership.close();

    assertEquals(Optional.empty(), leadership.current("orders"));
    assertEquals(Optional.empty(), leadership.current("payments"));
    assertTrue(leadership.tryAcquire("orders").isPresent(), "still granting");
  }
}
