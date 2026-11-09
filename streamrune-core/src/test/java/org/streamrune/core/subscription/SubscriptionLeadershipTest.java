package org.streamrune.core.subscription;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.subscription.SubscriptionLeadership.Lease;
import org.streamrune.core.types.SubscriptionName;

/** Contract test for the always-leader {@link SubscriptionLeadership#NOOP}. */
class SubscriptionLeadershipTest {

  @Test
  void noopIsAlwaysLeaderAtEpochZero() {
    SubscriptionLeadership noop = SubscriptionLeadership.NOOP;

    // Always acquires, always at the unfenced epoch 0.
    Optional<Lease> acquired = noop.tryAcquire(SubscriptionName.of("any"));
    assertTrue(acquired.isPresent());
    assertEquals(0L, acquired.get().epoch());

    // Reports the same live lease.
    Optional<Lease> current = noop.current(SubscriptionName.of("any"));
    assertTrue(current.isPresent());
    assertEquals(0L, current.get().epoch());

    // resign and close are no-ops that never throw.
    assertDoesNotThrow(() -> noop.resign(SubscriptionName.of("any")));
    assertDoesNotThrow(noop::close);

    // Still always-leader at epoch 0 after resign/close.
    assertTrue(noop.tryAcquire(SubscriptionName.of("any")).isPresent());
    assertEquals(0L, noop.tryAcquire(SubscriptionName.of("any")).get().epoch());
    assertTrue(noop.current(SubscriptionName.of("any")).isPresent());
    assertEquals(0L, noop.current(SubscriptionName.of("any")).get().epoch());
  }

  @Test
  void leaseRecordCarriesEpoch() {
    assertEquals(7L, new Lease(7L).epoch());
  }
}
