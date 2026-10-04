package org.streamrune.core.subscription;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SubscriptionLifecycleTest {

  @Test
  void lifecycleState_hasExactlyFourValues() {
    assertArrayEquals(
        new SubscriptionLifecycleState[] {
          SubscriptionLifecycleState.CREATED,
          SubscriptionLifecycleState.RUNNING,
          SubscriptionLifecycleState.PAUSED,
          SubscriptionLifecycleState.STOPPED
        },
        SubscriptionLifecycleState.values());
  }

  @Test
  void subscriptionLifecycle_hasRequiredMethodsWithCorrectSignatures() throws Exception {
    // state() must return SubscriptionLifecycleState
    java.lang.reflect.Method state = SubscriptionLifecycle.class.getMethod("state");
    assertEquals(SubscriptionLifecycleState.class, state.getReturnType());

    // pause() and resume() must exist (NoSuchMethodException = test failure)
    SubscriptionLifecycle.class.getMethod("pause");
    SubscriptionLifecycle.class.getMethod("resume");

    // SubscriptionLifecycle must be a subtype of EventSubscription
    assertTrue(EventSubscription.class.isAssignableFrom(SubscriptionLifecycle.class));
  }
}
