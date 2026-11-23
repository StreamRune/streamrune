package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.streamrune.core.tracing.StreamRuneTracing;

class LocalStripedLockerConnectionBudgetTest {

  @Test
  void theInProcessLockerHoldsNoConnection_tracedOrNot() {
    var locker = new LocalStripedLocker(16);

    assertEquals(0, locker.applicationPoolConnectionsPerLock());
    assertEquals(0, locker.traced(StreamRuneTracing.NOOP).applicationPoolConnectionsPerLock());
  }
}
