package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AggregateLockerDefaultsTest {

  @Test
  void aLockerThatSaysNothingIsCountedAsOneConnectionOfTheApplicationPool() {
    AggregateLocker custom = (streamId, timeout) -> () -> {};

    assertEquals(1, custom.applicationPoolConnectionsPerLock());
  }
}
