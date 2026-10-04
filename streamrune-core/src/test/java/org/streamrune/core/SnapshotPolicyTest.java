package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for SnapshotPolicy. */
class SnapshotPolicyTest {

  @Test
  void everyNEventsCreatesCorrectPolicy() {
    var policy = SnapshotPolicy.everyNEvents(10);

    assertInstanceOf(SnapshotPolicy.EveryNEvents.class, policy);
    assertEquals(10, ((SnapshotPolicy.EveryNEvents) policy).n());
  }

  @Test
  void everyNEventsThrowsOnInvalidN() {
    assertThrows(IllegalArgumentException.class, () -> SnapshotPolicy.everyNEvents(0));
    assertThrows(IllegalArgumentException.class, () -> SnapshotPolicy.everyNEvents(-1));
  }

  @Test
  void neverCreatesNeverPolicy() {
    var policy = SnapshotPolicy.never();

    assertInstanceOf(SnapshotPolicy.Never.class, policy);
  }

  @Test
  void sealedInterfaceAllowsExhaustivePatternMatching() {
    SnapshotPolicy every10 = SnapshotPolicy.everyNEvents(10);
    SnapshotPolicy never = SnapshotPolicy.never();

    String resultEvery =
        switch (every10) {
          case SnapshotPolicy.EveryNEvents e -> "every-" + e.n();
          case SnapshotPolicy.Never _ -> "never";
        };

    String resultNever =
        switch (never) {
          case SnapshotPolicy.EveryNEvents e -> "every-" + e.n();
          case SnapshotPolicy.Never _ -> "never";
        };

    assertEquals("every-10", resultEvery);
    assertEquals("never", resultNever);
  }

  @Test
  void everyNEvents_defaultSnapshotVersion_isOne() {
    SnapshotPolicy.EveryNEvents policy = new SnapshotPolicy.EveryNEvents(10, 1);
    assertEquals(1, policy.snapshotVersion());
  }

  @Test
  void everyNEvents_factory_defaultsToVersionOne() {
    SnapshotPolicy.EveryNEvents policy =
        (SnapshotPolicy.EveryNEvents) SnapshotPolicy.everyNEvents(5);
    assertEquals(1, policy.snapshotVersion());
  }

  @Test
  void everyNEvents_factory_acceptsExplicitVersion() {
    SnapshotPolicy.EveryNEvents policy =
        (SnapshotPolicy.EveryNEvents) SnapshotPolicy.everyNEvents(5, 3);
    assertEquals(3, policy.snapshotVersion());
  }

  @Test
  void everyNEvents_rejects_snapshotVersionZero() {
    assertThrows(IllegalArgumentException.class, () -> new SnapshotPolicy.EveryNEvents(5, 0));
  }

  @Test
  void everyNEvents_rejects_negativeSnapshotVersion() {
    assertThrows(IllegalArgumentException.class, () -> new SnapshotPolicy.EveryNEvents(5, -1));
  }
}
