package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.Version;

class AggregateHistoryTest {

  @Test
  void emptyReturnsInitialVersion() {
    AggregateHistory h = AggregateHistory.empty();
    assertNull(h.snapshotState());
    assertTrue(h.events().isEmpty());
    assertEquals(Version.initial(), h.version());
    assertEquals(Version.initial(), h.lastSnapshotVersion());
  }

  @Test
  void rejectsNullEvents() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new AggregateHistory(null, null, Version.initial(), Version.initial()));
  }

  @Test
  void rejectsNullVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new AggregateHistory(null, List.of(), null, Version.initial()));
  }

  @Test
  void rejectsNullLastSnapshotVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new AggregateHistory(null, List.of(), Version.initial(), null));
  }

  @Test
  void theFourArgumentConstructorIsAStreamNeverPurged() {
    var h = new AggregateHistory(null, List.of(), Version.initial(), Version.initial());
    assertEquals(0, h.snapshotPurgeGeneration());
    assertEquals(0, AggregateHistory.empty().snapshotPurgeGeneration());
  }

  @Test
  void carriesThePurgeGenerationItWasGiven() {
    var h = new AggregateHistory(null, List.of(), Version.initial(), Version.initial(), 3);
    assertEquals(3, h.snapshotPurgeGeneration());
  }

  @Test
  void rejectsANegativePurgeGeneration() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new AggregateHistory(null, List.of(), Version.initial(), Version.initial(), -1));
  }

  @Test
  void eventsAreDefensivelyCopied() {
    var events = new ArrayList<EventEnvelope>();
    var h = new AggregateHistory(null, events, Version.initial(), Version.initial());
    assertThrows(UnsupportedOperationException.class, () -> h.events().add(null));
  }
}
