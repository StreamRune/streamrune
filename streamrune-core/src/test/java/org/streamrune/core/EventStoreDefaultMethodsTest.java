package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Verifies the {@link EventStore} default methods fail fast instead of silently dropping the
 * snapshot schema version they promise to honor.
 */
class EventStoreDefaultMethodsTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  /** Minimal implementation that overrides only the abstract methods. */
  private static final class MinimalEventStore implements EventStore {

    boolean loadCalled;

    @Override
    public AggregateHistory load(StreamId streamId) {
      loadCalled = true;
      return new AggregateHistory(null, List.of(), Version.initial(), Version.initial());
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new AssertionError("3-arg saveSnapshot must not be reached by the versioned default");
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return List.of();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return List.of();
    }
  }

  private record DummyState() implements AggregateState {}

  @Test
  void versionedLoadDefault_delegatesWhenNoVersionCheckRequested() {
    var store = new MinimalEventStore();
    var history = store.load(StreamId.of(TYPE, AggregateId.of("s1")), 0);
    assertNotNull(history);
    assertTrue(store.loadCalled, "expectedSnapshotVersion == 0 must delegate to load(streamId)");
  }

  @Test
  void versionedLoadDefault_failsFastInsteadOfSkippingTheVersionCheck() {
    var store = new MinimalEventStore();
    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> store.load(StreamId.of(TYPE, AggregateId.of("s1")), 2));
    assertTrue(ex.getMessage().contains("snapshot schema versioning"), ex.getMessage());
    assertFalse(
        store.loadCalled, "the default must not silently return a possibly stale-schema snapshot");
  }

  @Test
  void versionedLoadDefault_rejectsExpectedSnapshotVersionBelowIgnoreSnapshot() {
    // IGNORE_SNAPSHOT (-1) is now a reserved, valid value — only values BELOW it (-2 and
    // lower) are rejected.
    var store = new MinimalEventStore();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> store.load(StreamId.of(TYPE, AggregateId.of("s1")), -2));
    assertTrue(
        ex.getMessage()
            .contains("expectedSnapshotVersion must be >= " + EventStore.IGNORE_SNAPSHOT),
        ex.getMessage());
    assertFalse(store.loadCalled, "a rejected argument must not trigger a load");
  }

  @Test
  void versionedLoadDefault_ignoreSnapshotDelegatesLikeZero() {
    // SnapshotPolicy.never() (the DEFAULT policy VirtualThreadCommandBus uses) drives
    // every load through IGNORE_SNAPSHOT, so an implementation that never overrides this method —
    // and therefore has no real snapshot storage to begin with, so load(streamId) never returns a
    // snapshot anyway — must keep working out of the box, exactly as it already did for 0. Only an
    // implementation that DOES store snapshots (and so already overrides this method for >= 1)
    // needs to give IGNORE_SNAPSHOT its real, snapshot-skipping meaning.
    var store = new MinimalEventStore();
    var history = store.load(StreamId.of(TYPE, AggregateId.of("s1")), EventStore.IGNORE_SNAPSHOT);
    assertNotNull(history);
    assertTrue(store.loadCalled, "IGNORE_SNAPSHOT must delegate to load(streamId) by default");
  }

  @Test
  void versionedSaveSnapshotDefault_failsFastInsteadOfDroppingTheVersion() {
    var store = new MinimalEventStore();
    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                store.saveSnapshot(
                    StreamId.of(TYPE, AggregateId.of("s1")), new Version(3), new DummyState(), 2));
    assertTrue(ex.getMessage().contains("snapshot schema versioning"), ex.getMessage());
  }

  @Test
  void versionedSaveSnapshotDefault_rejectsSnapshotVersionBelowOne() {
    var store = new MinimalEventStore();
    for (int invalid : new int[] {0, -1}) {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  store.saveSnapshot(
                      StreamId.of(TYPE, AggregateId.of("s1")),
                      new Version(3),
                      new DummyState(),
                      invalid));
      assertTrue(ex.getMessage().contains("snapshotVersion must be >= 1"), ex.getMessage());
    }
  }

  @Test
  void lastGlobalOffsetDefault_throwsUnsupportedOperation() {
    var store = new MinimalEventStore();
    var ex = assertThrows(UnsupportedOperationException.class, store::lastGlobalOffset);
    assertTrue(ex.getMessage().contains("lastGlobalOffset"), ex.getMessage());
  }

  // --- AppendResult ---

  @Test
  void appendResult_rejectsNullGlobalOffsets() {
    assertThrows(
        IllegalArgumentException.class, () -> new EventStore.AppendResult(null, Version.initial()));
  }

  @Test
  void appendResult_rejectsNullFinalVersion() {
    assertThrows(
        IllegalArgumentException.class, () -> new EventStore.AppendResult(List.of(), null));
  }

  @Test
  void appendResult_copiesGlobalOffsetsDefensively() {
    var mutable = new java.util.ArrayList<GlobalOffset>();
    mutable.add(GlobalOffset.of(1));
    var result = new EventStore.AppendResult(mutable, new Version(1));
    mutable.add(GlobalOffset.of(2));
    assertEquals(List.of(GlobalOffset.of(1)), result.globalOffsets());
    assertThrows(
        UnsupportedOperationException.class, () -> result.globalOffsets().add(GlobalOffset.of(3)));
  }

  // --- IdempotentAppendResult ---

  @Test
  void idempotentAppendResult_rejectsNullGlobalOffsets() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new EventStore.IdempotentAppendResult(null, Version.initial(), false));
  }

  @Test
  void idempotentAppendResult_rejectsNullFinalVersion() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new EventStore.IdempotentAppendResult(List.of(), null, false));
  }

  @Test
  void idempotentAppendResult_copiesGlobalOffsetsDefensively() {
    var mutable = new java.util.ArrayList<GlobalOffset>();
    mutable.add(GlobalOffset.of(5));
    var result = new EventStore.IdempotentAppendResult(mutable, new Version(2), false);
    mutable.add(GlobalOffset.of(99));
    assertEquals(List.of(GlobalOffset.of(5)), result.globalOffsets());
    assertThrows(
        UnsupportedOperationException.class, () -> result.globalOffsets().add(GlobalOffset.of(1)));
  }

  @Test
  void idempotentAppendResult_alreadyApplied_flagPreserved() {
    var fresh = new EventStore.IdempotentAppendResult(List.of(), Version.initial(), false);
    var replay = new EventStore.IdempotentAppendResult(List.of(), Version.initial(), true);
    assertFalse(fresh.alreadyApplied());
    assertTrue(replay.alreadyApplied());
  }

  @Test
  void appendWithKeyDefault_throwsUnsupportedOperation() {
    var store = new MinimalEventStore();
    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                store.appendWithKey(
                    StreamId.of(TYPE, AggregateId.of("s1")),
                    List.of(),
                    Version.initial(),
                    org.streamrune.core.types.IdempotencyKey.of("test-key"),
                    "com.example.Cmd"));
    assertTrue(ex.getMessage().contains("command inbox"), ex.getMessage());
  }
}
