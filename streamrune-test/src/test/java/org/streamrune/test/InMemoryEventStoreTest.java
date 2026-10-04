package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class InMemoryEventStoreTest {

  private static final AggregateType TYPE = AggregateType.of("cart");

  record Added(String sku) implements DomainEvent {}

  record CartState(int items) implements AggregateState {}

  private EventEnvelope envelope(StreamId id, long v, String sku) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        id,
        new Version(v),
        new EventType("Added"),
        new Added(sku),
        new EventMetadata(
            EventId.of("evt_" + v),
            CommandId.of("cmd_" + v),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  @Test
  void appendsAndLoadsEvents() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));

    var result = store.append(s, List.of(envelope(s, 1, "A")), Version.initial());
    assertEquals(1, result.globalOffsets().size());
    assertEquals(1, result.finalVersion().value());

    var history = store.load(s);
    assertEquals(1, history.events().size());
    assertEquals(1, history.version().value());
    assertNull(history.snapshotState());
  }

  @Test
  void emptyEventsReturnsExpectedVersion() {
    var store = new InMemoryEventStore();
    var result =
        store.append(StreamId.of(TYPE, AggregateId.of("cart-1")), List.of(), Version.initial());
    assertTrue(result.globalOffsets().isEmpty());
    assertEquals(0, result.finalVersion().value());
  }

  @Test
  void rejectsNullArgumentsLikePostgres() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    assertThrows(
        NullPointerException.class, () -> store.append(null, List.of(), Version.initial()));
    assertThrows(NullPointerException.class, () -> store.append(s, null, Version.initial()));
    assertThrows(NullPointerException.class, () -> store.append(s, List.of(), null));
    assertThrows(NullPointerException.class, () -> store.load(null));
  }

  @Test
  void throwsOptimisticLockOnVersionMismatch() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(s, List.of(envelope(s, 1, "A")), Version.initial());

    assertThrows(
        OptimisticLockException.class,
        () -> store.append(s, List.of(envelope(s, 1, "B")), Version.initial()));
  }

  @Test
  void twoAggregateTypesSharingAnIdValue_areTwoIndependentStreams() {
    var store = new InMemoryEventStore();
    StreamId product = StreamId.of(AggregateType.of("product"), AggregateId.of("sku-1"));
    StreamId inventory = StreamId.of(AggregateType.of("inventory"), AggregateId.of("sku-1"));

    store.append(product, List.of(envelope(product, 1, "A")), Version.initial());
    // Version 1 of the other stream is free: the stream key is the pair, not the id value.
    store.append(inventory, List.of(envelope(inventory, 1, "B")), Version.initial());

    assertEquals(1, store.load(product).events().size());
    assertEquals(1, store.load(inventory).events().size());
    assertEquals(product, store.load(product).events().getFirst().streamId());
    assertEquals(inventory, store.load(inventory).events().getFirst().streamId());
    assertThrows(
        OptimisticLockException.class,
        () -> store.append(inventory, List.of(envelope(inventory, 1, "C")), Version.initial()));
  }

  // Postgres parity: the exception message matches PostgresEventStore's unique-constraint
  // wording exactly, so message assertions written against this store also hold in
  // integration tests. The strict head check names BOTH the
  // expected and the actual head — it no longer "cannot report the actual head version" the way
  // the old 23505-only detection did, because it reads the head itself instead of inferring a
  // conflict from a failed insert.
  @Test
  void optimisticLockMessageMatchesPostgresWording() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(s, List.of(envelope(s, 1, "A")), Version.initial());

    var ex =
        assertThrows(
            OptimisticLockException.class,
            () -> store.append(s, List.of(envelope(s, 1, "B")), Version.initial()));
    assertEquals(
        "Version conflict on stream 'cart:cart-1': expected version 0 but actual head is 1",
        ex.getMessage());
  }

  // Postgres parity: a stream id is caller-supplied text, so the conflict message renders it
  // sanitized, as PostgresEventStore's does.
  @Test
  void optimisticLockMessageRendersAForgedStreamIdSanitized() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, new AggregateId("cart-1\n2026-10-04 WARN forged\r\u202e"));
    store.append(s, List.of(envelope(s, 1, "A")), Version.initial());

    var ex =
        assertThrows(
            OptimisticLockException.class,
            () -> store.append(s, List.of(envelope(s, 1, "B")), Version.initial()));

    assertEquals(
        "Version conflict on stream 'cart:cart-12026-10-04 WARN forged': expected version 0 but"
            + " actual head is 1",
        ex.getMessage());
  }

  // Contract note: this test used to be named
  // gapAppendBeyondCurrentHeadSucceedsLikePostgres and asserted the OPPOSITE outcome — that an
  // expectedVersion past the current head succeeded and left a version gap, calling that
  // "Postgres parity". The parity was real (both stores used the (aggregate_type, aggregate_id,
  // version) unique-constraint model exclusively) but the resulting contract was
  // self-contradictory: see
  // EventStore#append's javadoc, which said "throws if expectedVersion does not match the current
  // version" and "a future expectedVersion succeeds" in the same paragraph. Neither store can
  // create a version gap through append/appendWithKey any more; expectedVersion must equal the
  // actual head exactly, both for a stale (behind-head) AND a future (beyond-head) value.
  @Test
  void futureExpectedVersionIsRejected() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(s, List.of(envelope(s, 1, "A")), Version.initial());

    var ex =
        assertThrows(
            OptimisticLockException.class,
            () -> store.append(s, List.of(envelope(s, 6, "B")), new Version(5)));
    assertEquals(
        "Version conflict on stream 'cart:cart-1': expected version 5 but actual head is 1",
        ex.getMessage());

    // The rejected append left no trace — the stream is exactly as it was before the attempt —
    // and a correctly targeted append still succeeds normally afterward.
    var history = store.load(s);
    assertEquals(1, history.events().size());
    assertEquals(1, history.version().value());

    var result = store.append(s, List.of(envelope(s, 2, "C")), new Version(1));
    assertEquals(2, result.finalVersion().value());
  }

  // The documented zero-event no-op (EventStore#append) carries no
  // events to order against the stream, so it is deliberately EXEMPT from the strict head check
  // above — even a wildly mismatched expectedVersion succeeds, unchanged by the strict check.
  // Pinned so a future
  // change cannot silently tighten this without a failing test to update deliberately.
  @Test
  void emptyEventsSucceedsRegardlessOfExpectedVersionMismatch() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(s, List.of(envelope(s, 1, "A")), Version.initial());

    var result = store.append(s, List.of(), new Version(99));
    assertTrue(result.globalOffsets().isEmpty());
    assertEquals(
        99,
        result.finalVersion().value(),
        "the no-op echoes the caller's expectedVersion, not the actual head");

    var history = store.load(s);
    assertEquals(1, history.events().size(), "the zero-event no-op must not touch the stream");
    assertEquals(1, history.version().value());
  }

  @Test
  void multiEventAppendThrowsWhenAnyComputedVersionCollides() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-1"));
    store.append(
        s,
        List.of(envelope(s, 1, "A"), envelope(s, 2, "B"), envelope(s, 3, "C")),
        Version.initial());

    // expectedVersion 2 computes versions 3 and 4; version 3 already exists.
    assertThrows(
        OptimisticLockException.class,
        () -> store.append(s, List.of(envelope(s, 3, "X"), envelope(s, 4, "Y")), new Version(2)));
  }

  @Test
  void readGlobalStreamReturnsOrderedEvents() {
    var store = new InMemoryEventStore();
    StreamId a = StreamId.of(TYPE, AggregateId.of("cart-a"));
    StreamId b = StreamId.of(TYPE, AggregateId.of("cart-b"));
    store.append(a, List.of(envelope(a, 1, "X")), Version.initial());
    store.append(b, List.of(envelope(b, 1, "Y")), Version.initial());
    store.append(a, List.of(envelope(a, 2, "Z")), new Version(1));

    List<EventEnvelope> all = store.readGlobalStream(GlobalOffset.initial(), 100);
    assertEquals(3, all.size());
    assertEquals(1, all.get(0).globalOffset().value());
    assertEquals(3, all.get(2).globalOffset().value());
  }

  @Test
  void readGlobalStreamIsStrictlyOffsetOrderedUnderConcurrentAppends() throws Exception {
    var store = new InMemoryEventStore();
    int writers = 8;
    int eventsPerWriter = 25;
    var start = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(writers)) {
      for (int w = 0; w < writers; w++) {
        StreamId s = StreamId.of(TYPE, AggregateId.of("cart-" + w));
        executor.submit(
            () -> {
              start.await();
              for (int i = 0; i < eventsPerWriter; i++) {
                var history = store.load(s);
                store.append(s, List.of(envelope(s, i + 1, "X")), history.version());
              }
              return null;
            });
      }
      start.countDown();
      executor.shutdown();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }

    List<EventEnvelope> all = store.readGlobalStream(GlobalOffset.initial(), Integer.MAX_VALUE);
    assertEquals(writers * eventsPerWriter, all.size());
    List<Long> offsets = new ArrayList<>(all.stream().map(e -> e.globalOffset().value()).toList());
    for (int i = 0; i < offsets.size(); i++) {
      assertEquals(i + 1L, offsets.get(i), "offsets must be strictly increasing and gap-free");
    }
  }

  @Test
  void readGlobalStreamRespectsMaxCount() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "X")), Version.initial());
    store.append(s, List.of(envelope(s, 2, "Y")), new Version(1));
    store.append(s, List.of(envelope(s, 3, "Z")), new Version(2));

    assertEquals(2, store.readGlobalStream(GlobalOffset.initial(), 2).size());
  }

  @Test
  void readGlobalStreamPagingCoversAllEventsWithNoGapOrDuplicate() {
    // Parity with PostgresEventStore's gapless global reader: paging strictly by the
    // last delivered offset — the same pattern a projection/subscription checkpoint uses — must
    // walk the entire global stream exactly once, in a contiguous 1-based sequence, regardless of
    // how many streams the events came from or how small the page size is.
    var store = new InMemoryEventStore();
    int streamCount = 5;
    int eventsPerStream = 7;
    for (int s = 0; s < streamCount; s++) {
      StreamId streamId = StreamId.of(TYPE, AggregateId.of("paging-cart-" + s));
      List<EventEnvelope> events = new ArrayList<>();
      for (int i = 0; i < eventsPerStream; i++) {
        events.add(envelope(streamId, i + 1, "v" + i));
      }
      store.append(streamId, events, Version.initial());
    }
    int totalEvents = streamCount * eventsPerStream;

    List<Long> pagedOffsets = new ArrayList<>();
    GlobalOffset checkpoint = GlobalOffset.initial();
    int pageSize = 3;
    List<EventEnvelope> page;
    do {
      page = store.readGlobalStream(checkpoint, pageSize);
      assertTrue(page.size() <= pageSize, "a page must never exceed the requested max count");
      for (EventEnvelope e : page) {
        pagedOffsets.add(e.globalOffset().value());
      }
      if (!page.isEmpty()) {
        checkpoint = page.getLast().globalOffset();
      }
    } while (!page.isEmpty());

    assertEquals(totalEvents, pagedOffsets.size(), "paging must cover every appended event once");
    for (int i = 0; i < pagedOffsets.size(); i++) {
      assertEquals(
          i + 1L,
          pagedOffsets.get(i),
          "paged offsets must form a contiguous 1-based prefix with no gap or duplicate");
    }
  }

  @Test
  void readStreamFiltersByVersion() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "A"), envelope(s, 2, "B")), Version.initial());

    assertEquals(2, store.readStream(s, Version.initial(), 100).size());
    assertEquals(1, store.readStream(s, new Version(1), 100).size());
    assertEquals(0, store.readStream(s, new Version(2), 100).size());
  }

  @Test
  void readStreamRespectsMaxCount() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "A"), envelope(s, 2, "B")), Version.initial());

    assertEquals(1, store.readStream(s, Version.initial(), 1).size());
  }

  @Test
  void saveSnapshotPrunesReplayedEvents() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(
        s,
        List.of(envelope(s, 1, "A"), envelope(s, 2, "B"), envelope(s, 3, "C")),
        Version.initial());

    store.saveSnapshot(s, new Version(2), new CartState(2));
    var history = store.load(s);

    assertInstanceOf(CartState.class, history.snapshotState());
    assertEquals(1, history.events().size());
    assertEquals(3, history.events().getFirst().version().value());
    assertEquals(3, history.version().value());
    assertEquals(2, history.lastSnapshotVersion().value());
  }

  @Test
  void loadWithMatchingSnapshotVersionUsesSnapshot() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "A"), envelope(s, 2, "B")), Version.initial());
    store.saveSnapshot(s, new Version(2), new CartState(2), 3);

    var history = store.load(s, 3);
    assertInstanceOf(CartState.class, history.snapshotState());
    assertEquals(0, history.events().size());
    assertEquals(2, history.version().value());
  }

  @Test
  void loadWithMismatchedSnapshotVersionIgnoresSnapshotAndReplaysAllEvents() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "A"), envelope(s, 2, "B")), Version.initial());
    store.saveSnapshot(s, new Version(2), new CartState(2), 3);

    var history = store.load(s, 4);
    assertNull(history.snapshotState());
    assertEquals(2, history.events().size());
    assertEquals(2, history.version().value());
    assertEquals(0, history.lastSnapshotVersion().value());
  }

  @Test
  void loadWithZeroSnapshotVersionSkipsTheCheck() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "A")), Version.initial());
    store.saveSnapshot(s, new Version(1), new CartState(1), 7);

    var history = store.load(s, 0);
    assertInstanceOf(CartState.class, history.snapshotState());
  }

  @Test
  void loadWithIgnoreSnapshotNeverUsesTheSnapshot() {
    // EventStore.IGNORE_SNAPSHOT (the read-side counterpart of SnapshotPolicy.never())
    // must not use the snapshot even though it would match ANY version — unlike 0, it never even
    // looks the snapshot up. A stream that switched to never() with a leftover snapshot must
    // replay fully from events, not silently rehydrate the old snapshot.
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(
        s,
        List.of(envelope(s, 1, "A"), envelope(s, 2, "B"), envelope(s, 3, "C")),
        Version.initial());
    store.saveSnapshot(s, new Version(2), new CartState(2), 7);

    var history = store.load(s, EventStore.IGNORE_SNAPSHOT);

    assertNull(history.snapshotState(), "IGNORE_SNAPSHOT must never hand back a snapshot state");
    assertEquals(
        3, history.events().size(), "every event must replay, not just the post-snapshot ones");
    assertEquals(3, history.version().value());
    assertEquals(0, history.lastSnapshotVersion().value());
  }

  @Test
  void defaultSaveSnapshotStoresSchemaVersionOne() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "A")), Version.initial());
    store.saveSnapshot(s, new Version(1), new CartState(1));

    assertNotNull(store.load(s, 1).snapshotState());
    assertNull(store.load(s, 2).snapshotState());
  }

  @Test
  void saveSnapshotValidatesArgumentsLikePostgres() {
    var store = new InMemoryEventStore();
    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    assertThrows(
        NullPointerException.class,
        () -> store.saveSnapshot(null, new Version(1), new CartState(1)));
    assertThrows(IllegalArgumentException.class, () -> store.saveSnapshot(s, new Version(1), null));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.saveSnapshot(s, new Version(1), new CartState(1), 0));
  }

  @Test
  void lastGlobalOffsetTracksTheHighestAssignedOffset() {
    var store = new InMemoryEventStore();
    assertEquals(GlobalOffset.of(0), store.lastGlobalOffset());

    StreamId s = StreamId.of(TYPE, AggregateId.of("cart-a"));
    store.append(s, List.of(envelope(s, 1, "A"), envelope(s, 2, "B")), Version.initial());
    assertEquals(GlobalOffset.of(2), store.lastGlobalOffset());

    store.append(s, List.of(envelope(s, 3, "C")), new Version(2));
    assertEquals(GlobalOffset.of(3), store.lastGlobalOffset());
  }

  @Test
  void loadReturnsEmptyForUnknownStream() {
    EventStore store = new InMemoryEventStore();
    var history = store.load(StreamId.of(TYPE, AggregateId.of("missing")));
    assertTrue(history.events().isEmpty());
    assertEquals(0, history.version().value());
  }

  // hasCryptoCause must walk the cause chain with a depth bound so a cyclic chain
  // terminates instead of spinning forever.

  /** A throwable whose getCause() returns a settable field, so two of them can form a cycle. */
  static final class CyclicThrowable extends RuntimeException {
    private transient Throwable next;

    void setNext(Throwable next) {
      this.next = next;
    }

    @Override
    public synchronized Throwable getCause() {
      return next;
    }
  }

  @Test
  void hasCryptoCause_cyclicChainWithoutCrypto_terminatesAndReturnsFalse() {
    var a = new CyclicThrowable();
    var b = new CyclicThrowable();
    a.setNext(b);
    b.setNext(a); // a -> b -> a -> ... an unbounded walk would never terminate

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () ->
            assertFalse(
                org.streamrune.test.InMemoryEventStore.hasCryptoCause(a),
                "no CryptoOperationException in the cycle"));
  }

  @Test
  void hasCryptoCause_detectsWrappedCryptoCause() {
    var wrapped =
        new RuntimeException(
            "jackson", new org.streamrune.core.crypto.CryptoOperationException("decrypt failed"));
    assertTrue(org.streamrune.test.InMemoryEventStore.hasCryptoCause(wrapped));
  }
}
