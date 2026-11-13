package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The contract every {@link EventStore} must honour, so callers can switch backends without a
 * change in behaviour. Run by {@code InMemoryEventStoreContractTest} and {@code
 * PostgresEventStoreContractTest}; extend it and implement {@link #newStore()} to prove a custom
 * store.
 *
 * <p>The promises, as the {@link EventStore} javadoc states them:
 *
 * <ol>
 *   <li><b>Version assignment.</b> An append stores its {@code n} events as versions {@code
 *       expectedVersion + 1 .. expectedVersion + n} in list order, whatever versions the envelopes
 *       carry; a new stream starts at {@link Version#initial()}. The result holds one global offset
 *       per event in the order given, and the final version.
 *   <li><b>Strict conflict detection.</b> An append whose {@code expectedVersion} is not the
 *       stream's head, behind it or beyond it, fails with {@link OptimisticLockException} and
 *       stores nothing, so a stream never has a version gap.
 *   <li><b>Empty appends.</b> An empty event list stores nothing and returns {@code new
 *       AppendResult(List.of(), expectedVersion)}.
 *   <li><b>Atomicity.</b> A multi-event append stores all of its events or none, also when several
 *       race for one version.
 *   <li><b>No idempotence by default.</b> Appending an envelope twice stores two events.
 *   <li><b>Reads.</b> {@link EventStore#load} returns a stream's events in version order; {@link
 *       EventStore#readStream} reads one stream strictly after a version, {@link
 *       EventStore#readGlobalStream} every stream strictly after a global offset, in append order;
 *       both return at most {@code maxCount} events. The same id value under another aggregate type
 *       is another stream.
 *   <li><b>The global head.</b> {@link EventStore#lastGlobalOffset()} is {@link
 *       GlobalOffset#initial()} for an empty store and the offset of the last stored event after;
 *       nothing becomes readable at or below a head once it was returned, also while appends run
 *       concurrently.
 *   <li><b>Snapshots.</b> A saved snapshot is loaded with only the events after it; a later one
 *       replaces it; {@code load(streamId, n)} uses it only when its schema version is {@code n},
 *       uses any with {@code 0}, never reads it with {@link EventStore#IGNORE_SNAPSHOT}, and
 *       rejects {@code n} below that with {@link IllegalArgumentException}; a schema version below
 *       {@code 1} and a negative purge generation are rejected; a save at generation {@code 0} of a
 *       stream never purged is stored. For a store with a purge record ({@link
 *       #purgeSnapshot(StreamId)}): a purge removes the snapshot and raises the generation, and a
 *       save from a load before the purge writes nothing.
 *   <li><b>Idempotent append.</b> {@link EventStore#appendWithKey(StreamId, List, Version,
 *       IdempotencyKey, String)} stores the events once: a replay of the key returns the recorded
 *       offsets and version with {@code alreadyApplied()} and stores nothing, whatever it carries;
 *       a zero-event command claims its key too; a key recorded for another command type or stream
 *       is refused with {@link IllegalArgumentException}; a keyed append refused for its version
 *       leaves the key unclaimed. With no saga origin the six-argument method is the same append; a
 *       store that cannot fence ({@link EventStore#sagaFenceStorageIdentity()} is {@code null})
 *       refuses an append that names one with {@link UnsupportedOperationException}. The fence
 *       itself is the contract of {@link SagaForwardFenceContract}.
 *   <li><b>Capabilities.</b> The javadoc lets a store leave four capabilities out, each answered
 *       with {@link UnsupportedOperationException}: the idempotent append, snapshots, snapshot
 *       schema versions ({@code load(streamId, n)} with {@code n >= 1}) and the global head. Each
 *       has a {@code supports…()} hook, {@code true} by default; a store that leaves one out
 *       overrides its hook to return {@code false}, its cases are skipped, and the contract checks
 *       that the store refuses it instead of answering wrongly.
 * </ol>
 *
 * <p>The contract appends {@link ContractEvent}s under {@link #EVENT_TYPE} and saves {@link
 * ContractState} snapshots: a store that resolves types through a registry must resolve both.
 */
public abstract class EventStoreContract {

  /**
   * A fresh store with empty storage for each test; with a command inbox when {@link
   * #supportsIdempotentAppend()}.
   */
  protected abstract EventStore newStore();

  /** Whether the store implements {@code appendWithKey}; {@code true} by default. */
  protected boolean supportsIdempotentAppend() {
    return true;
  }

  /** Whether the store keeps snapshots; {@code true} by default. */
  protected boolean supportsSnapshots() {
    return true;
  }

  /**
   * Whether the store honours a snapshot schema version ({@code load(streamId, n)} with {@code n >=
   * 1}); {@code true} by default.
   */
  protected boolean supportsSnapshotSchemaVersions() {
    return true;
  }

  /** Whether the store reports {@code lastGlobalOffset()}; {@code true} by default. */
  protected boolean supportsLastGlobalOffset() {
    return true;
  }

  /**
   * Purges the snapshot of {@code streamId} as the store's {@code SnapshotPurger} does, raising the
   * stream's purge generation, and returns {@code true}. The default returns {@code false}: the
   * store keeps no purge record, and the purge cases are skipped.
   */
  protected boolean purgeSnapshot(StreamId streamId) {
    return false;
  }

  /** The event the contract appends. */
  public record ContractEvent(String value) implements DomainEvent {
    @JsonCreator
    public ContractEvent(@JsonProperty("value") String value) {
      this.value = value;
    }
  }

  /** The snapshot state the contract saves. */
  public record ContractState(String marker) implements AggregateState {
    @JsonCreator
    public ContractState(@JsonProperty("marker") String marker) {
      this.marker = marker;
    }
  }

  /** The event type name the contract's events are stored under. */
  public static final EventType EVENT_TYPE = new EventType("ContractEvent");

  protected static final AggregateType ORDER = AggregateType.of("contract_order");
  protected static final StreamId A = StreamId.of(ORDER, AggregateId.of("order-A"));
  protected static final StreamId B = StreamId.of(ORDER, AggregateId.of("order-B"));

  /** The same id value as {@link #A}, under another aggregate type: a different stream. */
  protected static final StreamId A_OTHER_TYPE =
      StreamId.of(AggregateType.of("contract_invoice"), AggregateId.of("order-A"));

  protected static final String COMMAND_TYPE = "com.example.contract.PlaceOrder";
  protected static final String OTHER_COMMAND_TYPE = "com.example.contract.CancelOrder";

  protected EventStore store;

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────────────────────

  /** The envelope of one contract event, its payload naming the stream and {@code version}. */
  protected static EventEnvelope event(StreamId streamId, long version) {
    return EventStoreFixture.event(
        streamId,
        new Version(version),
        EVENT_TYPE,
        new ContractEvent(streamId.aggregateId().value() + "@" + version));
  }

  /** The envelopes of {@code count} events carrying versions {@code from .. from + count - 1}. */
  protected static List<EventEnvelope> events(StreamId streamId, long from, int count) {
    return LongStream.range(from, from + count).mapToObj(v -> event(streamId, v)).toList();
  }

  protected List<EventEnvelope> stored(StreamId streamId) {
    return store.readStream(streamId, Version.initial(), 10_000);
  }

  protected List<EventEnvelope> global() {
    return store.readGlobalStream(GlobalOffset.initial(), 10_000);
  }

  protected static List<Long> versions(List<EventEnvelope> events) {
    return events.stream().map(e -> e.version().value()).toList();
  }

  protected static List<GlobalOffset> offsets(List<EventEnvelope> events) {
    return events.stream().map(EventEnvelope::globalOffset).toList();
  }

  protected static List<String> values(List<EventEnvelope> events) {
    return events.stream().map(e -> ((ContractEvent) e.event()).value()).toList();
  }

  protected static void assertStrictlyIncreasing(List<GlobalOffset> offsets) {
    for (int i = 1; i < offsets.size(); i++) {
      assertTrue(
          offsets.get(i).value() > offsets.get(i - 1).value(),
          "global offsets must increase: " + offsets);
    }
  }

  protected IdempotentAppendResult appendWithKey(
      StreamId streamId, long head, int count, IdempotencyKey key, String commandType) {
    return store.appendWithKey(
        streamId, events(streamId, head + 1, count), new Version(head), key, commandType);
  }

  // ── Version assignment ──────────────────────────────────────────────────────────────────────

  @Test
  public void aFirstAppendStoresVersionsFromOneInListOrder() {
    AppendResult result = store.append(A, events(A, 1, 3), Version.initial());

    assertEquals(new Version(3), result.finalVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(stored(A)));
    assertEquals(List.of("order-A@1", "order-A@2", "order-A@3"), values(stored(A)));
  }

  @Test
  public void versionsContinueFromTheExpectedVersion() {
    store.append(A, events(A, 1, 2), Version.initial());

    AppendResult result = store.append(A, events(A, 3, 2), new Version(2));

    assertEquals(new Version(4), result.finalVersion());
    assertEquals(List.of(1L, 2L, 3L, 4L), versions(stored(A)));
  }

  @Test
  public void theVersionsTheEnvelopesCarryAreNotConsulted() {
    var misnumbered = List.of(event(A, 7), event(A, 7), event(A, 42));

    AppendResult result = store.append(A, misnumbered, Version.initial());

    assertEquals(new Version(3), result.finalVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(stored(A)));
  }

  @Test
  public void anAppendReturnsOneGlobalOffsetPerEventInOrder() {
    AppendResult result = store.append(A, events(A, 1, 3), Version.initial());

    assertEquals(3, result.globalOffsets().size());
    assertStrictlyIncreasing(result.globalOffsets());
    assertEquals(result.globalOffsets(), offsets(stored(A)));
    assertEquals(result.globalOffsets(), offsets(global()));
  }

  @Test
  public void globalOffsetsIncreaseAcrossStreamsInAppendOrder() {
    AppendResult first = store.append(A, events(A, 1, 2), Version.initial());
    AppendResult second = store.append(B, events(B, 1, 2), Version.initial());

    assertTrue(
        second.globalOffsets().getFirst().value() > first.globalOffsets().getLast().value(),
        "a later append has greater offsets, whichever stream it writes");
  }

  @Test
  public void theOffsetsOfAnAppendAreTheOffsetsItsEventsReadBackWith() {
    AppendResult result = store.append(A, events(A, 1, 2), Version.initial());

    assertEquals(result.globalOffsets(), offsets(store.load(A).events()));
    assertEquals(result.globalOffsets(), offsets(stored(A)));
    assertEquals(result.globalOffsets(), offsets(global()));
  }

  // ── Strict conflict detection ───────────────────────────────────────────────────────────────

  @Test
  public void aStaleExpectedVersionIsRejectedAndStoresNothing() {
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 2, 1), new Version(1)));

    assertEquals(List.of(1L, 2L), versions(stored(A)));
    assertEquals(2, global().size());
  }

  @Test
  public void aFutureExpectedVersionIsRejectedAndStoresNothing() {
    store.append(A, events(A, 1, 2), Version.initial());

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 4, 1), new Version(3)));

    assertEquals(List.of(1L, 2L), versions(stored(A)));
    assertEquals(2, global().size());
  }

  @Test
  public void aFutureExpectedVersionOnANewStreamIsRejected() {
    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 2, 1), new Version(1)));

    assertTrue(stored(A).isEmpty());
    assertTrue(global().isEmpty());
  }

  @Test
  public void afterAConflictTheStreamContinuesWithoutAGap() {
    store.append(A, events(A, 1, 1), Version.initial());
    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 6, 1), new Version(5)));

    store.append(A, events(A, 2, 2), new Version(1));

    assertEquals(List.of(1L, 2L, 3L), versions(stored(A)));
  }

  // ── Empty appends ───────────────────────────────────────────────────────────────────────────

  @Test
  public void anEmptyAppendStoresNothingAndReturnsTheExpectedVersion() {
    AppendResult result = store.append(A, List.of(), new Version(5));

    assertEquals(new AppendResult(List.of(), new Version(5)), result);
    assertTrue(stored(A).isEmpty());
    assertTrue(global().isEmpty());
  }

  @Test
  public void anEmptyAppendToAnExistingStreamStoresNothing() {
    store.append(A, events(A, 1, 2), Version.initial());

    AppendResult result = store.append(A, List.of(), new Version(2));

    assertEquals(new AppendResult(List.of(), new Version(2)), result);
    assertEquals(2, stored(A).size());
  }

  // ── Atomicity ───────────────────────────────────────────────────────────────────────────────

  @Test
  public void aRejectedMultiEventAppendStoresNoneOfItsEvents() {
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 1, 3), Version.initial()));

    assertEquals(List.of("order-A@1"), values(stored(A)));
    assertEquals(1, global().size());
  }

  @Test
  public void multiEventAppendsRacingForOneVersionStoreExactlyOneWholeBatch() throws Exception {
    int racers = 8;
    var start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(racers);
    try {
      List<Future<Boolean>> outcomes = new ArrayList<>();
      for (int r = 0; r < racers; r++) {
        String batch = "racer-" + r;
        outcomes.add(
            pool.submit(
                () -> {
                  start.await();
                  var envelopes =
                      LongStream.rangeClosed(1, 3)
                          .mapToObj(
                              v ->
                                  EventStoreFixture.event(
                                      A,
                                      new Version(v),
                                      EVENT_TYPE,
                                      new ContractEvent(batch + "#" + v)))
                          .toList();
                  try {
                    store.append(A, envelopes, Version.initial());
                    return true;
                  } catch (OptimisticLockException e) {
                    return false;
                  }
                }));
      }
      start.countDown();
      int winners = 0;
      for (var outcome : outcomes) {
        if (outcome.get(60, TimeUnit.SECONDS)) {
          winners++;
        }
      }

      assertEquals(1, winners, "exactly one append wins the version");
      List<EventEnvelope> stream = stored(A);
      assertEquals(List.of(1L, 2L, 3L), versions(stream));
      String winner = values(stream).getFirst().split("#")[0];
      assertEquals(
          List.of(winner + "#1", winner + "#2", winner + "#3"),
          values(stream),
          "the stream holds the whole batch of the winner and nothing of the others");
      assertEquals(3, global().size());
    } finally {
      pool.shutdownNow();
    }
  }

  // ── No idempotence by default ───────────────────────────────────────────────────────────────

  @Test
  public void appendingTheSameEnvelopeTwiceStoresTwoEvents() {
    EventEnvelope envelope = event(A, 1);
    store.append(A, List.of(envelope), Version.initial());

    store.append(A, List.of(envelope), new Version(1));

    List<EventEnvelope> stream = stored(A);
    assertEquals(List.of(1L, 2L), versions(stream));
    assertEquals(envelope.metadata().eventId(), stream.get(0).metadata().eventId());
    assertEquals(envelope.metadata().eventId(), stream.get(1).metadata().eventId());
    assertNotEquals(stream.get(0).globalOffset(), stream.get(1).globalOffset());
  }

  // ── Reads ───────────────────────────────────────────────────────────────────────────────────

  @Test
  public void loadOfAnUnknownStreamIsEmpty() {
    AggregateHistory history = store.load(A);

    assertNull(history.snapshotState());
    assertTrue(history.events().isEmpty());
    assertEquals(Version.initial(), history.version());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(0, history.snapshotPurgeGeneration());
  }

  @Test
  public void loadReturnsEveryEventInVersionOrder() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(A, events(A, 3, 1), new Version(2));

    AggregateHistory history = store.load(A);

    assertNull(history.snapshotState());
    assertEquals(List.of(1L, 2L, 3L), versions(history.events()));
    assertEquals(List.of("order-A@1", "order-A@2", "order-A@3"), values(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void theSameIdUnderAnotherAggregateTypeIsAnotherStream() {
    store.append(A, events(A, 1, 2), Version.initial());

    store.append(A_OTHER_TYPE, events(A_OTHER_TYPE, 1, 1), Version.initial());

    assertEquals(2, store.load(A).events().size());
    assertEquals(1, store.load(A_OTHER_TYPE).events().size());
    assertEquals(A_OTHER_TYPE, stored(A_OTHER_TYPE).getFirst().streamId());
  }

  @Test
  public void aReadEventCarriesItsStreamTypePayloadAndMetadata() {
    EventEnvelope envelope = event(A, 1);
    store.append(A, List.of(envelope), Version.initial());

    EventEnvelope read = stored(A).getFirst();

    assertEquals(A, read.streamId());
    assertEquals(EVENT_TYPE, read.eventType());
    assertEquals(envelope.event(), read.event());
    assertEquals(envelope.metadata().eventId(), read.metadata().eventId());
    assertEquals(envelope.metadata().commandId(), read.metadata().commandId());
    assertEquals(envelope.metadata().correlationId(), read.metadata().correlationId());
  }

  @Test
  public void readStreamReadsStrictlyAfterTheGivenVersion() {
    store.append(A, events(A, 1, 5), Version.initial());

    assertEquals(List.of(3L, 4L, 5L), versions(store.readStream(A, new Version(2), 100)));
    assertTrue(store.readStream(A, new Version(5), 100).isEmpty());
  }

  @Test
  public void readStreamReturnsAtMostMaxCountEvents() {
    store.append(A, events(A, 1, 5), Version.initial());

    assertEquals(List.of(1L, 2L), versions(store.readStream(A, Version.initial(), 2)));
    assertEquals(List.of(4L), versions(store.readStream(A, new Version(3), 1)));
  }

  @Test
  public void readStreamReadsOnlyTheGivenStream() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(B, events(B, 1, 1), Version.initial());

    assertEquals(List.of("order-A@1", "order-A@2"), values(stored(A)));
    assertTrue(
        store
            .readStream(StreamId.of(ORDER, AggregateId.of("unknown")), Version.initial(), 10)
            .isEmpty());
  }

  @Test
  public void readGlobalStreamReturnsEveryStreamInAppendOrder() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(B, events(B, 1, 1), Version.initial());
    store.append(A, events(A, 3, 1), new Version(2));

    List<EventEnvelope> all = global();

    assertEquals(List.of("order-A@1", "order-A@2", "order-B@1", "order-A@3"), values(all));
    assertStrictlyIncreasing(offsets(all));
  }

  @Test
  public void readGlobalStreamReadsStrictlyAfterTheGivenOffset() {
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(B, events(B, 1, 2), Version.initial());
    GlobalOffset second = global().get(1).globalOffset();

    assertEquals(List.of("order-B@1", "order-B@2"), values(store.readGlobalStream(second, 100)));
  }

  @Test
  public void readGlobalStreamReturnsAtMostMaxCountEvents() {
    store.append(A, events(A, 1, 5), Version.initial());

    assertEquals(
        List.of("order-A@1", "order-A@2"),
        values(store.readGlobalStream(GlobalOffset.initial(), 2)));
  }

  @Test
  public void readGlobalStreamPastTheLastEventIsEmpty() {
    AppendResult result = store.append(A, events(A, 1, 2), Version.initial());

    assertTrue(store.readGlobalStream(result.globalOffsets().getLast(), 100).isEmpty());
  }

  @Test
  public void readGlobalStreamOfAnEmptyStoreIsEmpty() {
    assertTrue(global().isEmpty());
  }

  // ── The global head ─────────────────────────────────────────────────────────────────────────

  @Test
  public void theHeadOfAnEmptyStoreIsTheInitialOffset() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");

    assertEquals(GlobalOffset.initial(), store.lastGlobalOffset());
  }

  @Test
  public void theHeadIsTheOffsetOfTheLastStoredEvent() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    store.append(A, events(A, 1, 2), Version.initial());
    AppendResult last = store.append(B, events(B, 1, 3), Version.initial());

    assertEquals(last.globalOffsets().getLast(), store.lastGlobalOffset());
  }

  @Test
  public void aRejectedAppendDoesNotMoveTheHead() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    store.append(A, events(A, 1, 1), Version.initial());
    GlobalOffset head = store.lastGlobalOffset();

    assertThrows(
        OptimisticLockException.class, () -> store.append(A, events(A, 1, 2), Version.initial()));

    assertEquals(head, store.lastGlobalOffset());
  }

  @Test
  public void eventsStoredAfterTheHeadWasReadLieAboveIt() {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    store.append(A, events(A, 1, 2), Version.initial());
    GlobalOffset head = store.lastGlobalOffset();

    AppendResult later = store.append(B, events(B, 1, 2), Version.initial());

    later.globalOffsets().forEach(o -> assertTrue(o.value() > head.value(), "above " + head));
    assertEquals(later.globalOffsets(), offsets(store.readGlobalStream(head, 100)));
  }

  /**
   * Writers append to their own streams while a reader repeatedly reads the head and then every
   * readable event at or below it. Once all writers are done, the events at or below each head the
   * reader saw must be exactly the ones it read right after: none became readable later.
   */
  @Test
  public void nothingBecomesReadableAtOrBelowAHeadWhileAppendsRun() throws Exception {
    assumeTrue(supportsLastGlobalOffset(), "the store does not report its head");
    int writers = 4;
    int appendsEach = 25;
    var start = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      List<Future<?>> done = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        StreamId stream = StreamId.of(ORDER, AggregateId.of("writer-" + w));
        done.add(
            pool.submit(
                () -> {
                  start.await();
                  for (int i = 0; i < appendsEach; i++) {
                    store.append(stream, events(stream, i + 1, 1), new Version(i));
                  }
                  return null;
                }));
      }
      record Sample(long head, List<Long> readable) {}
      List<Sample> samples = new ArrayList<>();
      start.countDown();
      while (samples.isEmpty() || !done.stream().allMatch(Future::isDone)) {
        long head = store.lastGlobalOffset().value();
        List<Long> readable =
            global().stream().map(e -> e.globalOffset().value()).filter(o -> o <= head).toList();
        samples.add(new Sample(head, readable));
      }
      for (var writer : done) {
        writer.get(60, TimeUnit.SECONDS);
      }

      List<Long> all = global().stream().map(e -> e.globalOffset().value()).toList();
      assertEquals(writers * appendsEach, all.size());
      for (Sample sample : samples) {
        assertEquals(
            all.stream().filter(o -> o <= sample.head()).toList(),
            sample.readable(),
            "an event became readable at or below head " + sample.head() + " after it was read");
      }
      assertEquals(all.getLast(), store.lastGlobalOffset().value());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  public void aStoreWithoutAHeadRefusesToReportOne() {
    assumeFalse(supportsLastGlobalOffset(), "the store reports its head");

    assertThrows(UnsupportedOperationException.class, () -> store.lastGlobalOffset());
  }

  // ── Snapshots ───────────────────────────────────────────────────────────────────────────────

  @Test
  public void aSavedSnapshotIsLoadedWithOnlyTheEventsAfterIt() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 3), Version.initial());

    store.saveSnapshot(A, new Version(2), new ContractState("at-2"));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("at-2"), history.snapshotState());
    assertEquals(new Version(2), history.lastSnapshotVersion());
    assertEquals(List.of(3L), versions(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void aSnapshotAtTheHeadIsLoadedWithNoEvents() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    store.saveSnapshot(A, new Version(2), new ContractState("at-head"));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("at-head"), history.snapshotState());
    assertTrue(history.events().isEmpty());
    assertEquals(new Version(2), history.version());
  }

  @Test
  public void aLaterSnapshotReplacesTheEarlierOne() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 4), Version.initial());
    store.saveSnapshot(A, new Version(1), new ContractState("at-1"));

    store.saveSnapshot(A, new Version(3), new ContractState("at-3"));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("at-3"), history.snapshotState());
    assertEquals(List.of(4L), versions(history.events()));
  }

  @Test
  public void aSnapshotBelongsToItsStreamOnly() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());
    store.append(A_OTHER_TYPE, events(A_OTHER_TYPE, 1, 2), Version.initial());

    store.saveSnapshot(A, new Version(2), new ContractState("of-A"));

    assertNull(store.load(A_OTHER_TYPE).snapshotState());
    assertEquals(2, store.load(A_OTHER_TYPE).events().size());
  }

  @Test
  public void aSaveAtGenerationZeroOfANeverPurgedStreamIsStored() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 2), Version.initial());

    assertTrue(store.saveSnapshot(A, new Version(2), new ContractState("gen-0"), 1, 0));

    AggregateHistory history = store.load(A);
    assertEquals(new ContractState("gen-0"), history.snapshotState());
    assertEquals(0, history.snapshotPurgeGeneration());
  }

  @Test
  public void aSnapshotSchemaVersionBelowOneIsRejected() {
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        IllegalArgumentException.class,
        () -> store.saveSnapshot(A, new Version(1), new ContractState("v0"), 0));
    if (supportsSnapshots()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> store.saveSnapshot(A, new Version(1), new ContractState("v0"), 0, 0));
      assertNull(store.load(A).snapshotState());
    }
  }

  @Test
  public void aNegativePurgeGenerationIsRejected() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        IllegalArgumentException.class,
        () -> store.saveSnapshot(A, new Version(1), new ContractState("neg"), 1, -1));
    assertNull(store.load(A).snapshotState());
  }

  @Test
  public void loadWithTheSnapshotsSchemaVersionUsesIt() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("schema-2"), 2);

    AggregateHistory history = store.load(A, 2);

    assertEquals(new ContractState("schema-2"), history.snapshotState());
    assertEquals(List.of(3L), versions(history.events()));
  }

  @Test
  public void loadWithAnotherSchemaVersionIgnoresTheSnapshot() {
    assumeTrue(supportsSnapshotSchemaVersions(), "the store has no snapshot schema versions");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("schema-2"), 2);

    AggregateHistory history = store.load(A, 3);

    assertNull(history.snapshotState());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void loadWithZeroUsesAnySnapshot() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("any"), 5);

    AggregateHistory history = store.load(A, 0);

    assertEquals(new ContractState("any"), history.snapshotState());
    assertEquals(List.of(3L), versions(history.events()));
  }

  @Test
  public void loadIgnoringSnapshotsReadsEveryEvent() {
    store.append(A, events(A, 1, 3), Version.initial());
    if (supportsSnapshots()) {
      store.saveSnapshot(A, new Version(2), new ContractState("never-read"));
    }

    AggregateHistory history = store.load(A, EventStore.IGNORE_SNAPSHOT);

    assertNull(history.snapshotState());
    assertEquals(Version.initial(), history.lastSnapshotVersion());
    assertEquals(List.of(1L, 2L, 3L), versions(history.events()));
    assertEquals(new Version(3), history.version());
  }

  @Test
  public void loadWithZeroWorksOnEveryStore() {
    store.append(A, events(A, 1, 2), Version.initial());

    AggregateHistory history = store.load(A, 0);

    assertEquals(List.of(1L, 2L), versions(history.events()));
  }

  @Test
  public void aSchemaVersionBelowIgnoreSnapshotIsRejected() {
    store.append(A, events(A, 1, 1), Version.initial());
    if (supportsSnapshots()) {
      store.saveSnapshot(A, new Version(1), new ContractState("stored"));
    }

    assertThrows(
        IllegalArgumentException.class, () -> store.load(A, EventStore.IGNORE_SNAPSHOT - 1));
  }

  @Test
  public void aStoreWithoutSnapshotsRefusesToSaveOne() {
    assumeFalse(supportsSnapshots(), "the store keeps snapshots");
    store.append(A, events(A, 1, 1), Version.initial());

    assertThrows(
        UnsupportedOperationException.class,
        () -> store.saveSnapshot(A, new Version(1), new ContractState("none"), 1, 0));
    assertNull(store.load(A).snapshotState());
  }

  @Test
  public void aStoreWithoutSchemaVersionsRefusesToMatchOne() {
    assumeFalse(supportsSnapshotSchemaVersions(), "the store has snapshot schema versions");

    assertThrows(UnsupportedOperationException.class, () -> store.load(A, 1));
  }

  // ── Snapshot purge ──────────────────────────────────────────────────────────────────────────

  /** A stream of three events with a snapshot at version 2, then purged; returns the old load. */
  private AggregateHistory snapshottedThenPurged() {
    store.append(A, events(A, 1, 3), Version.initial());
    store.saveSnapshot(A, new Version(2), new ContractState("before-purge"));
    AggregateHistory beforePurge = store.load(A);
    assumeTrue(purgeSnapshot(A), "the store keeps no purge record");
    return beforePurge;
  }

  @Test
  public void aPurgeRemovesTheSnapshotAndRaisesTheGeneration() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    AggregateHistory beforePurge = snapshottedThenPurged();

    AggregateHistory afterPurge = store.load(A);

    assertNull(afterPurge.snapshotState());
    assertEquals(List.of(1L, 2L, 3L), versions(afterPurge.events()));
    assertTrue(
        afterPurge.snapshotPurgeGeneration() > beforePurge.snapshotPurgeGeneration(),
        "a purge raises the stream's generation");
  }

  @Test
  public void aSaveFromALoadBeforeThePurgeWritesNothing() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    AggregateHistory beforePurge = snapshottedThenPurged();

    boolean stored =
        store.saveSnapshot(
            A,
            new Version(3),
            new ContractState("stale"),
            1,
            beforePurge.snapshotPurgeGeneration());

    assertFalse(stored);
    assertNull(store.load(A).snapshotState(), "the purged snapshot stays purged");
  }

  @Test
  public void aSaveFromALoadAfterThePurgeIsStored() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    snapshottedThenPurged();
    AggregateHistory afterPurge = store.load(A);

    boolean stored =
        store.saveSnapshot(
            A, new Version(3), new ContractState("fresh"), 1, afterPurge.snapshotPurgeGeneration());

    assertTrue(stored);
    assertEquals(new ContractState("fresh"), store.load(A).snapshotState());
  }

  @Test
  public void theGenerationlessSaveOfAPurgedStreamIsRefused() {
    assumeTrue(supportsSnapshots(), "the store keeps no snapshots");
    snapshottedThenPurged();

    assertThrows(
        IllegalStateException.class,
        () -> store.saveSnapshot(A, new Version(3), new ContractState("gen-0")));
    assertNull(store.load(A).snapshotState());
  }

  // ── Idempotent append ───────────────────────────────────────────────────────────────────────

  @Test
  public void aFirstKeyedAppendStoresItsEvents() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");

    var result = appendWithKey(A, 0, 2, IdempotencyKey.of("key-first"), COMMAND_TYPE);

    assertFalse(result.alreadyApplied());
    assertEquals(new Version(2), result.finalVersion());
    assertEquals(2, result.globalOffsets().size());
    assertEquals(result.globalOffsets(), offsets(stored(A)));
    assertEquals(List.of(1L, 2L), versions(stored(A)));
  }

  @Test
  public void aReplayReturnsTheRecordedResultAndStoresNothing() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-replayed");
    var first = appendWithKey(A, 0, 2, key, COMMAND_TYPE);

    var replay = appendWithKey(A, 0, 2, key, COMMAND_TYPE);

    assertTrue(replay.alreadyApplied());
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(first.finalVersion(), replay.finalVersion());
    assertEquals(2, stored(A).size());
    assertEquals(2, global().size());
  }

  @Test
  public void aReplayIsAnsweredFromTheRecordWhateverItCarries() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-replay-moved-on");
    var first = appendWithKey(A, 0, 1, key, COMMAND_TYPE);
    store.append(A, events(A, 2, 2), new Version(1));

    var replay = appendWithKey(A, 0, 3, key, COMMAND_TYPE);

    assertTrue(replay.alreadyApplied(), "a stale version and other events are still a replay");
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(first.finalVersion(), replay.finalVersion());
    assertEquals(3, stored(A).size());
  }

  @Test
  public void aZeroEventCommandClaimsItsKey() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-zero-events");

    var first = store.appendWithKey(A, List.of(), Version.initial(), key, COMMAND_TYPE);
    var replay = store.appendWithKey(A, List.of(), Version.initial(), key, COMMAND_TYPE);

    assertFalse(first.alreadyApplied());
    assertTrue(first.globalOffsets().isEmpty());
    assertEquals(Version.initial(), first.finalVersion());
    assertTrue(replay.alreadyApplied(), "the zero-event command is not re-run on redelivery");
    assertTrue(stored(A).isEmpty());
    assertTrue(global().isEmpty());
  }

  @Test
  public void aKeyRecordedForAnotherCommandTypeIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-other-command");
    appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertThrows(
        IllegalArgumentException.class, () -> appendWithKey(A, 1, 1, key, OTHER_COMMAND_TYPE));

    assertEquals(1, stored(A).size());
  }

  @Test
  public void aKeyRecordedForAnotherStreamIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-other-stream");
    appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertThrows(IllegalArgumentException.class, () -> appendWithKey(B, 0, 1, key, COMMAND_TYPE));

    assertTrue(stored(B).isEmpty());
  }

  @Test
  public void aKeyRecordedForTheSameIdUnderAnotherAggregateTypeIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-other-type");
    appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertThrows(
        IllegalArgumentException.class, () -> appendWithKey(A_OTHER_TYPE, 0, 1, key, COMMAND_TYPE));

    assertTrue(stored(A_OTHER_TYPE).isEmpty());
  }

  @Test
  public void aKeyedAppendRefusedForAStaleVersionLeavesTheKeyUnclaimed() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    store.append(A, events(A, 1, 1), Version.initial());
    IdempotencyKey key = IdempotencyKey.of("key-stale");

    assertThrows(OptimisticLockException.class, () -> appendWithKey(A, 0, 1, key, COMMAND_TYPE));
    assertEquals(1, stored(A).size());

    var retried = appendWithKey(A, 1, 1, key, COMMAND_TYPE);
    assertFalse(retried.alreadyApplied(), "the refused append left its key unclaimed");
    assertEquals(List.of(1L, 2L), versions(stored(A)));
  }

  @Test
  public void aKeyedAppendWithAFutureVersionIsRefused() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");

    assertThrows(
        OptimisticLockException.class,
        () -> appendWithKey(A, 3, 1, IdempotencyKey.of("key-future"), COMMAND_TYPE));

    assertTrue(stored(A).isEmpty());
  }

  @Test
  public void distinctKeysAppendIndependently() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");

    var first = appendWithKey(A, 0, 1, IdempotencyKey.of("key-one"), COMMAND_TYPE);
    var second = appendWithKey(A, 1, 1, IdempotencyKey.of("key-two"), COMMAND_TYPE);

    assertFalse(first.alreadyApplied());
    assertFalse(second.alreadyApplied());
    assertEquals(new Version(2), second.finalVersion());
    assertEquals(List.of(1L, 2L), versions(stored(A)));
  }

  @Test
  public void withoutASagaOriginTheSixArgumentAppendIsTheKeyedAppend() {
    assumeTrue(supportsIdempotentAppend(), "the store has no command inbox");
    IdempotencyKey key = IdempotencyKey.of("key-no-origin");

    var first = store.appendWithKey(A, events(A, 1, 1), Version.initial(), key, COMMAND_TYPE, null);
    var replay = appendWithKey(A, 0, 1, key, COMMAND_TYPE);

    assertFalse(first.alreadyApplied());
    assertTrue(replay.alreadyApplied(), "both methods share one inbox");
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(1, stored(A).size());
  }

  @Test
  public void aStoreThatCannotFenceRefusesAFencedAppend() {
    assumeTrue(
        store.sagaFenceStorageIdentity() == null, "the store fences: see SagaForwardFenceContract");
    SagaOrigin forwardOf =
        SagaEnvelopes.metadataCausedBy(
                SagaForwardFenceContract.FenceState.class, SagaId.of("saga-1"))
            .sagaOrigin();

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            store.appendWithKey(
                A,
                events(A, 1, 1),
                Version.initial(),
                IdempotencyKey.of("key-fenced"),
                COMMAND_TYPE,
                forwardOf));

    assertTrue(stored(A).isEmpty(), "a store that cannot fence must not append unfenced");
  }

  @Test
  public void aStoreWithoutAnInboxRefusesAKeyedAppend() {
    assumeFalse(supportsIdempotentAppend(), "the store has a command inbox");

    assertThrows(
        UnsupportedOperationException.class,
        () -> appendWithKey(A, 0, 1, IdempotencyKey.of("key-unsupported"), COMMAND_TYPE));

    assertTrue(stored(A).isEmpty());
  }
}
