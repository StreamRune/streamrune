package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.stream.LongStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.CommandInbox.InboxResult;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The contract every {@link CommandInbox} must honour, together with the {@link EventStore} that
 * writes its rows. Run by {@code InMemoryCommandInboxContractTest} and {@code
 * PostgresCommandInboxContractTest}. To prove a custom inbox, extend it and implement the two
 * hooks:
 *
 * <ul>
 *   <li>{@link #newStore()} — an empty inbox and the event store whose keyed append records into
 *       it, as one {@link Inboxed} pair.
 *   <li>{@link #setProcessedAt(Inboxed, IdempotencyKey, Instant)} — moves a recorded row's
 *       processed time, so the retention cases age rows deterministically instead of waiting.
 * </ul>
 *
 * <p>The inbox has no write method of its own: a row is written by {@link EventStore#appendWithKey}
 * in the same atomic step as the command's events. The contract therefore writes through that
 * method and reads back through the inbox. The promises, as the {@link CommandInbox} and {@link
 * EventStore#appendWithKey} javadoc state them:
 *
 * <ol>
 *   <li>{@link CommandInbox#find} of a key never processed is empty.
 *   <li>A processed command is found under its key with the result it recorded: its command type,
 *       its stream (aggregate type included), its final version, the global offsets of its events
 *       in order, and the time it was processed. {@link InboxResult#requireBoundTo} of the row
 *       found accepts its own command type and stream and refuses the same id value under another
 *       aggregate type.
 *   <li>A zero-event command is recorded too, with no offsets and the expected version as its final
 *       version, so a redelivery is not re-run.
 *   <li>A keyed append that fails records nothing: the row and the events are visible together or
 *       not at all.
 *   <li>A replay leaves the recorded row as it was.
 *   <li>{@link CommandInbox#deleteProcessedBefore} deletes exactly the rows processed strictly
 *       before the cutoff, returns how many it deleted, and a deleted key is not found: a command
 *       presenting it again is processed again.
 * </ol>
 */
public abstract class CommandInboxContract {

  /**
   * An inbox and the event store that writes its rows.
   *
   * @param store the event store whose {@link EventStore#appendWithKey} records into {@code inbox}
   * @param inbox the inbox under test
   */
  public record Inboxed(EventStore store, CommandInbox inbox) {
    public Inboxed {
      Objects.requireNonNull(store, "store is required");
      Objects.requireNonNull(inbox, "inbox is required");
    }
  }

  /**
   * A new pair over empty storage: an inbox with no rows and an event store with no events whose
   * {@link EventStore#appendWithKey} records each processed command into that inbox, in the same
   * atomic step as the command's events, as the store's production wiring does. The suite calls it
   * once before each test.
   */
  protected abstract Inboxed newStore();

  /**
   * Sets the processed time of the row recorded under {@code key} in {@code inboxed}'s inbox to
   * {@code processedAt}, leaving the rest of the row as it was; afterwards {@link
   * CommandInbox#find} reports {@code processedAt} and {@link CommandInbox#deleteProcessedBefore}
   * compares it with its cutoff. The suite calls it only for a recorded key, with whole-second
   * instants any timestamp column stores exactly. It is the only way the suite ages a row; a store
   * reaches it through whatever its storage offers (an {@code UPDATE} of the timestamp column, a
   * test-only method of an in-memory inbox), never through the inbox's public API.
   */
  protected abstract void setProcessedAt(Inboxed inboxed, IdempotencyKey key, Instant processedAt);

  /** The event the contract's commands append. */
  public record InboxEvent(String value) implements DomainEvent {
    @JsonCreator
    public InboxEvent(@JsonProperty("value") String value) {
      this.value = value;
    }
  }

  /** The event type name the contract's events are stored under. */
  public static final EventType EVENT_TYPE = new EventType("InboxEvent");

  protected static final AggregateType PRODUCT = AggregateType.of("inbox_product");
  protected static final StreamId STREAM = StreamId.of(PRODUCT, AggregateId.of("p-1"));

  /** The same id value as {@link #STREAM}, under another aggregate type: a different stream. */
  protected static final StreamId STREAM_OTHER_TYPE =
      StreamId.of(AggregateType.of("inbox_inventory"), AggregateId.of("p-1"));

  protected static final String COMMAND_TYPE = "com.example.inbox.CreateProduct";

  /** A fixed processed time the retention cases age rows to and cut off around. */
  protected static final Instant AGED = Instant.parse("2026-01-01T00:00:00Z");

  /** The pair under test; fresh and empty for each test. */
  protected Inboxed inboxed;

  @BeforeEach
  void freshStore() {
    inboxed = newStore();
  }

  /** The inbox under test. */
  protected final CommandInbox inbox() {
    return inboxed.inbox();
  }

  /** The event store that writes the rows of {@link #inbox()}. */
  protected final EventStore eventStore() {
    return inboxed.store();
  }

  protected static List<EventEnvelope> events(StreamId streamId, long fromVersion, int count) {
    return LongStream.range(fromVersion, fromVersion + count)
        .mapToObj(
            v ->
                EventStoreFixture.event(
                    streamId, new Version(v), EVENT_TYPE, new InboxEvent("v" + v)))
        .toList();
  }

  /** A keyed append of {@code count} events at the head of a stream that is {@code head} long. */
  protected IdempotentAppendResult process(
      IdempotencyKey key, StreamId streamId, long head, int count) {
    return eventStore()
        .appendWithKey(
            streamId, events(streamId, head + 1, count), new Version(head), key, COMMAND_TYPE);
  }

  @Test
  public void aKeyNeverProcessedIsNotFound() {
    assertTrue(inbox().find(IdempotencyKey.of("never-processed")).isEmpty());
  }

  @Test
  public void aProcessedCommandIsFoundWithTheResultItRecorded() {
    IdempotencyKey key = IdempotencyKey.of("inbox-recorded");

    var result = process(key, STREAM, 0, 2);

    InboxResult row = inbox().find(key).orElseThrow();
    assertEquals(key, row.key());
    assertEquals(COMMAND_TYPE, row.commandType());
    assertEquals(STREAM, row.streamId());
    assertNotEquals(STREAM_OTHER_TYPE, row.streamId(), "the aggregate type is part of the stream");
    assertEquals(new Version(2), row.finalVersion());
    assertEquals(result.finalVersion(), row.finalVersion());
    assertEquals(result.globalOffsets(), row.globalOffsets());
    assertEquals(2, row.globalOffsets().size());
    assertNotNull(row.processedAt());
  }

  @Test
  public void aFoundRowIsBoundToItsOwnStreamAndNotToTheSameIdUnderAnotherType() {
    IdempotencyKey key = IdempotencyKey.of("inbox-bound");
    process(key, STREAM, 0, 1);

    InboxResult row = inbox().find(key).orElseThrow();

    assertDoesNotThrow(() -> row.requireBoundTo(COMMAND_TYPE, STREAM));
    assertThrows(
        IllegalArgumentException.class, () -> row.requireBoundTo(COMMAND_TYPE, STREAM_OTHER_TYPE));
  }

  @Test
  public void aZeroEventCommandIsRecordedWithNoOffsets() {
    process(IdempotencyKey.of("inbox-before-zero-events"), STREAM, 0, 2);
    IdempotencyKey key = IdempotencyKey.of("inbox-zero-events");

    var result = eventStore().appendWithKey(STREAM, List.of(), new Version(2), key, COMMAND_TYPE);

    InboxResult row = inbox().find(key).orElseThrow();
    assertTrue(row.globalOffsets().isEmpty());
    assertEquals(new Version(2), row.finalVersion(), "the expected version is the final version");
    assertEquals(result.finalVersion(), row.finalVersion());
    assertEquals(STREAM, row.streamId());
  }

  @Test
  public void aKeyedAppendThatFailsRecordsNothing() {
    process(IdempotencyKey.of("inbox-first"), STREAM, 0, 1);
    IdempotencyKey key = IdempotencyKey.of("inbox-conflicting");

    assertThrows(OptimisticLockException.class, () -> process(key, STREAM, 0, 1));

    assertTrue(inbox().find(key).isEmpty(), "the failed command left no row");
  }

  @Test
  public void aReplayLeavesTheRecordedRowAsItWas() {
    IdempotencyKey key = IdempotencyKey.of("inbox-replayed");
    process(key, STREAM, 0, 1);
    InboxResult recorded = inbox().find(key).orElseThrow();

    var replay = process(key, STREAM, 0, 1);

    assertTrue(replay.alreadyApplied());
    assertEquals(recorded, inbox().find(key).orElseThrow());
  }

  @Test
  public void aRowIsFoundWithTheProcessedTimeItWasAgedTo() {
    IdempotencyKey key = IdempotencyKey.of("inbox-aged");
    process(key, STREAM, 0, 1);
    InboxResult recorded = inbox().find(key).orElseThrow();

    setProcessedAt(inboxed, key, AGED);

    InboxResult aged = inbox().find(key).orElseThrow();
    assertEquals(AGED, aged.processedAt());
    assertEquals(
        new InboxResult(
            recorded.key(),
            recorded.commandType(),
            recorded.streamId(),
            recorded.finalVersion(),
            recorded.globalOffsets(),
            AGED),
        aged,
        "only the processed time changed");
  }

  @Test
  public void deleteProcessedBeforeDeletesEveryRowProcessedBeforeTheCutoff() {
    IdempotencyKey first = IdempotencyKey.of("inbox-old-1");
    IdempotencyKey second = IdempotencyKey.of("inbox-old-2");
    process(first, STREAM, 0, 1);
    process(second, STREAM_OTHER_TYPE, 0, 1);
    setProcessedAt(inboxed, first, AGED);
    setProcessedAt(inboxed, second, AGED.plusSeconds(1));

    int deleted = inbox().deleteProcessedBefore(AGED.plusSeconds(2));

    assertEquals(2, deleted);
    assertTrue(inbox().find(IdempotencyKey.of("inbox-old-1")).isEmpty());
    assertTrue(inbox().find(IdempotencyKey.of("inbox-old-2")).isEmpty());
  }

  @Test
  public void aKeyDeletedByRetentionIsProcessedAgain() {
    IdempotencyKey key = IdempotencyKey.of("inbox-expired");
    var first = process(key, STREAM, 0, 1);
    setProcessedAt(inboxed, key, AGED);
    assertEquals(1, inbox().deleteProcessedBefore(AGED.plusSeconds(1)));

    var again = process(key, STREAM, 1, 1);

    assertFalse(again.alreadyApplied(), "a deleted key does not answer a redelivery");
    assertEquals(new Version(2), again.finalVersion());
    assertEquals(1, again.globalOffsets().size());
    assertNotEquals(first.globalOffsets(), again.globalOffsets());
    assertEquals(
        2, eventStore().readStream(STREAM, Version.initial(), 10).size(), "its events are stored");
    InboxResult row = inbox().find(key).orElseThrow();
    assertEquals(again.globalOffsets(), row.globalOffsets());
    assertEquals(new Version(2), row.finalVersion());
  }

  @Test
  public void deleteProcessedBeforeKeepsARowProcessedAtTheCutoff() {
    IdempotencyKey key = IdempotencyKey.of("inbox-at-cutoff");
    process(key, STREAM, 0, 1);
    Instant processedAt = inbox().find(key).orElseThrow().processedAt();

    int deleted = inbox().deleteProcessedBefore(processedAt);

    assertEquals(0, deleted, "strictly before the cutoff: a row processed at it stays");
    assertTrue(inbox().find(key).isPresent());
  }

  @Test
  public void deleteProcessedBeforeKeepsRowsProcessedAfterTheCutoff() {
    IdempotencyKey older = IdempotencyKey.of("inbox-older");
    IdempotencyKey atCutoff = IdempotencyKey.of("inbox-at-aged-cutoff");
    IdempotencyKey newer = IdempotencyKey.of("inbox-newer");
    process(older, STREAM, 0, 1);
    process(atCutoff, STREAM, 1, 1);
    process(newer, STREAM, 2, 1);
    setProcessedAt(inboxed, older, AGED.minusSeconds(1));
    setProcessedAt(inboxed, atCutoff, AGED);
    setProcessedAt(inboxed, newer, AGED.plusSeconds(1));

    int deleted = inbox().deleteProcessedBefore(AGED);

    assertEquals(1, deleted);
    assertTrue(inbox().find(older).isEmpty());
    assertTrue(inbox().find(atCutoff).isPresent(), "a row processed at the cutoff stays");
    assertTrue(inbox().find(newer).isPresent());
  }

  @Test
  public void deleteProcessedBeforeOfAnEmptyInboxDeletesNothing() {
    assertEquals(0, inbox().deleteProcessedBefore(Instant.now().plusSeconds(86_400)));
  }
}
