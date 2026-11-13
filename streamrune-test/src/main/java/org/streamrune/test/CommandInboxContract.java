package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;
import java.util.stream.LongStream;
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
 * PostgresCommandInboxContractTest}; extend it and implement {@link #inbox()} and {@link
 * #eventStore()} to prove a custom inbox.
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
 *       in order, and the time it was processed.
 *   <li>A zero-event command is recorded too, with no offsets and the expected version as its final
 *       version, so a redelivery is not re-run.
 *   <li>A keyed append that fails records nothing: the row and the events are visible together or
 *       not at all.
 *   <li>A replay leaves the recorded row as it was.
 *   <li>{@link CommandInbox#deleteProcessedBefore} deletes exactly the rows processed strictly
 *       before the cutoff, returns how many it deleted, and a deleted key is not found.
 * </ol>
 */
public abstract class CommandInboxContract {

  /** The inbox under test; the same instance throughout one test, fresh and empty for each test. */
  protected abstract CommandInbox inbox();

  /**
   * An event store whose {@link EventStore#appendWithKey} records into {@link #inbox()}'s rows; the
   * same instance throughout one test, with empty storage for each test.
   */
  protected abstract EventStore eventStore();

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
  public void deleteProcessedBeforeDeletesEveryRowProcessedBeforeTheCutoff() {
    process(IdempotencyKey.of("inbox-old-1"), STREAM, 0, 1);
    process(IdempotencyKey.of("inbox-old-2"), STREAM_OTHER_TYPE, 0, 1);

    int deleted = inbox().deleteProcessedBefore(Instant.now().plusSeconds(86_400));

    assertEquals(2, deleted);
    assertTrue(inbox().find(IdempotencyKey.of("inbox-old-1")).isEmpty());
    assertTrue(inbox().find(IdempotencyKey.of("inbox-old-2")).isEmpty());
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
  public void deleteProcessedBeforeKeepsRowsProcessedAfterTheCutoff() throws InterruptedException {
    IdempotencyKey older = IdempotencyKey.of("inbox-older");
    IdempotencyKey newer = IdempotencyKey.of("inbox-newer");
    process(older, STREAM, 0, 1);
    Thread.sleep(20);
    process(newer, STREAM, 1, 1);
    Instant newerAt = inbox().find(newer).orElseThrow().processedAt();
    assertTrue(
        inbox().find(older).orElseThrow().processedAt().isBefore(newerAt),
        "the second command was processed later");

    int deleted = inbox().deleteProcessedBefore(newerAt);

    assertEquals(1, deleted);
    assertTrue(inbox().find(older).isEmpty());
    assertTrue(inbox().find(newer).isPresent());
  }

  @Test
  public void deleteProcessedBeforeOfAnEmptyInboxDeletesNothing() {
    assertEquals(0, inbox().deleteProcessedBefore(Instant.now().plusSeconds(86_400)));
  }
}
