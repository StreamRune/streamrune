package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The contract of an {@link EventStore} that fences a saga's forward command ({@link
 * EventStore#appendWithKey(StreamId, List, Version, IdempotencyKey, String, SagaOrigin)}) against
 * the rows of a {@link SagaStore}. Run by {@code InMemorySagaForwardFenceTest} and {@code
 * PostgresSagaForwardFenceTest}; extend it to prove a custom event store.
 *
 * <p>The rows are written through the {@link SagaStore} API only, so the contract states what a
 * fence must answer for each row a conforming saga store can hold: a row in its forward phase
 * admits the command; a row carrying a compensation episode stamp, a terminal row, a row of another
 * saga type and a missing row refuse it, and a refused command stores nothing and leaves its key
 * unclaimed.
 */
public abstract class SagaForwardFenceContract {

  /**
   * The event store under test, with a command inbox, fencing against {@link #sagaStore()}'s rows.
   * The same instance throughout one test, a fresh one (with empty storage) for each test.
   */
  protected abstract EventStore eventStore();

  /** The saga store whose rows {@link #eventStore()} fences against; fresh for each test. */
  protected abstract SagaStore sagaStore();

  /** The state the contract's saga rows carry. */
  public record FenceState(SagaStatus status) implements SagaState {
    @JsonCreator
    public FenceState(@JsonProperty("status") SagaStatus status) {
      this.status = status;
    }
  }

  /** The event the contract's commands append. */
  public record FenceEvent(String value) implements DomainEvent {
    @JsonCreator
    public FenceEvent(@JsonProperty("value") String value) {
      this.value = value;
    }
  }

  /** The event type name the contract's events are stored under. */
  public static final EventType EVENT_TYPE = new EventType("FenceEvent");

  protected static final SagaType TYPE = SagaType.fromClass(FenceState.class);
  protected static final SagaId ID = SagaId.of("fence-saga-1");
  protected static final SagaOrigin FORWARD_OF = forwardOf(ID);
  protected static final StreamId TARGET =
      StreamId.of(AggregateType.of("fence_target"), AggregateId.of("target-1"));
  protected static final String COMMAND_TYPE = "FenceCommand";

  /** The origin of a forward command of the contract's saga {@code sagaId}. */
  protected static SagaOrigin forwardOf(SagaId sagaId) {
    return SagaEnvelopes.metadataCausedBy(FenceState.class, sagaId).sagaOrigin();
  }

  protected static EventEnvelope event(StreamId streamId, long version) {
    return EventStoreFixture.event(
        streamId, new Version(version), EVENT_TYPE, new FenceEvent("v" + version));
  }

  /**
   * A one-event keyed append of the next event of {@code streamId}, fenced by {@code forwardOf}.
   */
  protected IdempotentAppendResult append(
      StreamId streamId, IdempotencyKey key, SagaOrigin forwardOf) {
    long head = stored(streamId).size();
    return eventStore()
        .appendWithKey(
            streamId,
            List.of(event(streamId, head + 1)),
            new Version(head),
            key,
            COMMAND_TYPE,
            forwardOf);
  }

  protected List<EventEnvelope> stored(StreamId streamId) {
    return eventStore().readStream(streamId, Version.initial(), 100);
  }

  protected void forwardRow(SagaId sagaId) {
    sagaStore().create(sagaId, TYPE, new FenceState(SagaStatus.RUNNING), SagaStatus.RUNNING);
  }

  /** A row in its compensation episode: created RUNNING at version 1, then claimed. */
  protected void stampedRow(SagaId sagaId) {
    forwardRow(sagaId);
    sagaStore().claimCompensating(sagaId, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);
  }

  private void assertRefused(SagaOrigin forwardOf, Executable append) {
    var refusal = assertThrows(SagaForwardFencedException.class, append);
    assertEquals(forwardOf.sagaId(), refusal.sagaId());
    assertEquals(forwardOf.sagaType(), refusal.sagaType());
  }

  @Test
  public void aForwardRowAcceptsTheCommand() {
    forwardRow(ID);

    var result = append(TARGET, IdempotencyKey.of("fwd-1"), FORWARD_OF);

    assertFalse(result.alreadyApplied());
    assertEquals(1, result.globalOffsets().size());
    assertEquals(1, stored(TARGET).size());
  }

  @Test
  public void aRowInItsEpisodeRefusesTheCommandStoresNothingAndLeavesTheKeyUnclaimed() {
    stampedRow(ID);
    IdempotencyKey key = IdempotencyKey.of("fwd-after-stamp");

    assertRefused(FORWARD_OF, () -> append(TARGET, key, FORWARD_OF));

    assertTrue(stored(TARGET).isEmpty(), "a refused command stores no event");
    var later = append(TARGET, key, null);
    assertFalse(later.alreadyApplied(), "the refused command left its key unclaimed");
    assertEquals(1, stored(TARGET).size());
  }

  @Test
  public void aTerminalRowRefusesTheCommand() {
    for (SagaStatus terminal :
        List.of(SagaStatus.COMPLETED, SagaStatus.COMPENSATED, SagaStatus.FAILED)) {
      SagaId sagaId = SagaId.of("fence-terminal-" + terminal.name().toLowerCase());
      sagaStore().create(sagaId, TYPE, new FenceState(terminal), terminal, false);
      StreamId target =
          StreamId.of(AggregateType.of("fence_target"), AggregateId.of(sagaId.value()));
      SagaOrigin origin = forwardOf(sagaId);

      assertRefused(
          origin, () -> append(target, IdempotencyKey.of("fwd-" + sagaId.value()), origin));

      assertTrue(stored(target).isEmpty(), terminal + ": a refused command stores no event");
    }
  }

  @Test
  public void aFaultedRowWithoutAStampAcceptsTheCommand() {
    forwardRow(ID);
    sagaStore().markFaulted(ID, TYPE, 1L);

    var result = append(TARGET, IdempotencyKey.of("fwd-faulted"), FORWARD_OF);

    assertFalse(result.alreadyApplied());
    assertEquals(1, stored(TARGET).size());
  }

  @Test
  public void noRowRefusesTheCommand() {
    assertRefused(FORWARD_OF, () -> append(TARGET, IdempotencyKey.of("fwd-no-row"), FORWARD_OF));

    assertTrue(stored(TARGET).isEmpty());
  }

  @Test
  public void aRowOfAnotherSagaTypeRefusesTheCommand() {
    SagaType other = SagaType.of("fence.AnotherSaga");
    sagaStore().create(ID, other, new FenceState(SagaStatus.RUNNING), SagaStatus.RUNNING);

    assertRefused(
        FORWARD_OF, () -> append(TARGET, IdempotencyKey.of("fwd-other-type"), FORWARD_OF));

    assertTrue(stored(TARGET).isEmpty());
  }

  @Test
  public void aCommandWithoutAForwardOriginIsNotFenced() {
    stampedRow(ID);

    var result = append(TARGET, IdempotencyKey.of("unfenced"), null);

    assertFalse(result.alreadyApplied());
    assertEquals(1, stored(TARGET).size());
  }

  @Test
  public void aKeyClaimedBeforeTheStampIsAnsweredFromTheInbox() {
    forwardRow(ID);
    IdempotencyKey key = IdempotencyKey.of("fwd-before-stamp");
    var first = append(TARGET, key, FORWARD_OF);
    sagaStore().claimCompensating(ID, TYPE, new FenceState(SagaStatus.COMPENSATING), 1L);

    var replay =
        eventStore()
            .appendWithKey(
                TARGET,
                List.of(event(TARGET, 1)),
                Version.initial(),
                key,
                COMMAND_TYPE,
                FORWARD_OF);

    assertTrue(
        replay.alreadyApplied(), "the command committed before the stamp; its result stands");
    assertEquals(first.globalOffsets(), replay.globalOffsets());
    assertEquals(1, stored(TARGET).size());
  }

  @Test
  public void aZeroEventForwardCommandIsFencedToo() {
    stampedRow(ID);
    IdempotencyKey key = IdempotencyKey.of("fwd-zero-events");

    assertRefused(
        FORWARD_OF,
        () ->
            eventStore()
                .appendWithKey(
                    TARGET, List.of(), Version.initial(), key, COMMAND_TYPE, FORWARD_OF));

    var later =
        eventStore().appendWithKey(TARGET, List.of(), Version.initial(), key, COMMAND_TYPE, null);
    assertFalse(later.alreadyApplied(), "the refused zero-event command left its key unclaimed");
  }

  @Test
  public void theStoreFencesAgainstTheSagaStoresRows() {
    Object identity = eventStore().sagaFenceStorageIdentity();

    assertNotNull(identity, "a fencing store reports the rows it reads");
    assertEquals(sagaStore().storageIdentity(), identity);
  }
}
