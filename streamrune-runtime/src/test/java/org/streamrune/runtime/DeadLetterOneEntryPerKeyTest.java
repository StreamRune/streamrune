package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.CommandDeadLetteredException.PendingOutcome;
import org.streamrune.test.FaultSchedule;
import org.streamrune.test.ForwardingDeadLetterQueue;
import org.streamrune.test.ForwardingEventStore;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.MutableClock;

/**
 * A keyed command that fails again under a key the dead letter queue already holds an entry of the
 * same command for stores no second entry: a client that re-sends under its key during an outage —
 * as a dead-lettered keyed command's caller is told it may — would otherwise add an entry per
 * re-send, inflating {@code streamrune.dlq.pending} and the replay work for one intended change.
 * The caller is answered with the entry that is stored, and with that entry's outcome: queued for
 * replay while the retry schedule still replays it, awaiting an operator once it has exhausted its
 * attempts or is older than the command-inbox window.
 *
 * <p>The check is a read before the existing insert, so it adds no crash point: a crash before the
 * read or between it and the insert leaves the state a failed send leaves without the check.
 */
class DeadLetterOneEntryPerKeyTest {

  private static final AggregateType COUNTER = AggregateType.of("counter");

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final InMemoryEventStore events = new InMemoryEventStore().withCommandInbox(inbox);
  private final FaultSchedule outage =
      FaultSchedule.none()
          .throwing(() -> new EventStoreException("connection reset by peer"))
          .failAlways("appendWithKey")
          .failAlways("append");
  private final ForwardingEventStore store = new ForwardingEventStore(events, outage);
  private final InMemoryDeadLetterQueue deadLetters = new InMemoryDeadLetterQueue();

  @Test
  void aReSendUnderTheSameKeyThatFailsAgain_storesNoSecondEntry_andIsAnsweredWithTheFirst() {
    var bus = bus(deadLetters).build();
    var key = IdempotencyKey.of("client-operation-1");

    CommandDeadLetteredException first = deadLettered(() -> bus.execute(new Adjust("c-1", 5), key));
    CommandDeadLetteredException second =
        deadLettered(() -> bus.execute(new Adjust("c-1", 5), key));

    assertThat(deadLetters.all()).singleElement();
    CommandId entry = deadLetters.all().getFirst().commandId();
    assertThat(first.commandId()).isEqualTo(entry);
    assertThat(second.commandId()).as("the entry that is stored").isEqualTo(entry);
    assertThat(second.pendingOutcome()).isEqualTo(PendingOutcome.QUEUED_FOR_REPLAY);
    assertThat(second.idempotencyKey()).isEqualTo(key);
    assertThat(second.getMessage()).contains(entry.value(), "same key");
  }

  @Test
  void twoKeysStoreTwoEntries() {
    var bus = bus(deadLetters).build();

    deadLettered(() -> bus.execute(new Adjust("c-2", 5), IdempotencyKey.of("op-a")));
    deadLettered(() -> bus.execute(new Adjust("c-2", 5), IdempotencyKey.of("op-b")));

    assertThat(deadLetters.all())
        .extracting(DeadLetterQueue.DeadLetterEntry::idempotencyKey)
        .containsExactlyInAnyOrder(IdempotencyKey.of("op-a"), IdempotencyKey.of("op-b"));
  }

  @Test
  void anUnkeyedCommandStoresAnEntryForEveryFailure() {
    var bus = bus(deadLetters).build();

    var first = deadLettered(() -> bus.execute(new Adjust("c-3", 5)));
    var second = deadLettered(() -> bus.execute(new Adjust("c-3", 5)));

    assertThat(deadLetters.all()).hasSize(2);
    assertThat(first.commandId()).isNotEqualTo(second.commandId());
    assertThat(List.of(first.pendingOutcome(), second.pendingOutcome()))
        .containsOnly(PendingOutcome.AWAITING_OPERATOR);
  }

  /**
   * An entry the retry schedule replays no more: its replays exhausted their attempts, and the
   * policy keeps such an entry for an operator. A re-send that fails again is answered as awaiting
   * an operator, naming that entry, and stores nothing: the operator decides about the one entry.
   */
  @Test
  void aReSendAgainstAnExhaustedEntry_isAnsweredAsAwaitingAnOperator() {
    var policy =
        new DeadLetterRetryPolicy(
            2,
            Duration.ofSeconds(30),
            2.0,
            false,
            DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY);
    var bus = bus(deadLetters).deadLetterRetryPolicy(policy).build();
    var key = IdempotencyKey.of("op-exhausted");
    var first = deadLettered(() -> bus.execute(new Adjust("c-4", 5), key));
    deadLetters.updateAttempts(first.commandId(), 1, Instant.now());

    var stillReplayed = deadLettered(() -> bus.execute(new Adjust("c-4", 5), key));

    assertThat(stillReplayed.pendingOutcome())
        .as("one attempt of two: still replayed on the schedule")
        .isEqualTo(PendingOutcome.QUEUED_FOR_REPLAY);

    deadLetters.updateAttempts(first.commandId(), 2, Instant.now());
    var exhausted = deadLettered(() -> bus.execute(new Adjust("c-4", 5), key));

    assertThat(exhausted.pendingOutcome()).isEqualTo(PendingOutcome.AWAITING_OPERATOR);
    assertThat(exhausted.commandId()).isEqualTo(first.commandId());
    assertThat(exhausted.idempotencyKey()).isEqualTo(key);
    assertThat(exhausted.getMessage()).contains("operator").doesNotContain("same key");
    assertThat(deadLetters.all()).singleElement();
  }

  /**
   * An entry whose command was first received longer ago than the command-inbox window: the retry
   * runner refuses to replay it on its schedule (the inbox may have pruned the row that
   * deduplicates the replay), so a re-send that fails again is answered as awaiting an operator.
   */
  @Test
  void aReSendAgainstAnEntryOlderThanTheInboxWindow_isAnsweredAsAwaitingAnOperator() {
    MutableClock clock = MutableClock.startingAt(Instant.now());
    var bus = bus(deadLetters).inboxRetentionMaxAge(Duration.ofDays(7)).clock(clock).build();
    var key = IdempotencyKey.of("op-stale");
    var first = deadLettered(() -> bus.execute(new Adjust("c-5", 5), key));

    clock.advance(Duration.ofDays(6));
    var withinTheWindow = deadLettered(() -> bus.execute(new Adjust("c-5", 5), key));
    clock.advance(Duration.ofDays(2));
    var pastTheWindow = deadLettered(() -> bus.execute(new Adjust("c-5", 5), key));

    assertThat(withinTheWindow.pendingOutcome()).isEqualTo(PendingOutcome.QUEUED_FOR_REPLAY);
    assertThat(pastTheWindow.pendingOutcome()).isEqualTo(PendingOutcome.AWAITING_OPERATOR);
    assertThat(pastTheWindow.commandId()).isEqualTo(first.commandId());
    assertThat(deadLetters.all()).singleElement();
  }

  /** A window of zero or less is an inbox that is never pruned: no entry is ever too old. */
  @Test
  void aZeroInboxWindowLeavesEveryEntryReplayedOnTheSchedule() {
    MutableClock clock = MutableClock.startingAt(Instant.now());
    var bus = bus(deadLetters).inboxRetentionMaxAge(Duration.ZERO).clock(clock).build();
    var key = IdempotencyKey.of("op-unpruned");
    deadLettered(() -> bus.execute(new Adjust("c-6", 5), key));

    clock.advance(Duration.ofDays(365));
    var later = deadLettered(() -> bus.execute(new Adjust("c-6", 5), key));

    assertThat(later.pendingOutcome()).isEqualTo(PendingOutcome.QUEUED_FOR_REPLAY);
  }

  /**
   * The inbox answers a key with the command that recorded it only, so a different command under
   * the same key — another command type, or the same type on another stream — is another operation:
   * it stores its own entry.
   */
  @Test
  void anotherCommandUnderTheSameKey_storesItsOwnEntry() {
    var bus = bus(deadLetters).build();
    var key = IdempotencyKey.of("op-shared");

    var adjust = deadLettered(() -> bus.execute(new Adjust("c-7", 5), key));
    var otherStream = deadLettered(() -> bus.execute(new Adjust("c-8", 5), key));
    var otherType = deadLettered(() -> bus.execute(new Reset("c-7"), key));
    var again = deadLettered(() -> bus.execute(new Adjust("c-8", 5), key));

    assertThat(deadLetters.all()).hasSize(3);
    assertThat(List.of(adjust.commandId(), otherStream.commandId(), otherType.commandId()))
        .doesNotHaveDuplicates();
    assertThat(again.commandId()).isEqualTo(otherStream.commandId());
  }

  /**
   * Of several entries under the key, the one the schedule still replays answers the re-send: two
   * sends that failed at the same instant can each have stored one.
   */
  @Test
  void ofSeveralEntriesUnderTheKey_theOneStillReplayedAnswers() {
    var policy =
        new DeadLetterRetryPolicy(
            1,
            Duration.ofSeconds(30),
            2.0,
            false,
            DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY);
    var bus = bus(deadLetters).deadLetterRetryPolicy(policy).build();
    var key = IdempotencyKey.of("op-raced");
    var exhausted = CommandId.of("raced-exhausted");
    var pending = CommandId.of("raced-pending");
    publishEntry(exhausted, new Adjust("c-9", 5), key);
    publishEntry(pending, new Adjust("c-9", 5), key);
    deadLetters.updateAttempts(exhausted, 1, Instant.now());

    var answered = deadLettered(() -> bus.execute(new Adjust("c-9", 5), key));

    assertThat(answered.commandId()).isEqualTo(pending);
    assertThat(answered.pendingOutcome()).isEqualTo(PendingOutcome.QUEUED_FOR_REPLAY);
    assertThat(deadLetters.all()).hasSize(2);
  }

  /**
   * The lookup failing — the queue's store is unreachable for the read — never costs the command
   * its entry: the bus stores one as it does without the lookup.
   */
  @Test
  void aFailedLookup_storesTheEntry() {
    var faults = FaultSchedule.none();
    var queue = new ForwardingDeadLetterQueue(deadLetters, faults);
    var bus = bus(queue).build();
    var key = IdempotencyKey.of("op-lookup-down");
    deadLettered(() -> bus.execute(new Adjust("c-10", 5), key));
    faults.failOnce("findByIdempotencyKey");

    var second = deadLettered(() -> bus.execute(new Adjust("c-10", 5), key));

    assertThat(deadLetters.all()).hasSize(2);
    assertThat(second.commandId()).isNotEqualTo(deadLetters.all().getFirst().commandId());
    assertThat(second.pendingOutcome()).isEqualTo(PendingOutcome.QUEUED_FOR_REPLAY);
    assertThat(second.getSuppressed()).isEmpty();
  }

  /** A queue that does not look entries up by key stores an entry for every failed send. */
  @Test
  void aQueueWithoutTheLookup_storesAnEntryForEveryFailure() {
    var queue =
        new ForwardingDeadLetterQueue(deadLetters) {
          @Override
          public List<DeadLetterEntry> findByIdempotencyKey(IdempotencyKey key) {
            throw new UnsupportedOperationException("no lookup by key");
          }
        };
    var bus = bus(queue).build();
    var key = IdempotencyKey.of("op-no-lookup");

    deadLettered(() -> bus.execute(new Adjust("c-11", 5), key));
    deadLettered(() -> bus.execute(new Adjust("c-11", 5), key));
    deadLettered(() -> bus.execute(new Adjust("c-11", 5), key));

    assertThat(deadLetters.all()).hasSize(3);
  }

  /**
   * The whole outage: a client re-sends under its key while the store is down and stores one entry;
   * once the store is back the runner's replay applies the command once, and the next re-send is
   * answered from the command inbox.
   */
  @Test
  void endToEnd_reSendsDuringAnOutageStoreOneEntry_whichIsReplayedOnce() {
    var bus = bus(deadLetters).build();
    var key = IdempotencyKey.of("op-outage");
    for (int resend = 0; resend < 5; resend++) {
      deadLettered(() -> bus.execute(new Adjust("c-12", 5), key));
    }
    assertThat(deadLetters.all()).singleElement();

    outage.reset();
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(deadLetters)
            .commandBus(bus)
            .objectMapper(new ObjectMapper())
            .policy(
                new DeadLetterRetryPolicy(
                    5,
                    Duration.ZERO,
                    2.0,
                    false,
                    DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY))
            .registerCommand(CounterCommand.class)
            .build();
    runner.processBatch();

    assertThat(deadLetters.all()).isEmpty();
    var result = bus.execute(new Adjust("c-12", 5), key);
    assertThat(result.idempotentReplay()).isTrue();
    assertThat(
            events.readStream(StreamId.of(COUNTER, AggregateId.of("c-12")), Version.initial(), 10))
        .singleElement();
  }

  // ---- fixtures ----------------------------------------------------------------------------

  private VirtualThreadCommandBus.Builder bus(DeadLetterQueue deadLetterQueue) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .commandInbox(inbox)
        .deadLetterQueue(deadLetterQueue)
        .objectMapper(new ObjectMapper())
        .register(COUNTER, CounterCommand.class, c -> AggregateId.of(c.id()), new CounterDecider());
  }

  private static CommandDeadLetteredException deadLettered(Runnable send) {
    Throwable thrown = catchThrowable(send::run);
    assertThat(thrown).isInstanceOf(CommandDeadLetteredException.class);
    return (CommandDeadLetteredException) thrown;
  }

  private void publishEntry(CommandId id, Adjust command, IdempotencyKey key) {
    deadLetters.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{\"id\":\"" + command.id() + "\",\"delta\":" + command.delta() + "}",
            Adjust.class.getName(),
            id,
            StreamId.of(COUNTER, AggregateId.of(command.id())),
            EventStoreException.class.getName(),
            "connection reset by peer",
            1,
            Instant.now(),
            null,
            null,
            null,
            key));
  }

  /** The commands of one counter aggregate. */
  sealed interface CounterCommand extends Command permits Adjust, Reset {
    String id();
  }

  record Adjust(String id, int delta) implements CounterCommand {}

  record Reset(String id) implements CounterCommand {}

  record Adjusted(String id, int delta) implements DomainEvent {}

  record CounterState(int value) implements AggregateState {}

  static final class CounterDecider implements Decider<CounterCommand, CounterState, Adjusted> {
    @Override
    public CounterState initialState() {
      return new CounterState(0);
    }

    @Override
    public List<Adjusted> decide(CounterCommand command, CounterState state) {
      return switch (command) {
        case Adjust adjust -> List.of(new Adjusted(adjust.id(), adjust.delta()));
        case Reset reset -> List.of(new Adjusted(reset.id(), -state.value()));
      };
    }

    @Override
    public CounterState evolve(CounterState state, Adjusted event) {
      return new CounterState(state.value() + event.delta());
    }
  }
}
