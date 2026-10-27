package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandFailureClassification;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventSchemaAheadException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.runtime.DeadLetterRetryRunner.RetryOutcome;
import org.streamrune.test.FaultSchedule;
import org.streamrune.test.ForwardingDeadLetterQueue;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InjectedFaultException;
import org.streamrune.test.MutableClock;
import org.streamrune.test.RecordingStreamRuneMetrics;
import org.streamrune.test.ScriptedSubscriptionLeadership;

/**
 * A dead-letter replay on a replica that is behind the stored schema: the replayed command's
 * aggregate holds an event a newer release stored at a schema version this replica's upcaster chain
 * does not reach, so the replay fails with {@link EventSchemaAheadException}.
 *
 * <p>During a rolling deployment of an event-schema change the retry runner's lease can sit on a
 * replica of the previous release for the whole window, and every replay it makes fails that way.
 * The failure says nothing about the command, which succeeds on an upgraded replica, so it counts
 * no attempt ({@link RetryOutcome#READER_BEHIND}): the entry's attempt count stays, and only its
 * last attempt time becomes the replay's. The default policy's five attempts would otherwise turn
 * an entry dead-lettered during a rollout into one only an operator replays after about fifteen
 * minutes, and {@code discardAfterMaxRetries} would drop the command. The new attempt time moves
 * the entry behind the entries queued after it, so a run of such entries at the head of the
 * least-recently-active order delays those entries by one poll per page it fills, not until the
 * rollout ends.
 *
 * <p><b>Crash points.</b> The time is recorded by one single-row write ({@code updateAttempts} with
 * the entry's own attempt count), the last step of such a replay. A crash before it leaves the
 * entry as it was, at the head of the order, and the next poll replays it again; a crash after it
 * leaves the entry behind the others, replayed once its backoff has passed. Both converge: the
 * entry stays until a replica that reads the stored version replays it, with no attempt counted
 * ({@link #aFailedAttemptTimeWriteLeavesTheEntryAsItWas_andTheNextPollReplaysItAgain}, {@link
 * #aCrashAfterTheAttemptTimeWriteLeavesTheEntryBehind_andItIsReplayedAfterItsBackoff}).
 */
class DeadLetterRetryReaderBehindTest {

  private static final AggregateType STOCK = AggregateType.of("stock");

  private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

  private final InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();

  @Test
  void aReplayBehindTheStoredSchemaCountsNoAttempt_acrossEveryPollOfTheWindow() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    var bus = new BehindTheStoredSchemaBus(Integer.MAX_VALUE);
    CommandId id = publishKeyedEntry(queue, "cmd-behind-1", new ReserveStock("S", "order-1"));
    var runner = runner(bus, policy(5, false), queue, clock, 10);

    for (int poll = 1; poll <= 6; poll++) {
      clock.advance(Duration.ofMinutes(1));
      runner.processBatch();
      DeadLetterQueue.DeadLetterEntry entry = queue.find(id).orElseThrow();
      assertThat(entry.dlqAttempts()).as("attempts after poll %d", poll).isZero();
      assertThat(entry.lastAttemptAt())
          .as("the replay's time is recorded, not an attempt")
          .isEqualTo(clock.instant());
    }

    assertThat(bus.replays()).as("the schedule replays the entry on every poll").isEqualTo(6);
    assertThat(metrics.count(MetricNames.DLQ_EXHAUSTED)).isZero();
  }

  @Test
  void underDiscardAfterMaxRetries_aReplayBehindTheStoredSchemaNeverDiscardsTheCommand() {
    var bus = new BehindTheStoredSchemaBus(Integer.MAX_VALUE);
    CommandId id = publishKeyedEntry("cmd-behind-2");
    var runner = runner(bus, policy(1, true));

    for (int poll = 1; poll <= 5; poll++) {
      runner.processBatch();
    }

    assertThat(deadLetterQueue.find(id))
        .as("one counted attempt would discard the command under this policy")
        .hasValueSatisfying(entry -> assertThat(entry.dlqAttempts()).isZero());
    assertThat(metrics.count(MetricNames.DLQ_EXHAUSTED)).isZero();
  }

  @Test
  void theEntryIsReplayedOnceTheReplicaReadsTheStoredSchema() {
    var bus = new BehindTheStoredSchemaBus(5);
    CommandId id = publishKeyedEntry("cmd-behind-3");
    var runner = runner(bus, policy(5, true));

    for (int poll = 1; poll <= 5; poll++) {
      runner.processBatch();
      assertThat(deadLetterQueue.find(id)).as("the entry after poll %d", poll).isPresent();
    }

    runner.processBatch();

    assertThat(deadLetterQueue.find(id)).as("replayed and discarded").isEmpty();
    assertThat(bus.replays()).isEqualTo(6);
    assertThat(bus.executed()).isEqualTo(1);
    assertThat(metrics.count(MetricNames.DLQ_EXHAUSTED)).isZero();
  }

  @Test
  void anOperatorRetryBehindTheStoredSchema_isReaderBehind_andCountsNoAttempt() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    var bus = new BehindTheStoredSchemaBus(1);
    CommandId id = publishKeyedEntry(queue, "cmd-behind-4", new ReserveStock("S", "order-1"));
    var runner = runner(bus, policy(5, false), queue, clock, 10);
    clock.advance(Duration.ofSeconds(5));

    assertThat(runner.retry(id)).isEqualTo(RetryOutcome.READER_BEHIND);
    DeadLetterQueue.DeadLetterEntry entry = queue.find(id).orElseThrow();
    assertThat(entry.dlqAttempts()).isZero();
    assertThat(entry.lastAttemptAt()).isEqualTo(clock.instant());

    assertThat(runner.retry(id)).isEqualTo(RetryOutcome.REPLAYED);
    assertThat(queue.find(id)).isEmpty();
  }

  /**
   * A replay that answers {@code READER_BEHIND} takes no slot of the poll's batch: counted against
   * the batch, a batch's worth of such entries at the head of the page would hold back every entry
   * behind them in the same page.
   */
  @Test
  void entriesBehindTheStoredSchemaTakeNoSlotOfThePollsBatch() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    List<CommandId> behind = new java.util.ArrayList<>();
    for (int i = 0; i < 10; i++) {
      behind.add(publishKeyedEntry(queue, "cmd-behind-head-" + i, new ReserveStock("S", "o-" + i)));
      clock.advance(Duration.ofSeconds(1));
    }
    CommandId fresh = publishKeyedEntry(queue, "cmd-fresh", new ReserveStock("T", "order-2"));
    clock.advance(Duration.ofSeconds(1));
    var bus = new BehindForOneAggregateBus("S");

    runner(bus, policy(5, false), queue, clock, 10).processBatch();

    assertThat(queue.find(fresh))
        .as("the entry behind ten replays behind the stored schema is replayed in the same poll")
        .isEmpty();
    assertThat(bus.executed()).isEqualTo(1);
    assertThat(bus.refused()).as("every entry behind the stored schema was tried").isEqualTo(10);
    assertThat(behind)
        .allSatisfy(
            id -> {
              DeadLetterQueue.DeadLetterEntry entry = queue.find(id).orElseThrow();
              assertThat(entry.dlqAttempts()).isZero();
              assertThat(entry.lastAttemptAt()).isEqualTo(clock.instant());
            });
  }

  /**
   * More entries behind the stored schema than a page holds: the first poll's page is all of them,
   * so it reaches no entry queued after them. Each one's replay records its time, which moves it
   * behind those entries and makes it due again only after its backoff, so the next poll reaches
   * the healthy entry and replays it — while none of the entries behind the stored schema has an
   * attempt counted.
   */
  @Test
  void aRunOfEntriesBehindTheStoredSchemaYieldsTheHead_soTheEntryBehindAFullPageIsReplayedNext() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    List<CommandId> behind = new java.util.ArrayList<>();
    for (int i = 0; i < 21; i++) {
      behind.add(publishKeyedEntry(queue, "cmd-run-" + i, new ReserveStock("S", "o-" + i)));
      clock.advance(Duration.ofSeconds(1));
    }
    CommandId healthy = publishKeyedEntry(queue, "cmd-healthy", new ReserveStock("T", "order-2"));
    var bus = new BehindForOneAggregateBus("S");
    // batchSize 2: the poll reads a page of twenty. The first replay of an entry is due 15 to 45 s
    // (30 s, with jitter) after its publish or its last replay.
    var runner = runner(bus, backoffPolicy(), queue, clock, 2);

    clock.advance(Duration.ofMinutes(1));
    runner.processBatch();

    assertThat(queue.find(healthy)).as("the first page is full of entries behind").isPresent();
    assertThat(bus.refused()).isEqualTo(20);

    clock.advance(Duration.ofSeconds(1));
    runner.processBatch();

    assertThat(queue.find(healthy)).as("replayed on the next poll").isEmpty();
    assertThat(bus.executed()).isEqualTo(1);
    assertThat(bus.refused())
        .as("the next poll replays only the entry the first page did not hold")
        .isEqualTo(21);
    assertThat(behind)
        .allSatisfy(id -> assertThat(queue.find(id).orElseThrow().dlqAttempts()).isZero());
  }

  /**
   * A poll that reads a full page, replays entries behind the stored schema and reaches the end of
   * the page with its batch not full has not reached the entries queued after the page: its summary
   * line says so at WARN. A poll whose page is not full states the same replays at INFO.
   */
  @Test
  void aPollThatReachesTheEndOfAFullPageOfEntriesBehindStatesItAtWarn() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    for (int i = 0; i < 21; i++) {
      publishKeyedEntry(queue, "cmd-full-" + i, new ReserveStock("S", "o-" + i));
      clock.advance(Duration.ofSeconds(1));
    }
    var bus = new BehindForOneAggregateBus("S");
    var runner = runner(bus, policy(5, false), queue, clock, 2);

    var poll = logDuring(runner::processBatch);

    assertThat(messagesAt(poll, Level.WARN)).as("one WARN line for the poll").hasSize(1);
    assertThat(messagesAt(poll, Level.WARN).getFirst())
        .contains("20 DLQ entries")
        .contains("behind the stored schema")
        .contains("full page of 20")
        .contains("not reached");
    assertThat(messagesAt(poll, Level.INFO)).isEmpty();

    var notFull = new InMemoryDeadLetterQueue(clock);
    publishKeyedEntry(notFull, "cmd-not-full", new ReserveStock("S", "o-x"));
    var small = logDuring(runner(bus, policy(5, false), notFull, clock, 2)::processBatch);

    assertThat(messagesAt(small, Level.WARN)).isEmpty();
    assertThat(messagesAt(small, Level.INFO))
        .singleElement()
        .satisfies(line -> assertThat(line).contains("1 DLQ entry").doesNotContain("full page"));
  }

  /**
   * The poll that fills its batch before it reaches the end of a full page has not been held back
   * by the entries behind the stored schema: its line stays at INFO.
   */
  @Test
  void aPollThatFillsItsBatchStatesItsReplaysBehindAtInfo() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    publishKeyedEntry(queue, "cmd-head-behind", new ReserveStock("S", "o-0"));
    clock.advance(Duration.ofSeconds(1));
    for (int i = 0; i < 25; i++) {
      publishKeyedEntry(queue, "cmd-healthy-" + i, new ReserveStock("T", "o-" + i));
      clock.advance(Duration.ofSeconds(1));
    }
    var bus = new BehindForOneAggregateBus("S");

    var poll = logDuring(runner(bus, policy(5, false), queue, clock, 2)::processBatch);

    assertThat(bus.executed()).as("the batch is full").isEqualTo(2);
    assertThat(bus.refused()).isEqualTo(1);
    assertThat(messagesAt(poll, Level.WARN)).isEmpty();
    assertThat(messagesAt(poll, Level.INFO).stream().filter(l -> l.contains("stored schema")))
        .singleElement()
        .satisfies(line -> assertThat(line).doesNotContain("full page"));
  }

  /**
   * A poll that stops in a full page because another replica took the runner's lease has not
   * reached the end of the page: the entries after the point where it stopped are the new lease
   * holder's, and its line stays at INFO.
   */
  @Test
  void aPollThatLosesTheLeaseInAFullPageStatesItsReplaysBehindAtInfo() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    for (int i = 0; i < 21; i++) {
      publishKeyedEntry(queue, "cmd-lease-" + i, new ReserveStock("S", "o-" + i));
      clock.advance(Duration.ofSeconds(1));
    }
    var leadership = ScriptedSubscriptionLeadership.granting();
    var bus =
        new BehindForOneAggregateBus("S") {
          @Override
          public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
            if (refused() == 4) {
              leadership.revoke();
            }
            return super.execute(command, key);
          }
        };
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(queue)
            .commandBus(bus)
            .objectMapper(objectMapper)
            .policy(policy(5, false))
            .batchSize(2)
            .clock(clock)
            .leadership(leadership)
            .registerCommand(ReserveStock.class)
            .build();

    var poll = logDuring(runner::processBatch);

    assertThat(bus.refused()).as("the poll stops at the next entry").isEqualTo(5);
    assertThat(messagesAt(poll, Level.INFO))
        .singleElement()
        .satisfies(line -> assertThat(line).contains("5 DLQ entries").doesNotContain("full page"));
  }

  /**
   * Crash point: the write of the replay's time fails before it reaches the store. The entry is
   * left as it was, at the head of the order, and the next poll replays it again and records the
   * time.
   */
  @Test
  void aFailedAttemptTimeWriteLeavesTheEntryAsItWas_andTheNextPollReplaysItAgain() {
    MutableClock clock = MutableClock.startingAt(START);
    var store = new InMemoryDeadLetterQueue(clock);
    var faults = FaultSchedule.none();
    var queue = new ForwardingDeadLetterQueue(store, faults);
    CommandId id = publishKeyedEntry(store, "cmd-crash-before", new ReserveStock("S", "o-1"));
    var bus = new BehindTheStoredSchemaBus(Integer.MAX_VALUE);
    var runner = runner(bus, policy(5, false), queue, clock, 10);
    faults.failOnce("updateAttempts");
    clock.advance(Duration.ofSeconds(5));

    assertThatThrownBy(runner::processBatch).isInstanceOf(InjectedFaultException.class);

    DeadLetterQueue.DeadLetterEntry untouched = store.find(id).orElseThrow();
    assertThat(untouched.dlqAttempts()).isZero();
    assertThat(untouched.lastAttemptAt()).as("the store was not reached").isNull();

    clock.advance(Duration.ofSeconds(5));
    runner.processBatch();

    DeadLetterQueue.DeadLetterEntry replayed = store.find(id).orElseThrow();
    assertThat(bus.replays()).isEqualTo(2);
    assertThat(replayed.dlqAttempts()).isZero();
    assertThat(replayed.lastAttemptAt()).isEqualTo(clock.instant());
  }

  /**
   * Crash point: the replay's time is written and the runner fails before it goes on. The entry is
   * behind the others and not due; once its backoff has passed it is replayed again, still with no
   * attempt counted, and the first poll on a replica that reads the stored version replays it.
   */
  @Test
  void aCrashAfterTheAttemptTimeWriteLeavesTheEntryBehind_andItIsReplayedAfterItsBackoff() {
    MutableClock clock = MutableClock.startingAt(START);
    var store = new InMemoryDeadLetterQueue(clock);
    var faults = FaultSchedule.none();
    var queue = new ForwardingDeadLetterQueue(store, faults);
    CommandId id = publishKeyedEntry(store, "cmd-crash-after", new ReserveStock("S", "o-1"));
    var bus = new BehindTheStoredSchemaBus(2);
    var runner = runner(bus, backoffPolicy(), queue, clock, 10);
    faults.crashAfter("updateAttempts");
    clock.advance(Duration.ofMinutes(1));

    assertThatThrownBy(runner::processBatch).isInstanceOf(InjectedFaultException.class);

    Instant crashedAt = clock.instant();
    assertThat(store.find(id).orElseThrow().lastAttemptAt()).isEqualTo(crashedAt);

    clock.advance(Duration.ofSeconds(1));
    runner.processBatch();
    assertThat(bus.replays()).as("not due within its backoff").isEqualTo(1);

    clock.advance(Duration.ofMinutes(1));
    runner.processBatch();
    assertThat(bus.replays()).isEqualTo(2);
    assertThat(store.find(id).orElseThrow().dlqAttempts()).isZero();

    clock.advance(Duration.ofMinutes(1));
    runner.processBatch();
    assertThat(store.find(id)).as("replayed on a replica that reads the version").isEmpty();
    assertThat(bus.executed()).isEqualTo(1);
  }

  @Test
  void everyReplayBehindTheStoredSchemaIsCountedByCommandType() {
    var bus = new BehindTheStoredSchemaBus(Integer.MAX_VALUE);
    CommandId first = publishKeyedEntry("cmd-counted-1");
    publishKeyedEntry("cmd-counted-2");
    var runner = runner(bus, policy(5, false));

    runner.processBatch();

    assertThat(
            metrics.count(
                MetricNames.DLQ_READER_BEHIND, MetricNames.TAG_COMMAND_TYPE, "ReserveStock"))
        .as("one sample per replay of the poll")
        .isEqualTo(2);

    assertThat(runner.retry(first)).isEqualTo(RetryOutcome.READER_BEHIND);

    assertThat(
            metrics.count(
                MetricNames.DLQ_READER_BEHIND, MetricNames.TAG_COMMAND_TYPE, "ReserveStock"))
        .as("an operator's retry is counted too")
        .isEqualTo(3);
  }

  /**
   * A poll states its replays behind the stored schema in one INFO line, however many there are:
   * during a rollout every poll meets them again, and a line per entry per poll would bury the log.
   * The line names what the replica lacks — each distinct event type with the version it is stored
   * at and the version this replica reads up to, once however many entries share it — and the
   * command types refused, so an operator at INFO sees whether the rollout is still in progress.
   * Each entry's own line is at DEBUG.
   */
  @Test
  void aPollStatesItsReplaysBehindTheStoredSchemaInOneInfoLine() {
    var bus = new BehindTheStoredSchemaBus(Integer.MAX_VALUE);
    publishKeyedEntry("cmd-logged-1");
    publishKeyedEntry("cmd-logged-2");
    publishKeyedEntry("cmd-logged-3");
    var runner = runner(bus, policy(5, false));

    List<String> info = infoLinesDuring(runner::processBatch);

    assertThat(info).as("one line for the poll").hasSize(1);
    assertThat(info.getFirst())
        .contains("3 DLQ entries")
        .contains("behind the stored schema")
        .contains(MetricNames.DLQ_READER_BEHIND)
        .containsOnlyOnce("StockReserved stored at schema version 2, read up to 1")
        .containsOnlyOnce("StockReserved")
        .containsOnlyOnce("ReserveStock");
  }

  /**
   * The line names five schemas and five command types at most, in the order the poll met them, and
   * counts the rest: a rollout that bumps many event types does not make one poll's line grow with
   * the page.
   */
  @Test
  void aPollNamesFiveSchemasAtMost_andCountsTheRest() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    for (int i = 0; i < 7; i++) {
      publishKeyedEntry(queue, "cmd-schema-" + i, new ReserveStock("S", "o-" + i));
      clock.advance(Duration.ofSeconds(1));
    }
    var bus =
        new BehindTheStoredSchemaBus(Integer.MAX_VALUE) {
          @Override
          RuntimeException refusal(Command command) {
            String n = ((ReserveStock) command).orderId().substring(2);
            return new EventSchemaAheadException(
                "stored at schema_version 3, reader reaches 2", new EventType("Event" + n), 3, 2);
          }
        };

    List<String> info =
        infoLinesDuring(runner(bus, policy(5, false), queue, clock, 10)::processBatch);

    assertThat(info)
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains("7 DLQ entries")
                    .contains(
                        "Event0 stored at schema version 3, read up to 2; Event1 stored at schema"
                            + " version 3, read up to 2; Event2")
                    .contains("Event4 stored at schema version 3, read up to 2; and 2 more")
                    .doesNotContain("Event5")
                    .doesNotContain("Event6")
                    .containsOnlyOnce("ReserveStock"));
  }

  /** Each command type refused in the poll is named once. */
  @Test
  void aPollNamesEachCommandTypeItsReplaysBehindRefused() {
    MutableClock clock = MutableClock.startingAt(START);
    var queue = new InMemoryDeadLetterQueue(clock);
    publishKeyedEntry(queue, "cmd-reserve-1", new ReserveStock("S", "o-1"));
    publishKeyedEntry(queue, "cmd-release-1", ReleaseStock.class, new ReleaseStock("S", "o-1"));
    publishKeyedEntry(queue, "cmd-reserve-2", new ReserveStock("S", "o-2"));
    var bus = new BehindTheStoredSchemaBus(Integer.MAX_VALUE);
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(queue)
            .commandBus(bus)
            .objectMapper(objectMapper)
            .policy(policy(5, false))
            .clock(clock)
            .registerCommand(ReserveStock.class)
            .registerCommand(ReleaseStock.class)
            .build();

    List<String> info = infoLinesDuring(runner::processBatch);

    assertThat(info)
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains("3 DLQ entries")
                    .contains("command types ReserveStock, ReleaseStock")
                    .containsOnlyOnce("ReserveStock")
                    .containsOnlyOnce("ReleaseStock"));
  }

  @Test
  void theRefusalIsRecognisedWhereverItSitsInTheCauseChain() {
    var bus =
        new BehindTheStoredSchemaBus(Integer.MAX_VALUE) {
          @Override
          RuntimeException refusal(Command command) {
            return new CommandExecutionException(
                command,
                CommandId.of("cmd-wrapped"),
                StreamId.of(STOCK, AggregateId.of("S")),
                "wrapped",
                super.refusal(command));
          }
        };
    CommandId id = publishKeyedEntry("cmd-behind-5");

    assertThat(runner(bus, policy(5, false)).retry(id)).isEqualTo(RetryOutcome.READER_BEHIND);
    assertThat(deadLetterQueue.find(id).orElseThrow().dlqAttempts()).isZero();
  }

  /**
   * The whole window on the real command bus: a client's keyed command is dead-lettered on a
   * replica of the previous release, that replica's retry runner replays it without counting an
   * attempt, and the first replay after the upgrade stores the command's events once.
   */
  @Test
  void endToEnd_aCommandDeadLetteredOnAnOldReplicaReplaysAfterTheUpgrade() {
    var inbox = new InMemoryCommandInbox();
    var store =
        InMemoryEventStore.serializing(
                SimpleEventTypeRegistry.builder().registerEvent(StockReserved.class).build(), null)
            .withCommandInbox(inbox);
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .deadLetterQueue(deadLetterQueue)
            .objectMapper(objectMapper)
            .register(
                STOCK, ReserveStock.class, c -> AggregateId.of(c.itemId()), new StockDecider())
            .build();
    store.withUpcasters(List.of(stockReservedAtVersionTwo())); // a replica of the next release
    bus.execute(new ReserveStock("S", "another-order"));
    store.withUpcasters(null); // this replica still runs the previous release

    IdempotencyKey clientKey = IdempotencyKey.of("client-key-1");
    Throwable clientSaw =
        catchThrowable(() -> bus.execute(new ReserveStock("S", "order-1"), clientKey));

    DeadLetterQueue.DeadLetterEntry entry = deadLetterQueue.all().getFirst();
    assertThat(clientSaw)
        .as("the client learns that its command is queued for a replay under its key")
        .isInstanceOfSatisfying(
            CommandDeadLetteredException.class,
            deadLettered -> {
              assertThat(deadLettered.commandId()).isEqualTo(entry.commandId());
              assertThat(deadLettered.idempotencyKey()).isEqualTo(clientKey);
            });
    assertThat(CommandFailureClassification.isReaderBehindStoredSchema(clientSaw)).isTrue();
    var runner = runner(bus, policy(5, false));
    for (int poll = 1; poll <= 5; poll++) {
      runner.processBatch();
    }
    assertThat(deadLetterQueue.find(entry.commandId()).orElseThrow().dlqAttempts()).isZero();

    store.withUpcasters(List.of(stockReservedAtVersionTwo())); // the replica is upgraded
    runner.processBatch();

    assertThat(deadLetterQueue.find(entry.commandId())).isEmpty();
    assertThat(
            store
                .readStream(StreamId.of(STOCK, AggregateId.of("S")), Version.initial(), 10)
                .stream()
                .map(e -> ((StockReserved) e.event()).orderId())
                .toList())
        .containsExactly("another-order", "order-1");
  }

  // ---- fixtures ----------------------------------------------------------------------------

  record ReserveStock(String itemId, String orderId) implements Command {}

  record ReleaseStock(String itemId, String orderId) implements Command {}

  record StockReserved(String itemId, String orderId) implements DomainEvent {}

  record StockState(int reserved) implements AggregateState {}

  static final class StockDecider implements Decider<ReserveStock, StockState, StockReserved> {
    @Override
    public StockState initialState() {
      return new StockState(0);
    }

    @Override
    public List<StockReserved> decide(ReserveStock command, StockState state) {
      return List.of(new StockReserved(command.itemId(), command.orderId()));
    }

    @Override
    public StockState evolve(StockState state, StockReserved event) {
      return new StockState(state.reserved() + 1);
    }
  }

  /** The next release's upcaster: version 2 changed nothing that needs rewriting. */
  static EventUpcaster stockReservedAtVersionTwo() {
    return new EventUpcaster() {
      @Override
      public EventType eventType() {
        return EventType.fromClass(StockReserved.class);
      }

      @Override
      public int currentVersion() {
        return 2;
      }

      @Override
      public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
        return eventData;
      }
    };
  }

  /**
   * A bus with a command inbox whose first {@code behindFor} keyed executions are refused as the
   * store refuses a load on a replica behind the stored schema; later ones execute.
   */
  static class BehindTheStoredSchemaBus implements CommandBus {
    private final int behindFor;
    private final AtomicInteger replays = new AtomicInteger();
    private final AtomicInteger executed = new AtomicInteger();

    BehindTheStoredSchemaBus(int behindFor) {
      this.behindFor = behindFor;
    }

    RuntimeException refusal(Command command) {
      return new EventSchemaAheadException(
          "Event type StockReserved is stored at schema_version 2; this reader's upcaster chain"
              + " reaches 1",
          new EventType("StockReserved"),
          2,
          1);
    }

    int replays() {
      return replays.get();
    }

    int executed() {
      return executed.get();
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("the runner replays a keyed entry under its key");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (replays.incrementAndGet() <= behindFor) {
        throw refusal(command);
      }
      executed.incrementAndGet();
      return new CommandResult(
          List.of(), StreamId.of(STOCK, AggregateId.of("S")), Version.initial(), List.of());
    }
  }

  /**
   * A bus with a command inbox that refuses every command on one aggregate as the store refuses a
   * load on a replica behind the stored schema, and executes the commands on every other aggregate.
   */
  static class BehindForOneAggregateBus extends BehindTheStoredSchemaBus {
    private final String behindItem;
    private final AtomicInteger refused = new AtomicInteger();

    BehindForOneAggregateBus(String behindItem) {
      super(0);
      this.behindItem = behindItem;
    }

    int refused() {
      return refused.get();
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      if (((ReserveStock) command).itemId().equals(behindItem)) {
        refused.incrementAndGet();
        throw refusal(command);
      }
      return super.execute(command, key);
    }
  }

  private CommandId publishKeyedEntry(String id) {
    return publishKeyedEntry(deadLetterQueue, id, new ReserveStock("S", "order-1"));
  }

  private CommandId publishKeyedEntry(DeadLetterQueue queue, String id, ReserveStock command) {
    return publishKeyedEntry(queue, id, ReserveStock.class, command);
  }

  private CommandId publishKeyedEntry(
      DeadLetterQueue queue, String id, Class<? extends Command> type, Command command) {
    CommandId commandId = CommandId.of(id);
    String itemId =
        command instanceof ReserveStock reserve
            ? reserve.itemId()
            : ((ReleaseStock) command).itemId();
    try {
      queue.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              objectMapper.writeValueAsString(command),
              type.getName(),
              commandId,
              StreamId.of(STOCK, AggregateId.of(itemId)),
              EventSchemaAheadException.class.getName(),
              "stored at schema_version 2, reader reaches 1",
              1,
              Instant.now(),
              null,
              null,
              null,
              IdempotencyKey.of("client-key-" + id)));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return commandId;
  }

  /** The INFO-and-above lines {@link DeadLetterRetryRunner} logs while {@code body} runs. */
  private static List<String> infoLinesDuring(Runnable body) {
    return logDuring(body).stream()
        .filter(e -> e.getLevel().isGreaterOrEqual(Level.INFO))
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  /** The events {@link DeadLetterRetryRunner} logs at INFO and above while {@code body} runs. */
  private static List<ILoggingEvent> logDuring(Runnable body) {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(DeadLetterRetryRunner.class);
    var previous = logger.getLevel();
    var appender = new ch.qos.logback.core.read.ListAppender<ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(Level.INFO);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
    return List.copyOf(appender.list);
  }

  /** The messages of {@code events} logged at exactly {@code level}. */
  private static List<String> messagesAt(List<ILoggingEvent> events, Level level) {
    return events.stream()
        .filter(e -> e.getLevel().equals(level))
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  private static DeadLetterRetryPolicy policy(int maxRetries, boolean discardAfterMaxRetries) {
    // A zero initial delay keeps every entry due on every poll; the default delays the first
    // replay 30 s after the publish and spaces five attempts over about fifteen minutes.
    return new DeadLetterRetryPolicy(
        maxRetries,
        Duration.ZERO,
        2.0,
        discardAfterMaxRetries,
        DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY);
  }

  /**
   * The default policy's backoff: the first replay of an entry is due 15 to 45 s (30 s with jitter)
   * after its publish or its last replay.
   */
  private static DeadLetterRetryPolicy backoffPolicy() {
    return new DeadLetterRetryPolicy(
        5, Duration.ofSeconds(30), 2.0, false, DeadLetterRetryPolicy.UnkeyedReplay.OPERATOR_ONLY);
  }

  private DeadLetterRetryRunner runner(CommandBus bus, DeadLetterRetryPolicy policy) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(deadLetterQueue)
        .commandBus(bus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .registerCommand(ReserveStock.class)
        .metrics(metrics)
        .build();
  }

  private DeadLetterRetryRunner runner(
      CommandBus bus,
      DeadLetterRetryPolicy policy,
      DeadLetterQueue queue,
      java.time.Clock clock,
      int batchSize) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(queue)
        .commandBus(bus)
        .objectMapper(objectMapper)
        .policy(policy)
        .pollInterval(Duration.ofMinutes(1))
        .batchSize(batchSize)
        .clock(clock)
        .registerCommand(ReserveStock.class)
        .metrics(metrics)
        .build();
  }
}
