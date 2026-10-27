package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

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
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.MutableClock;
import org.streamrune.test.RecordingStreamRuneMetrics;

/**
 * A dead-letter replay on a replica that is behind the stored schema: the replayed command's
 * aggregate holds an event a newer release stored at a schema version this replica's upcaster chain
 * does not reach, so the replay fails with {@link EventSchemaAheadException}.
 *
 * <p>During a rolling deployment of an event-schema change the retry runner's lease can sit on a
 * replica of the previous release for the whole window, and every replay it makes fails that way.
 * The failure says nothing about the command, which succeeds on an upgraded replica, so it counts
 * no attempt: the entry is left exactly as it was ({@link RetryOutcome#READER_BEHIND}). The default
 * policy's five attempts would otherwise turn an entry dead-lettered during a rollout into one only
 * an operator replays after about fifteen minutes, and {@code discardAfterMaxRetries} would drop
 * the command.
 */
class DeadLetterRetryReaderBehindTest {

  private static final AggregateType STOCK = AggregateType.of("stock");

  private final InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final RecordingStreamRuneMetrics metrics = new RecordingStreamRuneMetrics();

  @Test
  void aReplayBehindTheStoredSchemaCountsNoAttempt_acrossEveryPollOfTheWindow() {
    var bus = new BehindTheStoredSchemaBus(Integer.MAX_VALUE);
    CommandId id = publishKeyedEntry("cmd-behind-1");
    var runner = runner(bus, policy(5, false));

    for (int poll = 1; poll <= 6; poll++) {
      runner.processBatch();
      DeadLetterQueue.DeadLetterEntry entry = deadLetterQueue.find(id).orElseThrow();
      assertThat(entry.dlqAttempts()).as("attempts after poll %d", poll).isZero();
      assertThat(entry.lastAttemptAt()).as("the entry is left as it was").isNull();
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
  void anOperatorRetryBehindTheStoredSchema_isReaderBehind_andLeavesTheEntryAsItWas() {
    var bus = new BehindTheStoredSchemaBus(1);
    CommandId id = publishKeyedEntry("cmd-behind-4");
    var runner = runner(bus, policy(5, false));

    assertThat(runner.retry(id)).isEqualTo(RetryOutcome.READER_BEHIND);
    DeadLetterQueue.DeadLetterEntry untouched = deadLetterQueue.find(id).orElseThrow();
    assertThat(untouched.dlqAttempts()).isZero();
    assertThat(untouched.lastAttemptAt()).isNull();

    assertThat(runner.retry(id)).isEqualTo(RetryOutcome.REPLAYED);
    assertThat(deadLetterQueue.find(id)).isEmpty();
  }

  /**
   * A replay that answers {@code READER_BEHIND} takes no slot of the poll's batch. The entries left
   * unchanged keep the head of the least-recently-active order on every poll; counted against the
   * batch, a batch's worth of them would hold back every entry behind them until the rollout ends.
   */
  @Test
  void entriesBehindTheStoredSchemaTakeNoSlotOfThePollsBatch() {
    MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
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
              assertThat(entry.lastAttemptAt()).isNull();
            });
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
   * Each entry's own line, with the event type and both versions, is at DEBUG.
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
        .contains(MetricNames.DLQ_READER_BEHIND);
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
  static final class BehindForOneAggregateBus extends BehindTheStoredSchemaBus {
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
    CommandId commandId = CommandId.of(id);
    try {
      queue.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              objectMapper.writeValueAsString(command),
              ReserveStock.class.getName(),
              commandId,
              StreamId.of(STOCK, AggregateId.of(command.itemId())),
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
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(DeadLetterRetryRunner.class);
    var previous = logger.getLevel();
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(previous);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.INFO))
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
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
