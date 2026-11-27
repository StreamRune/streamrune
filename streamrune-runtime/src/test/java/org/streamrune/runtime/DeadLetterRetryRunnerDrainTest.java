package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.ForwardingDeadLetterQueue;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.MutableClock;

/**
 * One poll cycle of {@link DeadLetterRetryRunner} reads the next batch at once while each batch
 * comes back full and replayed an entry, and ends on a partial batch, a batch whose every replay
 * failed, its drain budget, a stop, or a lost lease. An entry is replayed at most once per cycle.
 */
@Timeout(60)
class DeadLetterRetryRunnerDrainTest {

  record Cmd(String value) implements Command {}

  private final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
  private final InMemoryDeadLetterQueue entries = new InMemoryDeadLetterQueue(clock);
  private final List<Integer> reads = new CopyOnWriteArrayList<>();
  private final DeadLetterQueue dlq =
      new ForwardingDeadLetterQueue(entries) {
        @Override
        public List<DeadLetterEntry> readRetryable(
            int maxRetries, int limit, boolean includeUnkeyed, Instant keyedCutoff) {
          reads.add(limit);
          return super.readRetryable(maxRetries, limit, includeUnkeyed, keyedCutoff);
        }
      };
  private final List<String> executed = new CopyOnWriteArrayList<>();
  private volatile Consumer<Cmd> behaviour = cmd -> {};

  /** Replays unkeyed commands; records each and runs {@link #behaviour} on it. */
  private final CommandBus bus =
      new CommandBus() {
        @Override
        public <C extends Command> CommandResult execute(C command) {
          Cmd cmd = (Cmd) command;
          executed.add(cmd.value());
          behaviour.accept(cmd);
          return null;
        }
      };

  /** {@link #bus} with a command inbox, as a runner that takes a lease requires. */
  private final CommandBus busWithAnInbox =
      new CommandBus() {
        @Override
        public <C extends Command> CommandResult execute(C command) {
          return bus.execute(command);
        }

        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          return bus.execute(command);
        }

        @Override
        public boolean supportsIdempotentExecution() {
          return true;
        }
      };

  private final Logger log = (Logger) LoggerFactory.getLogger(DeadLetterRetryRunner.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level levelBefore;

  @BeforeEach
  void captureTheLog() {
    levelBefore = log.getLevel();
    log.setLevel(Level.INFO);
    appender.start();
    log.addAppender(appender);
  }

  @AfterEach
  void releaseTheLog() {
    log.detachAppender(appender);
    log.setLevel(levelBefore);
  }

  private List<String> budgetLines() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.INFO)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(line -> line.contains("drain budget"))
        .toList();
  }

  /** Publishes entries e0..e(n-1), one second apart, each due at once. */
  private void backlog(int n) throws Exception {
    var mapper = new ObjectMapper();
    for (int i = 0; i < n; i++) {
      entries.publish(
          new DeadLetterQueue.DeadLetterPublishRequest(
              mapper.writeValueAsString(new Cmd("e" + i)),
              Cmd.class.getName(),
              CommandId.of("cmd-" + i),
              StreamId.of(AggregateType.of("lamp"), AggregateId.of("s" + i)),
              "Err",
              "msg",
              1,
              clock.instant(),
              null,
              null,
              null,
              null));
      clock.advance(Duration.ofSeconds(1));
    }
  }

  private DeadLetterRetryRunner.Builder runner(int batchSize) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(bus)
        .objectMapper(new ObjectMapper())
        .policy(
            new DeadLetterRetryPolicy(
                5, Duration.ZERO, 2.0, false, DeadLetterRetryPolicy.UnkeyedReplay.AUTOMATIC))
        .pollInterval(Duration.ofMinutes(1))
        .batchSize(batchSize)
        .clock(clock)
        .registerCommand(Cmd.class);
  }

  @Test
  void aFullBatchThatReplayed_isFollowedAtOnceByTheNextBatch() throws Exception {
    backlog(25);

    runner(10).build().pollCycle();

    assertThat(executed).hasSize(25);
    assertThat(entries.all()).isEmpty();
    assertThat(reads).hasSize(3);
  }

  @Test
  void aPartialBatch_endsTheCycle() throws Exception {
    backlog(5);

    runner(10).build().pollCycle();

    assertThat(executed).hasSize(5);
    assertThat(reads).hasSize(1);
  }

  @Test
  void aBatchWhoseEveryReplayFailed_endsTheCycle() throws Exception {
    backlog(25);
    behaviour =
        cmd -> {
          throw new IllegalStateException("still broken");
        };

    runner(10).build().pollCycle();

    assertThat(reads).hasSize(1);
    assertThat(executed).hasSize(10).doesNotHaveDuplicates();
    assertThat(entries.all()).hasSize(25);
  }

  @Test
  void anEntryThatFailedEarlierInTheCycle_isNotReplayedAgainInIt_evenWithNoBackoff()
      throws Exception {
    backlog(15);
    behaviour =
        cmd -> {
          if (cmd.value().equals("e0")) {
            throw new IllegalStateException("still broken");
          }
        };

    runner(10).build().pollCycle();

    assertThat(executed).hasSize(15).doesNotHaveDuplicates();
    assertThat(entries.all()).singleElement().satisfies(e -> assertThat(e.dlqAttempts()).isOne());
    assertThat(reads).hasSize(2);
  }

  @Test
  void theDrainBudget_endsTheCycle_andIsLoggedAtInfo() throws Exception {
    backlog(25);
    var now = new AtomicLong();
    var runner =
        runner(10).drainTicker(() -> now.getAndAdd(Duration.ofMinutes(10).toNanos())).build();

    runner.pollCycle();

    assertThat(reads).hasSize(1);
    assertThat(executed).hasSize(10);
    assertThat(budgetLines())
        .singleElement()
        .satisfies(line -> assertThat(line).contains("drain budget of 10 poll intervals"));
  }

  @Test
  void aCloseDuringTheDrain_startsNoFurtherBatch() throws Exception {
    backlog(3);
    var atTheFirstReplay = new CountDownLatch(1);
    var interrupted = new AtomicBoolean();
    behaviour =
        cmd -> {
          if (cmd.value().equals("e0")) {
            atTheFirstReplay.countDown();
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (!Thread.currentThread().isInterrupted() && System.nanoTime() < deadline) {
              Thread.onSpinWait();
            }
            interrupted.set(Thread.currentThread().isInterrupted());
          }
        };
    var runner = runner(1).pollInterval(Duration.ofHours(1)).build();

    runner.start();
    assertThat(atTheFirstReplay.await(20, TimeUnit.SECONDS)).isTrue();
    runner.close();

    assertThat(interrupted).isTrue();
    assertThat(reads).hasSize(1);
    assertThat(executed).containsExactly("e0");
  }

  @Test
  void aLeaseLostDuringTheDrain_startsNoFurtherBatch() throws Exception {
    backlog(3);
    var leadership = new RecordingLeadership();
    leadership.grant(DeadLetterRetryRunner.CONSUMER_NAME.value());
    behaviour = cmd -> leadership.revoke(DeadLetterRetryRunner.CONSUMER_NAME.value());

    runner(1).commandBus(busWithAnInbox).leadership(leadership).build().pollCycle();

    assertThat(executed).containsExactly("e0");
    assertThat(reads).hasSize(1);
    assertThat(entries.all()).hasSize(2);
  }
}
