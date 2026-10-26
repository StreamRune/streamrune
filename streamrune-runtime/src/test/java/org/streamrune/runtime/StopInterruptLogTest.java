package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.net.SocketException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.ForwardingEventStore;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * A read in flight when the subscription, or the projection runner it serves, is closed is cut
 * short by the close's own interrupt: on a virtual thread the interrupt closes the socket of a JDBC
 * read, the driver throws, and the thread's interrupt status stays set. That is the stop, not a
 * failure: no ERROR line, and no stack trace.
 */
class StopInterruptLogTest {

  private final Logger runtime = (Logger) LoggerFactory.getLogger("org.streamrune.runtime");
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level level;

  @BeforeEach
  void captureTheRuntimeLogs() {
    appender.start();
    runtime.addAppender(appender);
    // The test configuration passes WARN and above; the stop's line is an INFO.
    level = runtime.getLevel();
    runtime.setLevel(Level.INFO);
  }

  @AfterEach
  void release() {
    runtime.setLevel(level);
    runtime.detachAppender(appender);
    appender.stop();
  }

  /**
   * An event store whose global reads block, once armed, until the reading thread is interrupted,
   * and then fail as a JDBC read does.
   */
  private static final class InterruptibleReads extends ForwardingEventStore {
    final AtomicBoolean armed = new AtomicBoolean();
    final CountDownLatch inTheRead = new CountDownLatch(1);

    InterruptibleReads() {
      super(new InMemoryEventStore());
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      if (!armed.get()) {
        return super.readGlobalStream(afterOffset, maxCount);
      }
      inTheRead.countDown();
      try {
        Thread.sleep(Duration.ofSeconds(30));
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new EventStoreException(
            "Failed to read the global stream", new SocketException("Closed by interrupt"));
      }
      return List.of();
    }
  }

  private List<ILoggingEvent> errors() {
    return appender.list.stream().filter(event -> event.getLevel() == Level.ERROR).toList();
  }

  @Test
  void aSubscriptionClosedDuringARead_logsNoError() throws Exception {
    var store = new InterruptibleReads();
    store.armed.set(true);
    var subscription =
        PollingEventSubscription.builder()
            .subscriptionName("orders")
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(events -> {})
            .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .build();
    subscription.start();
    assertThat(store.inTheRead.await(5, TimeUnit.SECONDS)).isTrue();

    subscription.close();

    assertThat(errors()).as("the close cut the read short").isEmpty();
    assertThat(appender.list)
        .anySatisfy(
            event -> {
              assertThat(event.getLevel()).isEqualTo(Level.INFO);
              assertThat(event.getFormattedMessage()).contains("orders").contains("stopped");
              assertThat(event.getThrowableProxy()).isNull();
            });
  }

  @Test
  void aLiveProjectionClosedDuringARead_logsNoError() throws Exception {
    var store = new InterruptibleReads();
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .build();
    Thread run =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(ProjectionName.of("orders"), batch -> {}, AT_LEAST_ONCE_IDEMPOTENT));
    await().atMost(Duration.ofSeconds(10)).until(() -> runner.state() == ProjectionState.LIVE);
    store.armed.set(true);
    assertThat(store.inTheRead.await(5, TimeUnit.SECONDS)).isTrue();

    runner.close();
    run.join(10_000);

    assertThat(errors()).as("the close cut the live read short").isEmpty();
  }

  @Test
  void aProjectionClosedDuringACatchUpRead_logsNoError() throws Exception {
    var store = new InterruptibleReads();
    store.armed.set(true);
    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .build();
    Thread run =
        Thread.ofVirtual()
            .start(
                () ->
                    runner.run(ProjectionName.of("orders"), batch -> {}, AT_LEAST_ONCE_IDEMPOTENT));
    assertThat(store.inTheRead.await(5, TimeUnit.SECONDS)).isTrue();

    runner.close();
    run.join(10_000);

    assertThat(errors()).as("the close cut the catch-up read short").isEmpty();
    assertThat(runner.state()).isEqualTo(ProjectionState.STOPPED);
  }
}
