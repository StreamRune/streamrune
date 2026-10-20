package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.context.LifecycleAutoConfiguration;
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.http.converter.autoconfigure.HttpMessageConvertersAutoConfiguration;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.tomcat.autoconfigure.servlet.TomcatServletWebServerAutoConfiguration;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.boot.webmvc.autoconfigure.DispatcherServletAutoConfiguration;
import org.springframework.boot.webmvc.autoconfigure.WebMvcAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.spring.SseLiveFeedTest.OrderCommand;
import org.streamrune.spring.SseLiveFeedTest.OrderDecider;
import org.streamrune.spring.SseLiveFeedTest.OrderEvent;
import org.streamrune.spring.SseLiveFeedTest.OrderState;
import org.streamrune.test.InMemoryEventStore;

/**
 * The feed of the Server-Sent Events endpoint reads events only while a client is connected, and a
 * client that connects to a feed that was idle misses nothing stored after its opening comment.
 *
 * <p>Boots a real embedded Tomcat with the auto-configuration on an event store that counts how it
 * is read, connects a real HTTP client to {@code /api/sse/order/o-1}, and executes commands through
 * the {@link CommandBus} bean.
 */
class SseIdleFeedTest {

  private static final AggregateType ORDER = AggregateType.of("order");

  /** How often the test connects to a feed that is idle. */
  private static final int ROUNDS = 10;

  @Test
  void aClientOfAFeedThatWasIdleReceivesTheEventStoredRightAfterItsOpeningComment()
      throws Exception {
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(CountedReadsApplication.class)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .properties(
                "server.port=0",
                "streamrune.sse.enabled=true",
                "streamrune.sse.polling-interval=20ms",
                // A client that has closed its connection is noticed by the next keepalive write.
                "streamrune.sse.keep-alive-interval=50ms")
            .run();
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try {
      int port = ((WebServerApplicationContext) context).getWebServer().getPort();
      CountedReadsEventStore store = context.getBean(CountedReadsEventStore.class);
      CommandBus bus = context.getBean(CommandBus.class);
      SseController controller = context.getBean(SseController.class);

      for (int round = 1; round <= ROUNDS; round++) {
        // The client of the round before is gone from the controller's bookkeeping; the publisher
        // may still hold it for an instant, and a poll that found it there reads one more page. A
        // read of the head that returns from here on belongs to a poll that found nobody
        // subscribed: the publisher is empty, and the polling thread is past any read for that
        // client. Only then is the counter sampled.
        awaitIdlePolls(store, 1);
        // Stored for nobody: no client is connected.
        bus.execute(new OrderCommand.Place("o-1", "for-nobody-" + round));
        int payloadReadsBefore = store.payloadReads();
        awaitIdlePolls(store, 3);
        assertThat(store.payloadReads())
            .as("round %d: the idle feed reads the head, and no event", round)
            .isEqualTo(payloadReadsBefore);

        HttpResponse<InputStream> response =
            client.send(
                HttpRequest.newBuilder(
                        URI.create("http://localhost:" + port + "/api/sse/order/o-1"))
                    .header("Accept", "text/event-stream")
                    .timeout(Duration.ofSeconds(5))
                    .build(),
                HttpResponse.BodyHandlers.ofInputStream());
        assertThat(response.statusCode()).isEqualTo(200);
        List<List<String>> frames = new CopyOnWriteArrayList<>();
        Thread.ofVirtual().start(() -> collectFrames(response.body(), frames));
        await().atMost(Duration.ofSeconds(5)).until(() -> !frames.isEmpty());
        assertThat(frames.getFirst()).containsExactly(": keepalive");

        // No wait for the feed: the comment the client has read is the proof it is subscribed.
        String note = "placed-after-the-comment-" + round;
        bus.execute(new OrderCommand.Place("o-1", note));

        await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> frames.stream().flatMap(List::stream).anyMatch(l -> l.contains(note)));
        assertThat(frames.stream().flatMap(List::stream))
            .as("round %d: what was stored for nobody is behind the feed's position", round)
            .noneMatch(line -> line.contains("for-nobody"));

        response.body().close();
        await().atMost(Duration.ofSeconds(10)).until(() -> controller.activeCountForTest() == 0);
      }
    } finally {
      context.close();
      client.shutdownNow();
    }
  }

  /** Waits until {@code count} more reads of the head have returned: that many idle polls. */
  private static void awaitIdlePolls(CountedReadsEventStore store, int count) {
    int target = store.headReads() + count;
    await().atMost(Duration.ofSeconds(5)).until(() -> store.headReads() >= target);
  }

  /** Reads the stream as frames: the lines up to each blank line. */
  private static void collectFrames(InputStream body, List<List<String>> frames) {
    try (BufferedReader lines =
        new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
      List<String> frame = new ArrayList<>();
      String line;
      while ((line = lines.readLine()) != null) {
        if (line.isEmpty()) {
          frames.add(List.copyOf(frame));
          frame.clear();
        } else {
          frame.add(line);
        }
      }
    } catch (IOException _) {
      // the test closed the stream; it asserts on what was read
    }
  }

  /** An in-memory event store that counts the reads of event payloads and the reads of the head. */
  static final class CountedReadsEventStore implements EventStore {

    private final InMemoryEventStore delegate = new InMemoryEventStore();
    private final AtomicInteger payloadReads = new AtomicInteger();
    private final AtomicInteger headReads = new AtomicInteger();

    /** The calls of {@code readGlobalStream}: each reads, and decrypts, event payloads. */
    int payloadReads() {
      return payloadReads.get();
    }

    /** The reads of the head that have returned. */
    int headReads() {
      return headReads.get();
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      payloadReads.incrementAndGet();
      return delegate.readGlobalStream(afterOffset, maxCount);
    }

    @Override
    public GlobalOffset lastGlobalOffset() {
      GlobalOffset head = delegate.lastGlobalOffset();
      headReads.incrementAndGet();
      return head;
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      return delegate.load(streamId);
    }

    @Override
    public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
      return delegate.load(streamId, expectedSnapshotVersion);
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      return delegate.append(streamId, events, expectedVersion);
    }

    @Override
    public IdempotentAppendResult appendWithKey(
        StreamId streamId,
        List<EventEnvelope> events,
        Version expectedVersion,
        IdempotencyKey idempotencyKey,
        String commandType) {
      return delegate.appendWithKey(streamId, events, expectedVersion, idempotencyKey, commandType);
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      delegate.saveSnapshot(streamId, version, state);
    }

    @Override
    public void saveSnapshot(
        StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
      delegate.saveSnapshot(streamId, version, state, snapshotVersion);
    }

    @Override
    public boolean saveSnapshot(
        StreamId streamId,
        Version version,
        AggregateState state,
        int snapshotVersion,
        long snapshotPurgeGeneration) {
      saveSnapshot(streamId, version, state, snapshotVersion);
      return true;
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return delegate.readStream(streamId, afterVersion, maxCount);
    }
  }

  /** The servlet web stack plus the StreamRune auto-configuration, on the counting event store. */
  @Configuration(proxyBeanMethods = false)
  @ImportAutoConfiguration({
    PropertyPlaceholderAutoConfiguration.class,
    LifecycleAutoConfiguration.class,
    TomcatServletWebServerAutoConfiguration.class,
    DispatcherServletAutoConfiguration.class,
    WebMvcAutoConfiguration.class,
    HttpMessageConvertersAutoConfiguration.class,
    JacksonAutoConfiguration.class,
    StreamRuneAutoConfiguration.class
  })
  static class CountedReadsApplication {

    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    AuditStore auditStore() {
      return mock(AuditStore.class);
    }

    @Bean
    CountedReadsEventStore eventStore() {
      return new CountedReadsEventStore();
    }

    @Bean
    DeciderRegistration<OrderCommand, OrderState, OrderEvent> orderRegistration() {
      return new DeciderRegistration<>(
          ORDER,
          OrderCommand.class,
          command ->
              switch (command) {
                case OrderCommand.Place place -> AggregateId.of(place.orderId());
              },
          new OrderDecider());
    }

    @Bean
    SseAuthorizer sseAuthorizer() {
      return (caller, streamId) -> true;
    }
  }
}
