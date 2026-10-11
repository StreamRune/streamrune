package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.streamrune.core.CommandBus;
import org.streamrune.spring.SseLiveFeedTest.OrderCommand;

/**
 * A client that stops reading its stream is evicted without holding up the feed: the clients of
 * every other stream keep receiving their events.
 *
 * <p>Boots a real embedded Tomcat with the auto-configuration and the feed. One client opens {@code
 * /api/sse/order/stalled} over a raw socket and never reads; its delivery worker ends up inside a
 * blocking servlet write that only the container's write timeout ({@link #WRITE_TIMEOUT}) ends.
 * Events keep arriving on that stream until the publisher evicts the client as a slow consumer. The
 * eviction runs on the feed's polling thread, the one thread that publishes for every stream of the
 * application.
 */
class SseStalledClientTest {

  /** Tomcat's write timeout: how long the stalled client's blocked write lasts. */
  private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(20);

  /** How soon another stream's event must arrive once the stalled client is evicted. */
  private static final Duration DELIVERED_WITHIN = WRITE_TIMEOUT.dividedBy(4);

  /** Each event of the stalled stream: large enough for a few of them to fill the socket. */
  private static final String LARGE_NOTE = "x".repeat(64 * 1024);

  /**
   * The events that block the delivery worker: fewer than the publisher's queue of 256 holds, so
   * they cannot overflow it, and far more bytes than the socket buffers of a client that does not
   * read take.
   */
  private static final int EVENTS_THAT_FILL_THE_SOCKET = 150;

  /** The events that overflow the queue of 256 behind the blocked worker. */
  private static final int EVENTS_THAT_OVERFLOW_THE_QUEUE = 300;

  /** How long the delivery worker is given to write its way into the full socket. */
  private static final Duration WORKER_BLOCKS_WITHIN = Duration.ofSeconds(1);

  @Test
  void evictingAStalledClientDoesNotHoldTheFeed_theOtherStreamsEventIsDeliveredAtOnce()
      throws Exception {
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(SseLiveFeedTest.SseApplication.class)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .properties(
                "server.port=0",
                // The stalled request is still inside its blocked write when the test ends.
                "server.shutdown=immediate",
                "server.tomcat.connection-timeout=" + WRITE_TIMEOUT.toSeconds() + "s",
                "streamrune.sse.enabled=true",
                "streamrune.sse.polling-interval=20ms",
                // No periodic keepalive in the picture: only events are written.
                "streamrune.sse.keep-alive-interval=1h")
            .run();
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    Socket stalled = new Socket();
    try {
      int port = ((WebServerApplicationContext) context).getWebServer().getPort();
      SseController controller = context.getBean(SseController.class);

      // A small receive buffer, set before the connect so it bounds the advertised window.
      stalled.setReceiveBufferSize(4096);
      stalled.connect(new InetSocketAddress("localhost", port));
      OutputStream request = stalled.getOutputStream();
      request.write(
          "GET /api/sse/order/stalled HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n"
              .getBytes(StandardCharsets.US_ASCII));
      request.flush();

      HttpResponse<InputStream> healthy =
          client.send(
              HttpRequest.newBuilder(
                      URI.create("http://localhost:" + port + "/api/sse/order/healthy"))
                  .header("Accept", "text/event-stream")
                  .build(),
              HttpResponse.BodyHandlers.ofInputStream());
      assertThat(healthy.statusCode()).isEqualTo(200);
      List<String> healthyDataLines = new CopyOnWriteArrayList<>();
      Thread.ofVirtual().start(() -> collectDataLines(healthy.body(), healthyDataLines));
      await().atMost(Duration.ofSeconds(5)).until(() -> controller.activeCountForTest() == 2);

      CommandBus bus = context.getBean(CommandBus.class);
      for (int i = 0; i < EVENTS_THAT_FILL_THE_SOCKET; i++) {
        bus.execute(new OrderCommand.Place("stalled", LARGE_NOTE));
      }
      // Stored after them: once the healthy client has it, the feed has queued every event above
      // for the stalled client, whose worker is writing them into a socket nobody reads.
      bus.execute(new OrderCommand.Place("healthy", "placed-before-the-eviction"));
      await()
          .atMost(Duration.ofSeconds(10))
          .until(
              () ->
                  healthyDataLines.stream()
                      .anyMatch(line -> line.contains("placed-before-the-eviction")));
      Thread.sleep(WORKER_BLOCKS_WITHIN);
      assertThat(controller.activeCountForTest())
          .as("the stalled client is still subscribed: its queue has not overflowed")
          .isEqualTo(2);

      for (int i = 0; i < EVENTS_THAT_OVERFLOW_THE_QUEUE; i++) {
        bus.execute(new OrderCommand.Place("stalled", LARGE_NOTE));
      }
      // Stored after every event of the stalled stream: the feed reaches it only once it is past
      // the eviction.
      bus.execute(new OrderCommand.Place("healthy", "placed-after-the-eviction"));

      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () ->
                  assertThat(controller.activeCountForTest())
                      .as("the stalled client is evicted, the healthy one stays")
                      .isEqualTo(1));
      await()
          .atMost(DELIVERED_WITHIN)
          .untilAsserted(
              () ->
                  assertThat(healthyDataLines)
                      .as(
                          "the feed publishes the next stream's event without waiting for the"
                              + " stalled client's write to time out (%s)",
                          WRITE_TIMEOUT)
                      .anyMatch(line -> line.contains("placed-after-the-eviction")));
    } finally {
      // Ends the blocked write, so nothing of the stalled request outlives the test.
      stalled.close();
      context.close();
      client.shutdownNow();
    }
  }

  private static void collectDataLines(InputStream body, List<String> dataLines) {
    try (BufferedReader lines =
        new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
      String line;
      while ((line = lines.readLine()) != null) {
        if (line.startsWith("data:")) {
          dataLines.add(line);
        }
      }
    } catch (IOException _) {
      // the stream was cut when the application stopped; the test asserts on what was read
    }
  }
}
