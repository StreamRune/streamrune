package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/**
 * A client that closes its Server-Sent Events connection ends its stream quietly: no record at
 * {@code ERROR} (a {@code SEVERE} record of java.util.logging, which this module's tests and a
 * Quarkus application log through), only a {@code DEBUG} line.
 *
 * <p>Quarkus REST writes each frame of a stream asynchronously and logs a frame that cannot be
 * written once the response has started at {@code ERROR}, from its publisher response handler. A
 * frame that meets a connection the client has closed fails with a {@link ClosedChannelException},
 * which is the client going away, not a fault. Here each client reads the opening frame and resets
 * its connection, and events are published to the stream, one every two milliseconds beside the
 * keepalive, until the stream is torn down. The records counted are those of a closed connection: a
 * stream that outran a slow-client bound would end with an {@code ERROR} of its own, which this
 * test does not provoke.
 */
class SseClientDisconnectTest {

  private static final SseAuthorizer ALLOW_ALL = (caller, streamId) -> true;
  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);
  private static final String PUBLISHER_RESPONSE_HANDLER =
      "org.jboss.resteasy.reactive.server.handlers.PublisherResponseHandler";

  @Test
  void aClientThatClosesItsConnectionMidStream_endsItWithoutAnErrorLine() throws Exception {
    var publisher = new SseEventPublisher();
    var holder = new StreamRuneRequestContextHolder();
    var controller =
        new SseController(
            publisher, ALLOW_ALL, ANONYMOUS, holder, Duration.ofMinutes(5), Duration.ofMillis(20));
    controller.quietClientDisconnects();
    List<String> errors = new CopyOnWriteArrayList<>();
    Handler capture =
        new Handler() {
          @Override
          public void publish(LogRecord logRecord) {
            if (logRecord.getLevel().intValue() >= Level.SEVERE.intValue()
                && causedByAClosedConnection(logRecord.getThrown())) {
              errors.add(logRecord.getMessage() + " " + logRecord.getThrown());
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    Logger quarkusRest = Logger.getLogger(PUBLISHER_RESPONSE_HANDLER);
    quarkusRest.addHandler(capture);
    try (var server =
        QuarkusRestTestServer.start(
            controller,
            List.of(new StreamRuneRequestFilter(holder)),
            SseWireFormatTest.JacksonJsonWriter.class)) {
      for (int client = 0; client < 10; client++) {
        try (Socket socket = new Socket("localhost", server.port())) {
          OutputStream out = socket.getOutputStream();
          out.write(
              ("GET /api/sse/order/o-1 HTTP/1.1\r\nHost: localhost\r\n"
                      + "Accept: text/event-stream\r\n\r\n")
                  .getBytes(StandardCharsets.US_ASCII));
          out.flush();
          var in =
              new BufferedReader(
                  new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
          String line;
          while ((line = in.readLine()) != null && !": keepalive".equals(line)) {
            // the status line, the headers, then the opening frame
          }
          assertThat(line).as("the stream opened").isEqualTo(": keepalive");
          // The connection is reset, not shut down in an orderly way: the next frame meets it
          // closed.
          socket.setSoLinger(true, 0);
        }
        // An event every two milliseconds beside the keepalive, until the stream is torn down:
        // frames meet the reset connection, and no slow-client bound is reached.
        long published = 0;
        long until = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (controller.openStreamCount() > 0 && System.nanoTime() < until) {
          publisher.publish(
              SseWireFormatTest.envelope(
                  ++published, new SseWireFormatTest.OrderPlaced("o-1", (int) published)));
          LockSupport.parkNanos(Duration.ofMillis(2).toNanos());
        }
        await().atMost(Duration.ofSeconds(10)).until(() -> controller.openStreamCount() == 0);
      }
    } finally {
      quarkusRest.removeHandler(capture);
      controller.shutdown();
    }

    assertThat(errors).as("records at ERROR for a client that went away").isEmpty();
  }

  private static boolean causedByAClosedConnection(Throwable thrown) {
    for (Throwable c = thrown; c != null; c = c.getCause() == c ? null : c.getCause()) {
      if (c instanceof ClosedChannelException) {
        return true;
      }
    }
    return false;
  }
}
