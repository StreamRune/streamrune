package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.ShutdownDelayInitiatedEvent;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.MessageBodyWriter;
import jakarta.ws.rs.ext.Provider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/**
 * The Server-Sent Events frames as an HTTP client reads them off the socket.
 *
 * <p>{@link SseController} is served by Quarkus REST ({@link QuarkusRestTestServer}), so each frame
 * is written by the Quarkus REST Server-Sent Events serializer and its payload by the message body
 * writer Quarkus REST selects for it — the part of the wire contract a test of the frame objects
 * cannot see.
 */
class SseWireFormatTest {

  private static final SseAuthorizer ALLOW_ALL = (principal, streamId) -> true;
  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);
  private static final StreamId ORDER_1 =
      StreamId.of(AggregateType.of("order"), AggregateId.of("o-1"));
  private static final ObjectMapper JSON = new ObjectMapper();

  /** How soon a client sees the stream open: far below the keepalive interval of the test. */
  private static final Duration OPENS_WITHIN = Duration.ofSeconds(5);

  record OrderPlaced(String orderId, int quantity) implements DomainEvent {}

  /**
   * The JSON message body writer of the application: what a JSON extension such as {@code
   * quarkus-rest-jackson} registers for {@code application/json}.
   */
  @Provider
  @Produces(MediaType.APPLICATION_JSON)
  public static class JacksonJsonWriter implements MessageBodyWriter<Object> {

    @Override
    public boolean isWriteable(
        Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
      return true;
    }

    @Override
    public void writeTo(
        Object entity,
        Class<?> type,
        Type genericType,
        Annotation[] annotations,
        MediaType mediaType,
        MultivaluedMap<String, Object> httpHeaders,
        OutputStream entityStream)
        throws IOException {
      entityStream.write(JSON.writeValueAsBytes(entity));
    }
  }

  @Test
  void aDomainEventIsWrittenAsAJsonObjectUnderItsGlobalOffset_andAKeepAliveAsAComment()
      throws Exception {
    var publisher = new SseEventPublisher();
    var controller =
        new SseController(
            publisher, ALLOW_ALL, ANONYMOUS, Duration.ofMinutes(5), Duration.ofMillis(100));
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try (var server = QuarkusRestTestServer.start(controller, JacksonJsonWriter.class)) {
      HttpResponse<InputStream> response = open(client, server, "/api/sse/order/o-1");
      assertThat(response.statusCode()).isEqualTo(200);
      assertThat(response.headers().firstValue("Content-Type"))
          .hasValueSatisfying(type -> assertThat(type).startsWith("text/event-stream"));
      List<List<String>> frames = new CopyOnWriteArrayList<>();
      CompletableFuture<Ending> ending = new CompletableFuture<>();
      Thread.ofVirtual().start(() -> ending.complete(collectFrames(response.body(), frames)));
      await().atMost(Duration.ofSeconds(5)).until(() -> controller.openStreamCount() == 1);

      publisher.publish(envelope(42, new OrderPlaced("o-1", 3)));

      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> frames.stream().anyMatch(SseWireFormatTest::carriesData));
      List<String> eventFrame =
          frames.stream().filter(SseWireFormatTest::carriesData).findFirst().orElseThrow();
      assertThat(eventFrame)
          .as("an event frame is its id and its data, a default 'message' event: %s", eventFrame)
          .hasSize(2)
          .contains("id:42");
      String data =
          eventFrame.stream().filter(line -> line.startsWith("data:")).findFirst().orElseThrow();
      JsonNode payload = JSON.readTree(data.substring("data:".length()));
      assertThat(payload.isObject()).as("the data field is a JSON object: %s", data).isTrue();
      assertThat(payload.path("orderId").asText()).isEqualTo("o-1");
      assertThat(payload.path("quantity").asInt()).isEqualTo(3);
      assertThat(payload.size()).as("the event's own fields and nothing else").isEqualTo(2);

      // A keepalive is a comment-only frame.
      await().atMost(Duration.ofSeconds(5)).until(() -> frames.contains(List.of(": keepalive")));

      // A stream the resource completes while the server is serving ends on the wire as a
      // finished response. That is the state of an application at Quarkus's shutdown-delay event.
      // This server is a Quarkus REST deployment without an application lifecycle, and the test
      // calls the observer itself: when Quarkus fires the event, and that it stops its HTTP server
      // before the shutdown event, is not what this shows.
      controller.completeOpenStreams(new ShutdownDelayInitiatedEvent());
      assertThat(ending.get(5, TimeUnit.SECONDS))
          .as("a stream completed while the server is serving ends as a finished response")
          .isEqualTo(Ending.COMPLETED);
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  /**
   * The response is committed when the client is subscribed, not with the first event or the first
   * periodic keepalive: the endpoint writes one comment frame right after it registered the client
   * with the publisher. A client that has read that frame is therefore subscribed, and an event
   * published from then on reaches it.
   */
  @Test
  void theStreamOpensWithACommentFrameAtOnce_andAnEventPublishedAfterItIsDelivered()
      throws Exception {
    var publisher = new SseEventPublisher();
    // The periodic keepalive is far beyond every bound below: it is not what opens the stream.
    var controller =
        new SseController(
            publisher, ALLOW_ALL, ANONYMOUS, Duration.ofMinutes(5), Duration.ofSeconds(30));
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try (var server = QuarkusRestTestServer.start(controller, JacksonJsonWriter.class)) {
      HttpResponse<InputStream> response = open(client, server, "/api/sse/order/o-1", OPENS_WITHIN);
      assertThat(response.statusCode()).isEqualTo(200);
      List<List<String>> frames = new CopyOnWriteArrayList<>();
      Thread.ofVirtual().start(() -> collectFrames(response.body(), frames));
      await().atMost(OPENS_WITHIN).until(() -> !frames.isEmpty());
      assertThat(frames.getFirst()).containsExactly(": keepalive");

      // No wait for the subscription: the frame the client has read is the proof of it.
      publisher.publish(envelope(42, new OrderPlaced("o-1", 3)));

      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> frames.stream().anyMatch(SseWireFormatTest::carriesData));
      assertThat(frames.stream().filter(SseWireFormatTest::carriesData).findFirst().orElseThrow())
          .contains("id:42");
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  /**
   * A JSON message body writer that fails the way JSON-B's does: with an unchecked exception thrown
   * while a frame is serialized.
   */
  @Provider
  @Produces(MediaType.APPLICATION_JSON)
  public static class FailingJsonWriter implements MessageBodyWriter<Object> {

    @Override
    public boolean isWriteable(
        Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
      return true;
    }

    @Override
    public void writeTo(
        Object entity,
        Class<?> type,
        Type genericType,
        Annotation[] annotations,
        MediaType mediaType,
        MultivaluedMap<String, Object> httpHeaders,
        OutputStream entityStream) {
      throw new IllegalStateException("this event cannot be serialized");
    }
  }

  /**
   * An event the application's body writer cannot serialize ends the response. Quarkus REST
   * serializes a frame inside its subscriber's {@code onNext}, so the writer's exception is a
   * downstream that throws. The stream's own keepalive and deadline are far beyond the test: only
   * the endpoint's handling of the failed frame can end the response, and without it the client
   * would hold an open connection on which nothing is ever written again.
   */
  @Test
  void aFrameTheBodyWriterCannotSerializeEndsTheResponse() throws Exception {
    var publisher = new SseEventPublisher();
    var controller =
        new SseController(
            publisher, ALLOW_ALL, ANONYMOUS, Duration.ofMinutes(5), Duration.ofSeconds(30));
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try (var server = QuarkusRestTestServer.start(controller, FailingJsonWriter.class)) {
      HttpResponse<InputStream> response = open(client, server, "/api/sse/order/o-1", OPENS_WITHIN);
      assertThat(response.statusCode()).isEqualTo(200);
      List<List<String>> frames = new CopyOnWriteArrayList<>();
      CompletableFuture<Ending> ending = new CompletableFuture<>();
      Thread.ofVirtual().start(() -> ending.complete(collectFrames(response.body(), frames)));
      await().atMost(OPENS_WITHIN).until(() -> !frames.isEmpty());
      assertThat(controller.openStreamCount()).isEqualTo(1);

      publisher.publish(envelope(42, new OrderPlaced("o-1", 3)));

      assertThat(ending)
          .as("the response ended: the client is not left on a connection nothing writes to")
          .succeedsWithin(Duration.ofSeconds(5))
          .as("a failed stream is cut, so an EventSource reconnects; it is not completed")
          .isEqualTo(Ending.CUT);
      assertThat(frames)
          .as("the opening frame and nothing of the event that could not be serialized")
          .containsExactly(List.of(": keepalive"));
      await().atMost(Duration.ofSeconds(5)).until(() -> controller.openStreamCount() == 0);
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  static HttpResponse<InputStream> open(
      HttpClient client, QuarkusRestTestServer server, String path) throws Exception {
    return open(client, server, path, Duration.ofSeconds(60));
  }

  /** Opens the stream; fails unless the status line and the headers arrive within the bound. */
  static HttpResponse<InputStream> open(
      HttpClient client, QuarkusRestTestServer server, String path, Duration headersWithin)
      throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
            .header("Accept", "text/event-stream")
            .timeout(headersWithin)
            .build(),
        HttpResponse.BodyHandlers.ofInputStream());
  }

  static boolean carriesData(List<String> frame) {
    return frame.stream().anyMatch(line -> line.startsWith("data:"));
  }

  /** How a response body ended: completed by the server, or cut with the connection. */
  enum Ending {
    COMPLETED,
    CUT
  }

  /**
   * Reads the stream as frames, the lines up to each blank line, until the body ends, and reports
   * how it ended. A completed chunked response ends with its terminating chunk and reads as end of
   * stream; a connection cut before that chunk fails the read.
   */
  static Ending collectFrames(InputStream body, List<List<String>> frames) {
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
      return Ending.COMPLETED;
    } catch (IOException _) {
      return Ending.CUT;
    }
  }

  static EventEnvelope envelope(long globalOffset, DomainEvent event) {
    return new EventEnvelope(
        GlobalOffset.of(globalOffset),
        ORDER_1,
        new Version(1),
        new EventType("OrderPlaced"),
        event,
        new EventMetadata(
            EventId.of("evt-" + globalOffset),
            CommandId.of("cmd-" + globalOffset),
            null,
            null,
            CorrelationId.of("corr-" + globalOffset),
            null,
            null,
            Instant.now()));
  }
}
