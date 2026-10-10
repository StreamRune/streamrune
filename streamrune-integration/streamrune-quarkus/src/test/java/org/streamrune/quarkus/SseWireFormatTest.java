package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.ShutdownEvent;
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
import java.util.concurrent.CopyOnWriteArrayList;
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
    // The response headers reach the client with the first frame; a short keepalive makes that
    // frame arrive at once.
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
      Thread reader = Thread.ofVirtual().start(() -> collectFrames(response.body(), frames));
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

      assertThat(frames).as("a keepalive is a comment-only frame").contains(List.of(":keepalive"));

      controller.completeOpenStreams(mock(ShutdownEvent.class));
      reader.join(Duration.ofSeconds(5));
      assertThat(reader.isAlive()).as("the client's stream ended").isFalse();
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  private static HttpResponse<InputStream> open(
      HttpClient client, QuarkusRestTestServer server, String path) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
            .header("Accept", "text/event-stream")
            .build(),
        HttpResponse.BodyHandlers.ofInputStream());
  }

  private static boolean carriesData(List<String> frame) {
    return frame.stream().anyMatch(line -> line.startsWith("data:"));
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
      // the stream was cut instead of completed; the test asserts on what was read
    }
  }

  private static EventEnvelope envelope(long globalOffset, DomainEvent event) {
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
