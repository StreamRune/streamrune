package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ContractHttpTest {

  private HttpServer server;
  private final CountDownLatch streamDone = new CountDownLatch(1);

  @BeforeEach
  void serve() throws IOException {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/echo",
        exchange -> {
          byte[] body =
              (exchange.getRequestMethod()
                      + " "
                      + exchange.getRequestHeaders().getFirst("X-Probe")
                      + " "
                      + new String(
                          exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8))
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("X-Answer", "one");
          exchange.getResponseHeaders().add("X-Answer", "two");
          exchange.sendResponseHeaders(201, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.createContext(
        "/stream",
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
          exchange.sendResponseHeaders(200, 0);
          OutputStream out = exchange.getResponseBody();
          try {
            // A stream that does not end on its own: the client must not wait for its end.
            while (true) {
              out.write(": keepalive\n\n".getBytes(StandardCharsets.UTF_8));
              out.flush();
              Thread.sleep(50);
            }
          } catch (IOException | InterruptedException e) {
            streamDone.countDown();
          } finally {
            exchange.close();
          }
        });
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  @Test
  void aResponseIsReadWithItsStatusHeadersAndBody() {
    HttpResponse response =
        ContractHttp.send(
            server.getAddress().getPort(), "POST", "/echo", Map.of("X-Probe", "p"), "hello");
    assertEquals(201, response.status());
    assertEquals("POST p hello", response.body());
    assertEquals(List.of("one", "two"), response.headers().get("x-answer"));
    assertEquals("one", response.header("X-ANSWER"));
    assertNull(response.header("X-Missing"));
  }

  @Test
  void aRequestWithoutABodySendsNone() {
    HttpResponse response =
        ContractHttp.send(server.getAddress().getPort(), "GET", "/echo", Map.of(), null);
    assertEquals("GET null ", response.body());
  }

  @Test
  void anEventStreamIsReadUpToItsHeaders() throws InterruptedException {
    HttpResponse response =
        ContractHttp.send(server.getAddress().getPort(), "GET", "/stream", Map.of(), null);
    assertEquals(200, response.status());
    assertEquals("text/event-stream", response.header("Content-Type"));
    assertEquals("", response.body());
  }

  @Test
  void aServerThatCannotBeReachedFailsTheRequest() throws IOException {
    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }
    int closed = port;
    assertThrows(
        UncheckedIOException.class, () -> ContractHttp.send(closed, "GET", "/", Map.of(), null));
  }

  @Test
  void aResponseWithoutHeadersOrBodyIsEmpty() {
    var response = new HttpResponse(204, Map.of(), null);
    assertEquals("", response.body());
    assertEquals(Map.of(), response.headers());
    assertNull(response.header("Content-Type"));
    var empty = new HttpResponse(200, Map.of("X-Empty", List.of()), "");
    assertNull(empty.header("x-empty"));
  }
}
