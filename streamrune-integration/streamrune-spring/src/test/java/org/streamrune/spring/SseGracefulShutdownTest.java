package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.integration.SseAuthorizer;

/**
 * A Spring Boot application with a connected Server-Sent Events client shuts down without waiting
 * out the graceful-shutdown phase timeout.
 *
 * <p>Graceful shutdown drains in-flight requests before the web server stops, and an open SSE
 * stream is an in-flight (asynchronous) request that never finishes on its own: the keepalive keeps
 * it healthy and the emitter timeout is minutes long. Unless the framework ends its streams first,
 * the drain waits for the whole {@code spring.lifecycle.timeout-per-shutdown-phase}, gives up, and
 * the client sees its connection cut by the stopping server. This test boots a real embedded Tomcat
 * with the auto-configured {@link SseController}, connects a real HTTP client to the stream, and
 * measures {@code close()}.
 */
class SseGracefulShutdownTest {

  private static final Duration PHASE_TIMEOUT = Duration.ofSeconds(8);

  @Test
  void closingTheContextEndsConnectedStreamsInsteadOfWaitingOutTheShutdownPhase() throws Exception {
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(SseApplication.class)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .properties(
                "server.port=0",
                "server.shutdown=graceful",
                "spring.lifecycle.timeout-per-shutdown-phase=" + PHASE_TIMEOUT.toSeconds() + "s",
                "streamrune.sse.enabled=true",
                // The response headers reach the client with the first frame; a short keepalive
                // makes that frame arrive at once — and keeps the stream healthy, as in production.
                "streamrune.sse.keep-alive-interval=200ms")
            .run();
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    boolean closed = false;
    try {
      int port = ((WebServerApplicationContext) context).getWebServer().getPort();
      SseController controller = context.getBean(SseController.class);

      HttpResponse<InputStream> response =
          client.send(
              HttpRequest.newBuilder(
                      URI.create("http://localhost:" + port + "/api/sse/cart/cart-1"))
                  .header("Accept", "text/event-stream")
                  .build(),
              HttpResponse.BodyHandlers.ofInputStream());
      assertThat(response.statusCode()).isEqualTo(200);
      awaitOneOpenStream(controller);

      CompletableFuture<byte[]> body =
          CompletableFuture.supplyAsync(
              () -> {
                try (InputStream in = response.body()) {
                  return in.readAllBytes();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });

      long started = System.nanoTime();
      context.close();
      closed = true;
      Duration shutdown = Duration.ofNanos(System.nanoTime() - started);

      assertThat(shutdown)
          .as(
              "a connected SSE client must not hold graceful shutdown for the %s phase timeout",
              PHASE_TIMEOUT)
          .isLessThan(PHASE_TIMEOUT.dividedBy(2));
      assertThat(body.get(5, TimeUnit.SECONDS))
          .as("the client's stream ends normally (it reconnects elsewhere) instead of being cut")
          .isNotNull();
      assertThat(controller.activeCountForTest()).isZero();
    } finally {
      if (!closed) {
        context.close();
      }
      client.shutdownNow();
    }
  }

  private static void awaitOneOpenStream(SseController controller) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (controller.activeCountForTest() != 1 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertThat(controller.activeCountForTest()).isEqualTo(1);
  }

  /** The servlet web stack plus the StreamRune auto-configuration, on stub persistence. */
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
  static class SseApplication {

    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    EventStoreFactory eventStoreFactory() {
      return () -> mock(EventStore.class);
    }

    @Bean
    SseAuthorizer sseAuthorizer() {
      return (principal, streamId) -> true;
    }
  }
}
