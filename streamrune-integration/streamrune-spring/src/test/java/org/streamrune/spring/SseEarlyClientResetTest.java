package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/**
 * A client that resets its connection before the response is written leaves nothing behind: the
 * subscription is released at once, and the request does not hold a graceful shutdown.
 *
 * <p>The endpoint hands Spring MVC the opening frame before the handler method returns, so the
 * first write to the socket happens while Spring MVC takes the emitter over. Here that write meets
 * a connection the client has already reset: the client sends the request and closes with {@code
 * SO_LINGER 0} while the authorizer is still deciding. The stream timeout is the default five
 * minutes, far beyond the test. Two configurations:
 *
 * <ul>
 *   <li>{@code streamrune.sse.keep-alive-interval=0}: no keepalive tick comes along to notice the
 *       dead connection, so nothing but the endpoint's own handling of the failed write can
 *       unsubscribe the client;
 *   <li>{@code streamrune.sse.keep-alive-interval=200ms}: a tick would reap the subscription, but
 *       reaping does not end a request Spring MVC never finished taking over, and that request is
 *       what a graceful shutdown waits for.
 * </ul>
 */
class SseEarlyClientResetTest {

  private static final Duration PHASE_TIMEOUT = Duration.ofSeconds(8);

  @ParameterizedTest(name = "streamrune.sse.keep-alive-interval={0}")
  @ValueSource(strings = {"0", "200ms"})
  void aClientResetBeforeTheResponseIsWrittenReleasesTheSubscriptionAndDoesNotHoldTheShutdown(
      String keepAliveInterval) throws Exception {
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(SseApplication.class)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .properties(
                "server.port=0",
                "server.shutdown=graceful",
                "spring.lifecycle.timeout-per-shutdown-phase=" + PHASE_TIMEOUT.toSeconds() + "s",
                "streamrune.sse.enabled=true",
                "streamrune.sse.polling-interval=1h",
                "streamrune.sse.keep-alive-interval=" + keepAliveInterval)
            .run();
    boolean closed = false;
    try {
      int port = ((WebServerApplicationContext) context).getWebServer().getPort();
      SseController controller = context.getBean(SseController.class);
      DecidingAuthorizer authorizer = context.getBean(DecidingAuthorizer.class);
      CountingPublisher publisher = context.getBean(CountingPublisher.class);

      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("localhost", port));
        OutputStream request = socket.getOutputStream();
        request.write(
            "GET /api/sse/order/o-1 HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n"
                .getBytes(StandardCharsets.US_ASCII));
        request.flush();
        assertThat(authorizer.deciding.await(10, TimeUnit.SECONDS))
            .as("the request reached the authorizer")
            .isTrue();
        // An abortive close: the server's first write meets a reset connection.
        socket.setSoLinger(true, 0);
      }
      // The reset has reached the server before the handler goes on.
      Thread.sleep(300);
      authorizer.decided.countDown();

      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () -> {
                assertThat(publisher.subscribed).as("the client was subscribed").hasValue(1);
                assertThat(publisher.unsubscribed).as("and is unsubscribed again").hasValue(1);
                assertThat(controller.activeCountForTest()).isZero();
              });

      long started = System.nanoTime();
      context.close();
      closed = true;
      Duration shutdown = Duration.ofNanos(System.nanoTime() - started);
      assertThat(shutdown)
          .as(
              "the request of the reset client must not hold graceful shutdown for the %s phase"
                  + " timeout",
              PHASE_TIMEOUT)
          .isLessThan(PHASE_TIMEOUT.dividedBy(2));
    } finally {
      if (!closed) {
        context.close();
      }
    }
  }

  /** Lets the test hold a request between its arrival and the decision. */
  static final class DecidingAuthorizer implements SseAuthorizer {

    final CountDownLatch deciding = new CountDownLatch(1);
    final CountDownLatch decided = new CountDownLatch(1);

    @Override
    public boolean isAuthorized(UserId principal, StreamId streamId) {
      deciding.countDown();
      try {
        return decided.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  /** Counts the subscriptions the endpoint makes and releases. */
  static final class CountingPublisher extends SseEventPublisher {

    final AtomicInteger subscribed = new AtomicInteger();
    final AtomicInteger unsubscribed = new AtomicInteger();

    @Override
    public void subscribe(
        StreamId streamId, SseSubscriber subscriber, Consumer<Throwable> onDisconnect) {
      super.subscribe(streamId, subscriber, onDisconnect);
      subscribed.incrementAndGet();
    }

    @Override
    public void unsubscribe(StreamId streamId, SseSubscriber subscriber) {
      super.unsubscribe(streamId, subscriber);
      unsubscribed.incrementAndGet();
    }
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
      return SpringTestMocks::emptyEventStore;
    }

    @Bean
    DecidingAuthorizer sseAuthorizer() {
      return new DecidingAuthorizer();
    }

    @Bean
    CountingPublisher sseEventPublisher() {
      return new CountingPublisher();
    }
  }
}
