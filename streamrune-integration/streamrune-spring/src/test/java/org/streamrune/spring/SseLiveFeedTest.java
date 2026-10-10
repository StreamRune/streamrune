package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.SseEventFeed;
import org.streamrune.test.InMemoryEventStore;

/**
 * With {@code streamrune.sse.enabled=true} the Server-Sent Events endpoint is fed by the
 * integration: a command executed through the bus reaches a connected client of the event's stream
 * with no application code publishing it.
 *
 * <p>Boots a real embedded Tomcat with the auto-configuration, connects a real HTTP client to
 * {@code /api/sse/order/o-1}, and executes commands through the {@link CommandBus} bean.
 */
class SseLiveFeedTest {

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final ObjectMapper JSON = new ObjectMapper();

  sealed interface OrderCommand extends Command permits OrderCommand.Place {
    record Place(String orderId, String note) implements OrderCommand {}
  }

  record OrderState() implements AggregateState {}

  sealed interface OrderEvent extends DomainEvent permits OrderEvent.Placed {
    record Placed(String note) implements OrderEvent {}
  }

  static final class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {
    @Override
    public OrderState initialState() {
      return new OrderState();
    }

    @Override
    public List<OrderEvent> decide(OrderCommand command, OrderState state) {
      return switch (command) {
        case OrderCommand.Place place -> List.of(new OrderEvent.Placed(place.note()));
      };
    }

    @Override
    public OrderState evolve(OrderState state, OrderEvent event) {
      return state;
    }
  }

  @Test
  void aCommandExecutedThroughTheBusReachesTheClientOfItsStreamAndNoOtherStreamsEventDoes()
      throws Exception {
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(SseApplication.class)
            .web(WebApplicationType.SERVLET)
            .registerShutdownHook(false)
            .properties(
                "server.port=0",
                "streamrune.sse.enabled=true",
                "streamrune.sse.polling-interval=50ms",
                // The response headers reach the client with the first frame; a short keepalive
                // makes that frame arrive at once.
                "streamrune.sse.keep-alive-interval=100ms")
            .run();
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    boolean closed = false;
    try {
      int port = ((WebServerApplicationContext) context).getWebServer().getPort();
      HttpResponse<InputStream> response =
          client.send(
              HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/sse/order/o-1"))
                  .header("Accept", "text/event-stream")
                  .build(),
              HttpResponse.BodyHandlers.ofInputStream());
      assertThat(response.statusCode()).isEqualTo(200);
      List<String> dataLines = new CopyOnWriteArrayList<>();
      Thread reader = Thread.ofVirtual().start(() -> collectDataLines(response.body(), dataLines));
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> context.getBean(SseController.class).activeCountForTest() == 1);

      CommandBus bus = context.getBean(CommandBus.class);
      // The other stream's event is stored first: were it routed to this client, it would be
      // written before the frame the test waits for.
      bus.execute(new OrderCommand.Place("o-2", "placed-on-o-2"));
      bus.execute(new OrderCommand.Place("o-1", "placed-on-o-1"));

      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> dataLines.stream().anyMatch(line -> line.contains("placed-on-o-1")));
      assertThat(dataLines)
          .as("only the events of order/o-1 stored after the start are written to its client")
          .hasSize(1)
          .noneMatch(line -> line.contains("placed-on-o-2"))
          .noneMatch(line -> line.contains("stored-before-the-start"));
      // The data field is the event as a JSON object, written by Spring's JSON message converter.
      assertThat(JSON.readTree(dataLines.getFirst().substring("data:".length())))
          .isEqualTo(JSON.readTree("{\"note\":\"placed-on-o-1\"}"));

      SseEventFeed feed = context.getBean(SseEventFeed.class);
      assertThat(feed.isRunning()).as("the feed runs while the application does").isTrue();
      context.close();
      closed = true;
      assertThat(feed.isRunning()).as("the feed stops with the application").isFalse();
      reader.join(Duration.ofSeconds(5));
      assertThat(reader.isAlive()).as("the client's stream ended").isFalse();
    } finally {
      if (!closed) {
        context.close();
      }
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
      // the stream was cut instead of completed; the test asserts on what was read
    }
  }

  /** The servlet web stack plus the StreamRune auto-configuration, on an in-memory event store. */
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
    AuditStore auditStore() {
      return mock(AuditStore.class);
    }

    /** Holds one event of {@code order/o-1} before the application starts. */
    @Bean
    EventStore eventStore() {
      InMemoryEventStore store = new InMemoryEventStore();
      StreamId stream = StreamId.of(ORDER, AggregateId.of("o-1"));
      store.append(
          stream,
          List.of(
              new EventEnvelope(
                  GlobalOffset.of(1),
                  stream,
                  new Version(1),
                  new EventType("Placed"),
                  new OrderEvent.Placed("stored-before-the-start"),
                  new EventMetadata(
                      EventId.of("evt-history"),
                      CommandId.of("cmd-history"),
                      null,
                      null,
                      CorrelationId.of("corr-history"),
                      null,
                      null,
                      Instant.now()))),
          Version.initial());
      return store;
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
      return (principal, streamId) -> true;
    }
  }
}
