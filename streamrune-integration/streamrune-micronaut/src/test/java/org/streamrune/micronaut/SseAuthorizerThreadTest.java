package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.order.Ordered;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.annotation.RequestFilter;
import io.micronaut.http.annotation.ServerFilter;
import io.micronaut.http.filter.ServerFilterPhase;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import jakarta.inject.Singleton;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.test.InMemoryEventStore;

/**
 * Which thread the {@link SseAuthorizer} of the Server-Sent Events endpoint is called on.
 *
 * <p>The controller declares no executor of its own: it runs where the request filter chain leaves
 * the request. {@link StreamRuneContextFilter} runs on the blocking executor and proceeds
 * synchronously, so with the framework's filter the authorizer runs on a virtual thread of that
 * executor, inside the filter's binding of the request context, and may read a database. An
 * application that replaces the filter, or adds one behind it that runs on another executor,
 * decides the thread itself.
 *
 * <p>Each test boots the embedded server, opens {@code /api/sse/order/o-1} over HTTP and reads from
 * the fixture's authorizer the thread it was called on.
 */
class SseAuthorizerThreadTest {

  private static final String SPEC = "SseAuthorizerThreadTest";

  /** Replaces {@link StreamRuneContextFilter} by a filter that stays on the event loop. */
  private static final String EVENT_LOOP_FILTER = "sse-authorizer-thread-test.event-loop-filter";

  /** Adds a filter behind {@link StreamRuneContextFilter} that runs on another executor. */
  private static final String MOVING_FILTER = "sse-authorizer-thread-test.moving-filter";

  /** One call of the fixture's authorizer. */
  record Call(String thread, boolean virtual, boolean requestContextBound) {}

  private static final List<Call> CALLS = new CopyOnWriteArrayList<>();

  @BeforeEach
  void forgetEarlierCalls() {
    CALLS.clear();
  }

  @Test
  void withTheFrameworksRequestFilterTheAuthorizerRunsOnTheBlockingExecutorInTheRequestContext()
      throws Exception {
    openAStream(Map.of());

    assertThat(CALLS)
        .singleElement()
        .satisfies(
            call -> {
              assertThat(call.virtual())
                  .as("a virtual thread of the blocking executor, not '%s'", call.thread())
                  .isTrue();
              assertThat(call.requestContextBound())
                  .as("the authorizer runs inside the filter's binding of the request context")
                  .isTrue();
            });
  }

  /**
   * The limit of the statement above, as the documentation words it: the thread is the filter
   * chain's. A filter that stays on the event loop takes the controller, and the authorizer, there;
   * such an application puts its filter on the blocking executor or keeps its authorizer from
   * blocking.
   */
  @Test
  void withAFilterThatStaysOnTheEventLoopTheAuthorizerRunsOnTheEventLoop() throws Exception {
    openAStream(Map.of(EVENT_LOOP_FILTER, "true"));

    assertThat(CALLS)
        .singleElement()
        .satisfies(
            call -> {
              assertThat(call.virtual()).as("the thread '%s'", call.thread()).isFalse();
              assertThat(call.thread()).contains("eventLoop");
            });
  }

  /**
   * The same limit with the framework's filter in place: a filter the application adds behind it
   * that runs on an executor of its own takes the rest of the request, the controller and the
   * authorizer included, to that executor's thread. The framework filter bound the request context
   * on its own thread, and the binding does not follow.
   */
  @Test
  void withAFilterBehindTheFrameworksOnAnotherExecutorTheAuthorizerRunsThereOutsideTheContext()
      throws Exception {
    openAStream(Map.of(MOVING_FILTER, "true"));

    assertThat(CALLS)
        .singleElement()
        .satisfies(
            call -> {
              assertThat(call.thread())
                  .as("a thread of the executor the application's filter runs on")
                  .startsWith("io-executor");
              assertThat(call.requestContextBound())
                  .as("the framework filter's binding stays on the thread it was made on")
                  .isFalse();
            });
  }

  private static void openAStream(Map<String, Object> extra) throws Exception {
    Map<String, Object> props = new HashMap<>(extra);
    props.put("spec.name", SPEC);
    props.put("micronaut.server.port", "-1");
    props.put("micronaut.security.enabled", "false");
    props.put("streamrune.sse.enabled", "true");
    EmbeddedServer server = ApplicationContext.run(EmbeddedServer.class, props);
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try {
      HttpResponse<InputStream> response =
          client.send(
              java.net.http.HttpRequest.newBuilder(
                      URI.create("http://localhost:" + server.getPort() + "/api/sse/order/o-1"))
                  .header("Accept", "text/event-stream")
                  .timeout(Duration.ofSeconds(10))
                  .build(),
              HttpResponse.BodyHandlers.ofInputStream());
      assertThat(response.statusCode()).isEqualTo(200);
    } finally {
      client.shutdownNow();
      server.stop();
    }
  }

  /**
   * A filter the application adds behind {@link StreamRuneContextFilter} and runs on another
   * executor: Micronaut goes on from that executor's thread to the route.
   */
  @ServerFilter("/**")
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = MOVING_FILTER, value = "true")
  static class MovingFilter implements Ordered {

    @Override
    public int getOrder() {
      return ServerFilterPhase.SECURITY.after() + 100;
    }

    @RequestFilter
    @ExecuteOn(TaskExecutors.IO)
    void filter(HttpRequest<?> request) {
      // nothing: the request goes on from this executor's thread
    }
  }

  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class ApplicationFixture {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    SseAuthorizer sseAuthorizer() {
      return (principal, streamId) -> {
        Thread thread = Thread.currentThread();
        CALLS.add(
            new Call(thread.getName(), thread.isVirtual(), StreamRuneContext.CURRENT.isBound()));
        return true;
      };
    }
  }

  /**
   * What an application's own request filter can look like: no {@code @ExecuteOn}, so Micronaut
   * runs it, and the route after it, on the event loop the request arrived on.
   */
  @ServerFilter("/**")
  @Replaces(StreamRuneContextFilter.class)
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = EVENT_LOOP_FILTER, value = "true")
  static class EventLoopFilter {

    @RequestFilter
    void filter(HttpRequest<?> request) {
      // nothing: the request goes on where it is
    }
  }
}
