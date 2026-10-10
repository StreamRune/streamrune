package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.vertx.core.Context;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.AuthenticatedUserResolver;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/**
 * The thread the Server-Sent Events endpoint runs its blocking work on.
 *
 * <p>{@link SseController} is served by Quarkus REST on a Vert.x HTTP server ({@link
 * QuarkusRestTestServer}), so a request arrives on an event-loop thread as it does in a Quarkus
 * application. An {@link SseAuthorizer} that checks the ownership of a stream reads a database; so
 * may the identity resolution and the role resolution of the request filter. None of them may run
 * on the event loop.
 */
class SseEndpointThreadTest {

  private static final RequestIdentityPolicy ANONYMOUS = RequestIdentityPolicy.of(null, false);

  record OrderPlaced(String orderId) implements DomainEvent {}

  /** The thread one of the endpoint's collaborators was called on. */
  record CalledOn(String thread, boolean eventLoop) {

    static CalledOn currentThread() {
      return new CalledOn(Thread.currentThread().getName(), Context.isOnEventLoopThread());
    }
  }

  @Test
  void theAuthorizerRunsOnAWorkerThread_andAnAllowedCallerReceivesTheFrames() throws Exception {
    List<CalledOn> authorizerCalls = new CopyOnWriteArrayList<>();
    SseAuthorizer allowing =
        (principal, streamId) -> {
          authorizerCalls.add(CalledOn.currentThread());
          return true;
        };
    var publisher = new SseEventPublisher();
    var controller =
        new SseController(
            publisher, allowing, ANONYMOUS, Duration.ofMinutes(5), Duration.ofMillis(100));
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try (var server =
        QuarkusRestTestServer.start(controller, SseWireFormatTest.JacksonJsonWriter.class)) {
      HttpResponse<InputStream> response =
          SseWireFormatTest.open(client, server, "/api/sse/order/o-1");

      assertThat(response.statusCode()).isEqualTo(200);
      assertOffTheEventLoop(authorizerCalls, "the authorizer");
      List<List<String>> frames = new CopyOnWriteArrayList<>();
      Thread.ofVirtual().start(() -> SseWireFormatTest.collectFrames(response.body(), frames));
      await().atMost(Duration.ofSeconds(5)).until(() -> controller.openStreamCount() == 1);
      publisher.publish(SseWireFormatTest.envelope(7, new OrderPlaced("o-1")));
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> frames.stream().anyMatch(frame -> frame.contains("id:7")));
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  @Test
  void aRefusedCallerIsAnswered403_andTheAuthorizerRanOnAWorkerThread() throws Exception {
    List<CalledOn> authorizerCalls = new CopyOnWriteArrayList<>();
    SseAuthorizer refusing =
        (principal, streamId) -> {
          authorizerCalls.add(CalledOn.currentThread());
          return false;
        };
    var publisher = new SseEventPublisher();
    var controller = new SseController(publisher, refusing, ANONYMOUS);
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try (var server = QuarkusRestTestServer.start(controller)) {
      HttpResponse<String> response = get(client, server, "/api/sse/order/o-1");

      assertThat(response.statusCode()).isEqualTo(403);
      assertThat(response.headers().firstValue("Content-Type").orElse(""))
          .as("a refusal is an error response, not a stream that then fails")
          .doesNotContain("text/event-stream");
      assertOffTheEventLoop(authorizerCalls, "the authorizer");
      assertThat(controller.openStreamCount()).as("a refused caller is not subscribed").isZero();
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  @Test
  void anInvalidPathIsAnswered400_andTheAuthorizerIsNotConsulted() throws Exception {
    List<CalledOn> authorizerCalls = new CopyOnWriteArrayList<>();
    SseAuthorizer allowing =
        (principal, streamId) -> {
          authorizerCalls.add(CalledOn.currentThread());
          return true;
        };
    var publisher = new SseEventPublisher();
    var controller = new SseController(publisher, allowing, ANONYMOUS);
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try (var server = QuarkusRestTestServer.start(controller)) {
      // An aggregate type may not contain a space.
      HttpResponse<String> response = get(client, server, "/api/sse/not%20a%20type/o-1");

      assertThat(response.statusCode()).isEqualTo(400);
      assertThat(authorizerCalls).isEmpty();
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  /**
   * The request filter resolves the caller and the caller's roles before the resource method runs;
   * both are application code that may read a database. The authorizer then reads what the filter
   * stored. The holder here is one plain instance, not the request-scoped bean of an application:
   * this test shows the order and the threads, not the propagation of the CDI request scope.
   */
  @Test
  void theRequestFilterRunsOnAWorkerThread_andTheAuthorizerReadsTheContextItStored()
      throws Exception {
    List<CalledOn> principalCalls = new CopyOnWriteArrayList<>();
    List<CalledOn> roleCalls = new CopyOnWriteArrayList<>();
    AuthenticatedUserResolver principal =
        () -> {
          principalCalls.add(CalledOn.currentThread());
          return Optional.of(UserId.of("alice"));
        };
    UserRoleResolver roles =
        new UserRoleResolver() {
          @Override
          public UserAuthority resolve(UserId userId) {
            roleCalls.add(CalledOn.currentThread());
            return new UserAuthority(Set.of("support"), Set.of());
          }

          @Override
          public boolean requiresRequestContext() {
            return true;
          }
        };
    RequestIdentityPolicy identityPolicy = RequestIdentityPolicy.of(principal, false);
    var holder = new StreamRuneRequestContextHolder();
    var filter = new StreamRuneRequestFilter(holder, Set.of(), identityPolicy, roles);
    AtomicReference<StreamRuneContext.RequestContext> seenByTheAuthorizer = new AtomicReference<>();
    SseAuthorizer readingTheHolder =
        (caller, streamId) -> {
          seenByTheAuthorizer.set(holder.context());
          return false;
        };
    var publisher = new SseEventPublisher();
    var controller = new SseController(publisher, readingTheHolder, identityPolicy);
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try (var server = QuarkusRestTestServer.start(controller, List.of(filter))) {
      HttpResponse<String> response = get(client, server, "/api/sse/order/o-1");

      assertThat(response.statusCode()).isEqualTo(403);
      // Once by the filter, once by the endpoint.
      assertThat(principalCalls).hasSize(2);
      assertOffTheEventLoop(principalCalls, "the identity resolution");
      assertOffTheEventLoop(roleCalls, "the role resolution");
      assertThat(seenByTheAuthorizer.get()).as("the context the filter stored").isNotNull();
      assertThat(seenByTheAuthorizer.get().userId()).isEqualTo(UserId.of("alice"));
      assertThat(seenByTheAuthorizer.get().authority().hasRole("support")).isTrue();
    } finally {
      client.shutdownNow();
      controller.shutdown();
      publisher.close();
    }
  }

  private static void assertOffTheEventLoop(List<CalledOn> calls, String what) {
    assertThat(calls).as("%s was called", what).isNotEmpty();
    assertThat(calls)
        .as("%s runs on a worker thread, never on a Vert.x event loop: %s", what, calls)
        .allSatisfy(
            call -> {
              assertThat(call.eventLoop()).isFalse();
              assertThat(call.thread()).startsWith(QuarkusRestTestServer.WORKER_THREAD_PREFIX);
            });
  }

  private static HttpResponse<String> get(
      HttpClient client, QuarkusRestTestServer server, String path) throws Exception {
    return client.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
            .header("Accept", "text/event-stream")
            .timeout(Duration.ofSeconds(10))
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }
}
