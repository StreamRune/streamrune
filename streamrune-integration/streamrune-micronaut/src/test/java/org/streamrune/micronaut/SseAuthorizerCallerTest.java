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
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.filters.AuthenticationFetcher;
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
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.test.InMemoryEventStore;
import reactor.core.publisher.Mono;

/**
 * What the {@link SseAuthorizer} of the Server-Sent Events endpoint is handed: the request context
 * {@link StreamRuneContextFilter} bound for the request, with the caller and the roles the
 * configured {@code UserRoleResolver} resolved for that caller at the request edge.
 *
 * <p>Each test boots the embedded server with Micronaut Security enabled, so the caller is the
 * authenticated principal ({@link MicronautAuthenticatedUserResolver}) and the roles are the ones
 * Micronaut Security holds for it ({@link MicronautSecurityUserRoleResolver}), and opens {@code
 * /api/sse/order/o-1} over HTTP.
 */
class SseAuthorizerCallerTest {

  private static final String SPEC = "SseAuthorizerCallerTest";
  private static final String SUPPORT = "SUPPORT";

  /** Replaces {@link StreamRuneContextFilter} by a filter that stores no request context. */
  private static final String NO_CONTEXT_FILTER = "sse-authorizer-caller-test.no-context-filter";

  /** Adds a filter behind {@link StreamRuneContextFilter} that runs on the I/O executor. */
  private static final String OTHER_EXECUTOR_FILTER =
      "sse-authorizer-caller-test.other-executor-filter";

  /** One call of the fixture's authorizer. */
  record Asked(UserId userId, UserAuthority authority, boolean scopedValueBound) {}

  private static final List<Asked> ASKED = new CopyOnWriteArrayList<>();

  @BeforeEach
  void forgetEarlierCalls() {
    ASKED.clear();
  }

  @Test
  void aCallerWithTheRoleIsAdmittedAndTheAuthorizerSeesTheRoleInTheRequestContext()
      throws Exception {
    assertThat(statusOfAStreamRequestOf("alice", Map.of())).isEqualTo(200);

    assertThat(ASKED)
        .singleElement()
        .satisfies(
            asked -> {
              assertThat(asked.userId()).isEqualTo(UserId.of("alice"));
              assertThat(asked.authority()).isNotNull();
              assertThat(asked.authority().roles()).containsExactly(SUPPORT);
            });
  }

  @Test
  void aCallerWithoutTheRoleIsRefusedAndTheAuthorizerSeesNoRole() throws Exception {
    assertThat(statusOfAStreamRequestOf("bob", Map.of())).isEqualTo(403);

    assertThat(ASKED)
        .singleElement()
        .satisfies(
            asked -> {
              assertThat(asked.userId()).isEqualTo(UserId.of("bob"));
              assertThat(asked.authority()).isEqualTo(UserAuthority.EMPTY);
            });
  }

  @Test
  void anUnauthenticatedCallerIsRefusedAndTheAuthorizerSeesNoUserAndNoAuthority() throws Exception {
    assertThat(statusOfAStreamRequestOf(null, Map.of())).isEqualTo(403);

    assertThat(ASKED)
        .singleElement()
        .satisfies(
            asked -> {
              assertThat(asked.userId()).isNull();
              assertThat(asked.authority()).isNull();
            });
  }

  @Test
  void aRequestOfAFilterThatStoresNoRequestContextIsRefusedAndTheAuthorizerIsNotCalled()
      throws Exception {
    assertThat(statusOfAStreamRequestOf("alice", Map.of(NO_CONTEXT_FILTER, "true"))).isEqualTo(403);

    assertThat(ASKED).as("the request has no context, so the authorizer is not asked").isEmpty();
  }

  /**
   * A filter behind the framework's that runs on another executor can take the controller to a
   * thread {@link StreamRuneContext#CURRENT} is not bound on; which thread a request goes on from
   * depends on timing inside Micronaut. The request carries its context to either thread, so the
   * caller is admitted every time.
   */
  @RepeatedTest(10)
  void behindAFilterOnAnotherExecutorTheAuthorizerIsStillHandedTheRequestContext()
      throws Exception {
    assertThat(statusOfAStreamRequestOf("alice", Map.of(OTHER_EXECUTOR_FILTER, "true")))
        .isEqualTo(200);

    assertThat(ASKED)
        .singleElement()
        .satisfies(
            asked -> {
              assertThat(asked.userId()).isEqualTo(UserId.of("alice"));
              assertThat(asked.authority().roles()).containsExactly(SUPPORT);
            });
  }

  /**
   * Boots the application, opens the stream as {@code authenticatedAs} ({@code null}: an
   * unauthenticated request) and returns the status of the response.
   */
  private static int statusOfAStreamRequestOf(String authenticatedAs, Map<String, Object> extra)
      throws Exception {
    Map<String, Object> props = new HashMap<>(extra);
    props.put("spec.name", SPEC);
    props.put("micronaut.server.port", "-1");
    props.put("micronaut.security.enabled", "true");
    // Every route is open to anonymous callers: the SseAuthorizer, not route security, decides.
    props.put(
        "micronaut.security.intercept-url-map",
        List.of(Map.of("pattern", "/**", "access", List.of("isAnonymous()"))));
    props.put("streamrune.sse.enabled", "true");
    EmbeddedServer server = ApplicationContext.run(EmbeddedServer.class, props);
    HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    try {
      java.net.http.HttpRequest.Builder request =
          java.net.http.HttpRequest.newBuilder(
                  URI.create("http://localhost:" + server.getPort() + "/api/sse/order/o-1"))
              .header("Accept", "text/event-stream")
              .timeout(Duration.ofSeconds(10));
      if (authenticatedAs != null) {
        request.header(HeaderAuthenticationFixture.HEADER, authenticatedAs);
      }
      HttpResponse<InputStream> response =
          client.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
      return response.statusCode();
    } finally {
      client.shutdownNow();
      server.stop();
    }
  }

  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class ApplicationFixture {

    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    /** Admits only a caller whose captured authority has the support role; records every call. */
    @Singleton
    SseAuthorizer sseAuthorizer() {
      return (caller, streamId) -> {
        ASKED.add(
            new Asked(caller.userId(), caller.authority(), StreamRuneContext.CURRENT.isBound()));
        return caller.authority() != null && caller.authority().hasRole(SUPPORT);
      };
    }
  }

  /**
   * Real Micronaut Security authentication: a request carrying {@value #HEADER} is authenticated as
   * that name; {@code alice} has the support role and nobody else has a role.
   */
  @Singleton
  @Requires(property = "spec.name", value = SPEC)
  static class HeaderAuthenticationFixture implements AuthenticationFetcher<HttpRequest<?>> {
    static final String HEADER = "X-Test-Authenticated-As";

    @Override
    public Publisher<Authentication> fetchAuthentication(HttpRequest<?> request) {
      String name = request.getHeaders().get(HEADER);
      if (name == null) {
        return Mono.empty();
      }
      return Mono.just(
          Authentication.build(name, "alice".equals(name) ? List.of(SUPPORT) : List.of()));
    }
  }

  /** An application's own request filter that stores no request context. */
  @ServerFilter("/**")
  @Replaces(StreamRuneContextFilter.class)
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = NO_CONTEXT_FILTER, value = "true")
  static class NoContextFilter {

    @RequestFilter
    @ExecuteOn(TaskExecutors.BLOCKING)
    void filter(HttpRequest<?> request) {
      // nothing: the request goes on with no request context
    }
  }

  /** An application's filter behind the framework's, on an executor of its own. */
  @ServerFilter("/**")
  @Requires(property = "spec.name", value = SPEC)
  @Requires(property = OTHER_EXECUTOR_FILTER, value = "true")
  static class OtherExecutorFilter implements Ordered {

    @Override
    public int getOrder() {
      return ServerFilterPhase.SECURITY.after() + 1;
    }

    @RequestFilter
    @ExecuteOn(TaskExecutors.IO)
    void filter(HttpRequest<?> request) {
      // nothing: the request goes on from the I/O executor's thread
    }
  }
}
