package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SseEventPublisher;

/**
 * What the {@link SseAuthorizer} of the Server-Sent Events endpoint is handed: the request context
 * {@link StreamRuneRequestFilter} stored for the request, with the caller and the roles the
 * configured {@link UserRoleResolver} resolved for that caller at the request edge.
 *
 * <p>{@link SseController} and the request filter are served by Quarkus REST on a Vert.x HTTP
 * server ({@link QuarkusRestTestServer}) behind a trusted gateway ({@code X-User-Id} is the
 * caller), with a role resolver that knows one user with the role the authorizer of the fixture
 * asks for. The holder is one plain instance shared by the filter and the endpoint, not the
 * request-scoped bean of an application; {@code QuarkusRequestIdentityFailClosedTest} shows the
 * same hand-over through the request-scoped bean of a real Arc container.
 */
class SseAuthorizerCallerTest {

  private static final String SUPPORT = "SUPPORT";
  private static final RequestIdentityPolicy TRUSTED_GATEWAY = RequestIdentityPolicy.of(null, true);

  /** One call of the fixture's authorizer. */
  record Asked(UserId userId, UserAuthority authority, boolean isTheStoredContext) {}

  private final StreamRuneRequestContextHolder holder = new StreamRuneRequestContextHolder();
  private final List<Asked> asked = new CopyOnWriteArrayList<>();
  private final SseEventPublisher publisher = new SseEventPublisher();
  private final SseController controller =
      new SseController(
          publisher,
          roleGate(),
          TRUSTED_GATEWAY,
          holder,
          Duration.ofMinutes(5),
          Duration.ofSeconds(30));
  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  @AfterEach
  void release() {
    client.shutdownNow();
    controller.shutdown();
    publisher.close();
  }

  @Test
  void aCallerWithTheRoleIsAdmittedAndTheAuthorizerSeesTheRoleInTheRequestContext()
      throws Exception {
    try (var server = QuarkusRestTestServer.start(controller, List.of(requestFilter()))) {
      assertThat(statusOfAStreamRequestOf(server, "alice")).isEqualTo(200);

      assertThat(asked)
          .singleElement()
          .satisfies(
              call -> {
                assertThat(call.userId()).isEqualTo(UserId.of("alice"));
                assertThat(call.authority()).isNotNull();
                assertThat(call.authority().roles()).containsExactly(SUPPORT);
                assertThat(call.isTheStoredContext())
                    .as("the authorizer is handed the context the filter stored")
                    .isTrue();
              });
    }
  }

  @Test
  void aCallerWithoutTheRoleIsRefusedAndTheAuthorizerSeesNoRole() throws Exception {
    try (var server = QuarkusRestTestServer.start(controller, List.of(requestFilter()))) {
      assertThat(statusOfAStreamRequestOf(server, "bob")).isEqualTo(403);

      assertThat(asked)
          .singleElement()
          .satisfies(
              call -> {
                assertThat(call.userId()).isEqualTo(UserId.of("bob"));
                assertThat(call.authority()).isEqualTo(UserAuthority.EMPTY);
              });
    }
  }

  @Test
  void anUnauthenticatedCallerIsRefusedAndTheAuthorizerSeesNoUserAndNoAuthority() throws Exception {
    try (var server = QuarkusRestTestServer.start(controller, List.of(requestFilter()))) {
      assertThat(statusOfAStreamRequestOf(server, null)).isEqualTo(403);

      assertThat(asked)
          .singleElement()
          .satisfies(
              call -> {
                assertThat(call.userId()).isNull();
                assertThat(call.authority()).isNull();
              });
    }
  }

  @Test
  void aRequestNoFilterStoredAContextForIsRefusedAndTheAuthorizerIsNotCalled() throws Exception {
    // The application runs without the request filter: the holder stays empty.
    try (var server = QuarkusRestTestServer.start(controller)) {
      assertThat(statusOfAStreamRequestOf(server, "alice")).isEqualTo(403);

      assertThat(asked)
          .as("no request context is stored, so the authorizer is not asked")
          .isEmpty();
    }
  }

  /** The request filter of the application: the gateway's header is the caller. */
  private StreamRuneRequestFilter requestFilter() {
    return new StreamRuneRequestFilter(holder, Set.of(), TRUSTED_GATEWAY, new OneSupportUser());
  }

  /** Admits only a caller whose captured authority has the support role; records every call. */
  private SseAuthorizer roleGate() {
    return new SseAuthorizer() {
      @Override
      public boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId) {
        asked.add(new Asked(caller.userId(), caller.authority(), caller == holder.context()));
        return caller.authority() != null && caller.authority().hasRole(SUPPORT);
      }
    };
  }

  /** Opens the stream as {@code userId} ({@code null}: no {@code X-User-Id}) and closes it. */
  private int statusOfAStreamRequestOf(QuarkusRestTestServer server, String userId)
      throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(
                URI.create("http://localhost:" + server.port() + "/api/sse/order/o-1"))
            .header("Accept", "text/event-stream")
            .timeout(Duration.ofSeconds(10));
    if (userId != null) {
      request.header("X-User-Id", userId);
    }
    HttpResponse<InputStream> response =
        client.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
    response.body().close();
    return response.statusCode();
  }

  /**
   * Knows the roles of one user. It declares {@link #requiresRequestContext()}, as the resolver of
   * a security module does, so the request filter captures its answer at the request edge.
   */
  static final class OneSupportUser implements UserRoleResolver {

    @Override
    public UserAuthority resolve(UserId userId) {
      return "alice".equals(userId.value())
          ? new UserAuthority(Set.of(SUPPORT), Set.of())
          : UserAuthority.EMPTY;
    }

    @Override
    public boolean requiresRequestContext() {
      return true;
    }
  }
}
