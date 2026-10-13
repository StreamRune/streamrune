package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.quarkus.SagaOriginHttpFixture.Confirm;
import org.streamrune.quarkus.SagaOriginHttpFixture.Confirmed;
import org.streamrune.quarkus.SagaOriginHttpFixture.Finished;
import org.streamrune.quarkus.SagaOriginHttpFixture.FlowState;
import org.streamrune.quarkus.SagaOriginHttpFixture.OpenFlow;
import org.streamrune.quarkus.SagaOriginHttpFixture.Prepared;
import org.streamrune.quarkus.SagaOriginHttpFixture.Routing;

/**
 * A request through {@link StreamRuneRequestFilter} into a running saga, served by Quarkus REST
 * ({@link QuarkusRestTestServer}). The filter copies the client's {@code X-Correlation-Id} into the
 * request context, the resource binds that context around its command, and the command bus stores
 * the id in the event the command produces. A client that sends another saga's id there addresses
 * nothing: the runner routes a saga's replies by the origin the command bus wrote, and routes any
 * other event by the event's own data, or not at all.
 *
 * <p>The holder is one plain instance shared by the filter and the resource, not the request-scoped
 * bean of an application.
 */
class SagaOriginHttpRoutingTest {

  private static final String VICTIM = "victim";
  private static final String ATTACKER = "attacker";

  private final StreamRuneRequestContextHolder holder = new StreamRuneRequestContextHolder();
  private final HttpClient client =
      HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
  private SagaOriginHttpFixture fixture;

  @AfterEach
  void close() {
    client.shutdownNow();
    if (fixture != null) {
      fixture.close();
    }
  }

  /** The endpoint of the application: a caller confirms, or opens, a flow of its own. */
  @Path("/flows")
  public static final class FlowResource {
    private final SagaOriginHttpFixture fixture;
    private final StreamRuneRequestContextHolder requestContext;

    FlowResource(SagaOriginHttpFixture fixture, StreamRuneRequestContextHolder requestContext) {
      this.fixture = fixture;
      this.requestContext = requestContext;
    }

    @POST
    @Path("/{flowId}/confirm")
    public String confirm(@PathParam("flowId") String flowId) {
      StreamRuneRequestFilter.withContext(
          requestContext.context(), () -> fixture.execute(new Confirm(flowId)));
      return "ok";
    }

    @POST
    @Path("/{flowId}/open")
    public String open(@PathParam("flowId") String flowId) {
      StreamRuneRequestFilter.withContext(
          requestContext.context(), () -> fixture.execute(new OpenFlow(flowId)));
      return "ok";
    }
  }

  private QuarkusRestTestServer server() throws Exception {
    return QuarkusRestTestServer.start(
        new FlowResource(fixture, holder),
        List.of(
            new StreamRuneRequestFilter(
                holder, Set.of(), RequestIdentityPolicy.of(null, false), null)));
  }

  private int post(QuarkusRestTestServer server, String path, String correlationId)
      throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
            .header("X-Correlation-Id", correlationId)
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build();
    return client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode();
  }

  /** Two sagas at the step that waits for {@code Confirmed}. */
  private void victimAndAttackerWaitForConfirmed() {
    fixture.open(VICTIM);
    fixture.open(ATTACKER);
    assertThat(fixture.row(VICTIM).state().step()).isEqualTo("PREPARED");
    assertThat(fixture.row(ATTACKER).state().step()).isEqualTo("PREPARED");
  }

  /** The attacker confirms its own flow and names the victim's saga as the correlation id. */
  private void attackerConfirmsItsOwnFlowNamingTheVictimsSaga() throws Exception {
    try (var server = server()) {
      assertThat(
              post(
                  server,
                  "/flows/" + ATTACKER + "/confirm",
                  SagaOriginHttpFixture.sagaIdOf(VICTIM).value()))
          .isEqualTo(200);
    }
    fixture.deliver();
    assertThat(fixture.stored(Confirmed.class))
        .singleElement()
        .satisfies(
            confirmed -> {
              assertThat(confirmed.event()).isEqualTo(new Confirmed(ATTACKER));
              assertThat(confirmed.metadata().correlationId())
                  .as("the client's header is the stored correlation id")
                  .isEqualTo(CorrelationId.of(SagaOriginHttpFixture.sagaIdOf(VICTIM).value()));
              assertThat(confirmed.metadata().sagaOrigin()).isNull();
            });
  }

  private void assertUntouched(LoadedSaga<FlowState> before, String flowId) {
    LoadedSaga<FlowState> after = fixture.row(flowId);
    assertThat(after.version()).as("version of %s", flowId).isEqualTo(before.version());
    assertThat(after.lastAppliedOffset())
        .as("last applied offset of %s", flowId)
        .isEqualTo(before.lastAppliedOffset());
    assertThat(after).as("the whole row of %s", flowId).isEqualTo(before);
  }

  @Test
  void aBareSagaIdInXCorrelationId_doesNotReachTheVictim() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();
    LoadedSaga<FlowState> victim = fixture.row(VICTIM);
    LoadedSaga<FlowState> attacker = fixture.row(ATTACKER);

    attackerConfirmsItsOwnFlowNamingTheVictimsSaga();

    assertUntouched(victim, VICTIM);
    assertUntouched(attacker, ATTACKER);
    assertThat(fixture.stored(Finished.class)).isEmpty();
    assertThat(fixture.deadLetters.all()).isEmpty();
  }

  @Test
  void withAnEventDataHook_theEventReachesTheSagaItsOwnDataNames() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.EVENT_DATA_HOOK, false);
    victimAndAttackerWaitForConfirmed();
    LoadedSaga<FlowState> victim = fixture.row(VICTIM);

    attackerConfirmsItsOwnFlowNamingTheVictimsSaga();

    assertUntouched(victim, VICTIM);
    assertThat(fixture.row(ATTACKER).status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(fixture.stored(Finished.class))
        .extracting(EventEnvelope::event)
        .containsExactly(new Finished(ATTACKER));
    assertThat(fixture.deadLetters.all()).isEmpty();
  }

  @Test
  void theSagasOwnFlow_completesByOrigin() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, true);

    try (var server = server()) {
      assertThat(post(server, "/flows/own/open", "client-request-7")).isEqualTo(200);
    }
    fixture.deliver();

    assertThat(fixture.row("own").status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(fixture.row("own").state().step()).isEqualTo("FINISHED");
    for (var reply : List.of(Prepared.class, Confirmed.class, Finished.class)) {
      assertThat(fixture.stored(reply))
          .singleElement()
          .satisfies(
              stored ->
                  assertThat(stored.metadata().sagaOrigin())
                      .isEqualTo(SagaOriginHttpFixture.originOf("own")));
    }
    assertThat(fixture.deadLetters.all()).isEmpty();
  }
}
