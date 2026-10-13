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
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.quarkus.SagaOriginHttpFixture.Confirm;
import org.streamrune.quarkus.SagaOriginHttpFixture.Confirmed;
import org.streamrune.quarkus.SagaOriginHttpFixture.Finished;
import org.streamrune.quarkus.SagaOriginHttpFixture.FlowState;
import org.streamrune.quarkus.SagaOriginHttpFixture.OpenFlow;
import org.streamrune.quarkus.SagaOriginHttpFixture.Prepared;
import org.streamrune.quarkus.SagaOriginHttpFixture.Routing;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;

/**
 * A request through {@link StreamRuneRequestFilter} into a running saga, served by Quarkus REST
 * ({@link QuarkusRestTestServer}). The filter copies the client's {@code X-Correlation-Id} into the
 * request context, the resource binds that context around its command, and the command bus stores
 * the id in the event the command produces. A client that sends another saga's id there addresses
 * nothing: the runner routes a saga's replies by the origin the command bus wrote, and routes any
 * other event by the event's own data, or not at all.
 *
 * <p>The correlation id saga dispatch binds has a reserved form, {@code streamrune:saga:<sagaId>}.
 * The filter answers {@code 400 Bad Request} to a request that sends a correlation id with the
 * reserved prefix, as it does to an id that is too long or holds a control character, before the
 * resource runs; every other correlation id is stored as the client sent it.
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
    return post(server, path, Map.of("X-Correlation-Id", correlationId)).statusCode();
  }

  private HttpResponse<String> post(
      QuarkusRestTestServer server, String path, Map<String, String> headers) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + path))
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.noBody());
    headers.forEach(request::header);
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
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

  /**
   * A victim whose saga is {@code FAULTED} and shielded, with one dead-letter entry of its own. The
   * caller's event is not filed as an entry of the victim and does not touch its row, and the drain
   * of the victim feeds the victim's own entry and nothing of the caller's.
   */
  private void aFaultedShieldedVictimKeepsItsRowAndItsOneEntry(Routing routing) throws Exception {
    fixture = new SagaOriginHttpFixture(routing, false);
    fixture.poisonPreparedOf(VICTIM);
    fixture.open(VICTIM);
    fixture.open(ATTACKER);
    LoadedSaga<FlowState> victim = fixture.row(VICTIM);
    assertThat(victim.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(victim.deadLetterPending()).isTrue();
    var entries = List.copyOf(fixture.deadLetters.all());
    assertThat(entries)
        .singleElement()
        .satisfies(
            entry -> assertThat(entry.sagaId()).isEqualTo(SagaOriginHttpFixture.sagaIdOf(VICTIM)));

    attackerConfirmsItsOwnFlowNamingTheVictimsSaga();

    assertUntouched(victim, VICTIM);
    assertThat(fixture.deadLetters.all())
        .as("the caller's event is filed under no saga, the victim's least of all")
        .isEqualTo(entries);
    if (routing == Routing.EVENT_DATA_HOOK) {
      assertThat(fixture.row(ATTACKER).status()).isEqualTo(SagaStatus.COMPLETED);
    } else {
      assertThat(fixture.row(ATTACKER).state().step()).isEqualTo("PREPARED");
    }

    fixture.poisonPreparedOf(null);
    assertThat(fixture.replayAll(VICTIM)).containsExactly(ReplayOutcome.REPLAYED);

    assertThat(fixture.row(VICTIM).state())
        .as("the drain fed the victim its own Prepared and no Confirmed")
        .isEqualTo(new FlowState(SagaStatus.RUNNING, VICTIM, "PREPARED"));
    assertThat(fixture.deadLetters.all()).isEmpty();
    assertThat(fixture.stored(Finished.class))
        .extracting(EventEnvelope::event)
        .doesNotContain(new Finished(VICTIM));
  }

  @Test
  void aFaultedShieldedVictim_isNotReachedThroughItsDeadLetterEntries() throws Exception {
    aFaultedShieldedVictimKeepsItsRowAndItsOneEntry(Routing.BUILT_IN);
  }

  @Test
  void aFaultedShieldedVictim_isNotReachedThroughItsDeadLetterEntries_withAnEventDataHook()
      throws Exception {
    aFaultedShieldedVictimKeepsItsRowAndItsOneEntry(Routing.EVENT_DATA_HOOK);
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

  // ==== What the request edge refuses, and what it keeps ====

  /** The correlation id saga dispatch binds for the saga of a flow. */
  private static String reservedCorrelationIdOf(String flowId) {
    return SagaOriginHttpFixture.originOf(flowId).correlationId().value();
  }

  /**
   * The attacker's request, carrying {@code header}, is answered {@code 400} with the shared reason
   * as its body, and nothing of it reaches the application: the resource is not called, no context
   * is published, no event is stored and neither saga row changes.
   */
  private void assertRefused(String header, String value) throws Exception {
    List<EventEnvelope> storedBefore = fixture.stored();
    LoadedSaga<FlowState> victim = fixture.row(VICTIM);
    LoadedSaga<FlowState> attacker = fixture.row(ATTACKER);
    int callsBefore = fixture.endpointCalls();

    HttpResponse<String> response;
    try (var server = server()) {
      response = post(server, "/flows/" + ATTACKER + "/confirm", Map.of(header, value));
    }
    fixture.deliver();

    assertThat(response.statusCode()).as("status for a refused %s", header).isEqualTo(400);
    assertThat(response.body()).isEqualTo(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
    assertThat(response.headers().firstValue("Content-Type").orElse("")).startsWith("text/plain");
    assertThat(fixture.endpointCalls()).as("the resource is not called").isEqualTo(callsBefore);
    assertThat(holder.context()).as("no request context is published").isNull();
    assertThat(fixture.stored()).as("nothing is stored").isEqualTo(storedBefore);
    assertThat(fixture.row(VICTIM)).as("the victim's row").isEqualTo(victim);
    assertThat(fixture.row(ATTACKER)).as("the attacker's row").isEqualTo(attacker);
    assertThat(fixture.deadLetters.all()).isEmpty();
  }

  private void theReservedFormOfTheVictimsSagaIdIsAnswered400(Routing routing) throws Exception {
    fixture = new SagaOriginHttpFixture(routing, false);
    victimAndAttackerWaitForConfirmed();
    String reserved = reservedCorrelationIdOf(VICTIM);
    assertThat(reserved).isEqualTo("streamrune:saga:flow-saga-victim");
    // What a service that reads the saga's events and forwards their correlation id would send.
    assertThat(fixture.stored(Prepared.class))
        .filteredOn(prepared -> prepared.event().equals(new Prepared(VICTIM)))
        .singleElement()
        .satisfies(
            prepared ->
                assertThat(prepared.metadata().correlationId().value()).isEqualTo(reserved));

    assertRefused("X-Correlation-Id", reserved);
  }

  @Test
  void theReservedFormOfTheVictimsSagaId_isAnswered400() throws Exception {
    theReservedFormOfTheVictimsSagaIdIsAnswered400(Routing.BUILT_IN);
  }

  @Test
  void theReservedFormOfTheVictimsSagaId_isAnswered400_withAnEventDataHook() throws Exception {
    theReservedFormOfTheVictimsSagaIdIsAnswered400(Routing.EVENT_DATA_HOOK);
  }

  @Test
  void anyCorrelationIdWithTheReservedPrefix_isAnswered400() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();

    for (String reserved :
        List.of(
            IdConstraints.RESERVED_PREFIX,
            "streamrune:saga:",
            "streamrune:saga:no-such-saga",
            "streamrune:anything-else")) {
      assertRefused("X-Correlation-Id", reserved);
    }
  }

  @Test
  void a256CharacterCorrelationId_isAnswered400() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();

    assertRefused("X-Correlation-Id", "a".repeat(256));
  }

  @Test
  void a256CharacterTraceId_isAnswered400() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();

    assertRefused("X-Trace-Id", "a".repeat(256));
  }

  @Test
  void aControlCharacterInTheCorrelationId_isAnswered400() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();

    // A horizontal tab is the control character an HTTP header value may carry.
    assertRefused("X-Correlation-Id", "corr\tid");
  }

  @Test
  void aControlCharacterInTheTraceId_isAnswered400() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();

    assertRefused("X-Trace-Id", "trace\tid");
  }

  /**
   * A correlation id outside the reserved form is the caller's own tracing data and is stored as
   * sent, whatever it resembles: the longest accepted value, a bare saga id, the reserved word in
   * another case or without its colon, the reserved form further inside the value.
   */
  @Test
  void anOrdinaryCorrelationId_isKeptVerbatimOnTheStoredEvent() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();
    LoadedSaga<FlowState> victim = fixture.row(VICTIM);
    List<String> ordinary =
        List.of(
            "upstream-request-7",
            "a".repeat(255),
            SagaOriginHttpFixture.sagaIdOf(VICTIM).value(),
            "saga:flow-saga-victim",
            "Streamrune:saga:flow-saga-victim",
            "STREAMRUNE:saga:flow-saga-victim",
            "streamrune",
            "streamrune-saga-flow-saga-victim",
            "upstream/streamrune:saga:flow-saga-victim");

    try (var server = server()) {
      for (int i = 0; i < ordinary.size(); i++) {
        String flow = "caller-" + i;
        String sent = ordinary.get(i);
        assertThat(post(server, "/flows/" + flow + "/confirm", sent)).isEqualTo(200);
        fixture.deliver();

        assertThat(fixture.stored(Confirmed.class))
            .filteredOn(confirmed -> confirmed.event().equals(new Confirmed(flow)))
            .singleElement()
            .satisfies(
                confirmed -> {
                  assertThat(confirmed.metadata().correlationId().value()).isEqualTo(sent);
                  assertThat(confirmed.metadata().sagaOrigin()).isNull();
                });
      }
    }
    assertUntouched(victim, VICTIM);
  }

  @Test
  void withoutACorrelationIdHeader_oneIsGenerated() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);

    try (var server = server()) {
      assertThat(post(server, "/flows/first/confirm", Map.of()).statusCode()).isEqualTo(200);
      assertThat(post(server, "/flows/second/confirm", Map.of()).statusCode()).isEqualTo(200);
    }

    List<String> generated =
        fixture.stored(Confirmed.class).stream()
            .map(confirmed -> confirmed.metadata().correlationId().value())
            .toList();
    assertThat(generated).hasSize(2).doesNotHaveDuplicates();
    assertThat(generated)
        .allSatisfy(
            id -> {
              assertThat(id).isNotBlank();
              assertThat(IdConstraints.isReserved(id)).isFalse();
            });
  }
}
