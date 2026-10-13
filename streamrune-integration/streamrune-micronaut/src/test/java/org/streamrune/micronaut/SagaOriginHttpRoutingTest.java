package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MutableHttpRequest;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.micronaut.SagaOriginHttpFixture.Confirm;
import org.streamrune.micronaut.SagaOriginHttpFixture.Confirmed;
import org.streamrune.micronaut.SagaOriginHttpFixture.Finished;
import org.streamrune.micronaut.SagaOriginHttpFixture.FlowState;
import org.streamrune.micronaut.SagaOriginHttpFixture.OpenFlow;
import org.streamrune.micronaut.SagaOriginHttpFixture.Prepared;
import org.streamrune.micronaut.SagaOriginHttpFixture.Routing;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.test.InMemoryEventStore;

/**
 * A request through {@link StreamRuneContextFilter} into a running saga, on the embedded server.
 * The filter copies the client's {@code X-Correlation-Id} into the request context it binds around
 * the controller, and the command bus stores the id in the event the controller's command produces.
 * A client that sends another saga's id there addresses nothing: the runner routes a saga's replies
 * by the origin the command bus wrote, and routes any other event by the event's own data, or not
 * at all.
 *
 * <p>The correlation id saga dispatch binds has a reserved form, {@code streamrune:saga:<sagaId>}.
 * The filter answers {@code 400 Bad Request} to a request that sends a correlation id with the
 * reserved prefix, as it does to an id that is too long or holds a control character, before the
 * controller runs; every other correlation id is stored as the client sent it.
 */
@MicronautTest
@Property(name = "spec.name", value = "SagaOriginHttpRoutingTest")
@Property(name = "micronaut.security.enabled", value = "false")
class SagaOriginHttpRoutingTest {

  private static final String VICTIM = "victim";
  private static final String ATTACKER = "attacker";

  @Inject
  @Client("/")
  HttpClient client;

  @Inject FixtureSlot slot;

  @Inject EmbeddedServer server;

  @Inject RequestIdentityPolicy identityPolicy;

  private SagaOriginHttpFixture fixture;

  @AfterEach
  void close() {
    slot.fixture = null;
    if (fixture != null) {
      fixture.close();
    }
  }

  /** The saga and stores the controller of the running application serves in this test. */
  @Singleton
  @Requires(property = "spec.name", value = "SagaOriginHttpRoutingTest")
  static class FixtureSlot {
    volatile SagaOriginHttpFixture fixture;
  }

  /** The endpoint of the application: a caller confirms, or opens, a flow of its own. */
  @Controller("/flows")
  @Requires(property = "spec.name", value = "SagaOriginHttpRoutingTest")
  static class FlowController {
    private final FixtureSlot slot;

    FlowController(FixtureSlot slot) {
      this.slot = slot;
    }

    @Post("/{flowId}/confirm")
    String confirm(@PathVariable String flowId) {
      slot.fixture.execute(new Confirm(flowId));
      return "ok";
    }

    @Post("/{flowId}/open")
    String open(@PathVariable String flowId) {
      slot.fixture.execute(new OpenFlow(flowId));
      return "ok";
    }
  }

  /** The event store the module's own beans are built on; the saga under test has its own. */
  @Factory
  @Requires(property = "spec.name", value = "SagaOriginHttpRoutingTest")
  static class ModuleBeans {
    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }
  }

  private void use(SagaOriginHttpFixture created) {
    fixture = created;
    slot.fixture = created;
  }

  private int post(String path, String correlationId) {
    return post(path, Map.of("X-Correlation-Id", correlationId)).code();
  }

  /** The response to a request with {@code headers}, whatever its status. */
  private HttpResponse<String> post(String path, Map<String, String> headers) {
    MutableHttpRequest<String> request = HttpRequest.POST(path, "");
    headers.forEach(request::header);
    try {
      return client.toBlocking().exchange(request, String.class);
    } catch (HttpClientResponseException answered) {
      @SuppressWarnings("unchecked")
      HttpResponse<String> response = (HttpResponse<String>) answered.getResponse();
      return response;
    }
  }

  /** Two sagas at the step that waits for {@code Confirmed}. */
  private void victimAndAttackerWaitForConfirmed() {
    fixture.open(VICTIM);
    fixture.open(ATTACKER);
    assertThat(fixture.row(VICTIM).state().step()).isEqualTo("PREPARED");
    assertThat(fixture.row(ATTACKER).state().step()).isEqualTo("PREPARED");
  }

  /** The attacker confirms its own flow and names the victim's saga as the correlation id. */
  private void attackerConfirmsItsOwnFlowNamingTheVictimsSaga() {
    assertThat(
            post("/flows/" + ATTACKER + "/confirm", SagaOriginHttpFixture.sagaIdOf(VICTIM).value()))
        .isEqualTo(200);
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
  void aBareSagaIdInXCorrelationId_doesNotReachTheVictim() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
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
  void withAnEventDataHook_theEventReachesTheSagaItsOwnDataNames() {
    use(new SagaOriginHttpFixture(Routing.EVENT_DATA_HOOK, false));
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
  private void aFaultedShieldedVictimKeepsItsRowAndItsOneEntry(Routing routing) {
    use(new SagaOriginHttpFixture(routing, false));
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
  void aFaultedShieldedVictim_isNotReachedThroughItsDeadLetterEntries() {
    aFaultedShieldedVictimKeepsItsRowAndItsOneEntry(Routing.BUILT_IN);
  }

  @Test
  void aFaultedShieldedVictim_isNotReachedThroughItsDeadLetterEntries_withAnEventDataHook() {
    aFaultedShieldedVictimKeepsItsRowAndItsOneEntry(Routing.EVENT_DATA_HOOK);
  }

  @Test
  void theSagasOwnFlow_completesByOrigin() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, true));

    assertThat(post("/flows/own/open", "client-request-7")).isEqualTo(200);
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
   * as its body, and nothing of it reaches the application: the controller is not called, no event
   * is stored and neither saga row changes.
   */
  private void assertRefused(String header, String value) {
    List<EventEnvelope> storedBefore = fixture.stored();
    LoadedSaga<FlowState> victim = fixture.row(VICTIM);
    LoadedSaga<FlowState> attacker = fixture.row(ATTACKER);
    int callsBefore = fixture.endpointCalls();

    HttpResponse<String> response = post("/flows/" + ATTACKER + "/confirm", Map.of(header, value));
    fixture.deliver();

    assertThat(response.code()).as("status for a refused %s", header).isEqualTo(400);
    assertThat(response.getBody(String.class).orElse(""))
        .isEqualTo(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
    assertThat(response.getHeaders().get("Content-Type").replace(" ", ""))
        .isEqualToIgnoringCase("text/plain;charset=UTF-8");
    assertThat(fixture.endpointCalls()).as("the controller is not called").isEqualTo(callsBefore);
    assertThat(fixture.stored()).as("nothing is stored").isEqualTo(storedBefore);
    assertThat(fixture.row(VICTIM)).as("the victim's row").isEqualTo(victim);
    assertThat(fixture.row(ATTACKER)).as("the attacker's row").isEqualTo(attacker);
    assertThat(fixture.deadLetters.all()).isEmpty();
  }

  private void theReservedFormOfTheVictimsSagaIdIsAnswered400(Routing routing) {
    use(new SagaOriginHttpFixture(routing, false));
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
  void theReservedFormOfTheVictimsSagaId_isAnswered400() {
    theReservedFormOfTheVictimsSagaIdIsAnswered400(Routing.BUILT_IN);
  }

  @Test
  void theReservedFormOfTheVictimsSagaId_isAnswered400_withAnEventDataHook() {
    theReservedFormOfTheVictimsSagaIdIsAnswered400(Routing.EVENT_DATA_HOOK);
  }

  @Test
  void anyCorrelationIdWithTheReservedPrefix_isAnswered400() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
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
  void a256CharacterCorrelationId_isAnswered400() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    victimAndAttackerWaitForConfirmed();

    assertRefused("X-Correlation-Id", "a".repeat(256));
  }

  @Test
  void a256CharacterTraceId_isAnswered400() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    victimAndAttackerWaitForConfirmed();

    assertRefused("X-Trace-Id", "a".repeat(256));
  }

  @Test
  void aControlCharacterInTheCorrelationId_isAnswered400() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    victimAndAttackerWaitForConfirmed();

    // A horizontal tab is the control character an HTTP header value may carry.
    assertRefused("X-Correlation-Id", "corr\tid");
  }

  @Test
  void aControlCharacterInTheTraceId_isAnswered400() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    victimAndAttackerWaitForConfirmed();

    assertRefused("X-Trace-Id", "trace\tid");
  }

  /**
   * A correlation id outside the reserved form is the caller's own tracing data and is stored as
   * sent, whatever it resembles: the longest accepted value, a bare saga id, the reserved word in
   * another case or without its colon, the reserved form further inside the value.
   */
  @Test
  void anOrdinaryCorrelationId_isKeptVerbatimOnTheStoredEvent() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
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

    for (int i = 0; i < ordinary.size(); i++) {
      String flow = "caller-" + i;
      String sent = ordinary.get(i);
      assertThat(post("/flows/" + flow + "/confirm", sent)).isEqualTo(200);
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
    assertUntouched(victim, VICTIM);
  }

  @Test
  void withoutACorrelationIdHeader_oneIsGenerated() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));

    assertThat(post("/flows/first/confirm", Map.of()).code()).isEqualTo(200);
    assertThat(post("/flows/second/confirm", Map.of()).code()).isEqualTo(200);

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

  // ==== A header sent more than once ====

  private EventEnvelope confirmedOf(String flow) {
    return fixture.stored(Confirmed.class).stream()
        .filter(confirmed -> confirmed.event().equals(new Confirmed(flow)))
        .findFirst()
        .orElseThrow();
  }

  private boolean nothingConfirmedFor(String flow) {
    return fixture.stored(Confirmed.class).stream()
        .noneMatch(confirmed -> confirmed.event().equals(new Confirmed(flow)));
  }

  @Test
  void ofARepeatedCorrelationIdHeader_theFirstValueIsTaken() throws Exception {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    String reserved = reservedCorrelationIdOf(VICTIM);

    {
      RawHttp.Answer okFirst =
          RawHttp.post(
              server.getPort(),
              "/flows/first/confirm",
              List.of("X-Correlation-Id: ok", "X-Correlation-Id: " + reserved));
      assertThat(okFirst.status()).isEqualTo(200);
      assertThat(confirmedOf("first").metadata().correlationId().value()).isEqualTo("ok");

      RawHttp.Answer reservedFirst =
          RawHttp.post(
              server.getPort(),
              "/flows/second/confirm",
              List.of("X-Correlation-Id: " + reserved, "X-Correlation-Id: ok"));
      assertThat(reservedFirst.status()).isEqualTo(400);
      assertThat(reservedFirst.body()).isEqualTo(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
      assertThat(reservedFirst.contentType()).isEqualTo("text/plain;charset=utf-8");
      assertThat(nothingConfirmedFor("second")).isTrue();
    }
  }

  @Test
  void anEmptyFirstCorrelationIdHeader_isAnAbsentHeader_whateverFollows() throws Exception {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));

    {
      RawHttp.Answer answer =
          RawHttp.post(
              server.getPort(),
              "/flows/first/confirm",
              List.of("X-Correlation-Id:", "X-Correlation-Id: " + reservedCorrelationIdOf(VICTIM)));
      assertThat(answer.status()).isEqualTo(200);
    }

    assertThat(confirmedOf("first").metadata().correlationId().value())
        .as("a generated id")
        .isNotBlank()
        .doesNotContain("streamrune:");
  }

  @Test
  void aCommaJoinedCorrelationId_isOneValue() throws Exception {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    String reserved = reservedCorrelationIdOf(VICTIM);

    {
      assertThat(
              RawHttp.post(
                      server.getPort(),
                      "/flows/first/confirm",
                      List.of("X-Correlation-Id: ok," + reserved))
                  .status())
          .isEqualTo(200);
      assertThat(
              RawHttp.post(
                      server.getPort(),
                      "/flows/second/confirm",
                      List.of("X-Correlation-Id: " + reserved + ",ok"))
                  .status())
          .isEqualTo(400);
    }

    assertThat(confirmedOf("first").metadata().correlationId().value()).isEqualTo("ok," + reserved);
    assertThat(nothingConfirmedFor("second")).isTrue();
  }

  @Test
  void ofARepeatedTraceIdHeader_theFirstValueIsTaken() throws Exception {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    String overlong = "a".repeat(256);

    {
      assertThat(
              RawHttp.post(
                      server.getPort(),
                      "/flows/first/confirm",
                      List.of("X-Trace-Id: trace-1", "X-Trace-Id: " + overlong))
                  .status())
          .isEqualTo(200);
      assertThat(
              RawHttp.post(
                      server.getPort(),
                      "/flows/second/confirm",
                      List.of("X-Trace-Id: " + overlong, "X-Trace-Id: trace-1"))
                  .status())
          .isEqualTo(400);
    }

    assertThat(confirmedOf("first").metadata().traceId().value()).isEqualTo("trace-1");
    assertThat(nothingConfirmedFor("second")).isTrue();
  }

  // ==== The refusals in the identity mode of this application ====

  /**
   * An over-long {@code X-User-Id}: refused where the request supplies the user, behind a trusted
   * gateway, and ignored in the two modes that do not read the header. {@link
   * SagaOriginHttpRoutingTrustedGatewayTest} and {@link
   * SagaOriginHttpRoutingAuthenticatedPrincipalTest} run this class in the other two modes.
   */
  @Test
  void anOverlongUserIdHeader_isRefusedOnlyWhereTheRequestSuppliesTheUser() {
    use(new SagaOriginHttpFixture(Routing.BUILT_IN, false));
    victimAndAttackerWaitForConfirmed();

    if (identityPolicy.mode() == RequestIdentityPolicy.Mode.TRUSTED_GATEWAY) {
      assertRefused("X-User-Id", "u".repeat(256));
    } else {
      assertThat(post("/flows/caller/confirm", Map.of("X-User-Id", "u".repeat(256))).code())
          .isEqualTo(200);
    }
  }
}
