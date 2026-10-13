package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.spring.SagaOriginHttpFixture.Confirm;
import org.streamrune.spring.SagaOriginHttpFixture.Confirmed;
import org.streamrune.spring.SagaOriginHttpFixture.Finished;
import org.streamrune.spring.SagaOriginHttpFixture.FlowState;
import org.streamrune.spring.SagaOriginHttpFixture.OpenFlow;
import org.streamrune.spring.SagaOriginHttpFixture.Prepared;
import org.streamrune.spring.SagaOriginHttpFixture.Routing;

/**
 * A request through {@link ScopedValueFilter} into a running saga. The filter copies the client's
 * {@code X-Correlation-Id} into the request context, and the command bus stores it in the event the
 * request's command produces. A client that sends another saga's id there addresses nothing: the
 * runner routes a saga's replies by the origin the command bus wrote, and routes any other event by
 * the event's own data, or not at all.
 *
 * <p>The correlation id saga dispatch binds has a reserved form, {@code streamrune:saga:<sagaId>}.
 * The filter answers {@code 400 Bad Request} to a request that sends a correlation id with the
 * reserved prefix, as it does to an id that is too long or holds a control character, before the
 * endpoint runs; every other correlation id is stored as the client sent it.
 */
class SagaOriginHttpRoutingTest {

  private static final String VICTIM = "victim";
  private static final String ATTACKER = "attacker";

  private SagaOriginHttpFixture fixture;

  @AfterEach
  void close() {
    if (fixture != null) {
      fixture.close();
    }
  }

  /** The endpoint of the application: a caller confirms, or opens, a flow of its own. */
  @RestController
  static final class FlowController {
    private final SagaOriginHttpFixture fixture;

    FlowController(SagaOriginHttpFixture fixture) {
      this.fixture = fixture;
    }

    @PostMapping("/flows/{flowId}/confirm")
    void confirm(@PathVariable("flowId") String flowId) {
      fixture.execute(new Confirm(flowId));
    }

    @PostMapping("/flows/{flowId}/open")
    void open(@PathVariable("flowId") String flowId) {
      fixture.execute(new OpenFlow(flowId));
    }
  }

  private MockMvc mvc() {
    return MockMvcBuilders.standaloneSetup(new FlowController(fixture))
        .addFilters(new ScopedValueFilter())
        .build();
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
    mvc()
        .perform(
            post("/flows/" + ATTACKER + "/confirm")
                .header("X-Correlation-Id", SagaOriginHttpFixture.sagaIdOf(VICTIM).value()))
        .andExpect(status().isOk());
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

  /**
   * The three identity modes of the request filter — an authenticated principal, a trusted gateway
   * whose headers are the caller, and an anonymous request — each with every header a client can
   * send naming the victim's saga: the correlation id, the trace id, the user id, a role, and
   * baggage entries named like the metadata fields that carry a saga origin and a causation id. The
   * mode decides who the user is; it decides nothing about which saga an event reaches.
   */
  @TestFactory
  Stream<DynamicTest> inEveryIdentityMode_noHeaderOfTheRequestReachesTheVictim() {
    Map<String, RequestIdentityPolicy> modes = new LinkedHashMap<>();
    modes.put(
        "an authenticated principal",
        RequestIdentityPolicy.of(() -> Optional.of(UserId.of("mallory")), false));
    modes.put("a trusted gateway", RequestIdentityPolicy.of(null, true));
    modes.put("an anonymous request", RequestIdentityPolicy.of(null, false));
    List<DynamicTest> cases = new ArrayList<>();
    for (var mode : modes.entrySet()) {
      for (Routing routing : Routing.values()) {
        cases.add(
            DynamicTest.dynamicTest(
                mode.getKey() + ", " + routing,
                () -> everyHeaderNamesTheVictim(mode.getValue(), routing)));
      }
    }
    return cases.stream();
  }

  private static void everyHeaderNamesTheVictim(RequestIdentityPolicy mode, Routing routing)
      throws Exception {
    String victimSaga = SagaOriginHttpFixture.sagaIdOf(VICTIM).value();
    try (var flows = new SagaOriginHttpFixture(routing, false)) {
      flows.open(VICTIM);
      flows.open(ATTACKER);
      LoadedSaga<FlowState> victim = flows.row(VICTIM);
      MockMvc mvc =
          MockMvcBuilders.standaloneSetup(new FlowController(flows))
              .addFilters(new ScopedValueFilter(Set.of(), mode, null))
              .build();

      mvc.perform(
              post("/flows/" + ATTACKER + "/confirm")
                  .header("X-Correlation-Id", victimSaga)
                  .header("X-Trace-Id", victimSaga)
                  .header("X-User-Id", victimSaga)
                  .header("X-User-Role", "ADMIN")
                  .header("baggage", "sagaOrigin=" + victimSaga + ",sagaId=" + victimSaga))
          .andExpect(status().isOk());
      flows.deliver();

      assertThat(flows.stored(Confirmed.class))
          .singleElement()
          .satisfies(
              confirmed -> {
                assertThat(confirmed.metadata().correlationId())
                    .isEqualTo(CorrelationId.of(victimSaga));
                assertThat(confirmed.metadata().sagaOrigin()).isNull();
                assertThat(confirmed.metadata().causationId()).isNull();
              });
      assertThat(flows.row(VICTIM)).isEqualTo(victim);
      assertThat(flows.deadLetters.all()).isEmpty();
      if (routing == Routing.EVENT_DATA_HOOK) {
        assertThat(flows.row(ATTACKER).status()).isEqualTo(SagaStatus.COMPLETED);
      } else {
        assertThat(flows.row(ATTACKER).state().step()).isEqualTo("PREPARED");
      }
    }
  }

  @Test
  void theSagasOwnFlow_completesByOrigin() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, true);

    mvc()
        .perform(post("/flows/own/open").header("X-Correlation-Id", "client-request-7"))
        .andExpect(status().isOk());
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
   * The attacker's request, carrying {@code header}, is answered {@code 400} with the shared
   * reason, and nothing of it reaches the application: the endpoint is not called, no event is
   * stored and neither saga row changes.
   */
  private void assertRefused(MockMvc mvc, SagaOriginHttpFixture flows, String header, String value)
      throws Exception {
    List<EventEnvelope> storedBefore = flows.stored();
    LoadedSaga<FlowState> victim = flows.row(VICTIM);
    LoadedSaga<FlowState> attacker = flows.row(ATTACKER);
    int callsBefore = flows.endpointCalls();

    MockHttpServletResponse response =
        mvc.perform(post("/flows/" + ATTACKER + "/confirm").header(header, value))
            .andReturn()
            .getResponse();
    flows.deliver();

    assertThat(response.getStatus()).as("status for a refused %s", header).isEqualTo(400);
    assertThat(response.getErrorMessage()).isEqualTo(RequestIdentityPolicy.REFUSED_REQUEST_CONTEXT);
    assertThat(flows.endpointCalls()).as("the endpoint is not called").isEqualTo(callsBefore);
    assertThat(flows.stored()).as("nothing is stored").isEqualTo(storedBefore);
    assertThat(flows.row(VICTIM)).as("the victim's row").isEqualTo(victim);
    assertThat(flows.row(ATTACKER)).as("the attacker's row").isEqualTo(attacker);
    assertThat(flows.deadLetters.all()).isEmpty();
  }

  private void assertRefused(String header, String value) throws Exception {
    assertRefused(mvc(), fixture, header, value);
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

    for (int i = 0; i < ordinary.size(); i++) {
      String flow = "caller-" + i;
      String sent = ordinary.get(i);
      mvc()
          .perform(post("/flows/" + flow + "/confirm").header("X-Correlation-Id", sent))
          .andExpect(status().isOk());
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
  void withoutACorrelationIdHeader_oneIsGenerated() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);

    mvc().perform(post("/flows/first/confirm")).andExpect(status().isOk());
    mvc().perform(post("/flows/second/confirm")).andExpect(status().isOk());

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

  /**
   * The refusals do not depend on who the caller is: in each identity mode a reserved correlation
   * id, an over-long one and a control character in the trace id are answered {@code 400}. The user
   * id is refused where a request supplies it — the {@code X-User-Id} header behind a trusted
   * gateway — and ignored in the two modes that do not read the header.
   */
  @TestFactory
  Stream<DynamicTest> inEveryIdentityMode_aRefusedRequestContextIsAnswered400() {
    Map<String, RequestIdentityPolicy> modes = new LinkedHashMap<>();
    modes.put(
        "an authenticated principal",
        RequestIdentityPolicy.of(() -> Optional.of(UserId.of("mallory")), false));
    modes.put("a trusted gateway", RequestIdentityPolicy.of(null, true));
    modes.put("an anonymous request", RequestIdentityPolicy.of(null, false));
    Map<String, String[]> refused = new LinkedHashMap<>();
    refused.put(
        "the reserved form of the victim's saga id",
        new String[] {"X-Correlation-Id", reservedCorrelationIdOf(VICTIM)});
    refused.put(
        "a 256-character correlation id", new String[] {"X-Correlation-Id", "a".repeat(256)});
    refused.put("a control character in the trace id", new String[] {"X-Trace-Id", "trace\tid"});
    List<DynamicTest> cases = new ArrayList<>();
    for (var mode : modes.entrySet()) {
      for (var header : refused.entrySet()) {
        cases.add(
            DynamicTest.dynamicTest(
                mode.getKey() + ", " + header.getKey(),
                () -> {
                  try (var flows = new SagaOriginHttpFixture(Routing.EVENT_DATA_HOOK, false)) {
                    flows.open(VICTIM);
                    flows.open(ATTACKER);
                    assertRefused(
                        mvcIn(mode.getValue(), flows),
                        flows,
                        header.getValue()[0],
                        header.getValue()[1]);
                  }
                }));
      }
    }
    return cases.stream();
  }

  private static MockMvc mvcIn(RequestIdentityPolicy mode, SagaOriginHttpFixture flows) {
    return MockMvcBuilders.standaloneSetup(new FlowController(flows))
        .addFilters(new ScopedValueFilter(Set.of(), mode, null))
        .build();
  }

  @Test
  void anOverlongUserIdHeader_isAnswered400BehindATrustedGateway_andIgnoredElsewhere()
      throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, false);
    victimAndAttackerWaitForConfirmed();

    assertRefused(
        mvcIn(RequestIdentityPolicy.of(null, true), fixture),
        fixture,
        "X-User-Id",
        "u".repeat(256));

    mvcIn(RequestIdentityPolicy.of(() -> Optional.of(UserId.of("mallory")), false), fixture)
        .perform(post("/flows/principal/confirm").header("X-User-Id", "u".repeat(256)))
        .andExpect(status().isOk());
    mvcIn(RequestIdentityPolicy.of(null, false), fixture)
        .perform(post("/flows/anonymous/confirm").header("X-User-Id", "u".repeat(256)))
        .andExpect(status().isOk());
    assertThat(fixture.stored(Confirmed.class))
        .extracting(confirmed -> confirmed.metadata().userId())
        .containsExactly(UserId.of("mallory"), null);
  }
}
