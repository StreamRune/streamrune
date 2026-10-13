package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CorrelationId;
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

  @Test
  void theSagasOwnFlow_completesByOrigin() throws Exception {
    fixture = new SagaOriginHttpFixture(Routing.BUILT_IN, true);

    mvc()
        .perform(post("/flows/own/open").header("X-Correlation-Id", "client-request-7"))
        .andExpect(status().isOk());
    fixture.deliver();

    assertThat(fixture.row("own").status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(fixture.row("own").state().step()).isEqualTo("FINISHED");
    for (var reply : java.util.List.of(Prepared.class, Confirmed.class, Finished.class)) {
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
