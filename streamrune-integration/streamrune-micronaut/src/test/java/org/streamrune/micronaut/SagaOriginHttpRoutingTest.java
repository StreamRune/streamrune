package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.PathVariable;
import io.micronaut.http.annotation.Post;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.micronaut.SagaOriginHttpFixture.Confirm;
import org.streamrune.micronaut.SagaOriginHttpFixture.Confirmed;
import org.streamrune.micronaut.SagaOriginHttpFixture.Finished;
import org.streamrune.micronaut.SagaOriginHttpFixture.FlowState;
import org.streamrune.micronaut.SagaOriginHttpFixture.OpenFlow;
import org.streamrune.micronaut.SagaOriginHttpFixture.Prepared;
import org.streamrune.micronaut.SagaOriginHttpFixture.Routing;
import org.streamrune.test.InMemoryEventStore;

/**
 * A request through {@link StreamRuneContextFilter} into a running saga, on the embedded server.
 * The filter copies the client's {@code X-Correlation-Id} into the request context it binds around
 * the controller, and the command bus stores the id in the event the controller's command produces.
 * A client that sends another saga's id there addresses nothing: the runner routes a saga's replies
 * by the origin the command bus wrote, and routes any other event by the event's own data, or not
 * at all.
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
    return client
        .toBlocking()
        .exchange(HttpRequest.POST(path, "").header("X-Correlation-Id", correlationId))
        .code();
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
}
