package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;

/**
 * Where the command bus takes the saga provenance of a dispatch's events from, and what it compares
 * first. A saga dispatch hands the bus its saga beside the scoped values it binds ({@link
 * SagaDispatchOffer}); the bus compares the two at the start of the dispatch, and the events of the
 * command then carry the handed saga's origin, causation id and correlation id, and a fenced
 * forward command is fenced against the handed saga. Any other dispatch takes them from its
 * bindings.
 */
class SagaDispatchOfferProvenanceTest {

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent(VirtualThreadCommandBusForwardFenceTest.Shipped.class)
          .registerState(
              "ShipmentState", VirtualThreadCommandBusForwardFenceTest.ShipmentState.class)
          .build();

  private static final SagaType SAGA = SagaType.of("org.example.FulfilmentState");
  private static final SagaOrigin HANDED = SagaOrigin.dispatchedBy(SAGA, SagaId.of("handed"));
  private static final SagaOrigin OTHER = SagaOrigin.dispatchedBy(SAGA, SagaId.of("other"));
  private static final CausationId HANDED_CAUSE = CausationId.of("evt-handed");
  private static final CausationId OTHER_CAUSE = CausationId.of("evt-other");
  private static final IdempotencyKey KEY = new IdempotencyKey("saga:handed:1:0");

  private static SagaDispatchOffer handed(boolean forwardFenced) {
    return new SagaDispatchOffer(HANDED, HANDED_CAUSE, null, KEY, forwardFenced);
  }

  record SagasCommand(String id) implements org.streamrune.core.Command {}

  /**
   * Only the saga's own dispatch takes the offer: the saga's command instance under any key, or the
   * saga's key. A keyed dispatch of another command under another key — a bus decorator's own
   * command, dispatched on the thread before it delegates — leaves the offer for the saga's.
   */
  @Test
  void theOfferIsTakenByTheSagasCommandOrKeyOnly_andLeftInPlaceForAnyOtherDispatch() {
    var sagas = new SagasCommand("saga's");
    var offer = new SagaDispatchOffer(HANDED, HANDED_CAUSE, sagas, KEY, true);
    try {
      offer.offer();

      assertThat(SagaDispatchOffer.take(new SagasCommand("audit"), IdempotencyKey.of("audit-1")))
          .as("another command under another key")
          .isNull();
      assertThat(SagaDispatchOffer.pending()).as("left for the saga's own dispatch").isTrue();
      assertThat(SagaDispatchOffer.take(sagas, KEY)).isSameAs(offer);
      assertThat(SagaDispatchOffer.pending()).isFalse();
      assertThat(SagaDispatchOffer.take(sagas, KEY)).as("taken once").isNull();

      offer.offer();
      assertThat(SagaDispatchOffer.take(sagas, IdempotencyKey.of("rewritten:" + KEY.value())))
          .as("the saga's command under a key a decorator rewrote is still the saga's")
          .isSameAs(offer);

      offer.offer();
      assertThat(SagaDispatchOffer.take(new SagasCommand("saga's"), KEY))
          .as("the saga's key, whatever the instance")
          .isSameAs(offer);
    } finally {
      SagaDispatchOffer.withdraw();
    }
  }

  @Test
  void theEventsOfAHandedSagasDispatch_carryThatSagasProvenance_whateverIsReadLater() {
    var provenance =
        VirtualThreadCommandBus.EventProvenance.of(
            handed(true),
            new StreamRuneContext.DispatchCause(OTHER_CAUSE, OTHER),
            OTHER::correlationId);

    assertThat(provenance.origin()).isEqualTo(HANDED);
    assertThat(provenance.causationId()).isEqualTo(HANDED_CAUSE);
    assertThat(provenance.correlationId()).isEqualTo(HANDED.correlationId());
  }

  @Test
  void theEventsOfAnyOtherDispatch_carryTheBoundCauseAndTheRecordedCorrelationId() {
    var provenance =
        VirtualThreadCommandBus.EventProvenance.of(
            null, new StreamRuneContext.DispatchCause(OTHER_CAUSE, OTHER), OTHER::correlationId);

    assertThat(provenance.origin()).isEqualTo(OTHER);
    assertThat(provenance.causationId()).isEqualTo(OTHER_CAUSE);
    assertThat(provenance.correlationId()).isEqualTo(OTHER.correlationId());
  }

  @Test
  void withNeitherAHandedSagaNorACause_theEventsAreACausationRoot() {
    var recorded = HANDED.correlationId();
    var provenance = VirtualThreadCommandBus.EventProvenance.of(null, null, () -> recorded);

    assertThat(provenance.origin()).isNull();
    assertThat(provenance.causationId()).isNull();
    assertThat(provenance.correlationId()).isEqualTo(recorded);
  }

  @Test
  void aFencedForwardIsFencedAgainstTheHandedSaga_andAnUnfencedCommandAgainstNone() {
    ScopedValue.where(StreamRuneContext.SAGA_FORWARD, new StreamRuneContext.SagaForward(OTHER, KEY))
        .run(
            () -> {
              assertThat(VirtualThreadCommandBus.fenceOrigin(handed(true), KEY)).isEqualTo(HANDED);
              assertThat(VirtualThreadCommandBus.fenceOrigin(handed(false), KEY)).isNull();
              assertThat(VirtualThreadCommandBus.fenceOrigin(null, KEY))
                  .as("any other dispatch is fenced by its binding")
                  .isEqualTo(OTHER);
            });
  }

  @Test
  void theBusRefusesADispatchWhoseBoundCauseNamesAnotherCausation() {
    var context =
        new StreamRuneContext.RequestContext(
            null, null, HANDED.correlationId(), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, context)
        .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(OTHER_CAUSE, HANDED))
        .where(StreamRuneContext.SAGA_FORWARD, new StreamRuneContext.SagaForward(HANDED, KEY))
        .run(
            () ->
                assertThatThrownBy(
                        () ->
                            VirtualThreadCommandBus.refuseUnlessTheSagaDispatchsBindings(
                                handed(true), Object.class, KEY))
                    .isInstanceOf(SagaBindingsMismatchException.class));
    ScopedValue.where(StreamRuneContext.CURRENT, context)
        .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(HANDED_CAUSE, HANDED))
        .where(StreamRuneContext.SAGA_FORWARD, new StreamRuneContext.SagaForward(HANDED, KEY))
        .run(
            () ->
                assertThatCode(
                        () ->
                            VirtualThreadCommandBus.refuseUnlessTheSagaDispatchsBindings(
                                handed(true), Object.class, KEY))
                    .as("the dispatch's own bindings pass")
                    .doesNotThrowAnyException());
  }

  /**
   * The command bus builds the events of a command a saga handed it from the handed saga: through
   * saga dispatch, the events' provenance is asked of {@link
   * VirtualThreadCommandBus.EventProvenance#of} with the handed saga and without a cause read from
   * the bindings. On a JVM whose reads agree with the comparison the stored values are the same
   * either way, so the call itself is what this test pins.
   */
  @Test
  void theEventsOfAHandedSagasCommand_areBuiltFromTheHandedSaga() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore(TYPES).withCommandInbox(inbox);
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .register(
                AggregateType.of("shipment"),
                VirtualThreadCommandBusForwardFenceTest.Ship.class,
                command -> AggregateId.of(command.shipmentId()),
                new VirtualThreadCommandBusForwardFenceTest.ShipDecider())
            .build();
    var key = new IdempotencyKey("saga:handed:2:0");

    try (MockedStatic<VirtualThreadCommandBus.EventProvenance> provenance =
        mockStatic(VirtualThreadCommandBus.EventProvenance.class, CALLS_REAL_METHODS)) {
      SagaCommandDispatch.dispatch(
          bus,
          HANDED,
          EventId.of("evt-handed"),
          new VirtualThreadCommandBusForwardFenceTest.Ship("s-1", true),
          key,
          false);

      provenance.verify(
          () ->
              VirtualThreadCommandBus.EventProvenance.of(
                  argThat(
                      handed ->
                          handed != null
                              && HANDED.equals(handed.origin())
                              && key.equals(handed.key())),
                  isNull(),
                  any()));
    }
    var stored = store.readGlobalStream(GlobalOffset.initial(), 10);
    assertThat(stored).singleElement();
    assertThat(stored.getFirst().metadata().sagaOrigin()).isEqualTo(HANDED);
    assertThat(stored.getFirst().metadata().causationId()).isEqualTo(HANDED_CAUSE);
    assertThat(stored.getFirst().metadata().correlationId()).isEqualTo(HANDED.correlationId());
  }
}
