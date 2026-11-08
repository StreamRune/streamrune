package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;

/**
 * Where the command bus takes the saga provenance of a dispatch's events from, and what it compares
 * first. A saga dispatch hands the bus its saga beside the scoped values it binds ({@link
 * SagaDispatchOffer}); the bus compares the two at the start of the dispatch, and the events of the
 * command then carry the handed saga's origin, causation id and correlation id, and a fenced
 * forward command is fenced against the handed saga. Any other dispatch takes them from its
 * bindings.
 */
class SagaDispatchOfferProvenanceTest {

  private static final SagaType SAGA = SagaType.of("org.example.FulfilmentState");
  private static final SagaOrigin HANDED = SagaOrigin.dispatchedBy(SAGA, SagaId.of("handed"));
  private static final SagaOrigin OTHER = SagaOrigin.dispatchedBy(SAGA, SagaId.of("other"));
  private static final CausationId HANDED_CAUSE = CausationId.of("evt-handed");
  private static final CausationId OTHER_CAUSE = CausationId.of("evt-other");
  private static final IdempotencyKey KEY = new IdempotencyKey("saga:handed:1:0");

  private static SagaDispatchOffer handed(boolean forwardFenced) {
    return new SagaDispatchOffer(HANDED, HANDED_CAUSE, KEY, forwardFenced);
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
}
