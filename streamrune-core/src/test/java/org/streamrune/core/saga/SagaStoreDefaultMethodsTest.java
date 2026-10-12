package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.SagaType;

/**
 * The {@link SagaStore} defaults. The two optional ones: a {@code null} argument (the saga type,
 * the status filter, the age bound) is rejected as the caller bug it is ({@link
 * IllegalArgumentException}), ahead of the default's loud {@link UnsupportedOperationException} for
 * a store that does not support the capability. The two short forms of the episode-opening writes:
 * each delegates to the abstract long form with no cause.
 */
class SagaStoreDefaultMethodsTest {

  private static final SagaType TYPE = SagaType.of("TestType");

  /** The {@code episodeCause} each long-form call received, in call order. */
  private static final List<Optional<EventId>> RECEIVED_CAUSES = new ArrayList<>();

  /** Implements every abstract method benignly, so only the inherited defaults are under test. */
  private static final SagaStore STORE =
      new SagaStore() {
        @Override
        public void create(
            SagaId sagaId,
            SagaType sagaType,
            SagaState state,
            SagaStatus status,
            boolean deadLetterPending) {}

        @Override
        public void update(
            SagaId sagaId,
            SagaType sagaType,
            SagaState state,
            SagaStatus status,
            long expectedVersion) {}

        @Override
        public void createGenesisPending(
            SagaId sagaId,
            SagaType sagaType,
            SagaState state,
            SagaStatus status,
            boolean deadLetterPending,
            EventId episodeCause) {
          RECEIVED_CAUSES.add(Optional.ofNullable(episodeCause));
        }

        @Override
        public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {}

        @Override
        public void applyEvent(
            SagaId sagaId,
            SagaType sagaType,
            SagaState state,
            SagaStatus status,
            long expectedVersion,
            AppliedEvent applied) {}

        @Override
        public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
          return false;
        }

        @Override
        public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {}

        @Override
        public void claimCompensating(
            SagaId sagaId,
            SagaType sagaType,
            SagaState state,
            long expectedVersion,
            EventId episodeCause) {
          RECEIVED_CAUSES.add(Optional.ofNullable(episodeCause));
        }

        @Override
        public <S extends SagaState> Optional<LoadedSaga<S>> load(
            SagaId sagaId, SagaType sagaType, Class<S> stateType) {
          return Optional.empty();
        }

        @Override
        public void delete(SagaId sagaId, SagaType sagaType) {}

        @Override
        public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
          return List.of();
        }

        @Override
        public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
          return List.of();
        }
      };

  private record State(SagaStatus status) implements SagaState {}

  @Test
  void theShortFormsOfTheEpisodeOpeningWrites_delegateWithNoCause() {
    RECEIVED_CAUSES.clear();
    SagaId id = SagaId.of("saga-1");
    State state = new State(SagaStatus.COMPENSATING);

    STORE.createGenesisPending(id, TYPE, state, SagaStatus.COMPENSATING, false);
    STORE.claimCompensating(id, TYPE, state, 1L);
    assertThat(RECEIVED_CAUSES).containsExactly(Optional.empty(), Optional.empty());

    STORE.createGenesisPending(id, TYPE, state, SagaStatus.COMPENSATING, false, EventId.of("e-1"));
    STORE.claimCompensating(id, TYPE, state, 1L, EventId.of("e-2"));
    assertThat(RECEIVED_CAUSES)
        .containsExactly(
            Optional.empty(),
            Optional.empty(),
            Optional.of(EventId.of("e-1")),
            Optional.of(EventId.of("e-2")));
  }

  @Test
  void countByStatus_default_rejectsANullSagaType_beforeReportingTheMissingCapability() {
    assertThatThrownBy(() -> STORE.countByStatus(null, SagaStatus.COMPENSATING))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("sagaType must not be null");
    assertThatThrownBy(() -> STORE.countByStatus(TYPE, SagaStatus.COMPENSATING))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void findCompensating_default_rejectsANullSagaType_beforeReportingTheMissingCapability() {
    assertThatThrownBy(() -> STORE.findCompensating(null, Instant.EPOCH, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("sagaType must not be null");
    assertThatThrownBy(() -> STORE.findCompensating(TYPE, Instant.EPOCH, 10))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void countByStatus_default_rejectsANullStatus_beforeReportingTheMissingCapability() {
    assertThatThrownBy(() -> STORE.countByStatus(TYPE, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("status must not be null");
  }

  @Test
  void findCompensating_default_rejectsANullAgeBound_beforeReportingTheMissingCapability() {
    assertThatThrownBy(() -> STORE.findCompensating(TYPE, null, 10))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("updatedBefore must not be null");
  }
}
