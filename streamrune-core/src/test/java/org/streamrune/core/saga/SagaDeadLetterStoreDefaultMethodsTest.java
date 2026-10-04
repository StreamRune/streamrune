package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;

/**
 * The {@link SagaDeadLetterStore} defaults and the {@link SagaDeadLetterEntry} contract: the
 * default {@code findNullSagaEntry} scan is type-scoped, the optional {@code
 * findNullSagaEntriesByResolvedTarget} fails loud, and an entry always carries its saga type.
 */
class SagaDeadLetterStoreDefaultMethodsTest {

  private static final SagaId ID = SagaId.of("saga-1");
  private static final SagaType TYPE = SagaType.of("TestType");
  private static final SagaType OTHER = SagaType.of("OtherType");
  private static final GlobalOffset OFFSET = GlobalOffset.of(7L);

  private static SagaDeadLetterEntry nullSagaEntry(SagaType type, Instant faultedAt) {
    return new SagaDeadLetterEntry(
        null, type, OFFSET, EventType.of("Evt"), "java.lang.RuntimeException", "boom", faultedAt);
  }

  /**
   * Store that implements every abstract method benignly and returns {@code entries} from {@link
   * #findAll}, so the only behaviour under test is what the inherited defaults do.
   */
  private static SagaDeadLetterStore storeHolding(List<SagaDeadLetterEntry> entries) {
    return new SagaDeadLetterStore() {
      @Override
      public void publish(SagaDeadLetterEntry entry) {}

      @Override
      public void publishShielded(SagaDeadLetterEntry entry) {}

      @Override
      public List<SagaDeadLetterEntry> findAll(int limit) {
        return entries;
      }

      @Override
      public List<SagaDeadLetterEntry> findBySaga(SagaId sagaId) {
        return List.of();
      }

      @Override
      public boolean discard(SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset) {
        return false;
      }

      @Override
      public void establishFirstReplayAnchor(
          SagaId sagaId, SagaType sagaType, GlobalOffset eventOffset, Instant anchorIfAbsent) {}

      @Override
      public void setResolvedTarget(
          SagaType sagaType, GlobalOffset eventOffset, SagaId targetSagaId) {}

      @Override
      public boolean clearShieldIfDrained(SagaId sagaId, SagaType sagaType) {
        return false;
      }

      @Override
      public int deleteOlderThan(Instant cutoff) {
        return 0;
      }
    };
  }

  @Test
  void findNullSagaEntry_defaultScan_returnsOnlyTheRequestedTypesEntry() {
    var own = nullSagaEntry(TYPE, Instant.EPOCH);
    var foreign = nullSagaEntry(OTHER, Instant.EPOCH.plusSeconds(1));
    var store = storeHolding(List.of(foreign, own));

    assertThat(store.findNullSagaEntry(TYPE, OFFSET)).contains(own);
    assertThat(store.findNullSagaEntry(OTHER, OFFSET)).contains(foreign);
    assertThat(store.findNullSagaEntry(SagaType.of("Absent"), OFFSET)).isEmpty();
    assertThat(store.findNullSagaEntry(TYPE, GlobalOffset.of(8L))).isEmpty();
  }

  @Test
  void findNullSagaEntry_defaultScan_rejectsANullSagaType() {
    // No untyped lookup. Read as "any type", null would hand this caller the foreign
    // type's entry at the same offset.
    var foreign = nullSagaEntry(OTHER, Instant.EPOCH);
    assertThatThrownBy(() -> storeHolding(List.of(foreign)).findNullSagaEntry(null, OFFSET))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sagaType");
  }

  @Test
  void findNullSagaEntriesByResolvedTarget_inheritedDefault_rejectsANullSagaType() {
    assertThatThrownBy(() -> storeHolding(List.of()).findNullSagaEntriesByResolvedTarget(null, ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sagaType");
  }

  @Test
  void findNullSagaEntriesByResolvedTarget_inheritedDefault_failsLoud() {
    // An empty answer here IS the drain corruption the lookup prevents (later events
    // fed over a never-consumed earlier null-saga one) — the replayer degrades explicitly, never
    // silently.
    assertThatThrownBy(() -> storeHolding(List.of()).findNullSagaEntriesByResolvedTarget(TYPE, ID))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("findNullSagaEntriesByResolvedTarget");
  }

  @Test
  void countFaultedBacklog_inheritedDefault_failsLoud() {
    assertThatThrownBy(() -> storeHolding(List.of()).countFaultedBacklog())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("countFaultedBacklog");
  }

  @Test
  void entry_requiresItsSagaType() {
    // Every entry carries the quarantining runner's type, null-saga entries included;
    // every type-scoped operation matches an entry by it.
    assertThatThrownBy(() -> nullSagaEntry(null, Instant.EPOCH))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("sagaType");
  }
}
