package org.streamrune.core.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * {@link ProjectionDeadLetterStore#countPending()} is a defaulted SPI method that throws {@link
 * UnsupportedOperationException} for a store that does not override it — matching the fail-loudly
 * default-method policy the framework applies across its SPIs (e.g. {@code
 * DeadLetterQueue.countPending()}), so the projection runners disable the depth gauge gracefully
 * instead of a silent false 0.
 */
class ProjectionDeadLetterStoreDefaultMethodsTest {

  /** A minimal store that implements ONLY the abstract methods, inheriting every default. */
  static class AbstractOnlyStore implements ProjectionDeadLetterStore {
    @Override
    public void save(ProjectionDeadLetterEntry entry) {}

    @Override
    public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
      return List.of();
    }

    @Override
    public List<ProjectionDeadLetterEntry> readAll(int limit) {
      return List.of();
    }

    @Override
    public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {}
  }

  @Test
  void countPendingDefaultThrowsUnsupportedOperation() {
    var store = new AbstractOnlyStore();
    assertThatThrownBy(store::countPending)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("countPending()")
        .hasMessageContaining("streamrune.projections.dead_letter_backlog");
  }

  @Test
  void messageNamesTheImplementingClass() {
    assertThatThrownBy(new AbstractOnlyStore()::countPending)
        .hasMessageContaining(AbstractOnlyStore.class.getName());
  }

  @Test
  void overridingStoreDoesNotThrow() {
    ProjectionDeadLetterStore counting =
        new AbstractOnlyStore() {
          @Override
          public long countPending() {
            return 7L;
          }
        };
    assertThat(counting.countPending()).isEqualTo(7L);
  }
}
