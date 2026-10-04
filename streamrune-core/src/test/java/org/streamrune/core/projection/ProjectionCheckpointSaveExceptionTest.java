package org.streamrune.core.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

class ProjectionCheckpointSaveExceptionTest {

  @Test
  void carriesNameOffsetAndCause_andNamesTheAtLeastOnceConsequence() {
    var cause = new IllegalStateException("store down");
    var ex =
        new ProjectionCheckpointSaveException(
            ProjectionName.of("orders"), GlobalOffset.of(42), cause);
    assertThat(ex.projectionName()).isEqualTo(ProjectionName.of("orders"));
    assertThat(ex.offset()).isEqualTo(GlobalOffset.of(42));
    assertThat(ex.getCause()).isSameAs(cause);
    assertThat(ex.getMessage())
        .contains("projection 'orders'")
        .contains("offset 42")
        .contains("was applied but the checkpoint save failed")
        .contains("at-least-once");
  }

  @Test
  void sanitizesTheProjectionNameInTheMessage() {
    var ex =
        new ProjectionCheckpointSaveException(
            ProjectionName.of("bad\nname"), GlobalOffset.of(1), new RuntimeException("x"));
    assertThat(ex.getMessage()).doesNotContain("\n");
  }

  @Test
  void requiresNameAndOffset() {
    assertThatThrownBy(
            () ->
                new ProjectionCheckpointSaveException(
                    null, GlobalOffset.of(1), new RuntimeException("x")))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () ->
                new ProjectionCheckpointSaveException(
                    ProjectionName.of("p"), null, new RuntimeException("x")))
        .isInstanceOf(NullPointerException.class);
  }
}
