package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.core.annotation.ReflectiveAccess;
import org.junit.jupiter.api.Test;

class StreamRuneComponentTest {

  @Test
  void streamRuneComponentIncludesReflectiveAccess() {
    assertThat(StreamRuneComponent.class.isAnnotationPresent(ReflectiveAccess.class))
        .as("@StreamRuneComponent should include @ReflectiveAccess for GraalVM native images")
        .isTrue();
  }
}
