package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.junit.jupiter.api.Test;

class StreamRuneComponentTest {

  @Test
  void streamRuneComponentIncludesRegisterForReflection() {
    assertThat(StreamRuneComponent.class.isAnnotationPresent(RegisterForReflection.class))
        .as("@StreamRuneComponent should include @RegisterForReflection for GraalVM native images")
        .isTrue();
  }
}
