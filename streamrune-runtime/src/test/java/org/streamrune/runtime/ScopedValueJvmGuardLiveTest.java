package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.test.InMemoryEventStore;

/**
 * {@link ScopedValueJvmGuard} on the JVM the test runs on, and the command bus builder that runs
 * it. The build states what the guard must answer there: {@code
 * streamrune.test.expectScopedValueJvm} is {@code accepted} unless {@code -PexpectScopedValueJvm}
 * says otherwise. The {@code test} task runs it as every other test, with the JVMCI compiler off on
 * a GraalVM build; {@code scopedValueJvmGuardTest} runs it alone with the JVM flags {@code
 * -PjvmGuardTestFlags} names, none by default, and the GraalVM workflow runs that task on GraalVM
 * with each of them.
 */
class ScopedValueJvmGuardLiveTest {

  private static final boolean REFUSED =
      "refused".equals(System.getProperty("streamrune.test.expectScopedValueJvm", "accepted"));

  @Test
  void theGuardAndTheBusBuilderAnswerAsTheBuildExpects() {
    if (REFUSED) {
      assertThatThrownBy(() -> ScopedValueJvmGuard.check(false))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("-XX:-UseJVMCICompiler");
      assertThatThrownBy(
              () -> VirtualThreadCommandBus.builder().eventStore(new InMemoryEventStore()).build())
          .as("the bus builder runs the guard")
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("-XX:-UseJVMCICompiler");
      assertThatCode(
              () ->
                  VirtualThreadCommandBus.builder()
                      .eventStore(new InMemoryEventStore())
                      .allowUnreliableScopedValues(true)
                      .build()
                      .close())
          .as("the opt-out builds it")
          .doesNotThrowAnyException();
    } else {
      assertThatCode(() -> ScopedValueJvmGuard.check(false)).doesNotThrowAnyException();
      assertThatCode(
              () ->
                  VirtualThreadCommandBus.builder()
                      .eventStore(new InMemoryEventStore())
                      .build()
                      .close())
          .doesNotThrowAnyException();
    }
  }
}
