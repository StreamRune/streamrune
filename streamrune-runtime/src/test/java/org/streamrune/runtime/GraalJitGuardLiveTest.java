package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.test.InMemoryEventStore;

/**
 * {@link GraalJitGuard} on the JVM the test runs on, and the command bus builder that runs it. The
 * build states what the guard must answer there: {@code streamrune.test.expectGraalJitGuard} is
 * {@code accepted} unless {@code -PexpectGraalJitGuard} says otherwise. The {@code test} task runs
 * it as every other test, with the JVMCI compiler off on a GraalVM build; {@code graalJitGuardTest}
 * runs it alone with the JVM flags {@code -PgraalJitGuardTestFlags} names, none by default, and the
 * GraalVM workflow runs that task on GraalVM with each of them.
 */
class GraalJitGuardLiveTest {

  private static final EventTypeRegistry TYPES = SimpleEventTypeRegistry.builder().build();

  private static final boolean REFUSED =
      "refused".equals(System.getProperty("streamrune.test.expectGraalJitGuard", "accepted"));

  @Test
  void theGuardAndTheBusBuilderAnswerAsTheBuildExpects() {
    if (REFUSED) {
      assertThatThrownBy(() -> GraalJitGuard.check(false))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("-XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler");
      assertThatThrownBy(
              () ->
                  VirtualThreadCommandBus.builder()
                      .eventStore(new InMemoryEventStore(TYPES))
                      .build())
          .as("the bus builder runs the guard")
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("-XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler");
      assertThatCode(
              () ->
                  VirtualThreadCommandBus.builder()
                      .eventStore(new InMemoryEventStore(TYPES))
                      .allowGraalJit(true)
                      .build()
                      .close())
          .as("the opt-out builds it")
          .doesNotThrowAnyException();
    } else {
      assertThatCode(() -> GraalJitGuard.check(false)).doesNotThrowAnyException();
      assertThatCode(
              () ->
                  VirtualThreadCommandBus.builder()
                      .eventStore(new InMemoryEventStore(TYPES))
                      .build()
                      .close())
          .doesNotThrowAnyException();
    }
  }
}
