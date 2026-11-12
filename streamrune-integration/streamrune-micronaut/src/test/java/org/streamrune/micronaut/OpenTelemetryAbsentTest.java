package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * The integration on a classpath without any OpenTelemetry jar, an application that does not use
 * OpenTelemetry: the context starts, its tracing is {@link StreamRuneTracing#NOOP}, and a command
 * runs untraced. Runs under the {@code openTelemetryAbsentTest} Gradle task, which removes the jars
 * from the test runtime classpath; the normal {@code test} task leaves this class out.
 */
class OpenTelemetryAbsentTest {

  static final String SPEC = "open-telemetry-absent";

  /** The application's stores and its one command. */
  @Factory
  @Requires(property = "spec.name", value = SPEC)
  static class ApplicationBeans {
    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }

    @Singleton
    DeciderRegistration<Probe, ProbeState, Probed> probeRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("probe"), Probe.class, p -> AggregateId.of(p.id()), new ProbeDecider());
    }
  }

  @Test
  void preconditionOpenTelemetryIsReallyAbsent() {
    assertThrows(
        ClassNotFoundException.class, () -> Class.forName("io.opentelemetry.api.OpenTelemetry"));
    assertThrows(
        ClassNotFoundException.class, () -> Class.forName("io.opentelemetry.context.Context"));
  }

  @Test
  void theContextStartsAndACommandRunsUntraced() {
    try (ApplicationContext context =
        ApplicationContext.run(Map.of("spec.name", SPEC, "micronaut.security.enabled", "false"))) {
      assertSame(StreamRuneTracing.NOOP, context.getBean(StreamRuneTracing.class));
      VirtualThreadCommandBus bus = context.getBean(VirtualThreadCommandBus.class);
      assertTrue(
          bus.interceptors().stream().noneMatch(OpenTelemetryCommandInterceptor.class::isInstance));
      assertNotNull(bus.execute(new Probe("absent-1")));
    }
  }

  record Probe(String id) implements Command {}

  record ProbeState() implements AggregateState {}

  record Probed(String id) implements DomainEvent {}

  static final class ProbeDecider implements Decider<Probe, ProbeState, Probed> {
    @Override
    public ProbeState initialState() {
      return new ProbeState();
    }

    @Override
    public List<Probed> decide(Probe command, ProbeState state) {
      return List.of(new Probed(command.id()));
    }

    @Override
    public ProbeState evolve(ProbeState state, Probed event) {
      return state;
    }
  }
}
