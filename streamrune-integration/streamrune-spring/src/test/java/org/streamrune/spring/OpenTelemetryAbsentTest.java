package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStoreFactory;

/**
 * The auto-configuration on a classpath without any OpenTelemetry jar, an application that does not
 * use OpenTelemetry: the context starts, its tracing is {@link StreamRuneTracing#NOOP}, and a
 * command runs untraced. Runs under the {@code openTelemetryAbsentTest} Gradle task, which removes
 * the jars from the test runtime classpath; the normal {@code test} task leaves this class out.
 */
class OpenTelemetryAbsentTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(StreamRuneAutoConfiguration.class, ProjectionAutoConfig.class))
          .withUserConfiguration(Infrastructure.class);

  @Test
  void preconditionOpenTelemetryIsReallyAbsent() {
    assertThatThrownBy(() -> Class.forName("io.opentelemetry.api.OpenTelemetry"))
        .isInstanceOf(ClassNotFoundException.class);
    assertThatThrownBy(() -> Class.forName("io.opentelemetry.context.Context"))
        .isInstanceOf(ClassNotFoundException.class);
  }

  @Test
  void theContextStartsAndACommandRunsUntraced() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx.getBean(StreamRuneTracing.class)).isSameAs(StreamRuneTracing.NOOP);
          VirtualThreadCommandBus bus = ctx.getBean(VirtualThreadCommandBus.class);
          assertThat(bus.interceptors())
              .noneMatch(OpenTelemetryCommandInterceptor.class::isInstance);
          CommandBus.CommandResult result = bus.execute(new Probe("absent-1"));
          assertThat(result).isNotNull();
        });
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

  @Configuration
  static class Infrastructure {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    EventStoreFactory eventStoreFactory() {
      return InMemoryEventStoreFactory.builder().build();
    }

    @Bean
    DeciderRegistration<Probe, ProbeState, Probed> probeRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("probe"), Probe.class, p -> AggregateId.of(p.id()), new ProbeDecider());
    }
  }
}
