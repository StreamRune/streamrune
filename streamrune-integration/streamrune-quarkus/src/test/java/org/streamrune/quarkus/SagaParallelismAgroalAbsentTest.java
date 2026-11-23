package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.inject.Instance;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import javax.sql.DataSource;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The saga parallelism startup check in an application without Agroal: Agroal is a compile-only
 * dependency of this module, so the check must load and run when no Agroal type exists. Runs in
 * {@code agroalAbsentTest}, on a classpath without the Agroal jars; the main test task leaves it
 * out.
 */
class SagaParallelismAgroalAbsentTest {

  record State(SagaStatus status, String id) implements SagaState {}

  static final class Orchestrator implements SagaOrchestrator<State> {
    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("x");
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final CommandBus BUS =
      new CommandBus() {
        @Override
        public boolean supportsIdempotentExecution() {
          return true;
        }

        @Override
        public <C extends Command> CommandResult execute(C command) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          throw new UnsupportedOperationException();
        }
      };

  @Test
  void preconditionAgroalIsReallyAbsent() {
    assertThatThrownBy(() -> Class.forName("io.agroal.api.AgroalDataSource"))
        .isInstanceOf(ClassNotFoundException.class);
  }

  @Test
  void theCheckRunsWithAPoolThatIsNotAgroal() {
    var store = new InMemorySagaStore();
    SagaRunner<State> runner =
        SagaRunner.<State>builder()
            .orchestrator(new Orchestrator())
            .sagaStore(store)
            .commandBus(BUS)
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
            .forwardFence(false)
            .build();
    PollingEventSubscription.builder()
        .subscriptionName(SubscriptionName.of("order-saga"))
        .eventStore(new InMemoryEventStore(SimpleEventTypeRegistry.builder().build()))
        .offsetStore(new InMemoryOffsetStore())
        .config(
            org.streamrune.core.subscription.SubscriptionConfig.pollingOnly(Duration.ofHours(1)))
        .sagaRunner(runner)
        .build();
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .addDefaultInterceptors()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .build();
    var check =
        new SagaParallelismStartupCheck(
            List.of(runner),
            config.getConfigMapping(StreamRuneQuarkusProperties.class),
            instanceOf(DataSource.class, new PGSimpleDataSource()),
            instanceOf(AggregateLocker.class, new LocalStripedLocker(64)),
            instanceOf(Config.class, config));

    var logger = java.util.logging.Logger.getLogger(SagaParallelismStartupCheck.class.getName());
    List<String> info = new CopyOnWriteArrayList<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord logRecord) {
            if (logRecord.getLevel().equals(Level.INFO)) {
              info.add(logRecord.getMessage());
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(handler);
    try {
      check.check(new StartupEvent());
    } finally {
      logger.removeHandler(handler);
    }
    assertThat(info).anyMatch(m -> m.contains("up to 4 connections"));
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> instanceOf(Class<T> type, T value) {
    Instance<T> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(value);
    return instance;
  }
}
