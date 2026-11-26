package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * A page of sagas stepped in parallel over the SQLite stores runs to its end whatever the pool
 * size. Every write of a step is one store call that borrows one connection, waits for the file's
 * write lock and returns the connection, so a step never holds a connection while it needs a second
 * one: on a pool of one connection the steps take turns at the pool, and on a larger pool they wait
 * for the write lock. A step that needed two connections at once would wait for its own connection
 * on the pool of one and fail the page.
 */
@Timeout(120)
class SqliteSagaParallelStepsTest {

  private static final int SAGAS = 24;
  private static final AggregateType CUSTOMER = AggregateType.of("customer");
  private static final AggregateType WELCOME = AggregateType.of("welcome");
  private static final SagaType WELCOME_SAGA = SagaType.fromClass(WelcomeSagaState.class);

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent(CustomerRegistered.class)
          .registerEvent(WelcomeSent.class)
          .build();

  record RegisterCustomer(String customerId) implements Command {}

  record CustomerRegistered(String customerId) implements DomainEvent {}

  record CustomerState() implements AggregateState {}

  static final class CustomerDecider
      implements Decider<RegisterCustomer, CustomerState, CustomerRegistered> {
    @Override
    public CustomerState initialState() {
      return new CustomerState();
    }

    @Override
    public List<CustomerRegistered> decide(RegisterCustomer command, CustomerState state) {
      return List.of(new CustomerRegistered(command.customerId()));
    }

    @Override
    public CustomerState evolve(CustomerState state, CustomerRegistered event) {
      return state;
    }
  }

  record SendWelcome(String customerId) implements Command {}

  record WelcomeSent(String customerId) implements DomainEvent {}

  record WelcomeState() implements AggregateState {}

  static final class WelcomeDecider implements Decider<SendWelcome, WelcomeState, WelcomeSent> {
    @Override
    public WelcomeState initialState() {
      return new WelcomeState();
    }

    @Override
    public List<WelcomeSent> decide(SendWelcome command, WelcomeState state) {
      return List.of(new WelcomeSent(command.customerId()));
    }

    @Override
    public WelcomeState evolve(WelcomeState state, WelcomeSent event) {
      return state;
    }
  }

  record WelcomeSagaState(SagaStatus status, String customerId) implements SagaState {
    @JsonCreator
    WelcomeSagaState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("customerId") String customerId) {
      this.status = status;
      this.customerId = customerId;
    }
  }

  /** A registration starts the saga and its step sends the welcome, whose event completes it. */
  static final class WelcomeSaga implements SagaOrchestrator<WelcomeSagaState> {
    @Override
    public Class<WelcomeSagaState> stateType() {
      return WelcomeSagaState.class;
    }

    @Override
    public WelcomeSagaState initialState(SagaId sagaId) {
      return new WelcomeSagaState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof CustomerRegistered;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return switch (event.event()) {
        case CustomerRegistered registered -> sagaIdOf(registered.customerId());
        case WelcomeSent sent -> sagaIdOf(sent.customerId());
        default -> null;
      };
    }

    @Override
    public WelcomeSagaState evolve(WelcomeSagaState state, EventEnvelope event) {
      return switch (event.event()) {
        case CustomerRegistered registered ->
            new WelcomeSagaState(SagaStatus.RUNNING, registered.customerId());
        case WelcomeSent sent -> new WelcomeSagaState(SagaStatus.COMPLETED, sent.customerId());
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(WelcomeSagaState state, EventEnvelope event) {
      if (event.event() instanceof CustomerRegistered registered) {
        return List.of(
            SagaCommand.of(
                new SendWelcome(registered.customerId()), AggregateId.of(registered.customerId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        WelcomeSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  private static SagaId sagaIdOf(String customerId) {
    return SagaId.of("welcome-" + customerId);
  }

  @TempDir Path dir;

  @ParameterizedTest(name = "a pool of {0} connection(s)")
  @ValueSource(ints = {1, 2, 10})
  void aPageOfSagasSteppedInParallelRunsToItsEnd(int poolSize) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:sqlite:file:" + dir.resolve("parallel.db") + "?" + TestDatabases.REQUIRED_SETTINGS);
    config.setMaximumPoolSize(poolSize);
    // A step that waited for a connection its own thread holds fails here, well inside the timeout.
    config.setConnectionTimeout(10_000);
    config.setPoolName("parallel-steps-" + poolSize);
    try (HikariDataSource pool = new HikariDataSource(config);
        SqliteDatabase database = SqliteDatabase.open(pool)) {
      SqliteCommandInbox inbox = new SqliteCommandInbox(database);
      EventStore eventStore =
          new SqliteEventStoreFactory(database, TYPES).commandInbox(inbox).create();
      SagaStore sagas =
          new SqliteSagaStore(database, new ObjectMapper().registerModule(new JavaTimeModule()));
      SagaDeadLetterStore deadLetters = new SqliteSagaDeadLetterStore(database);
      try (VirtualThreadCommandBus bus = bus(eventStore, inbox)) {
        EventListener listener =
            SagaRunner.<WelcomeSagaState>builder()
                .orchestrator(new WelcomeSaga())
                .sagaStore(sagas)
                .sagaDeadLetterStore(deadLetters)
                .commandBus(bus)
                .parallelism(4)
                .build()
                .asEventListener();
        for (int i = 0; i < SAGAS; i++) {
          bus.execute(new RegisterCustomer("c-" + i));
        }

        // One page of start events: each step creates its saga row and appends its command.
        List<EventEnvelope> registrations =
            eventStore.readGlobalStream(GlobalOffset.initial(), 100);
        assertThat(registrations).hasSize(SAGAS);
        listener.onEvents(registrations);

        // One page of replies: each step completes its saga.
        List<EventEnvelope> welcomes =
            eventStore.readGlobalStream(registrations.getLast().globalOffset(), 100);
        assertThat(welcomes).as("every saga of the page sent its command").hasSize(SAGAS);
        listener.onEvents(welcomes);

        for (int i = 0; i < SAGAS; i++) {
          assertThat(
                  sagas
                      .load(sagaIdOf("c-" + i), WELCOME_SAGA, WelcomeSagaState.class)
                      .orElseThrow()
                      .status())
              .as("saga of c-%d", i)
              .isEqualTo(SagaStatus.COMPLETED);
        }
        assertThat(deadLetters.findAll(10)).isEmpty();
      }
    }
  }

  private static VirtualThreadCommandBus bus(EventStore eventStore, CommandInbox inbox) {
    return VirtualThreadCommandBus.builder()
        .eventStore(eventStore)
        .commandInbox(inbox)
        .register(
            CUSTOMER,
            RegisterCustomer.class,
            command -> AggregateId.of(command.customerId()),
            new CustomerDecider())
        .register(
            WELCOME,
            SendWelcome.class,
            command -> AggregateId.of(command.customerId()),
            new WelcomeDecider())
        .build();
  }
}
