package org.streamrune.integration.tck;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.StreamId;
import org.streamrune.integration.SseAuthorizer;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The beans {@link IntegrationWiringContract} adds to an application: what an application declares
 * besides the beans the integration configures. Each is a class with a public no-argument
 * constructor. A class that implements {@link Supplier} stands for the bean its {@link
 * Supplier#get()} returns, whose bean type is the type it supplies; any other class stands for an
 * instance of itself. {@link #beanType(Class)} and {@link #create(Class)} read a class that way, so
 * an adapter declares the bean the way an application of its framework declares one.
 */
public final class ContractBeans {

  private ContractBeans() {}

  /**
   * The bean type a bean class stands for.
   *
   * @param beanClass a class of the contract's beans
   * @return the type argument of its {@link Supplier} interface, or the class itself
   * @throws IllegalArgumentException for a supplier whose supplied type is not declared
   */
  public static Type beanType(Class<?> beanClass) {
    if (!Supplier.class.isAssignableFrom(beanClass)) {
      return beanClass;
    }
    for (Type implemented : beanClass.getGenericInterfaces()) {
      if (implemented instanceof ParameterizedType supplier
          && supplier.getRawType() == Supplier.class) {
        return supplier.getActualTypeArguments()[0];
      }
    }
    throw new IllegalArgumentException(
        beanClass.getName() + " must implement Supplier<T> with the bean type as T");
  }

  /**
   * The raw class of {@link #beanType(Class)}.
   *
   * @param beanClass a class of the contract's beans
   * @return the class of the bean type, without its type arguments
   */
  public static Class<?> beanClassOf(Class<?> beanClass) {
    Type type = beanType(beanClass);
    return type instanceof ParameterizedType parameterized
        ? (Class<?>) parameterized.getRawType()
        : (Class<?>) type;
  }

  /**
   * Creates the bean a bean class stands for.
   *
   * @param beanClass a class of the contract's beans
   * @return a new instance of the class, or what a new instance of a supplier supplies
   * @throws IllegalArgumentException when the class has no public no-argument constructor
   */
  public static Object create(Class<?> beanClass) {
    Object instance;
    try {
      instance = beanClass.getConstructor().newInstance();
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException(
          beanClass.getName() + " needs a public no-argument constructor", e);
    } catch (InvocationTargetException e) {
      throw new IllegalStateException(beanClass.getName() + " failed to construct", e.getCause());
    }
    return instance instanceof Supplier<?> supplier ? supplier.get() : instance;
  }

  /**
   * A real {@link SubscriptionLeadership}, not {@link SubscriptionLeadership#NOOP}: it grants no
   * lease to anyone. One of the two leaderships of {@code twoLeadershipBeansRefuseStart}, and the
   * leadership under which {@code sagaRunnerWithoutLeadershipLogsTheDocumentedWarn} reports an
   * ungated saga runner.
   */
  public static final class FirstLeadership implements SubscriptionLeadership {
    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return Optional.empty();
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return Optional.empty();
    }

    @Override
    public void resign(String consumerName) {}

    @Override
    public void close() {}
  }

  /** A second leadership, of another class than {@link FirstLeadership}, that grants no lease. */
  public static final class SecondLeadership implements SubscriptionLeadership {
    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return Optional.empty();
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return Optional.empty();
    }

    @Override
    public void resign(String consumerName) {}

    @Override
    public void close() {}
  }

  /**
   * An {@link SseAuthorizer} that admits every caller the request edge identified and refuses an
   * anonymous one: whether a request reaches its stream depends on its identity alone.
   */
  public static final class IdentifiedCallers implements SseAuthorizer {
    @Override
    public boolean isAuthorized(StreamRuneContext.RequestContext caller, StreamId streamId) {
      return caller.userId() != null;
    }
  }

  /** The application's outbox store, the test kit's in-memory one. */
  public static final class OutboxStoreBean implements Supplier<OutboxStore> {
    @Override
    public OutboxStore get() {
      return new InMemoryOutboxStore();
    }
  }

  /** The application's crypto engine, the test kit's in-memory one. */
  public static final class CryptoEngineBean implements Supplier<CryptoEngine> {
    @Override
    public CryptoEngine get() {
      return new InMemoryCryptoEngine();
    }
  }

  /** The application's command inbox, the test kit's in-memory one. */
  public static final class CommandInboxBean implements Supplier<CommandInbox> {
    @Override
    public CommandInbox get() {
      return new InMemoryCommandInbox();
    }
  }

  /** The application's command dead-letter queue, the test kit's in-memory one. */
  public static final class DeadLetterQueueBean implements Supplier<DeadLetterQueue> {
    @Override
    public DeadLetterQueue get() {
      return new InMemoryDeadLetterQueue();
    }
  }

  /**
   * A continuous projection the integration discovers and runs, named {@value #NAME}. It reads
   * every event and keeps nothing, so applying an event twice is harmless.
   */
  @ProjectionConfig(
      name = ContractProjection.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  public static final class ContractProjection implements Projection {

    /** The projection's name, the name of its subscription. */
    public static final String NAME = "contract_projection";

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** The state of the contract's saga. */
  public record ContractSagaState(SagaStatus status) implements SagaState {}

  /**
   * A {@link SagaRunner} built without a lease: its saga type is delivered on every replica. Its
   * stores are the test kit's in-memory ones and its command bus executes nothing.
   */
  public static final class UngatedSagaRunner implements Supplier<SagaRunner<ContractSagaState>> {
    @Override
    public SagaRunner<ContractSagaState> get() {
      var sagaStore = new InMemorySagaStore();
      return SagaRunner.<ContractSagaState>builder()
          .orchestrator(new ContractSagaOrchestrator())
          .sagaStore(sagaStore)
          .commandBus(new NoCommandBus())
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
          .forwardFence(false)
          .build();
    }
  }

  /** A saga that no event starts. */
  static final class ContractSagaOrchestrator implements SagaOrchestrator<ContractSagaState> {
    @Override
    public Class<ContractSagaState> stateType() {
      return ContractSagaState.class;
    }

    @Override
    public ContractSagaState initialState(SagaId sagaId) {
      return new ContractSagaState(SagaStatus.STARTED);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("contract-saga");
    }

    @Override
    public ContractSagaState evolve(ContractSagaState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(ContractSagaState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        ContractSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  /** A command bus that executes nothing: the contract's saga sends no command. */
  static final class NoCommandBus implements CommandBus {
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("the contract's saga sends no command");
    }
  }
}
