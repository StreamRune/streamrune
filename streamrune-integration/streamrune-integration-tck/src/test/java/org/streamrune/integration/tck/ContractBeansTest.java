package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.ParameterizedType;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.tck.ContractBeans.CommandInboxBean;
import org.streamrune.integration.tck.ContractBeans.ContractCommand;
import org.streamrune.integration.tck.ContractBeans.ContractCommandExecuted;
import org.streamrune.integration.tck.ContractBeans.ContractCommandRegistration;
import org.streamrune.integration.tck.ContractBeans.ContractCommandState;
import org.streamrune.integration.tck.ContractBeans.ContractProjection;
import org.streamrune.integration.tck.ContractBeans.ContractSagaState;
import org.streamrune.integration.tck.ContractBeans.CryptoEngineBean;
import org.streamrune.integration.tck.ContractBeans.DeadLetterQueueBean;
import org.streamrune.integration.tck.ContractBeans.FirstLeadership;
import org.streamrune.integration.tck.ContractBeans.IdentifiedCallers;
import org.streamrune.integration.tck.ContractBeans.OutboxStoreBean;
import org.streamrune.integration.tck.ContractBeans.RecordingAuditStore;
import org.streamrune.integration.tck.ContractBeans.SecondLeadership;
import org.streamrune.integration.tck.ContractBeans.UngatedSagaRunner;
import org.streamrune.runtime.SagaRunner;

class ContractBeansTest {

  @Test
  void aSupplierStandsForTheBeanItSuppliesAndAnyOtherClassForItself() {
    assertEquals(OutboxStore.class, ContractBeans.beanType(OutboxStoreBean.class));
    assertEquals(CryptoEngine.class, ContractBeans.beanClassOf(CryptoEngineBean.class));
    assertEquals(CommandInbox.class, ContractBeans.beanClassOf(CommandInboxBean.class));
    assertEquals(DeadLetterQueue.class, ContractBeans.beanClassOf(DeadLetterQueueBean.class));
    assertEquals(FirstLeadership.class, ContractBeans.beanType(FirstLeadership.class));
    assertEquals(FirstLeadership.class, ContractBeans.beanClassOf(FirstLeadership.class));
    var runner =
        assertInstanceOf(ParameterizedType.class, ContractBeans.beanType(UngatedSagaRunner.class));
    assertEquals(SagaRunner.class, runner.getRawType());
    assertEquals(ContractSagaState.class, runner.getActualTypeArguments()[0]);
    assertEquals(SagaRunner.class, ContractBeans.beanClassOf(UngatedSagaRunner.class));
  }

  @Test
  void createBuildsANewBeanEachTime() {
    assertInstanceOf(OutboxStore.class, ContractBeans.create(OutboxStoreBean.class));
    assertInstanceOf(CryptoEngine.class, ContractBeans.create(CryptoEngineBean.class));
    assertInstanceOf(CommandInbox.class, ContractBeans.create(CommandInboxBean.class));
    assertInstanceOf(DeadLetterQueue.class, ContractBeans.create(DeadLetterQueueBean.class));
    assertNotSame(
        ContractBeans.create(FirstLeadership.class), ContractBeans.create(FirstLeadership.class));
  }

  /** A supplier that does not say what it supplies. */
  @SuppressWarnings("rawtypes")
  public static final class Untyped implements Supplier {
    @Override
    public Object get() {
      return "x";
    }
  }

  /** A bean class without a public no-argument constructor. */
  public static final class NeedsAnArgument {
    public NeedsAnArgument(String argument) {}
  }

  /** A bean class whose constructor fails. */
  public static final class FailsToConstruct {
    public FailsToConstruct() {
      throw new IllegalStateException("no");
    }
  }

  @Test
  void aBeanClassTheContractCannotReadIsRefusedNamingIt() {
    var untyped =
        assertThrows(IllegalArgumentException.class, () -> ContractBeans.beanType(Untyped.class));
    assertTrue(untyped.getMessage().contains(Untyped.class.getName()));
    var argument =
        assertThrows(
            IllegalArgumentException.class, () -> ContractBeans.create(NeedsAnArgument.class));
    assertTrue(argument.getMessage().contains("public no-argument constructor"));
    var failing =
        assertThrows(
            IllegalStateException.class, () -> ContractBeans.create(FailsToConstruct.class));
    assertEquals("no", failing.getCause().getMessage());
  }

  @Test
  void theLeadershipsGrantNoLease() {
    for (SubscriptionLeadership leadership :
        List.of(new FirstLeadership(), new SecondLeadership())) {
      assertEquals(Optional.empty(), leadership.tryAcquire("c"));
      assertEquals(Optional.empty(), leadership.current("c"));
      leadership.resign("c");
      leadership.close();
      assertNotSame(SubscriptionLeadership.NOOP, leadership);
    }
  }

  @Test
  void theAuthorizerAdmitsAnIdentifiedCallerOnly() {
    var stream = StreamId.of(AggregateType.of("order"), AggregateId.of("order-1"));
    var authorizer = new IdentifiedCallers();
    assertTrue(authorizer.isAuthorized(caller(UserId.of("alice")), stream));
    assertFalse(authorizer.isAuthorized(caller(null), stream));
  }

  private static StreamRuneContext.RequestContext caller(UserId userId) {
    return new StreamRuneContext.RequestContext(
        null, userId, CorrelationId.of("c-1"), Instant.now(), Map.of(), null);
  }

  @Test
  void theProjectionIsDiscoverableAndKeepsNothing() {
    ProjectionConfig config = ContractProjection.class.getAnnotation(ProjectionConfig.class);
    assertEquals(ContractProjection.NAME, config.name());
    assertEquals(ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT, config.deliveryMode());
    new ContractProjection().process(List.of());
  }

  @Test
  void theSagaRunnerCarriesNoLeaseAndItsSagaStartsOnNoEvent() {
    SagaRunner<ContractSagaState> runner = new UngatedSagaRunner().get();
    assertFalse(runner.lease().isGated());
    var orchestrator = new ContractBeans.ContractSagaOrchestrator();
    var state = orchestrator.initialState(orchestrator.extractSagaId(null));
    assertEquals(SagaStatus.STARTED, state.status());
    assertFalse(orchestrator.isStartEvent(null));
    assertEquals(state, orchestrator.evolve(state, null));
    assertEquals(List.of(), orchestrator.handle(state, null));
    assertEquals(List.of(), orchestrator.compensate(state, new RuntimeException(), null));
    var bus = new ContractBeans.NoCommandBus();
    assertTrue(bus.supportsIdempotentExecution());
    assertThrows(UnsupportedOperationException.class, () -> bus.execute(null));
  }

  @Test
  void theContractCommandStoresOneEventOnTheAggregateItsIdNames() {
    var registration = new ContractCommandRegistration().get();
    var command = new ContractCommand("order-1");
    assertEquals(AggregateType.of(ContractCommandRegistration.TYPE), registration.aggregateType());
    assertEquals(ContractCommand.class, registration.commandType());
    assertEquals(AggregateId.of("order-1"), registration.idExtractor().apply(command));
    var decider = registration.decider();
    ContractCommandState state = decider.initialState();
    assertEquals(List.of(new ContractCommandExecuted("order-1")), decider.decide(command, state));
    assertEquals(state, decider.evolve(state, new ContractCommandExecuted("order-1")));
  }

  @Test
  void theAuditStoreKeepsWhatItSavesInOrder() {
    var store = new RecordingAuditStore();
    var first = entry("cmd-1");
    var second = entry("cmd-2");
    store.save(first);
    store.save(second);
    assertEquals(List.of(first, second), store.entries());
  }

  private static AuditEntry entry(String commandId) {
    return new AuditEntry(
        CommandId.of(commandId),
        ContractCommand.class.getSimpleName(),
        AggregateType.of(ContractCommandRegistration.TYPE),
        AggregateId.of("order-1"),
        UserId.of("alice"),
        Instant.now(),
        AuditOutcome.SUCCESS,
        null,
        1,
        CorrelationId.of("corr-1"));
  }
}
