package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.enterprise.inject.Instance;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.AuditCommandInterceptor;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;

/**
 * The Quarkus command-bus producer must impose the canonical, security-relevant interceptor order
 * regardless of the order the container yields the beans. The critical property: Audit runs OUTSIDE
 * Authorization, so a command the authorization policy rejects is still audited.
 *
 * <p>The interceptors are handed to the producer in the WRONG order (authorization before audit).
 * If the producer did not sort them, the authorization denial would throw before the audit
 * interceptor's {@code before()} ran, so its {@code onError()} would never fire and the denied
 * attempt would go UNAUDITED. The test asserts the denial IS audited.
 */
class QuarkusInterceptorOrderingTest {

  record WidgetCommand(String id) implements Command {}

  record WidgetState() implements AggregateState {}

  sealed interface WidgetEvent extends DomainEvent {
    record Created() implements WidgetEvent {}
  }

  static final class WidgetDecider implements Decider<WidgetCommand, WidgetState, WidgetEvent> {
    @Override
    public WidgetState initialState() {
      return new WidgetState();
    }

    @Override
    public List<WidgetEvent> decide(WidgetCommand command, WidgetState state) {
      return List.of(new WidgetEvent.Created());
    }

    @Override
    public WidgetState evolve(WidgetState state, WidgetEvent event) {
      return state;
    }
  }

  @Test
  void policyDeniedCommandIsAudited_provingAuditSortsOutsideAuthorization() {
    var producers = new StreamRuneProducers();
    var saved = new CopyOnWriteArrayList<AuditEntry>();
    AuditStore auditStore = saved::add;

    var authz = new AuthorizationCommandInterceptor(CommandAuthorizationPolicy.denyAll("denied"));
    var audit = new AuditCommandInterceptor(auditStore);
    // WRONG order on purpose: authorization before audit. The producer must reorder them.
    var containerOrder = TestBeanManagers.withInterceptors(authz, audit);

    var registrations =
        List.<DeciderRegistration<?, ?, ?>>of(
            new DeciderRegistration<>(
                AggregateType.of("widget"),
                WidgetCommand.class,
                c -> AggregateId.of(c.id()),
                new WidgetDecider()));

    VirtualThreadCommandBus bus =
        producers.virtualThreadCommandBus(
            new InMemoryEventStore(),
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            unsatisfied(), // no application SnapshotPolicy bean
            containerOrder,
            unsatisfied(), // the never-read interceptor injection point
            registrations,
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    try {
      assertThrows(AuthorizationException.class, () -> bus.execute(new WidgetCommand("w-1")));
      assertTrue(
          saved.stream().anyMatch(e -> e.outcome() == AuditOutcome.FAILURE),
          "a policy-denied command must be audited as FAILURE — proving audit runs outside authz");
    } finally {
      bus.close();
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(true);
    when(inst.isResolvable()).thenReturn(false);
    return inst;
  }
}
