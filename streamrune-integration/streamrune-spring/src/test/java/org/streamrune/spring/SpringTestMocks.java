package org.streamrune.spring;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStore;

/** Shared Mockito fixtures for the Spring auto-configuration tests. */
final class SpringTestMocks {

  private SpringTestMocks() {}

  /**
   * An {@link EventStoreFactory} mock whose {@code create()} returns a mock {@link EventStore}: the
   * minimal event-store wiring most auto-configuration tests need. The store mock is created before
   * the stubbing starts, never inside {@code thenReturn(...)}.
   */
  static EventStoreFactory eventStoreFactoryReturningMockStore() {
    EventStore store = mock(EventStore.class);
    EventStoreFactory factory = mock(EventStoreFactory.class);
    when(factory.create()).thenReturn(store);
    return factory;
  }

  /**
   * An {@link OutboxStore} mock that states its ordering mode and claim lease, as the SPI requires.
   * An un-stubbed mock returns {@code null} from {@code orderingMode()}, which {@code
   * OutboxPoller.build()} refuses, and {@code Duration.ZERO} from {@code claimLease()}, which opts
   * the poller out of its lease guard and publish deadline. Use it wherever the auto-configuration
   * builds the outbox poller over a mocked store.
   */
  static OutboxStore outboxStoreStatingOrderingMode() {
    OutboxStore store = mock(OutboxStore.class);
    when(store.orderingMode()).thenReturn(OutboxOrderingMode.AVAILABILITY_FIRST);
    when(store.claimLease()).thenReturn(Duration.ofMinutes(4));
    return store;
  }
}
