package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.spi.Context;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Singleton;
import org.junit.jupiter.api.Test;
import org.streamrune.runtime.DeadLetterRetentionSweeper;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;

/** Tests for {@link StreamRuneLifecycle}. */
class StreamRuneLifecycleTest {

  /**
   * A bean manager whose singleton context holds what {@code bus} resolves to, as Arc's holds the
   * bus once it was created; a bus resolving to {@code null} is one nothing created.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static BeanManager beanManagerHolding(Instance<VirtualThreadCommandBus> bus) {
    BeanManager beanManager = mock(BeanManager.class);
    if (!bus.isResolvable()) {
      return beanManager;
    }
    VirtualThreadCommandBus created = bus.get();
    clearInvocations(bus);
    Bean<VirtualThreadCommandBus> bean = mock(Bean.class);
    when(bean.getScope()).thenReturn((Class) Singleton.class);
    Instance.Handle<VirtualThreadCommandBus> handle = mock(Instance.Handle.class);
    when(handle.getBean()).thenReturn(bean);
    when(bus.getHandle()).thenReturn(handle);
    Context context = mock(Context.class);
    when(context.get(bean)).thenReturn(created);
    when(beanManager.getContext(Singleton.class)).thenReturn(context);
    return beanManager;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> satisfied(T value) {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(false);
    when(inst.isResolvable()).thenReturn(true);
    when(inst.get()).thenReturn(value);
    return inst;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(true);
    when(inst.isResolvable()).thenReturn(false);
    return inst;
  }

  private static StreamRuneLifecycle lifecycle(
      Instance<MultiProjectionRunner> multi,
      Instance<ScheduledProjectionRunner> scheduled,
      Instance<OutboxPoller> poller,
      Instance<DeadLetterRetryRunner> dlr,
      Instance<VirtualThreadCommandBus> bus) {
    return new StreamRuneLifecycle(
        multi,
        scheduled,
        poller,
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        dlr,
        unsatisfied(),
        bus,
        beanManagerHolding(bus));
  }

  private static StreamRuneLifecycle lifecycle(
      Instance<MultiProjectionRunner> multi,
      Instance<ScheduledProjectionRunner> scheduled,
      Instance<OutboxPoller> poller,
      Instance<OutboxRetentionSweeper> sweeper,
      Instance<DeadLetterRetryRunner> dlr,
      Instance<VirtualThreadCommandBus> bus) {
    return new StreamRuneLifecycle(
        multi,
        scheduled,
        poller,
        sweeper,
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        dlr,
        unsatisfied(),
        bus,
        beanManagerHolding(bus));
  }

  private static StreamRuneLifecycle lifecycleWithSagaDeadLetterSweeper(
      Instance<SagaDeadLetterRetentionSweeper> sagaSweeper) {
    return new StreamRuneLifecycle(
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        sagaSweeper,
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        mock(jakarta.enterprise.inject.spi.BeanManager.class));
  }

  private static StreamRuneLifecycle lifecycleWithDeadLetterRetentionSweeper(
      Instance<DeadLetterRetentionSweeper> dlqSweeper) {
    return new StreamRuneLifecycle(
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        dlqSweeper,
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        mock(jakarta.enterprise.inject.spi.BeanManager.class));
  }

  @Test
  void startsAllResolvableRunners() {
    var multi = mock(MultiProjectionRunner.class);
    var scheduled = mock(ScheduledProjectionRunner.class);
    var poller = mock(OutboxPoller.class);
    var dlr = mock(DeadLetterRetryRunner.class);

    var lifecycle =
        lifecycle(
            satisfied(multi),
            satisfied(scheduled),
            satisfied(poller),
            satisfied(dlr),
            unsatisfied());

    lifecycle.onStart(mock(StartupEvent.class));

    verify(multi).start();
    verify(scheduled).start();
    verify(poller).start();
    verify(dlr).start();
    assertSame(multi, lifecycle.multiProjectionRunner());
    assertSame(scheduled, lifecycle.scheduledProjectionRunner());
    assertSame(poller, lifecycle.outboxPoller());
    assertSame(dlr, lifecycle.deadLetterRetryRunner());
  }

  @Test
  void skipsAbsentRunners() {
    var lifecycle =
        lifecycle(unsatisfied(), unsatisfied(), unsatisfied(), unsatisfied(), unsatisfied());

    assertDoesNotThrow(() -> lifecycle.onStart(mock(StartupEvent.class)));
    assertNull(lifecycle.multiProjectionRunner());
    assertNull(lifecycle.scheduledProjectionRunner());
    assertNull(lifecycle.outboxPoller());
    assertNull(lifecycle.deadLetterRetryRunner());
  }

  @Test
  void skipsNullProducts() {
    // The runner producers are dependent-scoped optional beans: the Instance is
    // resolvable but get() yields null when the feature has no registrations.
    var lifecycle =
        lifecycle(
            satisfied(null), satisfied(null), satisfied(null), satisfied(null), unsatisfied());

    assertDoesNotThrow(() -> lifecycle.onStart(mock(StartupEvent.class)));
    assertNull(lifecycle.multiProjectionRunner());
    assertNull(lifecycle.outboxPoller());
  }

  @Test
  void stopsStartedRunnersAndCommandBusOnShutdown() throws Exception {
    var multi = mock(MultiProjectionRunner.class);
    var scheduled = mock(ScheduledProjectionRunner.class);
    var poller = mock(OutboxPoller.class);
    var dlr = mock(DeadLetterRetryRunner.class);
    var bus = mock(VirtualThreadCommandBus.class);

    var lifecycle =
        lifecycle(
            satisfied(multi),
            satisfied(scheduled),
            satisfied(poller),
            satisfied(dlr),
            satisfied(bus));

    lifecycle.onStart(mock(StartupEvent.class));
    lifecycle.onStop(mock(ShutdownEvent.class));

    verify(multi).close();
    verify(scheduled).close();
    verify(poller).close();
    verify(dlr).close();
    verify(bus).close();
    assertNull(lifecycle.multiProjectionRunner());
    assertNull(lifecycle.scheduledProjectionRunner());
    assertNull(lifecycle.outboxPoller());
    assertNull(lifecycle.deadLetterRetryRunner());
  }

  @Test
  void aBusNothingCreatedIsNotCreatedToBeClosed() {
    Instance<VirtualThreadCommandBus> bus = satisfied(null);
    var lifecycle = lifecycle(unsatisfied(), unsatisfied(), unsatisfied(), unsatisfied(), bus);

    lifecycle.onStart(mock(StartupEvent.class));
    lifecycle.onStop(mock(ShutdownEvent.class));

    verify(bus, never()).get();
  }

  @Test
  void aBusThatCannotBeLookedUpDoesNotStopTheShutdown() {
    var multi = mock(MultiProjectionRunner.class);
    Instance<VirtualThreadCommandBus> bus = unsatisfied();
    when(bus.isResolvable()).thenReturn(true);
    var lifecycle = lifecycle(satisfied(multi), unsatisfied(), unsatisfied(), unsatisfied(), bus);
    when(bus.getHandle()).thenThrow(new IllegalStateException("the bus producer failed"));

    lifecycle.onStart(mock(StartupEvent.class));
    assertDoesNotThrow(() -> lifecycle.stopBeforeDestroy(new Object()));

    verify(multi).close();
    verify(bus, never()).get();
  }

  @Test
  void theContainerShutdownAfterTheShutdownEventStopsNothingTwice() throws Exception {
    var multi = mock(MultiProjectionRunner.class);
    var bus = mock(VirtualThreadCommandBus.class);
    var lifecycle =
        lifecycle(satisfied(multi), unsatisfied(), unsatisfied(), unsatisfied(), satisfied(bus));

    lifecycle.onStart(mock(StartupEvent.class));
    lifecycle.onStop(mock(ShutdownEvent.class));
    lifecycle.stopBeforeDestroy(new Object());
    lifecycle.stopOnDestroy();

    verify(multi, times(1)).close();
    verify(bus, times(1)).close();
  }

  @Test
  void theContainerShutdownStopsWhatAFailedStartLeftRunning() throws Exception {
    var multi = mock(MultiProjectionRunner.class);
    var dlr = mock(DeadLetterRetryRunner.class);
    var lifecycle =
        lifecycle(satisfied(multi), unsatisfied(), unsatisfied(), satisfied(dlr), unsatisfied());

    lifecycle.onStart(mock(StartupEvent.class));
    lifecycle.stopBeforeDestroy(new Object());

    verify(multi).close();
    verify(dlr).close();
    assertNull(lifecycle.multiProjectionRunner());
    assertNull(lifecycle.deadLetterRetryRunner());
  }

  @Test
  void shutdownWithoutStartupDoesNotThrow() {
    var lifecycle =
        lifecycle(unsatisfied(), unsatisfied(), unsatisfied(), unsatisfied(), unsatisfied());
    assertDoesNotThrow(() -> lifecycle.onStop(mock(ShutdownEvent.class)));
  }

  @Test
  void shutdownContinuesWhenOneRunnerFailsToClose() {
    var multi = mock(MultiProjectionRunner.class);
    var poller = mock(OutboxPoller.class);
    doThrow(new RuntimeException("boom")).when(poller).close();

    var lifecycle =
        lifecycle(satisfied(multi), unsatisfied(), satisfied(poller), unsatisfied(), unsatisfied());

    lifecycle.onStart(mock(StartupEvent.class));
    assertDoesNotThrow(() -> lifecycle.onStop(mock(ShutdownEvent.class)));
    verify(multi).close();
  }

  @Test
  void outboxRetentionSweeperStartedAndStopped() throws Exception {
    var sweeper = mock(OutboxRetentionSweeper.class);

    var lifecycle =
        lifecycle(
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            satisfied(sweeper),
            unsatisfied(),
            unsatisfied());

    lifecycle.onStart(mock(StartupEvent.class));
    verify(sweeper).start();
    assertSame(sweeper, lifecycle.outboxRetentionSweeper());

    lifecycle.onStop(mock(ShutdownEvent.class));
    verify(sweeper).close();
    assertNull(lifecycle.outboxRetentionSweeper());
  }

  @Test
  void outboxRetentionSweeperSkippedWhenAbsent() {
    var lifecycle =
        lifecycle(
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());

    assertDoesNotThrow(() -> lifecycle.onStart(mock(StartupEvent.class)));
    assertNull(lifecycle.outboxRetentionSweeper());
  }

  @Test
  void sagaDeadLetterRetentionSweeperStartedAndStopped() throws Exception {
    var sweeper = mock(SagaDeadLetterRetentionSweeper.class);

    var lifecycle = lifecycleWithSagaDeadLetterSweeper(satisfied(sweeper));

    lifecycle.onStart(mock(StartupEvent.class));
    verify(sweeper).start();
    assertSame(sweeper, lifecycle.sagaDeadLetterRetentionSweeper());

    lifecycle.onStop(mock(ShutdownEvent.class));
    verify(sweeper).close();
    assertNull(lifecycle.sagaDeadLetterRetentionSweeper());
  }

  @Test
  void sagaDeadLetterRetentionSweeperSkippedWhenAbsent() {
    var lifecycle = lifecycleWithSagaDeadLetterSweeper(unsatisfied());

    assertDoesNotThrow(() -> lifecycle.onStart(mock(StartupEvent.class)));
    assertNull(lifecycle.sagaDeadLetterRetentionSweeper());
  }

  @Test
  void deadLetterRetentionSweeperStartedAndStopped() throws Exception {
    var sweeper = mock(DeadLetterRetentionSweeper.class);

    var lifecycle = lifecycleWithDeadLetterRetentionSweeper(satisfied(sweeper));

    lifecycle.onStart(mock(StartupEvent.class));
    verify(sweeper).start();
    assertSame(sweeper, lifecycle.deadLetterRetentionSweeper());

    lifecycle.onStop(mock(ShutdownEvent.class));
    verify(sweeper).close();
    assertNull(lifecycle.deadLetterRetentionSweeper());
  }

  @Test
  void deadLetterRetentionSweeperSkippedWhenAbsent() {
    var lifecycle = lifecycleWithDeadLetterRetentionSweeper(unsatisfied());

    assertDoesNotThrow(() -> lifecycle.onStart(mock(StartupEvent.class)));
    assertNull(lifecycle.deadLetterRetentionSweeper());
  }
}
