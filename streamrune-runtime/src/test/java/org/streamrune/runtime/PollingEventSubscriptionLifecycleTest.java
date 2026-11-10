package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.lenient;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/** Lifecycle state-machine tests for PollingEventSubscription. */
@ExtendWith(MockitoExtension.class)
class PollingEventSubscriptionLifecycleTest {

  @Mock EventStore eventStore;
  @Mock OffsetStore offsetStore;
  @Mock EventListener listener;

  @BeforeEach
  void stubIdlePolling() {
    lenient().when(offsetStore.getLastOffset(any())).thenReturn(GlobalOffset.initial());
    lenient().when(eventStore.readGlobalStream(any(), anyInt())).thenReturn(List.of());
  }

  private PollingEventSubscription buildSub() {
    return PollingEventSubscription.builder()
        .subscriptionName(SubscriptionName.of("test"))
        .eventStore(eventStore)
        .offsetStore(offsetStore)
        .listener(listener)
        .build();
  }

  // ==================== State assertions ====================

  @Test
  void state_isCreated_beforeStart() {
    var sub = buildSub();
    assertEquals(SubscriptionLifecycleState.CREATED, sub.state());
  }

  @Test
  void state_isRunning_afterStart() {
    try (var sub = buildSub()) {
      sub.start();
      assertEquals(SubscriptionLifecycleState.RUNNING, sub.state());
      assertTrue(sub.isRunning());
    }
  }

  @Test
  void state_isPaused_afterPause() {
    try (var sub = buildSub()) {
      sub.start();
      sub.pause();
      assertEquals(SubscriptionLifecycleState.PAUSED, sub.state());
      assertFalse(sub.isRunning());
    }
  }

  @Test
  void state_isRunning_afterResume() {
    try (var sub = buildSub()) {
      sub.start();
      sub.pause();
      sub.resume();
      assertEquals(SubscriptionLifecycleState.RUNNING, sub.state());
      assertTrue(sub.isRunning());
    }
  }

  @Test
  void resume_keepsRunning_afterStalePrePausePollThreadFullyUnwinds() throws Exception {
    // Regression guard. A pause()->resume() must NOT be silently STOPPED when the lingering
    // pre-pause poll thread finally unwinds. The pre-d6e221a2 bug ran
    // lifecycleState.compareAndSet(RUNNING, STOPPED) in a finally on EVERY poll-thread exit, so the
    // stale thread clobbered the RUNNING state resume() had just set — permanently freezing a
    // healthy subscription. The fix confines the stop-CAS to the catch(Throwable)/Error path. The
    // existing state_isRunning_afterResume reads RUNNING synchronously (before the stale thread
    // unwinds), so it would NOT catch a reintroduced finally-CAS; this test forces the stale thread
    // to unwind AFTER resume() and asserts RUNNING survives.
    var inFirstPoll = new CountDownLatch(1);
    var releaseFirstPoll = new CountDownLatch(1);
    var firstCall = new AtomicBoolean(true);

    OffsetStore blockingOffsetStore =
        new OffsetStore() {
          @Override
          public GlobalOffset getLastOffset(ProjectionName name) {
            if (firstCall.getAndSet(false)) {
              inFirstPoll.countDown();
              // Park the pre-pause poll thread here across pause()+resume(). Swallow pause()'s
              // interrupt so the thread stays parked and unwinds ONLY after the test releases it
              // (which happens after resume()).
              boolean released = false;
              while (!released) {
                try {
                  released = releaseFirstPoll.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException _) {
                  // keep waiting: this stale thread must unwind only post-resume
                }
              }
              // Re-assert interrupt so the loop's next Thread.sleep() exits this stale thread
              // NORMALLY (the every-exit path a finally-CAS would have clobbered on), not via
              // Error.
              Thread.currentThread().interrupt();
            }
            return GlobalOffset.initial();
          }

          @Override
          public void saveOffset(ProjectionName name, GlobalOffset offset) {}
        };

    try (var sub =
        PollingEventSubscription.builder()
            .subscriptionName(SubscriptionName.of("b6-regression"))
            .eventStore(eventStore)
            .offsetStore(blockingOffsetStore)
            .listener(listener)
            .build()) {
      sub.start();
      assertTrue(
          inFirstPoll.await(2, TimeUnit.SECONDS), "poll thread must reach pollOnce before pause");
      sub.pause(); // interrupts the parked thread; it swallows the interrupt and stays parked
      sub.resume(); // sets RUNNING and starts a fresh poll thread
      releaseFirstPoll.countDown(); // let the stale thread unwind AFTER resume()

      // The stale thread now exits normally while state==RUNNING. Assert RUNNING is NOT clobbered
      // across a window covering the unwind; a reintroduced every-exit CAS would flip it to
      // STOPPED.
      for (int i = 0; i < 60; i++) {
        assertEquals(
            SubscriptionLifecycleState.RUNNING,
            sub.state(),
            "resumed subscription must stay RUNNING after the stale poll thread unwinds");
        assertTrue(sub.isRunning());
        Thread.sleep(10);
      }
    }
  }

  @Test
  void state_isStopped_afterClose() {
    var sub = buildSub();
    sub.start();
    sub.close();
    assertEquals(SubscriptionLifecycleState.STOPPED, sub.state());
    assertFalse(sub.isRunning());
  }

  @Test
  void state_isStopped_afterClose_fromPaused() {
    var sub = buildSub();
    sub.start();
    sub.pause();
    sub.close();
    assertEquals(SubscriptionLifecycleState.STOPPED, sub.state());
    assertFalse(sub.isRunning());
  }

  // ==================== pause() guards ====================

  @Test
  void pause_throwsIllegalStateException_whenCreated() {
    var sub = buildSub();
    var ex = assertThrows(IllegalStateException.class, sub::pause);
    assertTrue(
        ex.getMessage().contains("CREATED"), "message should contain CREATED: " + ex.getMessage());
  }

  @Test
  void pause_throwsIllegalStateException_whenAlreadyPaused() {
    try (var sub = buildSub()) {
      sub.start();
      sub.pause();
      var ex = assertThrows(IllegalStateException.class, sub::pause);
      assertTrue(
          ex.getMessage().contains("PAUSED"), "message should contain PAUSED: " + ex.getMessage());
    }
  }

  @Test
  void pause_throwsIllegalStateException_whenStopped() {
    var sub = buildSub();
    sub.start();
    sub.close();
    var ex = assertThrows(IllegalStateException.class, sub::pause);
    assertTrue(
        ex.getMessage().contains("STOPPED"), "message should contain STOPPED: " + ex.getMessage());
  }

  // ==================== resume() guards ====================

  @Test
  void resume_throwsIllegalStateException_whenRunning() {
    try (var sub = buildSub()) {
      sub.start();
      var ex = assertThrows(IllegalStateException.class, sub::resume);
      assertTrue(
          ex.getMessage().contains("RUNNING"),
          "message should contain RUNNING: " + ex.getMessage());
    }
  }

  @Test
  void resume_throwsIllegalStateException_whenStopped() {
    var sub = buildSub();
    sub.start();
    sub.close();
    var ex = assertThrows(IllegalStateException.class, sub::resume);
    assertTrue(
        ex.getMessage().contains("STOPPED"), "message should contain STOPPED: " + ex.getMessage());
  }

  // ==================== start() guard ====================

  @Test
  void start_throwsIllegalStateException_whenAlreadyRunning() {
    try (var sub = buildSub()) {
      sub.start();
      assertThrows(IllegalStateException.class, sub::start);
    }
  }

  @Test
  void close_isIdempotent() {
    var sub = buildSub();
    sub.start();
    sub.close();
    // Second close() must not throw
    sub.close();
    assertEquals(SubscriptionLifecycleState.STOPPED, sub.state());
  }

  @Test
  void start_throwsIllegalStateException_whenStopped() {
    var sub = buildSub();
    sub.start();
    sub.close();
    var ex = assertThrows(IllegalStateException.class, sub::start);
    assertTrue(
        ex.getMessage().contains("STOPPED"), "message should contain STOPPED: " + ex.getMessage());
  }
}
