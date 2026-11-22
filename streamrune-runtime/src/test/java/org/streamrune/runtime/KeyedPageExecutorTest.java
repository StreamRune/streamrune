package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneContext.DispatchCause;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

class KeyedPageExecutorTest {

  private static final PageStopRule NEVER = (failure, n) -> false;
  private static final PageStopRule ALWAYS = (failure, n) -> true;

  private final KeyedPageExecutor executor =
      new KeyedPageExecutor("test-page-", StreamRuneTracing.NOOP);

  /** Two latches per item: the step signals it entered, then waits to be released. */
  private static final class Gate {
    final CountDownLatch entered = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    void pass() {
      entered.countDown();
      await(release);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS), "timed out waiting for a latch");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static Map<Integer, Gate> gates(int n) {
    Map<Integer, Gate> gates = new ConcurrentHashMap<>();
    for (int i = 0; i < n; i++) {
      gates.put(i, new Gate());
    }
    return gates;
  }

  private static List<Integer> range(int n) {
    return IntStream.range(0, n).boxed().toList();
  }

  @Test
  void itemsOfDifferentKeysRunAtTheSameTime() throws Exception {
    var gates = gates(2);
    var result = new AtomicReference<PageResult>();
    Thread caller =
        Thread.ofPlatform()
            .start(
                () ->
                    result.set(
                        executor.run(
                            range(2),
                            i -> i,
                            2,
                            i -> gates.get(i).pass(),
                            NEVER,
                            PageObserver.NONE)));
    await(gates.get(0).entered);
    await(gates.get(1).entered);
    gates.values().forEach(g -> g.release.countDown());
    caller.join(5000);
    assertNull(result.get().failure());
    assertEquals(2, result.get().items());
  }

  @Test
  void itemsOfOneKeyRunInListOrderAndNeverOverlap() {
    List<Integer> items = range(30);
    List<Integer> order = new CopyOnWriteArrayList<>();
    Map<Integer, AtomicInteger> active = new ConcurrentHashMap<>();
    AtomicInteger maxPerKey = new AtomicInteger();
    var result =
        executor.run(
            items,
            i -> i % 3,
            3,
            i -> {
              int now = active.computeIfAbsent(i % 3, k -> new AtomicInteger()).incrementAndGet();
              maxPerKey.accumulateAndGet(now, Math::max);
              order.add(i);
              Thread.yield();
              active.get(i % 3).decrementAndGet();
            },
            NEVER,
            PageObserver.NONE);
    assertNull(result.failure());
    assertEquals(1, maxPerKey.get());
    for (int key = 0; key < 3; key++) {
      final int k = key;
      List<Integer> ofKey = order.stream().filter(i -> i % 3 == k).toList();
      assertEquals(items.stream().filter(i -> i % 3 == k).toList(), ofKey);
    }
  }

  @Test
  void neverMoreThanParallelismStepsInFlight() {
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger max = new AtomicInteger();
    executor.run(
        range(20),
        i -> i,
        3,
        i -> {
          max.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
          sleep(20);
          inFlight.decrementAndGet();
        },
        NEVER,
        PageObserver.NONE);
    assertEquals(3, max.get());
  }

  @Test
  void groupsStartInTheOrderOfTheirFirstItem() throws Exception {
    var gates = gates(4);
    List<Integer> started = new CopyOnWriteArrayList<>();
    var done = new AtomicReference<PageResult>();
    Thread caller =
        Thread.ofPlatform()
            .start(
                () ->
                    done.set(
                        executor.run(
                            range(4),
                            i -> String.valueOf("dcba".charAt(i)),
                            2,
                            i -> {
                              started.add(i);
                              gates.get(i).pass();
                            },
                            NEVER,
                            PageObserver.NONE)));
    await(gates.get(0).entered);
    await(gates.get(1).entered);
    assertEquals(Set.of(0, 1), new HashSet<>(started));
    assertFalse(gates.get(2).entered.await(100, TimeUnit.MILLISECONDS));
    gates.values().forEach(g -> g.release.countDown());
    caller.join(5000);
    assertEquals(4, started.size());
    assertEquals(Set.of(0, 1), new HashSet<>(started.subList(0, 2)));
    assertEquals(Set.of(2, 3), new HashSet<>(started.subList(2, 4)));
  }

  @Test
  void aPageOfOneKeyRunsOnTheCallingThread() {
    Set<Thread> threads = Collections.synchronizedSet(new HashSet<>());
    List<Integer> order = new ArrayList<>();
    executor.run(
        range(3),
        i -> "same",
        4,
        i -> {
          threads.add(Thread.currentThread());
          order.add(i);
        },
        NEVER,
        PageObserver.NONE);
    assertEquals(Set.of(Thread.currentThread()), threads);
    assertEquals(List.of(0, 1, 2), order);
  }

  @Test
  void parallelismOneRunsOnTheCallingThread_andEndsAtTheFirstFailure() {
    Set<Thread> threads = Collections.synchronizedSet(new HashSet<>());
    List<Integer> ran = new ArrayList<>();
    AtomicBoolean asked = new AtomicBoolean();
    var result =
        executor.run(
            range(5),
            i -> i,
            1,
            i -> {
              threads.add(Thread.currentThread());
              ran.add(i);
              if (i == 2) {
                throw new IllegalArgumentException("boom");
              }
            },
            (f, n) -> {
              asked.set(true);
              return false;
            },
            PageObserver.NONE);
    assertEquals(Set.of(Thread.currentThread()), threads);
    assertEquals(List.of(0, 1, 2), ran);
    assertFalse(asked.get());
    assertEquals(1, result.failed());
    assertEquals(2, result.notStarted());
    assertNull(result.stoppedBy());
    assertThrows(IllegalArgumentException.class, result::rethrow);
  }

  @Test
  void anEmptyPageReturnsAnEmptyResult() {
    var result = executor.run(List.<Integer>of(), i -> i, 4, i -> {}, NEVER, PageObserver.NONE);
    assertEquals(0, result.items());
    assertEquals(0, result.groups());
    assertEquals(0, result.failed());
    assertEquals(0, result.notStarted());
    assertNull(result.failure());
    assertNull(result.stoppedBy());
    assertFalse(result.interrupted());
    result.rethrow();
  }

  @Test
  void keysAreComparedByEquals() {
    AtomicInteger active = new AtomicInteger();
    AtomicInteger max = new AtomicInteger();
    List<String> keys = List.of(new String("a"), new String("a"), new String("a"));
    var result =
        executor.run(
            keys,
            k -> k,
            3,
            k -> {
              max.accumulateAndGet(active.incrementAndGet(), Math::max);
              sleep(10);
              active.decrementAndGet();
            },
            NEVER,
            PageObserver.NONE);
    assertEquals(1, result.groups());
    assertEquals(1, max.get());
  }

  @Test
  void aFailedItemEndsItsGroupOnly() {
    // keys: 0 -> items 0,3,6,9 ; 1 -> 1,4,7 ; 2 -> 2,5,8
    List<Integer> ran = new CopyOnWriteArrayList<>();
    var result =
        executor.run(
            range(10),
            i -> i % 3,
            3,
            i -> {
              ran.add(i);
              if (i == 3) {
                throw new IllegalStateException("item 3");
              }
            },
            (f, n) -> false,
            PageObserver.NONE);
    assertEquals(1, result.failed());
    assertEquals(2, result.notStarted());
    assertFalse(ran.contains(6));
    assertFalse(ran.contains(9));
    assertEquals(8, ran.size());
    assertNull(result.stoppedBy());
    assertEquals("item 3", result.failure().getMessage());
  }

  @Test
  void theRuleStopsThePage() throws Exception {
    var gates = gates(4);
    var done = new AtomicReference<PageResult>();
    List<Integer> started = new CopyOnWriteArrayList<>();
    IllegalStateException boom = new IllegalStateException("boom");
    AtomicReference<Thread> failing = new AtomicReference<>();
    CountDownLatch failedItemEnded = new CountDownLatch(1);
    PageObserver observer =
        (inFlight, notYetStarted) -> {
          if (Thread.currentThread() == failing.get()) {
            failedItemEnded.countDown();
          }
        };
    Thread caller =
        Thread.ofPlatform()
            .start(
                () ->
                    done.set(
                        executor.run(
                            range(4),
                            i -> i % 2,
                            2,
                            i -> {
                              started.add(i);
                              if (i == 0) {
                                await(gates.get(1).entered);
                                failing.set(Thread.currentThread());
                                throw boom;
                              }
                              gates.get(i).pass();
                            },
                            ALWAYS,
                            observer)));
    // the observer's step-ended call for item 0 comes after the page is marked stopped
    await(failedItemEnded);
    assertFalse(
        caller.join(Duration.ofMillis(100)),
        "item 1 is in flight and must finish before run returns");
    gates.get(1).release.countDown();
    caller.join(5000);
    PageResult result = done.get();
    assertNotNull(result);
    assertEquals(List.of(0, 1), started.stream().sorted().toList());
    assertSame(boom, result.stoppedBy());
    assertEquals(2, result.notStarted());
    assertEquals(1, result.failed());
  }

  @Test
  void theRuleIsAskedWithTheRunningFailureCount() {
    List<Integer> counts = new CopyOnWriteArrayList<>();
    executor.run(
        range(3),
        i -> i,
        3,
        i -> {
          throw new IllegalStateException("f" + i);
        },
        (f, n) -> {
          counts.add(n);
          return false;
        },
        PageObserver.NONE);
    assertEquals(List.of(1, 2, 3), counts.stream().sorted().toList());
  }

  @Test
  void theThrownFailureIsTheLowestPositionWithTheOthersSuppressed() {
    CountDownLatch allIn = new CountDownLatch(3);
    var result =
        executor.run(
            range(3),
            i -> i,
            3,
            i -> {
              allIn.countDown();
              await(allIn);
              // the lowest position fails last
              sleep((3 - i) * 30L);
              throw new IllegalStateException("f" + i);
            },
            NEVER,
            PageObserver.NONE);
    assertEquals("f0", result.failure().getMessage());
    assertEquals(2, result.failure().getSuppressed().length);
    assertEquals(3, result.failed());
  }

  @Test
  void atMostEightFailuresAreSuppressed() {
    var result =
        executor.run(
            range(12),
            i -> i,
            4,
            i -> {
              throw new IllegalStateException("f" + i);
            },
            NEVER,
            PageObserver.NONE);
    assertEquals(12, result.failed());
    assertEquals(KeyedPageExecutor.MAX_SUPPRESSED, result.failure().getSuppressed().length);
    assertEquals(8, result.failure().getSuppressed().length);
  }

  @Test
  void anErrorWinsOverAnException_andStopsThePage() {
    CountDownLatch bothIn = new CountDownLatch(2);
    AssertionError error = new AssertionError("error");
    AtomicReference<Thread> erring = new AtomicReference<>();
    CountDownLatch errorItemEnded = new CountDownLatch(1);
    // the observer's step-ended call for the Error's item comes after the page is marked stopped
    PageObserver observer =
        (inFlight, notYetStarted) -> {
          if (Thread.currentThread() == erring.get()) {
            errorItemEnded.countDown();
          }
        };
    var result =
        executor.run(
            range(4),
            i -> i,
            2,
            i -> {
              bothIn.countDown();
              await(bothIn);
              if (i == 0) {
                await(errorItemEnded);
                throw new IllegalStateException("exception at the lowest position");
              }
              erring.set(Thread.currentThread());
              throw error;
            },
            NEVER,
            observer);
    assertSame(error, result.failure());
    assertSame(error, result.stoppedBy());
    assertEquals(1, result.failure().getSuppressed().length);
    assertEquals(2, result.notStarted());
    assertEquals(1, result.failurePosition());
    assertThrows(AssertionError.class, result::rethrow);
  }

  @Test
  void theCallerInterruptedWhileWaitingStopsThePage_waitsForItemsInFlight_andKeepsItsInterrupt()
      throws Exception {
    var gates = gates(4);
    Map<Integer, String> workerOf = new ConcurrentHashMap<>();
    List<Boolean> workerSawInterrupt = new CopyOnWriteArrayList<>();
    List<Integer> started = new CopyOnWriteArrayList<>();
    var done = new AtomicReference<PageResult>();
    var callerInterruptedAfter = new AtomicBoolean();
    Thread caller =
        Thread.ofPlatform()
            .start(
                () -> {
                  done.set(
                      executor.run(
                          range(4),
                          i -> i,
                          2,
                          i -> {
                            started.add(i);
                            workerOf.put(i, Thread.currentThread().getName());
                            gates.get(i).pass();
                            workerSawInterrupt.add(Thread.currentThread().isInterrupted());
                          },
                          NEVER,
                          PageObserver.NONE));
                  callerInterruptedAfter.set(Thread.interrupted());
                });
    await(gates.get(0).entered);
    await(gates.get(1).entered);
    caller.interrupt();
    awaitWaitingAgainAfterItsInterrupt(caller);
    assertNull(done.get(), "run must wait for the items in flight");
    // the caller waits for its workers in start order and the interrupt cut its first wait short:
    // let the other worker end first, run must still wait for the first one
    int heldByFirstWorker = workerOf.get(0).equals("test-page-0") ? 0 : 1;
    gates.get(1 - heldByFirstWorker).release.countDown();
    Thread.sleep(100);
    assertNull(done.get(), "run must keep waiting for the worker whose wait was interrupted");
    gates.get(heldByFirstWorker).release.countDown();
    caller.join(5000);
    PageResult result = done.get();
    assertNotNull(result);
    assertTrue(result.interrupted());
    assertNull(result.stoppedBy());
    assertInstanceOf(IllegalStateException.class, result.failure());
    assertEquals(
        "Interrupted while a page of saga steps was in flight", result.failure().getMessage());
    assertTrue(callerInterruptedAfter.get());
    assertEquals(List.of(false, false), workerSawInterrupt);
    assertEquals(2, started.size());
    assertEquals(0, result.failed());
    assertEquals(2, result.notStarted());
    assertEquals(-1, result.failurePosition());
  }

  @Test
  void aWorkerRunsUnderTheCallersTraceContextRequestContextAndCause() {
    ScopedValue<String> marker = ScopedValue.newInstance();
    Thread testThread = Thread.currentThread();
    AtomicReference<Thread> propagatingCalledOn = new AtomicReference<>();
    StreamRuneTracing tracing =
        new StreamRuneTracing() {
          @Override
          public boolean enabled(TracingArea area) {
            return false;
          }

          @Override
          public TraceScope start(
              TracingArea area, String spanName, SpanKind kind, SpanAttributes attributes) {
            return TraceScope.NOOP;
          }

          @Override
          public TraceScope start(
              TracingArea area,
              String spanName,
              SpanKind kind,
              SpanAttributes attributes,
              SpanLink link,
              SpanBaggage baggage) {
            return TraceScope.NOOP;
          }

          @Override
          public Runnable propagating(Runnable task) {
            propagatingCalledOn.set(Thread.currentThread());
            return () -> ScopedValue.where(marker, "traced").run(task);
          }
        };
    var context =
        new RequestContext(
            TraceId.of("trace-1"),
            UserId.of("user-1"),
            CorrelationId.of("corr-1"),
            Instant.now(),
            Map.of());
    var cause = new DispatchCause(CausationId.of("cause-1"), null);
    List<String> markers = new CopyOnWriteArrayList<>();
    List<RequestContext> contexts = new CopyOnWriteArrayList<>();
    List<DispatchCause> causes = new CopyOnWriteArrayList<>();
    List<Thread> threads = new CopyOnWriteArrayList<>();
    var result =
        ScopedValue.where(StreamRuneContext.CURRENT, context)
            .where(StreamRuneContext.CAUSE, cause)
            .call(
                () ->
                    new KeyedPageExecutor("trace-test-", tracing)
                        .run(
                            range(4),
                            i -> i,
                            2,
                            i -> {
                              threads.add(Thread.currentThread());
                              markers.add(marker.isBound() ? marker.get() : "unbound");
                              contexts.add(StreamRuneContext.capture());
                              causes.add(StreamRuneContext.cause());
                            },
                            NEVER,
                            PageObserver.NONE));
    assertNull(result.failure());
    assertSame(testThread, propagatingCalledOn.get());
    assertFalse(threads.contains(testThread));
    assertEquals(List.of("traced", "traced", "traced", "traced"), markers);
    assertTrue(contexts.stream().allMatch(c -> c == context));
    assertTrue(causes.stream().allMatch(c -> c == cause));
  }

  @Test
  void aPlainStringKeyKeepsOrderAndBound() {
    List<String> items = List.of("x1", "y1", "x2", "z1", "y2", "x3", "z2", "y3");
    List<String> order = new CopyOnWriteArrayList<>();
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger max = new AtomicInteger();
    Map<String, AtomicInteger> perKey = new ConcurrentHashMap<>();
    AtomicInteger maxPerKey = new AtomicInteger();
    executor.run(
        items,
        s -> s.substring(0, 1),
        2,
        s -> {
          max.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
          maxPerKey.accumulateAndGet(
              perKey.computeIfAbsent(s.substring(0, 1), k -> new AtomicInteger()).incrementAndGet(),
              Math::max);
          order.add(s);
          sleep(10);
          perKey.get(s.substring(0, 1)).decrementAndGet();
          inFlight.decrementAndGet();
        },
        NEVER,
        PageObserver.NONE);
    assertTrue(max.get() <= 2);
    assertEquals(1, maxPerKey.get());
    for (String key : List.of("x", "y", "z")) {
      assertEquals(
          items.stream().filter(s -> s.startsWith(key)).toList(),
          order.stream().filter(s -> s.startsWith(key)).toList());
    }
  }

  @Test
  void theObserverSeesInFlightAndNotYetStarted() {
    List<int[]> seen = Collections.synchronizedList(new ArrayList<>());
    executor.run(
        range(6),
        i -> i,
        2,
        i -> sleep(10),
        NEVER,
        (inFlight, notYetStarted) -> seen.add(new int[] {inFlight, notYetStarted}));
    int[] last = seen.getLast();
    assertEquals(0, last[0]);
    assertEquals(0, last[1]);
    assertEquals(2, seen.stream().mapToInt(a -> a[0]).max().orElseThrow());
    assertEquals(12, seen.size());
    assertEquals(5, seen.getFirst()[1]);
  }

  @Test
  void resultCountsItemsGroupsAndTimes() {
    var result = executor.run(range(6), i -> i % 3, 3, i -> sleep(15), NEVER, PageObserver.NONE);
    assertEquals(6, result.items());
    assertEquals(3, result.groups());
    assertEquals(0, result.failed());
    assertEquals(0, result.notStarted());
    assertTrue(result.stepNanos() >= TimeUnit.MILLISECONDS.toNanos(6 * 15 - 6));
    assertTrue(result.wallNanos() > 0);
    result.rethrow();
  }

  @Test
  void theThrownFailureNamesItsPosition() {
    var result =
        executor.run(
            range(6),
            i -> i,
            3,
            i -> {
              if (i == 4 || i == 2) {
                throw new IllegalStateException("f" + i);
              }
            },
            NEVER,
            PageObserver.NONE);
    assertEquals(2, result.failurePosition());
    assertEquals("f2", result.failure().getMessage());
    var clean = executor.run(range(2), i -> i, 2, i -> {}, NEVER, PageObserver.NONE);
    assertEquals(-1, clean.failurePosition());
  }

  @Test
  void theStopDecisionComesBeforeTheObserversStepEndedCall() {
    AtomicReference<Thread> failing = new AtomicReference<>();
    AtomicBoolean ruleDecided = new AtomicBoolean();
    AtomicReference<Boolean> decidedAtEnd = new AtomicReference<>();
    PageObserver observer =
        (inFlight, notYetStarted) -> {
          if (Thread.currentThread() == failing.get()) {
            decidedAtEnd.set(ruleDecided.get());
          }
        };
    executor.run(
        range(2),
        i -> i,
        2,
        i -> {
          if (i == 0) {
            failing.set(Thread.currentThread());
            throw new IllegalStateException("boom");
          }
        },
        (f, n) -> {
          ruleDecided.set(true);
          return true;
        },
        observer);
    assertEquals(Boolean.TRUE, decidedAtEnd.get());
  }

  @Test
  void aFailureOfTheObserverAtStepStartFailsThePageInsteadOfLosingItems() {
    for (Throwable thrown :
        List.<Throwable>of(new IllegalStateException("obs"), new Error("obs"))) {
      AtomicInteger ran = new AtomicInteger();
      var result =
          executor.run(
              range(8),
              i -> i % 4,
              4,
              i -> ran.incrementAndGet(),
              NEVER,
              (inFlight, notYetStarted) -> {
                throw sneaky(thrown);
              });
      assertEquals(0, ran.get());
      assertSame(thrown, result.failure());
      assertSame(thrown, result.stoppedBy());
      assertEquals(8, result.notStarted());
      assertThrows(thrown.getClass(), result::rethrow);
    }
  }

  @Test
  void aFailureOfTheObserverAtStepEndFailsThePage() {
    for (Throwable thrown :
        List.<Throwable>of(new IllegalStateException("obs"), new Error("obs"))) {
      AtomicInteger last = new AtomicInteger();
      AtomicInteger ran = new AtomicInteger();
      var result =
          executor.run(
              range(8),
              i -> i % 4,
              4,
              i -> ran.incrementAndGet(),
              NEVER,
              (inFlight, notYetStarted) -> {
                // the calls are ordered: a count below the one of the call before is a step ended
                if (last.getAndSet(inFlight) > inFlight) {
                  throw sneaky(thrown);
                }
              });
      assertSame(thrown, result.failure());
      assertSame(thrown, result.stoppedBy());
      assertTrue(ran.get() >= 1, "the step whose end call failed did run");
      assertThrows(thrown.getClass(), result::rethrow);
    }
  }

  @Test
  void aFailureOfTheObserverOnTheInlinePathFailsThePage() {
    var result =
        executor.run(
            range(3),
            i -> "same",
            4,
            i -> {},
            NEVER,
            (inFlight, notYetStarted) -> {
              throw new IllegalStateException("obs");
            });
    assertEquals("obs", result.failure().getMessage());
    assertEquals(0, result.failurePosition());
  }

  @Test
  void aStopRuleThatThrowsStillFailsThePage() {
    IllegalStateException stepFailure = new IllegalStateException("step");
    IllegalArgumentException ruleFailure = new IllegalArgumentException("rule");
    var result =
        executor.run(
            range(4),
            i -> i,
            2,
            i -> {
              if (i == 0) {
                throw stepFailure;
              }
            },
            (f, n) -> {
              throw ruleFailure;
            },
            PageObserver.NONE);
    assertEquals(1, result.failed());
    assertSame(stepFailure, result.failure());
    assertSame(ruleFailure, result.stoppedBy());
    assertThrows(IllegalStateException.class, result::rethrow);
  }

  @Test
  void oneExceptionInstanceThrownByTwoItemsIsNotSuppressedIntoItself() {
    IllegalStateException shared = new IllegalStateException("shared");
    var result =
        executor.run(
            range(2),
            i -> i,
            2,
            i -> {
              throw shared;
            },
            NEVER,
            PageObserver.NONE);
    assertSame(shared, result.failure());
    assertEquals(2, result.failed());
    assertEquals(0, shared.getSuppressed().length);
  }

  @Test
  void aParallelismBelowOneIsRefused() {
    assertThrows(
        IllegalArgumentException.class,
        () -> executor.run(range(1), i -> i, 0, i -> {}, NEVER, PageObserver.NONE));
  }

  @Test
  void aWorkerThatCannotStartLeavesNoWorkerRunning() throws Exception {
    // the second thread is already finished, so its start() throws IllegalThreadStateException
    CountDownLatch startGate = new CountDownLatch(1);
    AtomicBoolean firstWorkerEnded = new AtomicBoolean();
    KeyedPageExecutor.WorkerThreads factory =
        (n, task) -> {
          if (n == 0) {
            return Thread.ofVirtual()
                .unstarted(
                    () -> {
                      await(startGate);
                      task.run();
                      firstWorkerEnded.set(true);
                    });
          }
          Thread dead = Thread.ofVirtual().start(() -> {});
          try {
            dead.join();
          } catch (InterruptedException e) {
            throw new IllegalStateException(e);
          }
          return dead;
        };
    Thread.ofPlatform()
        .start(
            () -> {
              sleep(200);
              startGate.countDown();
            });
    AtomicInteger ran = new AtomicInteger();
    assertThrows(
        IllegalThreadStateException.class,
        () ->
            new KeyedPageExecutor(factory, StreamRuneTracing.NOOP)
                .run(range(2), i -> i, 2, i -> ran.incrementAndGet(), NEVER, PageObserver.NONE));
    assertTrue(firstWorkerEnded.get(), "run must wait for the worker that did start");
    assertEquals(0, ran.get(), "the page was stopped before the started worker took an item");
  }

  @Test
  void aWorkerWhoseTaskIsDroppedFailsThePage() {
    // a wrapper that never runs its task: the thread ends at once and runs no item
    KeyedPageExecutor.WorkerThreads dropping = (n, task) -> Thread.ofVirtual().unstarted(() -> {});
    AtomicInteger ran = new AtomicInteger();

    PageResult result =
        new KeyedPageExecutor(dropping, StreamRuneTracing.NOOP)
            .run(range(3), i -> i, 4, i -> ran.incrementAndGet(), NEVER, PageObserver.NONE);

    assertEquals(0, ran.get());
    assertEquals(3, result.notStarted());
    assertInstanceOf(IllegalStateException.class, result.failure());
    assertTrue(result.failure().getMessage().startsWith("3 of 3 worker(s)"));
    assertEquals(-1, result.failurePosition());
    assertEquals(0, result.failed(), "no item failed");
  }

  @Test
  void aWorkerThatDiesWhileItRecordsAFailureFailsThePage() {
    // the recording of a step's failure throws an Error (an out-of-memory error, say), and so does
    // the worker's own recording of that Error: the worker ends with nothing recorded
    IllegalStateException boom = new IllegalStateException("boom");
    KeyedPageExecutor.FailureRecording failing =
        (position, failure) -> {
          if (failure == boom || failure instanceof OutOfMemoryError) {
            throw new OutOfMemoryError("recording failed");
          }
        };
    KeyedPageExecutor.WorkerThreads quiet =
        (n, task) ->
            Thread.ofVirtual()
                .name("dying-" + n)
                .uncaughtExceptionHandler((t, e) -> {})
                .unstarted(task);
    AtomicInteger ran = new AtomicInteger();

    PageResult result =
        new KeyedPageExecutor(quiet, StreamRuneTracing.NOOP, failing)
            .run(
                range(2),
                i -> i,
                2,
                i -> {
                  ran.incrementAndGet();
                  if (i == 0) {
                    throw boom;
                  }
                },
                NEVER,
                PageObserver.NONE);

    assertEquals(2, ran.get());
    assertInstanceOf(IllegalStateException.class, result.failure());
    assertTrue(
        result.failure().getMessage().startsWith("1 of 2 worker(s)"),
        result.failure().getMessage());
    assertEquals(-1, result.failurePosition());
  }

  @Test
  void aWorkerWhoseTaskIsHandedOffFailsThePage_andTheTaskStartsNoItemWhenItRunsLater()
      throws Exception {
    CountDownLatch handedOff = new CountDownLatch(1);
    List<Thread> strays = new java.util.concurrent.CopyOnWriteArrayList<>();
    KeyedPageExecutor.WorkerThreads handingOff =
        (n, task) ->
            Thread.ofVirtual()
                .unstarted(
                    () ->
                        strays.add(
                            Thread.ofVirtual()
                                .start(
                                    () -> {
                                      await(handedOff);
                                      task.run();
                                    })));
    AtomicInteger ran = new AtomicInteger();
    AtomicInteger observed = new AtomicInteger();

    PageResult result =
        new KeyedPageExecutor(handingOff, StreamRuneTracing.NOOP)
            .run(
                range(6),
                i -> i,
                2,
                i -> ran.incrementAndGet(),
                NEVER,
                (inFlight, notYetStarted) -> observed.incrementAndGet());
    handedOff.countDown();
    for (Thread stray : strays) {
      stray.join(5000);
    }

    assertInstanceOf(IllegalStateException.class, result.failure());
    assertTrue(result.failure().getMessage().startsWith("2 of 2 worker(s)"));
    assertEquals(6, result.notStarted());
    assertEquals(0, ran.get(), "a stray task starts no item of the failed page");
    assertEquals(0, observed.get(), "nor reports a step");
  }

  @Test
  void aRuleThatThrowsStopsThePageBeforeTheEndOfTheStepIsReported() {
    List<String> calls = new java.util.concurrent.CopyOnWriteArrayList<>();
    PageStopRule throwing =
        (failure, n) -> {
          throw new IllegalStateException("rule down");
        };
    // keys 0 and 1 start together; 0 is let go, fails, and its rule throws; 1 is held until the end
    // call of 0, when the page must already be stopped, so 1's worker takes no further key
    var gates = gates(2);
    PageResult result =
        executor.run(
            range(4),
            i -> i,
            2,
            i -> {
              if (i < 2) {
                gates.get(i).pass();
              }
              if (i == 0) {
                throw new IllegalArgumentException("step 0");
              }
            },
            throwing,
            (inFlight, notYetStarted) -> {
              calls.add(inFlight + "/" + notYetStarted);
              if (calls.size() == 2) {
                gates.get(0).release.countDown(); // both started
              } else if (calls.size() == 3) {
                gates.get(1).release.countDown(); // the end of 0
              }
            });

    assertInstanceOf(IllegalArgumentException.class, result.failure());
    assertEquals("rule down", result.stoppedBy().getMessage());
    assertEquals(2, result.notStarted(), "keys 2 and 3 never started");
    assertEquals(1, result.failed());
  }

  @Test
  void runReturnsOnlyWhenEveryWorkerEnded() throws Exception {
    // once with the worker that started first ending last, once with it ending first
    for (int endsLast = 0; endsLast < 2; endsLast++) {
      var page = new KeyedPageExecutor("barrier-", StreamRuneTracing.NOOP);
      var gates = gates(2);
      Map<String, Integer> itemOf = new ConcurrentHashMap<>();
      var done = new AtomicReference<PageResult>();
      Thread caller =
          Thread.ofPlatform()
              .start(
                  () ->
                      done.set(
                          page.run(
                              range(2),
                              i -> i,
                              2,
                              i -> {
                                itemOf.put(Thread.currentThread().getName(), i);
                                gates.get(i).pass();
                              },
                              NEVER,
                              PageObserver.NONE)));
      await(gates.get(0).entered);
      await(gates.get(1).entered);
      int last = itemOf.get("barrier-" + endsLast);
      gates.get(1 - last).release.countDown();
      assertFalse(
          caller.join(Duration.ofMillis(100)), "run must wait for the worker still in a step");
      gates.get(last).release.countDown();
      assertTrue(caller.join(Duration.ofSeconds(5)));
      assertNull(done.get().failure());
    }
  }

  @Test
  void workerThreadsAreNamedFromZeroInEveryPage() {
    for (int page = 0; page < 2; page++) {
      Set<String> names = Collections.synchronizedSet(new HashSet<>());
      CountDownLatch allIn = new CountDownLatch(3);
      executor.run(
          range(3),
          i -> i,
          3,
          i -> {
            names.add(Thread.currentThread().getName());
            allIn.countDown();
            await(allIn);
          },
          NEVER,
          PageObserver.NONE);
      assertEquals(Set.of("test-page-0", "test-page-1", "test-page-2"), names);
    }
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> RuntimeException sneaky(Throwable t) throws E {
    throw (E) t;
  }

  /**
   * Waits until {@code caller}, interrupted while it waited for its workers, has taken the
   * interrupt (its flag is cleared when its wait throws) and waits again: it stopped the page in
   * between, so a worker released now starts no further item.
   */
  private static void awaitWaitingAgainAfterItsInterrupt(Thread caller) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (caller.isInterrupted() || caller.getState() != Thread.State.WAITING) {
      assertTrue(System.nanoTime() < deadline, "the caller did not take its interrupt");
      Thread.onSpinWait();
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
