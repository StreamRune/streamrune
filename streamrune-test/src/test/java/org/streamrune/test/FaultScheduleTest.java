package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class FaultScheduleTest {

  private static final Set<String> METHODS = Set.of("append", "load");

  /** One call through the schedule as a decorator makes it; returns what the delegate saw. */
  private static List<String> callThrough(FaultSchedule schedule, String method) {
    List<String> delegateCalls = new ArrayList<>();
    schedule.call(
        method,
        () -> {
          delegateCalls.add(method);
          return "result";
        });
    return delegateCalls;
  }

  @Test
  void none_failsNothing_andCountsEveryCall() {
    var schedule = FaultSchedule.none().forMethods(METHODS);

    assertEquals("result", schedule.call("append", () -> "result"));
    schedule.run("append", () -> {});
    schedule.call("load", () -> 1);

    assertEquals(2, schedule.calls("append"));
    assertEquals(1, schedule.calls("load"));
    assertEquals(3, schedule.calls());
  }

  @Test
  void failOnce_throwsOnTheNextCallOnly_beforeTheDelegateRuns() {
    var schedule = FaultSchedule.none().forMethods(METHODS);
    schedule.call("append", () -> "first");
    schedule.failOnce("append");

    List<String> seen = new ArrayList<>();
    var failure =
        assertThrows(
            InjectedFaultException.class,
            () ->
                schedule.call(
                    "append",
                    () -> {
                      seen.add("append");
                      return "second";
                    }));

    assertTrue(seen.isEmpty(), "the delegate must not run: " + seen);
    assertTrue(failure.getMessage().startsWith("injected: append"), failure.getMessage());
    assertEquals(List.of("append"), callThrough(schedule, "append"));
    assertEquals(3, schedule.calls("append"), "a failed call is counted too");
  }

  @Test
  void failOnce_twice_failsTheNextTwoCalls() {
    var schedule = FaultSchedule.none().forMethods(METHODS).failOnce("load").failOnce("load");

    assertThrows(InjectedFaultException.class, () -> callThrough(schedule, "load"));
    assertThrows(InjectedFaultException.class, () -> callThrough(schedule, "load"));
    assertEquals(List.of("load"), callThrough(schedule, "load"));
  }

  @Test
  void failAt_throwsOnTheNthCall_countedFromTheFirstCall() {
    var schedule = FaultSchedule.none().forMethods(METHODS).failAt("append", 3);

    assertEquals(List.of("append"), callThrough(schedule, "append"));
    assertEquals(List.of("append"), callThrough(schedule, "append"));
    var failure = assertThrows(InjectedFaultException.class, () -> callThrough(schedule, "append"));
    assertTrue(failure.getMessage().contains("call 3"), failure.getMessage());
    assertEquals(List.of("append"), callThrough(schedule, "append"));
  }

  @Test
  void failAt_refusesACallThatHappenedAlready_andANumberBelowOne() {
    var schedule = FaultSchedule.none().forMethods(METHODS);
    callThrough(schedule, "append");

    assertThrows(IllegalArgumentException.class, () -> schedule.failAt("append", 1));
    assertThrows(IllegalArgumentException.class, () -> schedule.failAt("append", 0));
  }

  @Test
  void failAlways_throwsOnEveryLaterCall() {
    var schedule = FaultSchedule.none().forMethods(METHODS).failAlways("load");

    for (int i = 0; i < 3; i++) {
      assertThrows(InjectedFaultException.class, () -> callThrough(schedule, "load"));
    }
    assertEquals(List.of("append"), callThrough(schedule, "append"));
    assertEquals(3, schedule.calls("load"));
  }

  @Test
  void crashAfter_isThrowAfterDelegating_onTheNextCallOnly() {
    var schedule = FaultSchedule.none().forMethods(METHODS).crashAfter("append");

    FaultSchedule.Call first = schedule.before("append");
    FaultSchedule.Call second = schedule.before("append");

    assertTrue(first.throwsAfterDelegating(), first.toString());
    assertEquals(1, first.number());
    assertFalse(second.throwsAfterDelegating(), second.toString());
    assertEquals(2, second.number());
  }

  @Test
  void crashAfter_runsTheDelegate_thenThrows() {
    var schedule = FaultSchedule.none().forMethods(METHODS).crashAfter("append");

    List<String> seen = new ArrayList<>();
    var failure =
        assertThrows(
            InjectedFaultException.class,
            () ->
                schedule.run(
                    "append",
                    () -> {
                      seen.add("append");
                    }));

    assertEquals(List.of("append"), seen, "the delegate ran: the write happened");
    assertTrue(failure.getMessage().contains("after the delegate"), failure.getMessage());
    assertEquals(List.of("append"), callThrough(schedule, "append"));
  }

  @Test
  void aDelegateThatThrows_propagatesItsOwnFailure_andSpendsTheCrash() {
    var schedule = FaultSchedule.none().forMethods(METHODS).crashAfter("append");
    var own = new IllegalStateException("the store's own failure");

    var thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                schedule.call(
                    "append",
                    () -> {
                      throw own;
                    }));

    assertSame(own, thrown);
    assertEquals(List.of("append"), callThrough(schedule, "append"));
  }

  @Test
  void throwing_replacesTheInjectedFailure() {
    var schedule =
        FaultSchedule.none()
            .forMethods(METHODS)
            .throwing(() -> new IllegalStateException("database down"))
            .failOnce("load")
            .crashAfter("append");

    var before = assertThrows(IllegalStateException.class, () -> callThrough(schedule, "load"));
    var after = assertThrows(IllegalStateException.class, () -> callThrough(schedule, "append"));

    assertEquals("database down", before.getMessage());
    assertEquals("database down", after.getMessage());
  }

  @Test
  void throwing_refusesASupplierThatReturnsNothing() {
    var schedule = FaultSchedule.none().forMethods(METHODS).throwing(() -> null).failOnce("load");

    assertThrows(NullPointerException.class, () -> callThrough(schedule, "load"));
  }

  @Test
  void anUnknownMethodName_isRefused_onceTheMethodsAreRegistered() {
    var schedule = FaultSchedule.none().forMethods(METHODS);

    assertThrows(IllegalArgumentException.class, () -> schedule.failOnce("apend"));
    assertThrows(IllegalArgumentException.class, () -> schedule.failAt("apend", 1));
    assertThrows(IllegalArgumentException.class, () -> schedule.failAlways("apend"));
    assertThrows(IllegalArgumentException.class, () -> schedule.crashAfter("apend"));
    assertThrows(IllegalArgumentException.class, () -> schedule.calls("apend"));
    assertThrows(IllegalArgumentException.class, () -> schedule.before("apend"));
  }

  @Test
  void aNameScheduledBeforeTheMethodsAreRegistered_isCheckedWhenTheyAre() {
    var schedule = FaultSchedule.none().failOnce("apend");

    var refused = assertThrows(IllegalArgumentException.class, () -> schedule.forMethods(METHODS));
    assertTrue(refused.getMessage().contains("apend"), refused.getMessage());
  }

  @Test
  void aScheduleSharedByTwoDecorators_knowsTheMethodsOfBoth() {
    var schedule = FaultSchedule.none().forMethods(Set.of("append")).forMethods(Set.of("save"));

    schedule.failOnce("save").failOnce("append");

    assertThrows(InjectedFaultException.class, () -> callThrough(schedule, "save"));
    assertThrows(InjectedFaultException.class, () -> callThrough(schedule, "append"));
  }

  @Test
  void reset_forgetsTheCallsAndTheFaults_andKeepsTheFailureAndTheMethods() {
    var schedule =
        FaultSchedule.none()
            .forMethods(METHODS)
            .throwing(() -> new IllegalStateException("database down"))
            .failAlways("load")
            .failAt("append", 2)
            .crashAfter("append")
            .failOnce("append");
    assertThrows(IllegalStateException.class, () -> callThrough(schedule, "append"));

    schedule.reset();

    assertEquals(0, schedule.calls());
    assertEquals(List.of("load"), callThrough(schedule, "load"));
    assertEquals(List.of("append"), callThrough(schedule, "append"));
    assertEquals(List.of("append"), callThrough(schedule, "append"));
    schedule.failOnce("load");
    assertThrows(IllegalStateException.class, () -> callThrough(schedule, "load"));
    assertThrows(IllegalArgumentException.class, () -> schedule.failOnce("apend"));
  }

  @Test
  void methodsOf_namesTheInstanceMethodsOfAnInterface_defaultsIncluded() {
    assertEquals(Set.of("run"), FaultSchedule.methodsOf(Runnable.class));
    Set<String> comparator = FaultSchedule.methodsOf(java.util.Comparator.class);
    assertTrue(comparator.contains("compare"), comparator.toString());
    assertTrue(comparator.contains("reversed"), "a default method: " + comparator);
    assertFalse(comparator.contains("naturalOrder"), "a static method: " + comparator);
  }

  @Test
  void callsFromManyThreads_areAllCounted() throws Exception {
    var schedule = FaultSchedule.none().forMethods(METHODS);
    int threads = 8;
    int perThread = 500;
    var start = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await(5, TimeUnit.SECONDS);
                  for (int i = 0; i < perThread; i++) {
                    schedule.run("append", () -> {});
                  }
                  return null;
                }));
      }
      start.countDown();
      for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
      }
    }
    assertEquals(threads * perThread, schedule.calls("append"));
  }
}
