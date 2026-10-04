package org.streamrune.runtime.gdpr;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;

/**
 * A forget that did not finish is completed by calling {@code forget} again: no durable queue, just
 * a re-call. These tests enumerate every point at which a forget can stop, and prove that the
 * re-call always completes the shred and the purges from there, and that the stop itself never
 * leaves the subject readable again.
 *
 * <p>The durable state a forget touches is modelled in {@link World}: the engine's tombstones and
 * keys, the audit log and two read models. A forget's durable effects happen in a fixed order — the
 * tombstone, the key deletion (the order of the filesystem and Vault engines; PostgreSQL commits
 * both in one transaction, which only removes the stop between them), the shred's SUCCESS row, then
 * per purger its purge and its audit row. {@link #STEPS} lists them, and a test pins that list
 * against a clean run, so a step added later fails here until it is covered.
 *
 * <ul>
 *   <li><b>Crash</b>: the process dies just before step {@code k}. Step {@code k} and everything
 *       after it never reaches storage; the forget is then called again by a fresh service over the
 *       same storage (the restarted process).
 *   <li><b>Failure</b>: step {@code k} throws an exception once (a key store, audit store or read
 *       model briefly unavailable) and the run carries on as the service's failure semantics say;
 *       the forget is then called again.
 * </ul>
 *
 * <p>After every stop, the incomplete-erasure check the GDPR guide documents ({@link
 * #erasureComplete}) reports the erasure as incomplete; after the re-call it reports it complete,
 * and the key, the tombstone and both read models are in their erased state.
 */
class ForgetSubjectServiceCrashRecoveryTest {

  private static final SubjectId SUBJECT = SubjectId.of("customer-7");
  private static final List<String> PURGERS = List.of("orders", "profiles");

  /** Every durable effect of one forget, in order. */
  private static final List<String> STEPS =
      List.of(
          "tombstone",
          "key-delete",
          "audit:SUCCESS:shred",
          "purge:orders",
          "audit:SUCCESS:purged read model 'orders'",
          "purge:profiles",
          "audit:SUCCESS:purged read model 'profiles'");

  /** The process dying: an Error, as far as the service can tell. */
  static final class ProcessDeath extends Error {
    ProcessDeath(String step) {
      super("process died before " + step, null, false, false);
    }
  }

  /** The durable state one forget touches, and the stop to inject into it. */
  static final class World {
    final Set<SubjectId> tombstones = new HashSet<>();
    final Set<SubjectId> keys = new HashSet<>(Set.of(SUBJECT));
    final List<AuditEntry> auditLog = new ArrayList<>();
    final Map<String, Set<SubjectId>> readModels = new LinkedHashMap<>();
    final List<String> stepsTaken = new ArrayList<>();
    int crashBefore = -1;
    int failOnce = -1;
    boolean dead;

    World() {
      PURGERS.forEach(name -> readModels.put(name, new HashSet<>(Set.of(SUBJECT))));
    }

    /** Runs one durable effect, unless the process is dead or this step is to stop. */
    void step(String name, Runnable effect) {
      if (dead) {
        throw new ProcessDeath(name); // a dead process writes nothing more
      }
      int index = stepsTaken.size();
      if (index == crashBefore) {
        dead = true;
        throw new ProcessDeath(name);
      }
      if (index == failOnce) {
        failOnce = -1;
        stepsTaken.add("failed:" + name);
        throw new IllegalStateException("transient failure at " + name);
      }
      effect.run();
      stepsTaken.add(name);
    }

    /** The process restarts: storage survives, nothing else does. */
    void restart() {
      dead = false;
      crashBefore = -1;
      failOnce = -1;
    }

    /** A new service over this storage, as a restarted process would build it. */
    ForgetSubjectService newService() {
      CryptoEngine engine = mock(CryptoEngine.class);
      doAnswer(
              inv -> {
                SubjectId s = inv.getArgument(0);
                // Tombstone first, key second: the order of the filesystem and Vault engines.
                step("tombstone", () -> tombstones.add(s));
                step("key-delete", () -> keys.remove(s));
                return null;
              })
          .when(engine)
          .deleteKey(any());
      AuditStore audit =
          entry -> step("audit:" + entry.outcome() + ":" + label(entry), () -> auditLog.add(entry));
      List<SubjectDataPurger> purgers = new ArrayList<>();
      for (String name : PURGERS) {
        purgers.add(
            new SubjectDataPurger() {
              @Override
              public String name() {
                return name;
              }

              @Override
              public void purge(SubjectId s) {
                step("purge:" + name, () -> readModels.get(name).remove(s));
              }
            });
      }
      return ForgetSubjectService.builder()
          .cryptoEngine(engine)
          .auditStore(audit)
          .purgers(purgers)
          .build();
    }

    private static String label(AuditEntry entry) {
      return entry.errorMessage() == null ? "shred" : entry.errorMessage();
    }

    void assertErased() {
      assertTrue(tombstones.contains(SUBJECT), "the subject stays tombstoned");
      assertFalse(keys.contains(SUBJECT), "the key is deleted");
      readModels.forEach(
          (name, rows) -> assertFalse(rows.contains(SUBJECT), "read model " + name + " purged"));
      assertTrue(erasureComplete(auditLog, SUBJECT, PURGERS), "the audit log shows a full erasure");
    }

    /** A stop never leaves a deleted key without its tombstone: the subject stays unreadable. */
    void assertNeverResurrectable() {
      if (!keys.contains(SUBJECT)) {
        assertTrue(tombstones.contains(SUBJECT), "a deleted key always has its tombstone");
      }
    }
  }

  /**
   * The documented incomplete-erasure check over the audit log: the latest {@code GDPR_FORGET}
   * SUCCESS row with no summary (the shred row) for the subject's hash must be followed by a
   * SUCCESS row {@code purged read model '<name>'} for every registered purger.
   */
  static boolean erasureComplete(List<AuditEntry> log, SubjectId subject, List<String> purgers) {
    AggregateId hash = AggregateId.of(subject.redacted());
    int shredRow = -1;
    for (int i = 0; i < log.size(); i++) {
      AuditEntry e = log.get(i);
      if ("GDPR_FORGET".equals(e.commandType())
          && hash.equals(e.aggregateId())
          && e.outcome() == AuditOutcome.SUCCESS
          && e.errorMessage() == null) {
        shredRow = i;
      }
    }
    if (shredRow < 0) {
      return false;
    }
    List<AuditEntry> after = log.subList(shredRow + 1, log.size());
    return purgers.stream()
        .allMatch(
            name ->
                after.stream()
                    .anyMatch(
                        e ->
                            "GDPR_FORGET".equals(e.commandType())
                                && hash.equals(e.aggregateId())
                                && e.outcome() == AuditOutcome.SUCCESS
                                && ("purged read model '" + name + "'").equals(e.errorMessage())));
  }

  static IntStream stopPoints() {
    return IntStream.range(0, STEPS.size());
  }

  @Test
  void aCleanRunTakesExactlyTheEnumeratedSteps() {
    World world = new World();

    ForgetResult result = world.newService().forget(SUBJECT, UserId.of("dpo"));

    assertTrue(result.fullyErased());
    assertEquals(STEPS, world.stepsTaken, "a new durable step must be added to the enumeration");
    world.assertErased();
  }

  @ParameterizedTest(name = "process dies before step {0}")
  @MethodSource("stopPoints")
  void aCrashBeforeAnyStep_isCompletedByCallingForgetAgain(int crashBefore) {
    World world = new World();
    world.crashBefore = crashBefore;

    assertThrows(ProcessDeath.class, () -> world.newService().forget(SUBJECT, UserId.of("dpo")));

    assertEquals(STEPS.subList(0, crashBefore), world.stepsTaken, "only a prefix reached storage");
    world.assertNeverResurrectable();
    assertFalse(
        erasureComplete(world.auditLog, SUBJECT, PURGERS),
        "the audit log must show the erasure as incomplete after a crash before "
            + STEPS.get(crashBefore));

    world.restart();
    ForgetResult result = world.newService().forget(SUBJECT, UserId.of("dpo"));

    assertTrue(result.keyDeleted());
    assertTrue(result.fullyErased(), "the re-call completes the erasure");
    world.assertErased();
  }

  @ParameterizedTest(name = "step {0} fails once")
  @MethodSource("stopPoints")
  void aFailedStep_isCompletedByCallingForgetAgain(int failingStep) {
    World world = new World();
    world.failOnce = failingStep;
    ForgetSubjectService service = world.newService();

    try {
      ForgetResult first = service.forget(SUBJECT, UserId.of("dpo"));
      // Past the shred, a failure never escapes; a failed purge shows in the result.
      assertTrue(failingStep >= 2, "a failure inside deleteKey propagates");
      assertEquals(
          !STEPS.get(failingStep).startsWith("purge:"),
          first.fullyErased(),
          "only a failed purge reports fullyErased() = false");
    } catch (IllegalStateException _) {
      assertTrue(failingStep < 2, "only a failure inside deleteKey propagates");
    }
    world.assertNeverResurrectable();
    assertFalse(
        erasureComplete(world.auditLog, SUBJECT, PURGERS),
        "the audit log must show the erasure as incomplete after a failure at "
            + STEPS.get(failingStep));

    ForgetResult again = service.forget(SUBJECT, UserId.of("dpo"));

    assertTrue(again.fullyErased(), "the re-call completes the erasure");
    world.assertErased();
  }

  /** Two stops in a row, then a re-call: the second partial run is as safe as the first. */
  @Test
  void repeatedPartialRuns_stillConverge() {
    World world = new World();
    world.crashBefore = 4; // after the orders purge, before its audit row
    assertThrows(ProcessDeath.class, () -> world.newService().forget(SUBJECT, null));
    world.restart();
    world.crashBefore = world.stepsTaken.size() + 1; // second run dies after its tombstone
    assertThrows(ProcessDeath.class, () -> world.newService().forget(SUBJECT, null));
    world.assertNeverResurrectable();
    world.restart();

    assertTrue(world.newService().forget(SUBJECT, null).fullyErased());
    world.assertErased();
  }

  /** A completed erasure called again stays complete and changes nothing it already did. */
  @Test
  void aCompletedErasure_calledAgain_isHarmless() {
    World world = new World();
    ForgetSubjectService service = world.newService();
    service.forget(SUBJECT, null);

    ForgetResult again = service.forget(SUBJECT, null);

    assertTrue(again.fullyErased());
    world.assertErased();
    assertEquals(2 * STEPS.size(), world.stepsTaken.size(), "every step ran again, harmlessly");
  }
}
