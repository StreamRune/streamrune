package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;

/**
 * What every {@link ForgottenSubjectStore} must honour, taken from the interface's Javadoc: a
 * tombstone is recorded idempotently and lifted idempotently; the first-forget instant and the
 * erased key's creation evidence belong to the tombstone and are first-write-wins (a repeated
 * erasure never overwrites them, a tombstone recorded without evidence stays without it); both
 * disappear with the tombstone; null arguments fail with {@link IllegalArgumentException}; the
 * store is safe for concurrent use. A durable store (see {@link #durable()}) also keeps all of it
 * across a restart. Run by {@code InMemoryForgottenSubjectStoreContractTest} and {@code
 * JdbcForgottenSubjectStoreContractTest}.
 *
 * <p>Every test uses subject ids of its own, so the store behind {@link #newStore()} may share its
 * storage between tests.
 */
public abstract class ForgottenSubjectStoreContract {

  private static final Instant EVIDENCE = Instant.parse("2026-01-01T10:00:00.123456Z");
  private static final Instant OTHER_EVIDENCE = Instant.parse("2026-01-02T11:00:00.654321Z");

  /** A store that starts with no tombstone for any subject id the tests use. */
  protected abstract ForgottenSubjectStore newStore();

  /**
   * Whether tombstones survive a restart (the JDBC store: yes; a heap-only store: no). A durable
   * store must also override {@link #sameStateNewInstance(ForgottenSubjectStore)}.
   */
  protected boolean durable() {
    return false;
  }

  /**
   * A second store instance over the same storage as {@code store}, as after a process restart:
   * nothing held in the first instance's memory carries over. Called only when {@link #durable()}
   * is true.
   */
  protected ForgottenSubjectStore sameStateNewInstance(ForgottenSubjectStore store) {
    throw new UnsupportedOperationException(
        "a durable store's contract test must override sameStateNewInstance");
  }

  private ForgottenSubjectStore store;

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  private static SubjectId subject() {
    return SubjectId.of("forgotten-contract-" + UUID.randomUUID());
  }

  @Test
  void aSubjectIsNotForgottenUntilTombstoned_andOthersAreUnaffected() {
    var subject = subject();
    var other = subject();
    assertFalse(store.isForgotten(subject));

    store.forget(subject);

    assertTrue(store.isForgotten(subject));
    assertFalse(store.isForgotten(other));
  }

  @Test
  void forgetIsIdempotent() {
    var subject = subject();
    store.forget(subject);
    assertDoesNotThrow(() -> store.forget(subject));
    assertDoesNotThrow(() -> store.forget(subject, EVIDENCE));
    assertTrue(store.isForgotten(subject));
  }

  @Test
  void reinstateLiftsTheTombstone_isIdempotent_andTheSubjectCanBeForgottenAgain() {
    var subject = subject();
    store.forget(subject);

    store.reinstate(subject);
    assertFalse(store.isForgotten(subject));
    assertDoesNotThrow(() -> store.reinstate(subject));
    assertFalse(store.isForgotten(subject));

    store.forget(subject);
    assertTrue(store.isForgotten(subject));
  }

  @Test
  void reinstatingASubjectThatWasNeverForgottenIsANoOp() {
    var subject = subject();
    var forgotten = subject();
    store.forget(forgotten);

    assertDoesNotThrow(() -> store.reinstate(subject));

    assertFalse(store.isForgotten(subject));
    assertTrue(store.isForgotten(forgotten));
  }

  @Test
  void forgottenAtIsEmptyUntilTombstoned_thenPresent_andLeavesWithTheTombstone() {
    var subject = subject();
    assertEquals(Optional.empty(), store.forgottenAt(subject));

    store.forget(subject);
    assertTrue(store.forgottenAt(subject).isPresent());

    store.reinstate(subject);
    assertEquals(Optional.empty(), store.forgottenAt(subject));
  }

  @Test
  void forgottenAtIsTheFirstForgetInstant_aRepeatedForgetNeverMovesIt() throws Exception {
    var subject = subject();
    store.forget(subject);
    Instant first = store.forgottenAt(subject).orElseThrow();

    Thread.sleep(5);
    store.forget(subject);
    store.forget(subject, EVIDENCE);

    assertEquals(first, store.forgottenAt(subject).orElseThrow());
  }

  @Test
  void keyCreatedAtIsEmptyWithoutATombstone() {
    assertEquals(Optional.empty(), store.keyCreatedAt(subject()));
  }

  @Test
  void keyCreatedAtRoundTripsAMicrosecondInstant() {
    var subject = subject();
    store.forget(subject, EVIDENCE);

    assertTrue(store.isForgotten(subject));
    assertEquals(Optional.of(EVIDENCE), store.keyCreatedAt(subject));
  }

  @Test
  void aTombstoneRecordedWithoutEvidenceHasNone() {
    var plain = subject();
    var nullEvidence = subject();

    store.forget(plain);
    store.forget(nullEvidence, null);

    assertTrue(store.isForgotten(nullEvidence));
    assertEquals(Optional.empty(), store.keyCreatedAt(plain));
    assertEquals(Optional.empty(), store.keyCreatedAt(nullEvidence));
  }

  @Test
  void evidenceIsFirstWriteWins_aRepeatedErasureNeverOverwritesIt() {
    var subject = subject();
    store.forget(subject, EVIDENCE);

    store.forget(subject, OTHER_EVIDENCE);
    store.forget(subject, null);
    store.forget(subject);

    assertEquals(Optional.of(EVIDENCE), store.keyCreatedAt(subject));
  }

  @Test
  void aTombstoneFirstRecordedWithoutEvidenceNeverGainsSomeLater() {
    // The first erasure's record is the tombstone; the key a repeated deleteKey reads the second
    // time may already be post-forget residue, so evidence offered later must not be adopted.
    var subject = subject();
    store.forget(subject);

    store.forget(subject, EVIDENCE);

    assertEquals(Optional.empty(), store.keyCreatedAt(subject));
  }

  @Test
  void evidenceBelongsToTheTombstone_reinstateTakesItAway() {
    var subject = subject();
    store.forget(subject, EVIDENCE);

    store.reinstate(subject);
    assertEquals(Optional.empty(), store.keyCreatedAt(subject));

    // A later erasure of the same subject must not be proven against the previous one's key.
    store.forget(subject);
    assertEquals(Optional.empty(), store.keyCreatedAt(subject));
    store.reinstate(subject);
    store.forget(subject, OTHER_EVIDENCE);
    assertEquals(Optional.of(OTHER_EVIDENCE), store.keyCreatedAt(subject));
  }

  @Test
  void evidenceIsPerSubject() {
    var a = subject();
    var b = subject();
    store.forget(a, EVIDENCE);
    store.forget(b, OTHER_EVIDENCE);

    assertEquals(Optional.of(EVIDENCE), store.keyCreatedAt(a));
    assertEquals(Optional.of(OTHER_EVIDENCE), store.keyCreatedAt(b));
  }

  @Test
  void nullSubjectIdIsRefusedByEveryMethod() {
    assertThrows(IllegalArgumentException.class, () -> store.forget(null));
    assertThrows(IllegalArgumentException.class, () -> store.forget(null, EVIDENCE));
    assertThrows(IllegalArgumentException.class, () -> store.isForgotten(null));
    assertThrows(IllegalArgumentException.class, () -> store.reinstate(null));
    assertThrows(IllegalArgumentException.class, () -> store.forgottenAt(null));
    assertThrows(IllegalArgumentException.class, () -> store.keyCreatedAt(null));
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentErasuresOfOneSubjectAllSucceed_andExactlyOneEvidenceWins() throws Exception {
    int threads = 6;
    for (int round = 0; round < 5; round++) {
      var subject = subject();
      var barrier = new CyclicBarrier(threads);
      var failures = new ConcurrentLinkedQueue<Throwable>();
      List<Instant> offered = new ArrayList<>();
      List<Thread> workers = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        Instant evidence = EVIDENCE.plusSeconds(i);
        offered.add(evidence);
        workers.add(
            Thread.ofPlatform()
                .start(
                    () -> {
                      try {
                        barrier.await();
                        store.forget(subject, evidence);
                      } catch (Throwable t) {
                        failures.add(t);
                      }
                    }));
      }
      for (Thread worker : workers) {
        worker.join();
      }

      assertTrue(failures.isEmpty(), "no erasure may fail: " + failures);
      assertTrue(store.isForgotten(subject));
      Instant winner = store.keyCreatedAt(subject).orElseThrow();
      assertTrue(offered.contains(winner), "the stored evidence is one that was offered");
      assertEquals(winner, store.keyCreatedAt(subject).orElseThrow(), "and it stays put");
    }
  }

  @Test
  void aDurableStoreKeepsTombstonesAndEvidenceAcrossARestart() {
    assumeTrue(durable(), "heap-only store: tombstones do not survive a restart");
    var withEvidence = subject();
    var without = subject();
    var never = subject();
    store.forget(withEvidence, EVIDENCE);
    store.forget(without);
    Instant forgottenAt = store.forgottenAt(withEvidence).orElseThrow();

    var restarted = sameStateNewInstance(store);

    assertTrue(restarted.isForgotten(withEvidence));
    assertTrue(restarted.isForgotten(without));
    assertFalse(restarted.isForgotten(never));
    assertEquals(Optional.of(EVIDENCE), restarted.keyCreatedAt(withEvidence));
    assertEquals(Optional.empty(), restarted.keyCreatedAt(without));
    assertEquals(forgottenAt, restarted.forgottenAt(withEvidence).orElseThrow());
  }

  @Test
  void aDurableStoreKeepsALiftedTombstoneLiftedAcrossARestart() {
    assumeTrue(durable(), "heap-only store: tombstones do not survive a restart");
    var subject = subject();
    store.forget(subject, EVIDENCE);
    store.reinstate(subject);

    var restarted = sameStateNewInstance(store);

    assertFalse(restarted.isForgotten(subject));
    assertEquals(Optional.empty(), restarted.keyCreatedAt(subject));
    assertEquals(Optional.empty(), restarted.forgottenAt(subject));
    restarted.forget(subject);
    assertTrue(store.isForgotten(subject), "both instances share the same storage");
  }
}
