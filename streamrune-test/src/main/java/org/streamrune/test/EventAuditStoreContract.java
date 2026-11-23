package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/**
 * What every {@link EventAuditStore} must honour, taken from the interface's Javadoc: an entry is
 * identified by its {@code eventId}, so persisting one that exists fails (never a silent
 * duplicate); {@code saveAll} is atomic (a failed batch leaves nothing behind) and an empty list is
 * a no-op; null arguments fail with {@link NullPointerException}. The store is write-only, so
 * "persisted" is observed through the store's own duplicate rule: an eventId that was persisted
 * fails to save again, one that was not saves. What the entries read back as is pinned by {@link
 * EventAuditQueryContract}. There is no in-memory audit store: run by {@code
 * PostgresAuditContractsTest}.
 */
public abstract class EventAuditStoreContract {

  private static final Instant T0 = Instant.parse("2026-01-01T10:00:00.123Z");

  /** A fresh, empty store. */
  protected abstract EventAuditStore newStore();

  private EventAuditStore store;

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  private static EventAuditEntry entry(String eventId) {
    return new EventAuditEntry(
        EventId.of(eventId),
        EventType.of("OrderPlaced"),
        StreamId.of(AggregateType.of("order"), AggregateId.of("order-1")),
        new Version(1),
        CommandId.of("cmd-1"),
        CorrelationId.of("corr-1"),
        CausationId.of("cause-1"),
        UserId.of("user-1"),
        T0);
  }

  private static EventAuditEntry rootEntry(String eventId) {
    return new EventAuditEntry(
        EventId.of(eventId),
        EventType.of("OrderPlaced"),
        StreamId.of(AggregateType.of("order"), AggregateId.of("order-1")),
        new Version(1),
        CommandId.of("cmd-1"),
        CorrelationId.of("corr-1"),
        null,
        null,
        T0);
  }

  /** True when an entry with this eventId is persisted: saving it again fails. */
  private boolean persisted(String eventId) {
    try {
      store.save(entry(eventId));
      return false;
    } catch (EventStoreException duplicate) {
      return true;
    }
  }

  // ---------------------------------------------------------------- save

  @Test
  void savePersistsAnEntry() {
    store.save(entry("e-1"));

    assertTrue(persisted("e-1"));
  }

  @Test
  void saveAcceptsARootEntryWithoutCausationOrUser() {
    assertDoesNotThrow(() -> store.save(rootEntry("e-1")));
    assertTrue(persisted("e-1"));
  }

  @Test
  void savingAnExistingEventIdFailsAndIsNeverASilentDuplicate() {
    store.save(entry("e-1"));

    assertThrows(EventStoreException.class, () -> store.save(entry("e-1")));
    assertThrows(EventStoreException.class, () -> store.saveAll(List.of(entry("e-1"))));
  }

  @Test
  void aNullEntryIsRefusedWithANullPointerException() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }

  /** Two writers racing the same eventId: exactly one persists it, the other gets the error. */
  @Test
  void twoConcurrentSavesOfOneEventIdPersistOneAndFailTheOther() throws Exception {
    for (int round = 0; round < 20; round++) {
      String id = "race-" + round;
      CyclicBarrier start = new CyclicBarrier(2);
      ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
      List<Thread> threads = new ArrayList<>();
      for (int writer = 0; writer < 2; writer++) {
        threads.add(
            Thread.ofVirtual()
                .start(
                    () -> {
                      try {
                        start.await();
                        store.save(entry(id));
                      } catch (Throwable t) {
                        failures.add(t);
                      }
                    }));
      }
      for (Thread thread : threads) {
        thread.join();
      }
      assertEquals(1, failures.size(), "round " + round + ": " + failures);
      assertTrue(failures.peek() instanceof EventStoreException, () -> "" + failures.peek());
      assertTrue(persisted(id), id);
    }
  }

  // ---------------------------------------------------------------- saveAll

  @Test
  void saveAllPersistsEveryEntryOfTheBatch() {
    store.saveAll(List.of(entry("e-1"), rootEntry("e-2"), entry("e-3")));

    assertTrue(persisted("e-1"));
    assertTrue(persisted("e-2"));
    assertTrue(persisted("e-3"));
  }

  @Test
  void anEmptyBatchIsANoOp() {
    assertDoesNotThrow(() -> store.saveAll(List.of()));
  }

  /** A null element is refused up front: no other entry of the batch is persisted. */
  @Test
  void aBatchContainingNullIsRefusedWithANullPointerExceptionAndPersistsNothing() {
    assertThrows(
        NullPointerException.class,
        () -> store.saveAll(Arrays.asList(entry("a"), null, entry("b"))));

    assertFalse(persisted("a"));
    assertFalse(persisted("b"));
  }

  @Test
  void aNullBatchIsRefusedWithANullPointerException() {
    assertThrows(NullPointerException.class, () -> store.saveAll(null));
  }

  /** A failed batch leaves no partial batch behind, whichever position the duplicate holds. */
  @Test
  void aBatchWithAnExistingEventIdFailsAndPersistsNoneOfItsEntries() {
    store.save(entry("existing"));

    assertThrows(
        EventStoreException.class,
        () -> store.saveAll(List.of(entry("first"), entry("existing"), entry("last"))));
    assertThrows(
        EventStoreException.class, () -> store.saveAll(List.of(entry("existing"), entry("after"))));

    assertFalse(persisted("first"));
    assertFalse(persisted("last"));
    assertFalse(persisted("after"));
  }

  @Test
  void aBatchRepeatingAnEventIdWithinItselfFailsAndPersistsNoneOfItsEntries() {
    assertThrows(
        EventStoreException.class,
        () -> store.saveAll(List.of(entry("a"), entry("b"), entry("a"))));

    assertFalse(persisted("a"));
    assertFalse(persisted("b"));
  }

  /**
   * Two batches sharing one eventId: exactly one batch persists, whole, and the other persists
   * nothing.
   */
  @Test
  void twoConcurrentBatchesSharingAnEventIdPersistExactlyOneWholeBatch() throws Exception {
    for (int r = 0; r < 20; r++) {
      int round = r;
      String shared = "shared-" + round;
      CyclicBarrier start = new CyclicBarrier(2);
      boolean[] succeeded = new boolean[2];
      List<Thread> threads = new ArrayList<>();
      for (int writer = 0; writer < 2; writer++) {
        int w = writer;
        threads.add(
            Thread.ofVirtual()
                .start(
                    () -> {
                      try {
                        start.await();
                        store.saveAll(
                            List.of(
                                entry(shared),
                                entry("own-" + round + "-" + w + "-a"),
                                entry("own-" + round + "-" + w + "-b")));
                        succeeded[w] = true;
                      } catch (Exception expectedForTheLoser) {
                        succeeded[w] = false;
                      }
                    }));
      }
      for (Thread thread : threads) {
        thread.join();
      }
      int winner = succeeded[0] ? 0 : 1;
      int loser = 1 - winner;
      assertTrue(succeeded[winner] ^ succeeded[loser], "round " + round + ": exactly one wins");
      assertTrue(persisted(shared), shared);
      assertTrue(persisted("own-" + round + "-" + winner + "-a"));
      assertTrue(persisted("own-" + round + "-" + winner + "-b"));
      assertFalse(persisted("own-" + round + "-" + loser + "-a"));
      assertFalse(persisted("own-" + round + "-" + loser + "-b"));
    }
  }
}
