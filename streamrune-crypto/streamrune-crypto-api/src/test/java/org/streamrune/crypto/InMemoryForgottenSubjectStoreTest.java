package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.streamrune.core.types.SubjectId;

class InMemoryForgottenSubjectStoreTest {

  private final InMemoryForgottenSubjectStore store = new InMemoryForgottenSubjectStore();

  @Test
  void forgetThenIsForgotten() {
    var subject = SubjectId.of("s-1");
    assertFalse(store.isForgotten(subject));
    store.forget(subject);
    assertTrue(store.isForgotten(subject));
  }

  @Test
  void reinstateClearsTombstone() {
    var subject = SubjectId.of("s-2");
    store.forget(subject);
    store.reinstate(subject);
    assertFalse(store.isForgotten(subject));
  }

  @Test
  void forgetIsIdempotent() {
    var subject = SubjectId.of("s-3");
    store.forget(subject);
    store.forget(subject);
    assertTrue(store.isForgotten(subject));
  }

  @Test
  void rejectsNullSubject() {
    assertThrows(IllegalArgumentException.class, () -> store.forget(null));
    assertThrows(IllegalArgumentException.class, () -> store.isForgotten(null));
    assertThrows(IllegalArgumentException.class, () -> store.reinstate(null));
    assertThrows(IllegalArgumentException.class, () -> store.forgottenAt(null));
    assertThrows(IllegalArgumentException.class, () -> store.forget(null, java.time.Instant.now()));
    assertThrows(IllegalArgumentException.class, () -> store.keyCreatedAt(null));
  }

  @Test
  void keyCreatedAtMirrorsTheDurableStore_firstWriteWins_andEmptyWithoutEvidence() {
    // This double must predict production, including
    // its FAIL-CLOSED cases. A tombstone written through the one-argument forget carries no
    // evidence — exactly like a durable row written without it — so the Vault revival must refuse
    // under this store too, not silently succeed.
    var evidenced = SubjectId.of("s-key-evidence");
    var original = java.time.Instant.now().minusSeconds(31_536_000);
    assertTrue(store.keyCreatedAt(evidenced).isEmpty(), "not tombstoned -> no evidence");

    store.forget(evidenced, original);
    assertTrue(store.isForgotten(evidenced));
    assertEquals(original, store.keyCreatedAt(evidenced).orElseThrow());

    // The documented idempotent re-run may read post-forget residue; it must not overwrite.
    store.forget(evidenced, java.time.Instant.now());
    assertEquals(original, store.keyCreatedAt(evidenced).orElseThrow());

    // Lifting the tombstone takes its evidence with it, as the durable row's DELETE does.
    store.reinstate(evidenced);
    assertTrue(store.keyCreatedAt(evidenced).isEmpty());

    // No evidence available at erasure time, and the one-argument forget: both read as unprovable.
    var unprovable = SubjectId.of("s-key-no-evidence");
    store.forget(unprovable, null);
    assertTrue(store.isForgotten(unprovable), "the erasure still stands");
    assertTrue(store.keyCreatedAt(unprovable).isEmpty());

    var oneArgument = SubjectId.of("s-key-one-argument-forget");
    store.forget(oneArgument);
    assertTrue(store.keyCreatedAt(oneArgument).isEmpty());
  }

  @Test
  void forgottenAtRecordsTheFirstForgetInstant_firstWriteWins() {
    // Mirrors JdbcForgottenSubjectStore's
    // forgotten_at semantics (DEFAULT NOW() + conflict-DO-NOTHING = first-write-wins), so the
    // Vault engine's crashed-vs-residue discrimination behaves the same under this test store.
    var subject = SubjectId.of("s-at");
    assertTrue(store.forgottenAt(subject).isEmpty(), "not tombstoned -> empty");

    var before = java.time.Instant.now();
    store.forget(subject);
    var first = store.forgottenAt(subject).orElseThrow();
    assertFalse(first.isBefore(before), "forgotten-at must be the forget instant");

    store.forget(subject); // idempotent re-forget must not advance the instant
    assertEquals(first, store.forgottenAt(subject).orElseThrow());

    store.reinstate(subject);
    assertTrue(store.forgottenAt(subject).isEmpty(), "reinstated -> empty again");
  }
}
