package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class SnapshotMigrationChainTest {

  record V1State(String name) implements AggregateState {}

  record V2State(String name, int age) implements AggregateState {}

  record V3State(String name, int age, boolean active) implements AggregateState {}

  private static SnapshotMigration step(
      int from, int to, java.util.function.Function<AggregateState, AggregateState> fn) {
    return new SnapshotMigration() {
      @Override
      public int fromVersion() {
        return from;
      }

      @Override
      public int toVersion() {
        return to;
      }

      @Override
      public AggregateState migrate(AggregateState state) {
        return fn.apply(state);
      }
    };
  }

  private static final SnapshotMigration ONE_TO_TWO =
      step(1, 2, s -> new V2State(((V1State) s).name(), 0));

  private static final SnapshotMigration TWO_TO_THREE =
      step(2, 3, s -> new V3State(((V2State) s).name(), ((V2State) s).age(), true));

  private static SnapshotMigration typedStep(
      int from,
      int to,
      Class<? extends AggregateState> toType,
      java.util.function.Function<AggregateState, AggregateState> fn) {
    return new SnapshotMigration() {
      @Override
      public int fromVersion() {
        return from;
      }

      @Override
      public int toVersion() {
        return to;
      }

      @Override
      public AggregateState migrate(AggregateState state) {
        return fn.apply(state);
      }

      @Override
      public Class<? extends AggregateState> toType() {
        return toType;
      }
    };
  }

  @Test
  void emptyChainIsAlwaysUnmigratable() {
    var chain = SnapshotMigrationChain.of(List.of());
    assertTrue(chain.isEmpty());
    assertTrue(chain.migrate(new V1State("a"), 1, 2).isEmpty());
  }

  @Test
  void nullListIsEmptyChain() {
    var chain = SnapshotMigrationChain.of(null);
    assertTrue(chain.isEmpty());
  }

  @Test
  void singleStepMigrates() {
    var chain = SnapshotMigrationChain.of(List.of(ONE_TO_TWO));
    Optional<AggregateState> result = chain.migrate(new V1State("Alice"), 1, 2);
    assertTrue(result.isPresent());
    assertInstanceOf(V2State.class, result.get());
    assertEquals("Alice", ((V2State) result.get()).name());
    assertEquals(0, ((V2State) result.get()).age());
  }

  @Test
  void multiStepChainAppliesInAscendingOrder() {
    var chain = SnapshotMigrationChain.of(List.of(ONE_TO_TWO, TWO_TO_THREE));
    Optional<AggregateState> result = chain.migrate(new V1State("Bob"), 1, 3);
    assertTrue(result.isPresent());
    assertInstanceOf(V3State.class, result.get());
    V3State v3 = (V3State) result.get();
    assertEquals("Bob", v3.name());
    assertEquals(0, v3.age());
    assertTrue(v3.active());
  }

  @Test
  void multiStepChainIgnoresRegistrationOrder() {
    // Register out of order; the chain must still walk from -> to correctly.
    var chain = SnapshotMigrationChain.of(List.of(TWO_TO_THREE, ONE_TO_TWO));
    Optional<AggregateState> result = chain.migrate(new V1State("Cara"), 1, 3);
    assertTrue(result.isPresent());
    assertInstanceOf(V3State.class, result.get());
  }

  @Test
  void gappedChainIsUnmigratable() {
    // 1->2 only, but expected 3: no 2->3 step -> gap -> empty (caller discards + replays).
    var chain = SnapshotMigrationChain.of(List.of(ONE_TO_TWO));
    assertTrue(chain.migrate(new V1State("d"), 1, 3).isEmpty());
  }

  @Test
  void chainStoppingShortOfExpectedIsUnmigratable() {
    // 1->2 and 2->3 registered but expected 4: chain overshoots stored, never reaches 4.
    var chain = SnapshotMigrationChain.of(List.of(ONE_TO_TWO, TWO_TO_THREE));
    assertTrue(chain.migrate(new V1State("e"), 1, 4).isEmpty());
  }

  @Test
  void migratePastExpectedIsUnmigratable() {
    // 1->2 and 2->3 registered, stored 1 expected 2 but a step jumps past expected is impossible;
    // here stored 2 expected 3 works, but stored 1 expected 2 with only a 1->3 step would
    // overshoot.
    var oneToThree = step(1, 3, s -> new V3State(((V1State) s).name(), 0, false));
    var chain = SnapshotMigrationChain.of(List.of(oneToThree));
    // stored 1 expected 2: the only step jumps to 3, overshooting expected 2 -> unmigratable.
    assertTrue(chain.migrate(new V1State("f"), 1, 2).isEmpty());
    // stored 1 expected 3: the 1->3 step lands exactly on expected.
    assertTrue(chain.migrate(new V1State("g"), 1, 3).isPresent());
  }

  @Test
  void migrationReturningNullFailsFastWithNamedStep() {
    // A step that returns null is a programming error; surface it here with a clear diagnostic
    // naming the offending fromVersion->toVersion step, not as a downstream NPE.
    var returnsNull = step(1, 2, s -> null);
    var chain = SnapshotMigrationChain.of(List.of(returnsNull));
    var ex = assertThrows(NullPointerException.class, () -> chain.migrate(new V1State("z"), 1, 2));
    assertTrue(ex.getMessage().contains("returned null"));
    assertTrue(ex.getMessage().contains("1->2"));
  }

  // ── A step's declared toType() must be honored ──────────────────────────────────

  @Test
  void migrationReturningWrongToTypeFailsFastWithNamedStep() {
    // The self-perpetuating corruption case: a 1->2 step that declares it produces V2State but
    // (by bug) returns its input UNCHANGED. Without the toType() check this silently "succeeds" —
    // the caller would persist the V1-shaped object tagged as snapshotVersion 2, and because a
    // future load would see the stored version already matches the expected one, this step would
    // never run again and the corruption would be permanent and invisible.
    var wrongType = typedStep(1, 2, V2State.class, s -> s);
    var chain = SnapshotMigrationChain.of(List.of(wrongType));
    var ex = assertThrows(IllegalStateException.class, () -> chain.migrate(new V1State("z"), 1, 2));
    assertTrue(ex.getMessage().contains("1->2"));
    assertTrue(ex.getMessage().contains(V2State.class.getName()));
    assertTrue(ex.getMessage().contains(V1State.class.getName()));
  }

  @Test
  void migrationReturningDeclaredToTypeSucceeds() {
    // The positive case: a step that DOES declare toType() and honors it must migrate normally —
    // the check must not reject a correct step.
    var correct = typedStep(1, 2, V2State.class, s -> new V2State(((V1State) s).name(), 0));
    var chain = SnapshotMigrationChain.of(List.of(correct));
    Optional<AggregateState> result = chain.migrate(new V1State("ok"), 1, 2);
    assertTrue(result.isPresent());
    assertInstanceOf(V2State.class, result.get());
  }

  @Test
  void migrationWithNoDeclaredToTypeSkipsTheCheck() {
    // A step that does not override toType() (the default, null) gets no result-type check — the
    // ONE_TO_TWO/TWO_TO_THREE steps used throughout this file never declare it.
    assertNull(ONE_TO_TWO.toType());
    var chain = SnapshotMigrationChain.of(List.of(ONE_TO_TWO));
    assertTrue(chain.migrate(new V1State("no-check"), 1, 2).isPresent());
  }

  @Test
  void storedEqualsExpectedReturnsStateUnchanged() {
    var chain = SnapshotMigrationChain.of(List.of(ONE_TO_TWO));
    V1State state = new V1State("h");
    Optional<AggregateState> result = chain.migrate(state, 2, 2);
    assertTrue(result.isPresent());
    assertSame(state, result.get());
  }

  @Test
  void rejectsDuplicateFromVersion() {
    var dup = step(1, 5, s -> s);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> SnapshotMigrationChain.of(List.of(ONE_TO_TWO, dup)));
    assertTrue(ex.getMessage().contains("fromVersion"));
    assertTrue(ex.getMessage().contains("1"));
  }

  @Test
  void rejectsToVersionEqualToFromVersion() {
    var bad = step(2, 2, s -> s);
    assertThrows(IllegalArgumentException.class, () -> SnapshotMigrationChain.of(List.of(bad)));
  }

  @Test
  void rejectsToVersionLessThanFromVersion() {
    var bad = step(3, 2, s -> s);
    assertThrows(IllegalArgumentException.class, () -> SnapshotMigrationChain.of(List.of(bad)));
  }

  @Test
  void rejectsNullMigrationInList() {
    var list = new java.util.ArrayList<SnapshotMigration>();
    list.add(ONE_TO_TWO);
    list.add(null);
    assertThrows(IllegalArgumentException.class, () -> SnapshotMigrationChain.of(list));
  }

  @Test
  void emptyChainReportsEmpty() {
    assertTrue(SnapshotMigrationChain.of(List.of()).isEmpty());
    assertFalse(SnapshotMigrationChain.of(List.of(ONE_TO_TWO)).isEmpty());
  }

  @Test
  void canReachMatchesMigrateReachability() {
    var chain = SnapshotMigrationChain.of(List.of(ONE_TO_TWO, TWO_TO_THREE));
    // Complete chains.
    assertTrue(chain.canReach(1, 3));
    assertTrue(chain.canReach(2, 3));
    assertTrue(chain.canReach(3, 3)); // no-op, already at expected
    // Gaps / overshoots / backwards.
    assertFalse(chain.canReach(1, 4)); // no 3->4 step
    assertFalse(chain.canReach(3, 2)); // backwards
    assertFalse(SnapshotMigrationChain.of(List.of(ONE_TO_TWO)).canReach(1, 3)); // gap at 2->3
  }

  @Test
  void canReachOnEmptyChainOnlyForEqualVersions() {
    var empty = SnapshotMigrationChain.of(List.of());
    assertTrue(empty.canReach(2, 2));
    assertFalse(empty.canReach(1, 2));
  }

  @Test
  void canReachRejectsOvershoot() {
    var oneToThree = step(1, 3, s -> new V3State(((V1State) s).name(), 0, false));
    var chain = SnapshotMigrationChain.of(List.of(oneToThree));
    assertFalse(chain.canReach(1, 2)); // 1->3 overshoots expected 2
    assertTrue(chain.canReach(1, 3));
  }
}
