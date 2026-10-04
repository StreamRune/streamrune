package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;

class SnapshotMigrationTest {

  record V1State(String name) implements AggregateState {}

  record V2State(String name, int age) implements AggregateState {}

  @Test
  void snapshotMigration_fromAndToVersions() {
    SnapshotMigration migration =
        new SnapshotMigration() {
          @Override
          public int fromVersion() {
            return 1;
          }

          @Override
          public int toVersion() {
            return 2;
          }

          @Override
          public AggregateState migrate(AggregateState state) {
            V1State v1 = (V1State) state;
            return new V2State(v1.name(), 0);
          }
        };

    assertEquals(1, migration.fromVersion());
    assertEquals(2, migration.toVersion());

    AggregateState result = migration.migrate(new V1State("Alice"));
    assertInstanceOf(V2State.class, result);
    assertEquals("Alice", ((V2State) result).name());
    assertEquals(0, ((V2State) result).age());
  }
}
