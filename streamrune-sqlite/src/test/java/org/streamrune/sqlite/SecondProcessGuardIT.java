package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The instance guard against a second operating-system process: a JVM forked from the test class
 * path that opens the same database file through {@link GuardChild}.
 */
class SecondProcessGuardIT {

  @TempDir Path dir;

  @Test
  void secondProcessFailsAtOpen() throws Exception {
    Path db = dir.resolve("db.sqlite");
    try (ForkedJvm holder = ForkedJvm.start("hold", db)) {
      holder.awaitLineStartingWith(GuardChild.READY);

      assertThatThrownBy(() -> SqliteDatabase.open(TestDatabases.dataSource(db)))
          .isInstanceOf(SqliteGuardException.class)
          .hasMessageContaining(db.toRealPath().toString())
          .hasMessageContaining("another process");

      holder.send("close");
      holder.awaitLineStartingWith(GuardChild.CLOSED);
      assertThat(holder.awaitExit()).isZero();
    }
    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      assertThat(database.file()).isEqualTo(db.toRealPath());
      assertThat(ForkedJvm.probe(db)).startsWith(GuardChild.REFUSED);
    }
  }

  @Test
  void crashedHolderDoesNotBlockRestart() throws Exception {
    Path db = dir.resolve("db.sqlite");
    try (ForkedJvm holder = ForkedJvm.start("hold", db)) {
      holder.awaitLineStartingWith(GuardChild.READY);
      assertThatThrownBy(() -> SqliteDatabase.open(TestDatabases.dataSource(db)))
          .isInstanceOf(SqliteGuardException.class);

      holder.send("halt");
      assertThat(holder.awaitExit()).isEqualTo(3);
    }

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      assertThat(database.file()).isEqualTo(db.toRealPath());
    }
  }

  @Test
  void killedHolderDoesNotBlockRestart() throws Exception {
    Path db = dir.resolve("db.sqlite");
    try (ForkedJvm holder = ForkedJvm.start("hold", db)) {
      holder.awaitLineStartingWith(GuardChild.READY);

      holder.close();
    }

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      assertThat(database.file()).isEqualTo(db.toRealPath());
    }
  }

  @Test
  void deniedNativeAccessFailsAtOpenWithoutAWeakerLock() throws Exception {
    assumeThat(LockPlatform.current().usesFlock()).isTrue();
    Path db = dir.resolve("db.sqlite");

    try (ForkedJvm child = ForkedJvm.startWithNativeAccessDenied("probe", db)) {
      String verdict = child.awaitLineStartingWith(GuardChild.ACQUIRED, GuardChild.REFUSED);

      // ACQUIRED would mean a lock without flock, which closing another descriptor drops.
      assertThat(verdict)
          .startsWith(GuardChild.REFUSED)
          .contains("--enable-native-access=ALL-UNNAMED")
          .contains("flock");
      assertThat(child.awaitExit()).isZero();
    }
    assertThat(ForkedJvm.probe(db)).startsWith(GuardChild.ACQUIRED);
  }
}
