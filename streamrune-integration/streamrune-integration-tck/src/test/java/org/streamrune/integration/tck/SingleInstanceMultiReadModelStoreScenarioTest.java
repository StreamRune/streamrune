package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;
import org.streamrune.integration.tck.SingleInstanceMultiReadModelStoreScenario.SecondaryFile;

/**
 * {@link SingleInstanceMultiReadModelStoreScenario} run against {@link SqliteReferenceApplication},
 * an application wired on the SQLite backend without a framework: each inherited test passes, on
 * two real SQLite files. The tests declared here show that the scenario fails an application that
 * does not hold a file's instance guard, and that the second store needs its file named.
 */
class SingleInstanceMultiReadModelStoreScenarioTest
    extends SingleInstanceMultiReadModelStoreScenario {

  @Override
  protected Application start(
      DataSource dataSource, Map<String, String> properties, Set<Class<?>> extraBeans) {
    return SqliteReferenceApplication.start(dataSource, properties, extraBeans);
  }

  @Test
  void aFileWhoseGuardNoProcessHoldsFailsTheGuardAssertion(@TempDir Path directory) {
    try (var databases = new SqliteTestDatabases(directory)) {
      DataSource unopened = databases.create();
      Path file = databases.fileOf(unopened);
      var failure =
          assertThrows(AssertionFailedError.class, () -> assertGuardHeld(file, "an unopened file"));
      assertTrue(failure.getMessage().contains(GuardProbe.OPENED), failure.getMessage());
    }
  }

  @Test
  void anApplicationNotOnSqliteIsRefused() {
    try (Application application = SqliteReferenceApplication.start(null, Map.of(), Set.of())) {
      assertFalse(application.started());
    }
  }

  @Test
  void theSecondaryStoreNeedsItsFileNamed() {
    SecondaryFile.use(null);
    var failure = assertThrows(IllegalStateException.class, SecondaryFile::store);
    assertTrue(failure.getMessage().contains("SecondaryFile.use"), failure.getMessage());
  }

  @Test
  void aPoolOfOtherDatabasesHasNoFile(@TempDir Path directory) {
    try (var databases = new SqliteTestDatabases(directory)) {
      DataSource foreign = new IntegrationWiringContractTest.StubDataSource();
      assertThrows(IllegalArgumentException.class, () -> databases.fileOf(foreign));
    }
    assertThrows(IllegalArgumentException.class, () -> new SqliteTestDatabases(null));
  }
}
