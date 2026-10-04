package org.streamrune.testsupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Pins the shared PostgreSQL test image switch: unset or blank means the default (the newest
 * supported server), anything else is used verbatim, and the value the build hands to the test JVM
 * is the one every container ends up using.
 */
class PostgresTestImageTest {

  @Test
  void anUnsetPropertyResolvesToTheDefaultImage() {
    assertEquals(PostgresTestImage.DEFAULT, PostgresTestImage.resolve(null));
  }

  @Test
  void aBlankPropertyResolvesToTheDefaultImage() {
    // `-PpostgresImage=` must not turn into an empty image name that Testcontainers rejects
    // minutes later, container by container.
    assertEquals(PostgresTestImage.DEFAULT, PostgresTestImage.resolve(""));
    assertEquals(PostgresTestImage.DEFAULT, PostgresTestImage.resolve("   "));
  }

  @Test
  void aConfiguredImageIsUsedVerbatimAndTrimmed() {
    String custom = "registry.example/pg-custom:9";
    assertEquals(custom, PostgresTestImage.resolve(custom));
    assertEquals(custom, PostgresTestImage.resolve("  " + custom + " \n"));
  }

  @Test
  void theResolvedNameHonoursTheSystemPropertyTheBuildSets() {
    // The Gradle test task sets the property from -PpostgresImage before the JVM starts; the
    // constant is class-initialised once, so this is the end-to-end pin of that wiring.
    assertEquals(
        PostgresTestImage.resolve(System.getProperty(PostgresTestImage.PROPERTY)),
        PostgresTestImage.NAME);
  }
}
