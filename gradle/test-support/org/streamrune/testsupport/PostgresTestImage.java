package org.streamrune.testsupport;

/**
 * The one PostgreSQL image every Testcontainers-backed test in this repository starts.
 *
 * <p>StreamRune declares PostgreSQL 17 or newer and its CI proves it on both ends of that range:
 * the default build runs on {@value #DEFAULT} and the {@code postgres-17} CI job re-runs the
 * PostgreSQL-backed suites on {@code postgres:17-alpine}. A test therefore never names an image
 * itself — it asks this class, so a single switch moves the whole suite to another server version.
 *
 * <p>The switch is the {@value #PROPERTY} system property. Gradle sets it on the PostgreSQL-backed
 * modules' test tasks from the project property {@code -PpostgresImage}:
 *
 * <pre>{@code
 * ./gradlew postgresTest -PpostgresImage=postgres:17-alpine
 * }</pre>
 *
 * <p>This file lives outside any module and is compiled into the test source set of each module
 * listed in the root build's {@code postgresBackedModules}; the root build's {@code
 * verifyPostgresTestImage} check fails the build when a test starts a PostgreSQL container without
 * going through it.
 */
public final class PostgresTestImage {

  /** System property naming the image to start. */
  public static final String PROPERTY = "streamrune.test.postgres.image";

  /** The image used when {@link #PROPERTY} is unset or blank: the newest supported server. */
  public static final String DEFAULT = "postgres:18-alpine";

  /** The image to hand to {@code new PostgreSQLContainer<>(...)}. */
  public static final String NAME = resolve(System.getProperty(PROPERTY));

  private PostgresTestImage() {}

  /**
   * Resolves the image from a configured value.
   *
   * @param configured the {@link #PROPERTY} value, possibly {@code null} or blank
   * @return the trimmed configured image, or {@link #DEFAULT} when none is configured
   */
  static String resolve(String configured) {
    if (configured == null || configured.isBlank()) {
      return DEFAULT;
    }
    return configured.strip();
  }
}
