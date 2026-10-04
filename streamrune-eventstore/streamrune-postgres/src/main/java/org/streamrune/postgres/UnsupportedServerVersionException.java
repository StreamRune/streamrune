package org.streamrune.postgres;

/**
 * Thrown at startup when the PostgreSQL server is older than the oldest major version StreamRune
 * supports ({@link PostgresServerVersion#MINIMUM_MAJOR_VERSION}).
 *
 * <p>Deliberately not a {@link SchemaValidationException}: nothing is wrong with the schema, and
 * the schema is never read — the refusal comes before the first migration, so an unsupported server
 * is never written to.
 */
public final class UnsupportedServerVersionException extends RuntimeException {

  private final String serverVersion;
  private final int minimumMajorVersion;

  UnsupportedServerVersionException(String message, String serverVersion, int minimumMajorVersion) {
    super(message);
    this.serverVersion = serverVersion;
    this.minimumMajorVersion = minimumMajorVersion;
  }

  /**
   * The version string the server reported, e.g. {@code "16.4"}, exactly as the driver returned it
   * (so it is server-controlled text: sanitize it before rendering it anywhere other than this
   * exception's own message, which already does).
   *
   * @return the reported server version
   */
  public String serverVersion() {
    return serverVersion;
  }

  /**
   * The oldest PostgreSQL major version this build supports.
   *
   * @return the minimum supported major version
   */
  public int minimumMajorVersion() {
    return minimumMajorVersion;
  }
}
