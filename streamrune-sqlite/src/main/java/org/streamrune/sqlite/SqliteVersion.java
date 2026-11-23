package org.streamrune.sqlite;

import org.streamrune.core.types.LogSanitizer;

/**
 * The oldest SQLite library StreamRune runs on, and the startup check that enforces it.
 *
 * <p>The SQLite stores use JSON functions and the {@code ->} operator, {@code RETURNING} and {@code
 * STRICT} tables, so the library must be {@value #MINIMUM} or newer. {@link SqliteDatabase#open}
 * reads the version of the library the driver loaded ({@code select sqlite_version()}) and refuses
 * an older one before any store touches the file.
 */
final class SqliteVersion {

  /** The oldest SQLite version StreamRune supports. */
  static final String MINIMUM = "3.45.0";

  private static final int[] MINIMUM_PARTS = parse(MINIMUM);

  private SqliteVersion() {}

  /**
   * Refuses a SQLite library older than {@value #MINIMUM}.
   *
   * @param version the version the library reports, {@code major.minor.patch}
   * @throws IllegalStateException if the version is older than {@value #MINIMUM}, or is not three
   *     dot-separated numbers
   */
  static void require(String version) {
    int[] parts = parse(version);
    if (compare(parts, MINIMUM_PARTS) < 0) {
      throw new IllegalStateException(
          "Unsupported SQLite library: SQLite "
              + LogSanitizer.sanitizeForLog(version)
              + " is older than StreamRune supports. StreamRune requires SQLite "
              + MINIMUM
              + " or newer. Use a sqlite-jdbc driver that bundles a newer library; nothing was"
              + " written.");
    }
  }

  private static int[] parse(String version) {
    String[] fields = version.split("\\.", -1);
    if (fields.length != 3) {
      throw unreadable(version);
    }
    int[] parts = new int[3];
    for (int i = 0; i < 3; i++) {
      try {
        parts[i] = Integer.parseInt(fields[i]);
      } catch (NumberFormatException e) {
        throw unreadable(version);
      }
    }
    return parts;
  }

  private static int compare(int[] left, int[] right) {
    for (int i = 0; i < left.length; i++) {
      if (left[i] != right[i]) {
        return Integer.compare(left[i], right[i]);
      }
    }
    return 0;
  }

  private static IllegalStateException unreadable(String version) {
    return new IllegalStateException(
        "StreamRune cannot read the SQLite library version '"
            + LogSanitizer.sanitizeForLog(version)
            + "'; it requires SQLite "
            + MINIMUM
            + " or newer.");
  }
}
