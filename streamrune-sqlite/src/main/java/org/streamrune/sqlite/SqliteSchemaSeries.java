package org.streamrune.sqlite;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A schema series StreamRune ships for SQLite: a directory of Flyway scripts, the explicit list of
 * those scripts and the Flyway history table the series is recorded in. {@link
 * SqliteDatabase#migrate(SqliteSchemaSeries)} applies a series and {@link
 * SqliteDatabase#validate(SqliteSchemaSeries)} checks it.
 *
 * <p>Each series has its own history table, never Flyway's default {@code flyway_schema_history},
 * which belongs to the application; the names are those of the Postgres series. Several series can
 * therefore live in one database file, next to the application's own tables.
 *
 * <p><b>Explicit script lists.</b> Flyway is handed each script by its exact class-path name and
 * never lists a directory, because a GraalVM native image serves resources under a protocol
 * Flyway's classpath scan does not understand. A new script must be added to its directory AND to
 * the list here; a test compares each list with its directory. In a native image every series
 * directory, {@code <directory>/*.sql}, must be registered as a resource.
 */
public enum SqliteSchemaSeries {

  /**
   * The event store: the event log, its global-offset counter, snapshots, subscription checkpoints,
   * sagas, the outbox, the command inbox, the dead-letter tables and the audit tables.
   */
  EVENT_STORE(
      "db/sqlite-migration",
      "flyway_schema_history_streamrune",
      "streamrune-sqlite",
      List.of("V001__baseline.sql")),

  /** The read-model side: the checkpoint table of a read-model store. */
  READ_MODEL(
      "db/sqlite-readmodel-migration",
      "flyway_schema_history_readmodel",
      "streamrune-sqlite",
      List.of()),

  /**
   * The crypto key store and the GDPR erasure tables, shipped in {@code streamrune-sqlite-crypto}.
   */
  CRYPTO(
      "db/sqlite-crypto-migration",
      "flyway_schema_history_crypto",
      "streamrune-sqlite-crypto",
      List.of());

  private static final Pattern VERSIONED_SCRIPT = Pattern.compile("V(\\d+)__.+\\.sql");

  private final String directory;
  private final String historyTable;
  private final String artifact;
  private final List<String> scripts;

  SqliteSchemaSeries(String directory, String historyTable, String artifact, List<String> scripts) {
    this.directory = directory;
    this.historyTable = historyTable;
    this.artifact = artifact;
    this.scripts = List.copyOf(scripts);
  }

  /**
   * The class-path directory of the series' scripts, e.g. {@code db/sqlite-migration}.
   *
   * @return the directory, without a {@code classpath:} prefix
   */
  public String directory() {
    return directory;
  }

  /**
   * The Flyway history table the series is recorded in.
   *
   * @return the table name
   */
  public String historyTable() {
    return historyTable;
  }

  /**
   * The file names of the series' scripts, in version order.
   *
   * @return the scripts; empty for a series that ships none yet
   */
  public List<String> scripts() {
    return scripts;
  }

  /** The artifact that ships the scripts, named when a script is not on the class path. */
  String artifact() {
    return artifact;
  }

  /** The version of the last script: what a fully migrated database records; 0 for none. */
  int expectedVersion() {
    return scripts.isEmpty() ? 0 : versionOf(scripts.getLast());
  }

  /**
   * The version of a versioned script name, {@code V<digits>__<description>.sql}.
   *
   * @throws IllegalArgumentException if {@code script} is not such a name
   */
  static int versionOf(String script) {
    Matcher matcher = VERSIONED_SCRIPT.matcher(script);
    if (!matcher.matches()) {
      throw new IllegalArgumentException(
          "script must be named V<digits>__<description>.sql, got: " + script);
    }
    return Integer.parseInt(matcher.group(1));
  }
}
