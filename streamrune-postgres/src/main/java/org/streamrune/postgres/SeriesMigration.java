package org.streamrune.postgres;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.types.LogSanitizer;

/**
 * One shipped migration series, configured on its own Flyway history table and ready to apply: the
 * way every framework factory runs a series ({@link PostgresEventStoreFactory} the event-store and
 * crypto series, {@link PostgresReadModelStore} the read-model series).
 *
 * <p>{@link #prepare} resolves the series' scripts and configures Flyway without touching the
 * database ({@link ShippedMigrationSeries#configure}), so a caller that applies several series can
 * prepare all of them first and refuse a missing script before anything is written. {@link
 * #migrate()} then applies the series, converging when replicas start together ({@link
 * #migrateConverging}).
 *
 * <p><b>Baseline 0.</b> Every framework series runs on a history table of its own, never Flyway's
 * default, and is baselined at version {@code 0}: it applies the WHOLE series, but tolerates a
 * schema that is already non-empty because it holds the application's tables or another series'.
 * With the default history table "no history + non-empty schema" could only mean the series' own
 * tables were there; with a namespaced history it also means "the application (or another series)
 * migrated first", and Flyway refuses that outright unless baselining is allowed. Baselining at
 * {@code 0} skips nothing: every framework migration is greater than {@code 0}, so the series still
 * applies in full. It is inert once the history table exists, so it changes nothing on any later
 * run.
 */
final class SeriesMigration {

  private static final Logger log = LoggerFactory.getLogger(SeriesMigration.class);

  /**
   * How Flyway starts the message with which it refuses to baseline a history table it finds
   * already there. {@link #migrateConverging} reads it as the sign of the first-start race.
   */
  static final String BASELINE_REFUSAL = "Unable to baseline schema history table";

  private final Flyway flyway;

  private SeriesMigration(Flyway flyway) {
    this.flyway = flyway;
  }

  /**
   * Configures {@code series} on {@code dataSource}, recorded in {@code historyTable} and baselined
   * at {@code 0}. Touches no database.
   *
   * @param series the series to apply
   * @param dataSource the database to apply it to
   * @param historyTable the series' own Flyway history table
   * @return the prepared migration
   * @throws IllegalStateException if a script of the series is not on the class path
   */
  static SeriesMigration prepare(
      ShippedMigrationSeries series, DataSource dataSource, String historyTable) {
    return new SeriesMigration(
        series
            .configure(dataSource)
            .table(historyTable)
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load());
  }

  /** Applies the series, through {@link #migrateConverging}. */
  void migrate() {
    migrateConverging(flyway::migrate);
  }

  /**
   * Runs {@code migrate}, and runs it once more when Flyway refused to baseline a history table
   * that another instance created while this one was starting.
   *
   * <p><b>Why a second run.</b> Flyway decides how to treat a database without its history table
   * before it takes its lock: it reads that the table is absent, then whether the schema is empty,
   * and baselines a non-empty schema (every series allows it, see above). Replicas that start
   * together on a new database race through that decision. One of them creates the history table,
   * its first step under the lock; another, which had already read the table as absent, now finds
   * the schema non-empty because of that very table, tries to baseline it and is refused: {@code
   * Unable to baseline schema history table ... as it already exists, and is empty}, or {@code ...
   * as it already contains migrations} when the first replica got further. The refused run applied
   * nothing. Flyway only reaches that refusal after reading the table as absent and then finding it
   * there, so it always means another instance created the table meanwhile. The second run starts
   * with the table present, where Flyway takes its lock before it reads the history, so it applies
   * whatever is still pending — usually nothing — and every migration is applied once, however many
   * replicas started together.
   *
   * <p>Every other failure is rethrown as it is, including one that leaves the history table behind
   * (a pooler without a second server connection fails that way); so is a failure of the second
   * run, with the refusal attached as suppressed.
   *
   * @param migrate one Flyway run of the series
   */
  static void migrateConverging(Runnable migrate) {
    try {
      migrate.run();
    } catch (FlywayException first) {
      String message = first.getMessage();
      if (message == null || !message.startsWith(BASELINE_REFUSAL)) {
        throw first;
      }
      log.info(
          "Another instance created the schema history table while this one was starting ({});"
              + " running the migration again",
          LogSanitizer.sanitizeForLog(message.lines().findFirst().orElse(message)));
      try {
        migrate.run();
      } catch (FlywayException second) {
        second.addSuppressed(first);
        throw second;
      }
    }
  }
}
