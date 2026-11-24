package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;

/**
 * The SQLite {@link ReadModelStore}: read models in {@code <name>_view} tables written by a {@link
 * SqliteProjectionRepository}, which is also the store's {@link #processor()}, and their
 * checkpoints in {@code readmodel_checkpoint}, read through {@link SqliteReadModelCheckpoints}. All
 * three run on one {@link SqliteDatabase}, so a batch's read-model writes and its checkpoint
 * advance commit in one transaction ({@link AtomicBatchProcessor#transactional()}) under the
 * checkpoint-row fence ({@link AtomicBatchProcessor#fencesCheckpoint()}). The backend's default
 * store is named {@code sqlite}.
 *
 * <p>{@link #create} is also the factory of the read-model schema, as {@link
 * SqliteEventStoreFactory} is of the event-store schema: it applies {@link
 * SqliteSchemaSeries#READ_MODEL} on its own history table and validates it. The read-model database
 * need not be the event-store database; when it is (the default), the file carries both series,
 * each on its own history table, and neither factory applies the other's series.
 *
 * <p><b>A second database file.</b> Read models kept in another file than the events are opened
 * through a {@link SqliteDatabase} of their own, so that file has its own instance guard: a second
 * process can open neither file, and each guard is released with its own handle.
 *
 * <p><b>The handle stays the caller's.</b> As the PostgreSQL store uses its DataSource as given,
 * this store uses the {@link SqliteDatabase} as given: it holds no resource of its own (no pool, no
 * thread, no reference to the instance guard beyond the caller's handle), so it has nothing to
 * close. Whoever opened the handle closes it, after every store built on it has stopped; closing it
 * releases the instance guard, after which the store must not be used, because its writes would
 * then be unguarded against a second instance.
 */
public final class SqliteReadModelStore implements ReadModelStore {

  private final String name;
  private final SqliteProjectionRepository repository;
  private final SqliteReadModelCheckpoints checkpoints;

  private SqliteReadModelStore(
      String name, SqliteProjectionRepository repository, SqliteReadModelCheckpoints checkpoints) {
    this.name = name;
    this.repository = repository;
    this.checkpoints = checkpoints;
  }

  /**
   * Creates the store on {@code database}: applies the read-model series when {@code migrate},
   * validates it when {@code validate}, and builds the checkpoints and the repository on the handle
   * as given.
   *
   * <p>The arguments are checked before the database is touched. The SQLite library version and the
   * connection settings were checked when the handle was opened. With both flags off the database
   * is not touched at all and the store is built on the schema as it is. Migration applies the
   * series baselined at {@code 0}, so it fits a file that already holds the event-store series or
   * the application's tables, and applies nothing on a second start.
   *
   * <p>Crash points: the migration runs each script in one transaction with the history row that
   * records it, so a crash during it leaves the script unapplied and the next start applies it from
   * its start; a crash after it leaves the series applied and the next start applies nothing.
   * Validation and building the store write nothing.
   *
   * @param name the store's name ({@link ReadModelStore#name()}); not blank, and not {@link
   *     ReadModelStore#DEFAULT}, which is reserved for the backend's default store
   * @param database the open read-model database, used as given and never closed by the store
   * @param objectMapper serialises the read models; build it with {@link
   *     SqliteProjectionRepository#createObjectMapper} to encrypt {@code @Encrypted} fields at rest
   * @param migrate whether to apply the read-model series
   * @param validate whether to validate the read-model schema
   * @return the store
   * @throws IllegalArgumentException if an argument is {@code null}, or {@code name} is blank or
   *     {@link ReadModelStore#DEFAULT}
   * @throws IllegalStateException if {@code database} is closed (whatever the flags), or validation
   *     finds the schema missing a table or column or behind this build
   * @throws org.streamrune.core.EventStoreException if the migration fails
   */
  public static SqliteReadModelStore create(
      String name,
      SqliteDatabase database,
      ObjectMapper objectMapper,
      boolean migrate,
      boolean validate) {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("name is required and must not be blank");
    }
    if (ReadModelStore.DEFAULT.equals(name)) {
      throw new IllegalArgumentException(
          "'default' is reserved: it names the backend's default read-model store");
    }
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (objectMapper == null) {
      throw new IllegalArgumentException("objectMapper is required");
    }
    // With both flags off nothing else would notice a closed handle, and the store would write
    // without the instance guard.
    database.requireOpen();
    if (migrate) {
      database.migrate(SqliteSchemaSeries.READ_MODEL);
    }
    if (validate) {
      database.validate(SqliteSchemaSeries.READ_MODEL);
    }
    var checkpoints = new SqliteReadModelCheckpoints(database);
    return new SqliteReadModelStore(
        name, new SqliteProjectionRepository(database, objectMapper, checkpoints), checkpoints);
  }

  @Override
  public String name() {
    return name;
  }

  /**
   * The repository projections routed here write to: the same instance as {@link #processor()}.
   *
   * @return the store's {@link SqliteProjectionRepository}
   */
  @Override
  public ProjectionRepository repository() {
    return repository;
  }

  /**
   * The checkpoints over {@code readmodel_checkpoint} of the store's database: the ones {@link
   * #processor()} advances.
   *
   * @return the store's {@link SqliteReadModelCheckpoints}
   */
  @Override
  public OffsetStore checkpoints() {
    return checkpoints;
  }

  /**
   * The store's {@link SqliteProjectionRepository} as its batch processor: transactional and
   * fencing, over {@code readmodel_checkpoint}.
   *
   * @return the same instance as {@link #repository()}
   */
  @Override
  public AtomicBatchProcessor processor() {
    return repository;
  }
}
