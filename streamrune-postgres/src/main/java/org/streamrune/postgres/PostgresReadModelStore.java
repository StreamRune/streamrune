package org.streamrune.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.sql.DataSource;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;

/**
 * The PostgreSQL {@link ReadModelStore}: read models in {@code <name>_view} tables written by a
 * {@link JdbcProjectionRepository}, which is also the store's {@link #processor()}, and their
 * checkpoints in {@code readmodel_checkpoint}, read through {@link PostgresReadModelCheckpoints}.
 * All three run on one {@link DataSource}, so a batch's read-model writes and its checkpoint
 * advance commit in one transaction ({@link AtomicBatchProcessor#transactional()}) under the
 * checkpoint-row fence ({@link AtomicBatchProcessor#fencesCheckpoint()}).
 *
 * <p>{@link #create} is also the factory of the read-model schema, as {@link
 * PostgresEventStoreFactory} is of the event-store schema: it checks the server version, applies
 * the read-model series ({@code classpath:db/readmodel-migration}) on its own history table {@value
 * #READ_MODEL_HISTORY_TABLE}, and validates it. The read-model database need not be the event-store
 * database; when it is (the default), it carries both series, each on its own history table, and
 * neither factory applies the other's series.
 *
 * <p><b>The DataSource is used as given.</b> As with the event-store factory, the store borrows one
 * connection per operation and builds no pool of its own; closing the DataSource stays with the
 * application.
 */
public final class PostgresReadModelStore implements ReadModelStore {

  /**
   * Flyway history table of the read-model series. Not Flyway's default {@code
   * flyway_schema_history}, which belongs to the application, and not the event-store series'
   * table, so the two series never share a version namespace.
   */
  static final String READ_MODEL_HISTORY_TABLE = "flyway_schema_history_readmodel";

  private final String name;
  private final JdbcProjectionRepository repository;
  private final PostgresReadModelCheckpoints checkpoints;

  private PostgresReadModelStore(
      String name, JdbcProjectionRepository repository, PostgresReadModelCheckpoints checkpoints) {
    this.name = name;
    this.repository = repository;
    this.checkpoints = checkpoints;
  }

  /**
   * Creates the store on {@code dataSource}: checks the server version, applies the read-model
   * series when {@code migrate}, validates it when {@code validate}, and builds the repository and
   * the checkpoints on the DataSource as given.
   *
   * <p>The arguments are checked before the database is touched. The server-version check comes
   * next and cannot be switched off: the two flags govern the schema, not the product. Migration
   * applies the series baselined at {@code 0}, so it fits a schema that already holds the
   * event-store series or the application's tables, and converges when replicas start together (see
   * {@link SeriesMigration}).
   *
   * @param name the store's name ({@link ReadModelStore#name()}); not blank
   * @param dataSource the read-model database's pool, used as given
   * @param objectMapper serialises the read models; build it with {@link
   *     JdbcProjectionRepository#createObjectMapper} to encrypt {@code @Encrypted} fields at rest
   * @param migrate whether to apply the read-model series
   * @param validate whether to validate the read-model schema
   * @return the store
   * @throws IllegalArgumentException if an argument is {@code null} or {@code name} is blank
   * @throws UnsupportedServerVersionException if the server is older than {@link
   *     PostgresServerVersion#MINIMUM_MAJOR_VERSION} — checked before anything is migrated
   * @throws org.streamrune.core.EventStoreException if the server version cannot be read
   * @throws SchemaValidationException if validation finds an ERROR-severity issue
   * @throws org.flywaydb.core.api.FlywayException if the migration fails
   */
  public static PostgresReadModelStore create(
      String name,
      DataSource dataSource,
      ObjectMapper objectMapper,
      boolean migrate,
      boolean validate) {
    if (name == null || name.isBlank()) {
      throw new IllegalArgumentException("name is required and must not be blank");
    }
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (objectMapper == null) {
      throw new IllegalArgumentException("objectMapper is required");
    }
    PostgresServerVersion.requireSupported(dataSource);
    if (migrate) {
      SeriesMigration.prepare(
              ShippedMigrationSeries.READ_MODEL, dataSource, READ_MODEL_HISTORY_TABLE)
          .migrate();
    }
    if (validate) {
      SchemaValidationResult result = SchemaValidator.validateReadModel(dataSource);
      if (!result.isValid()) {
        throw new SchemaValidationException(result);
      }
    }
    return new PostgresReadModelStore(
        name,
        new JdbcProjectionRepository(dataSource, objectMapper),
        new PostgresReadModelCheckpoints(dataSource));
  }

  @Override
  public String name() {
    return name;
  }

  /**
   * The repository projections routed here write to: the same instance as {@link #processor()}.
   *
   * @return the store's {@link JdbcProjectionRepository}
   */
  @Override
  public ProjectionRepository repository() {
    return repository;
  }

  /**
   * The checkpoints over {@code readmodel_checkpoint} on the store's DataSource: the ones {@link
   * #processor()} advances.
   *
   * @return the store's {@link PostgresReadModelCheckpoints}
   */
  @Override
  public OffsetStore checkpoints() {
    return checkpoints;
  }

  /**
   * The store's {@link JdbcProjectionRepository} as its batch processor: transactional and fencing,
   * over {@code readmodel_checkpoint}.
   *
   * @return the same instance as {@link #repository()}
   */
  @Override
  public AtomicBatchProcessor processor() {
    return repository;
  }
}
