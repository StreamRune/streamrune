package org.streamrune.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.Versioned;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.runtime.jdbc.AfterCommitActions;
import org.streamrune.runtime.jdbc.ReadModelRules;
import org.streamrune.runtime.jdbc.StoreObjectMappers;

/**
 * PostgreSQL-backed {@link ProjectionRepository} with automatic table creation. Tables are created
 * as: `{projection_name}_view` with JSONB storage — {@code id VARCHAR(255) COLLATE "C" PRIMARY KEY,
 * data JSONB NOT NULL, version BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ} — through {@code
 * CREATE TABLE IF NOT EXISTS}, so a table of that name the application created itself must already
 * have that shape. The id is in code-point order ({@code COLLATE "C"}) so the primary key serves
 * the paged reads' order by id (see {@link #findAll(ProjectionName, Class, PageRequest)}); on a
 * table whose id has another collation the order stays right (the statements name the collation)
 * and the primary key does not serve it. A projection name must match {@code [a-z_][a-z0-9_]{0,57}}
 * and is used exactly as given: any other name is rejected with an {@link
 * IllegalArgumentException}, never rewritten, so no two names share a table; the projection runners
 * check the name at registration through {@link #checkProjectionName}. Also implements {@link
 * AtomicBatchProcessor} to provide atomic projection update + offset save within a single
 * transaction, guarded against split-brain double-apply by three checks under one checkpoint-row
 * lock: the epoch fence, the first-offset overlap guard, and the monotonic offset guard (see {@link
 * #executeAtomically}). {@link #executeReplay} applies a dead-lettered range under the same
 * checkpoint-row lock without moving the checkpoint, so a replay and a live batch of one projection
 * never interleave.
 *
 * <p><b>When a table is created.</b> By the first read or write of a projection name through this
 * repository, by {@link #declareIndex}, or by {@link #prepareReadModel}, which the runners call
 * before each batch of a registration that writes through the handed repository. A checkpoint
 * commit alone creates no table, so an at-least-once projection that keeps its read models
 * elsewhere leaves no empty {@code <name>_view} behind.
 *
 * <p><b>Where the checkpoints live.</b> Every repository checkpoints in {@code
 * readmodel_checkpoint} on its own DataSource — the read-model series ({@code
 * db/readmodel-migration}), which {@link PostgresReadModelStore#create} applies — never in the
 * event store's {@code subscription_checkpoint}. A runner that commits through this processor must
 * therefore read the projection's position through {@link PostgresReadModelCheckpoints} on the same
 * DataSource; paired with a {@link PostgresOffsetStore} it would read the wrong table and never see
 * the batches it commits. {@link PostgresReadModelStore} is the way to get the matching pair
 * ({@link PostgresReadModelStore#checkpointTarget()}); an at-least-once projection that writes
 * elsewhere checkpoints with the events instead, through {@link
 * PostgresOffsetStore#subscriptionCheckpoint()}.
 *
 * <p>{@link #transactional()} and {@link #fencesCheckpoint()} are both {@code true}: the updater
 * and the checkpoint write run in one transaction; the repository handed to the updater is bound to
 * it and never {@code null}; a superseded epoch is rejected before any read-model write and stamped
 * durably. No mode makes an effect outside this database exactly-once.
 *
 * <p><b>No statement bound of its own.</b> Unlike {@link PostgresEventStore}, whose {@code
 * statementTimeout} bounds every event-store statement, this repository sets no timeout: its
 * statements run as long as the session's {@code statement_timeout} (a role, database or pool
 * setting) allows, like the other StreamRune stores. A batch transaction holds the projection's
 * checkpoint-row lock while it runs the projection's own writes, whose cost the framework cannot
 * know, and the read side serves application queries ({@code findAll} over a whole view); a
 * framework default sized for an event-store append would cancel legitimate work there, and a
 * cancelled batch is simply retried, failing again. Bound read-model work with the session setting,
 * sized for the largest batch and view you run.
 *
 * <p>The projection-name rule, the argument checks, the refusal of a filter or sort on an {@code
 * Encrypted} component, the conversion of stored JSON back into read models, the index name and
 * statement template and the optimistic-lock conflict are {@link ReadModelRules}'s, one instance
 * per repository; the after-commit actions of a checkpoint transaction are {@link
 * AfterCommitActions}'. This repository keeps its statements and their key expressions, its
 * transactions and locks, and its record of the tables it created.
 */
public final class JdbcProjectionRepository implements ProjectionRepository, AtomicBatchProcessor {

  private static final Logger log = LoggerFactory.getLogger(JdbcProjectionRepository.class);

  private final DataSource dataSource;
  private final ObjectMapper objectMapper;

  /** The fence over this repository's checkpoint table (see {@link #executeAtomically}). */
  private final PostgresCheckpointFence checkpointFence;

  /**
   * The rules of this repository, built once over its mapper. It holds the per-instance cache of
   * each read-model type's {@code Encrypted} JSON names, and the transaction-scoped repository
   * handed to an updater uses this same instance.
   */
  private final ReadModelRules rules;

  // Rows fetched per driver round trip in findAll. pgjdbc honors fetchSize only inside an
  // explicit transaction (autocommit off); under autocommit it materializes the whole result
  // set before returning the first row, doubling peak memory for large projections.
  private static final int FIND_ALL_FETCH_SIZE = 500;

  // Cache to track which tables have been checked/created (avoids race condition)
  private final Set<ProjectionName> tablesChecked = ConcurrentHashMap.newKeySet();

  private static final String UPSERT =
      "INSERT INTO %1$s (id, data, version, updated_at) VALUES (?, ?::jsonb, 0, NOW()) "
          + "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, "
          + "version = %1$s.version + 1, updated_at = NOW()";

  private static final String VERSIONED_UPDATE =
      "UPDATE %s SET data = ?::jsonb, version = version + 1, updated_at = NOW() "
          + "WHERE id = ? AND version = ?";

  private static final String SELECT_BY_ID = "SELECT data FROM %s WHERE id = ?";
  private static final String SELECT_BY_ID_VERSIONED = "SELECT data, version FROM %s WHERE id = ?";
  private static final String SELECT_BY_ID_FOR_UPDATE =
      "SELECT data, version FROM %s WHERE id = ? FOR UPDATE";
  private static final String SELECT_ALL = "SELECT data FROM %s";
  private static final String DELETE = "DELETE FROM %s WHERE id = ?";

  /**
   * Creates a repository with a crypto-BLIND {@link ObjectMapper} ({@link JavaTimeModule} only, no
   * {@link org.streamrune.crypto.CryptoShreddingModule}).
   *
   * <p><b>This is a plaintext read-model path.</b> A mapper without {@code CryptoShreddingModule}
   * persists {@code @Encrypted} read-model fields as PLAINTEXT — nothing is ever encrypted or
   * decrypted, and deleting a subject's key (GDPR forget) has no effect on read models stored that
   * way. The framework auto-configs (Spring/Quarkus/Micronaut) do NOT use this constructor: they
   * inject the optional {@link CryptoEngine} and build the repository with {@link
   * #createObjectMapper(CryptoEngine, StreamRuneMetrics)} so a {@code @Encrypted} read-model field
   * is encrypted at rest and falls within GDPR forget scope. This convenience constructor stays for
   * single-node tests and applications that deliberately do not encrypt read models; its
   * crypto-blindness is now as visible as the saga store's (whose factory names the same choice).
   */
  public JdbcProjectionRepository(DataSource dataSource) {
    this(dataSource, createObjectMapper(null, StreamRuneMetrics.NOOP));
  }

  /**
   * Builds an {@link ObjectMapper} configured the same way {@code PostgresSagaStore} and {@code
   * PostgresEventStore} configure theirs: {@link JavaTimeModule} always registered, plus {@link
   * org.streamrune.crypto.CryptoShreddingModule} when {@code cryptoEngine} is non-null so
   * {@code @Encrypted} read-model fields are encrypted at rest (and fall within GDPR forget scope).
   *
   * <p>Intended for framework auto-configs wiring up the default {@link ProjectionRepository} bean,
   * and for tests exercising the crypto-aware read-model path.
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} to build a crypto-blind
   *     mapper — {@link JavaTimeModule} only, so {@code @Encrypted} read-model fields are written
   *     and read back as PLAINTEXT and are outside GDPR forget scope
   * @return a new {@link ObjectMapper}, independent from any other mapper instance
   */
  public static ObjectMapper createObjectMapper(CryptoEngine cryptoEngine) {
    return createObjectMapper(cryptoEngine, StreamRuneMetrics.NOOP);
  }

  /**
   * Same as {@link #createObjectMapper(CryptoEngine)} but threads a {@link StreamRuneMetrics}
   * collector into {@link org.streamrune.crypto.CryptoShreddingModule} so the crypto-redaction
   * signals ({@code streamrune.crypto.subject_redacted} / systemic-failure) fire when
   * {@code @Encrypted} read-model fields decrypt to {@code [REDACTED]} because a key is gone.
   * Without this the read path builds the mapper with NOOP metrics and the alerting is silent.
   *
   * @param cryptoEngine the crypto engine to wire in, or {@code null} for a crypto-blind mapper
   * @param metrics the crypto-redaction metrics collector; {@link StreamRuneMetrics#NOOP} for none,
   *     not {@code null}
   * @return a new {@link ObjectMapper}, independent from any other mapper instance
   */
  public static ObjectMapper createObjectMapper(
      CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    return StoreObjectMappers.readModels(cryptoEngine, metrics);
  }

  /**
   * Creates a repository whose read models are (de)serialised with {@code objectMapper} — build it
   * with {@link #createObjectMapper(CryptoEngine, StreamRuneMetrics)} to encrypt {@code @Encrypted}
   * fields at rest. Its checkpoints live in {@code readmodel_checkpoint} on {@code dataSource} (see
   * the class documentation, "Where the checkpoints live").
   *
   * @param dataSource the read-model database's pool, used as given
   * @param objectMapper serialises the read models
   * @throws IllegalArgumentException if an argument is {@code null}
   */
  public JdbcProjectionRepository(DataSource dataSource, ObjectMapper objectMapper) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (objectMapper == null) {
      throw new IllegalArgumentException("objectMapper is required");
    }
    this.dataSource = dataSource;
    this.objectMapper = objectMapper;
    this.rules = new ReadModelRules(objectMapper);
    this.checkpointFence = new PostgresCheckpointFence(CheckpointTable.READ_MODEL);
  }

  // Connection discipline: with PostgresCryptoEngine (all three integrations wire it onto the
  // application DataSource), serialising or deserialising an @Encrypted read model borrows a
  // connection from THIS SAME POOL. Every outer path below therefore turns the read model into JSON
  // BEFORE dataSource.getConnection() and turns the rows back into objects AFTER the connection is
  // closed, so no call holds one connection while it waits for a second (PostgresEventStore and
  // PostgresSagaStore follow the same rule). Only TransactionScopedRepository (de)serialises while
  // a connection is held: the batch transaction it is bound to is the point of it, so a batch over
  // @Encrypted read models needs a second connection while it runs.

  @Override
  public <T> void save(ProjectionName projectionName, String id, T readModel) {
    ReadModelRules.validateInputs(projectionName, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
    ensureTableExists(projectionName);
    String json = toJson(readModel, "Failed to save projection: ", projectionName, id);
    try (var conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "projection save",
          c -> {
            saveJsonOn(c, projectionName, id, json);
            return null;
          });
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to save projection: " + ReadModelRules.shown(projectionName, id), e);
    }
  }

  private void saveJsonOn(Connection conn, ProjectionName projectionName, String id, String json) {
    String table = ReadModelRules.tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(UPSERT, table))) {
      ps.setString(1, id);
      ps.setString(2, json);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to save projection: " + ReadModelRules.shown(projectionName, id), e);
    }
  }

  @Override
  public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
    ReadModelRules.validateInputs(projectionName, id);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    ensureTableExists(projectionName);
    Optional<String> json;
    try (var conn = dataSource.getConnection()) {
      json = findJsonByIdOn(conn, projectionName, id);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find projection: " + ReadModelRules.shown(projectionName, id), e);
    }
    return json.map(
        j -> rules.fromJson(j, type, "Failed to find projection: ", projectionName, id));
  }

  private Optional<String> findJsonByIdOn(
      Connection conn, ProjectionName projectionName, String id) {
    String table = ReadModelRules.tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(SELECT_BY_ID, table))) {
      ps.setString(1, id);
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(rs.getString("data"));
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find projection: " + ReadModelRules.shown(projectionName, id), e);
    }
  }

  @Override
  public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
    ReadModelRules.validateProjectionName(projectionName);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    ensureTableExists(projectionName);
    List<String> rows;
    try (var conn = dataSource.getConnection()) {
      // Read inside an explicit transaction so the driver streams the rows in
      // FIND_ALL_FETCH_SIZE batches instead of materializing the whole result set twice.
      conn.setAutoCommit(false);
      try {
        rows = findAllJsonOn(conn, projectionName);
        conn.commit();
      } catch (RuntimeException e) {
        conn.rollback();
        throw e;
      } finally {
        // Restore auto-commit before the connection returns to the pool
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find all projections: " + ReadModelRules.shown(projectionName), e);
    }
    return rules.fromJson(rows, type, "Failed to find all projections: ", projectionName);
  }

  private List<String> findAllJsonOn(Connection conn, ProjectionName projectionName) {
    String table = ReadModelRules.tableName(projectionName);
    List<String> rows = new ArrayList<>();
    try (var ps = conn.prepareStatement(String.format(SELECT_ALL, table))) {
      // Streams only when the connection is in an explicit transaction: the public findAll
      // opens one, executeAtomically's TransactionScopedRepository already runs in one.
      ps.setFetchSize(FIND_ALL_FETCH_SIZE);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          rows.add(rs.getString("data"));
        }
      }
      return rows;
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find all projections: " + ReadModelRules.shown(projectionName), e);
    }
  }

  @Override
  public void delete(ProjectionName projectionName, String id) {
    ReadModelRules.validateInputs(projectionName, id);
    ensureTableExists(projectionName);
    try (var conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "projection delete",
          c -> {
            deleteOn(c, projectionName, id);
            return null;
          });
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to delete projection: " + ReadModelRules.shown(projectionName, id), e);
    }
  }

  private void deleteOn(Connection conn, ProjectionName projectionName, String id) {
    String table = ReadModelRules.tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(DELETE, table))) {
      ps.setString(1, id);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to delete projection: " + ReadModelRules.shown(projectionName, id), e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One statement: the count and the page are two subqueries of one {@code SELECT}, so they read
   * one snapshot whatever the connection's isolation level. The order is {@code ORDER BY} the
   * sort's expressions, each JSON field as {@code (data->>'field') COLLATE "C"} and the id as
   * {@code id COLLATE "C"}; {@link ProjectionIndex#forSort} declares the index that serves it.
   */
  @Override
  public <T> Page<T> findAll(
      ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
    ReadModelRules.validateProjectionName(projectionName);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (pageRequest == null) {
      throw new IllegalArgumentException("pageRequest cannot be null");
    }
    rules.refuseEncryptedKeys(type, null, pageRequest.sort());
    return page(projectionName, type, null, pageRequest);
  }

  /**
   * {@inheritDoc}
   *
   * <p>One statement, as for {@link #findAll(ProjectionName, Class, PageRequest)}, with {@code
   * WHERE (data->>'field') COLLATE "C" = ?} in the count and in the page. {@link
   * ProjectionIndex#forFilter} declares the index that serves it.
   */
  @Override
  public <T> Page<T> findAllBy(
      ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
    ReadModelRules.validateFilteredRead(projectionName, type, filter, pageRequest);
    rules.refuseEncryptedKeys(type, filter, pageRequest.sort());
    return page(projectionName, type, filter, pageRequest);
  }

  private <T> Page<T> page(
      ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
    String sql = pageSql(projectionName, filter, pageRequest.sort()); // validates before any I/O
    ensureTableExists(projectionName);
    Page<String> rows;
    try (var conn = dataSource.getConnection()) {
      rows = pageJsonOn(conn, projectionName, sql, filter, pageRequest);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find paginated projections: " + ReadModelRules.shown(projectionName), e);
    }
    return rules.fromJson(rows, type, projectionName);
  }

  /**
   * The statement of a paged read: the count of the selected rows and one page of them, as two
   * subqueries joined into one result, so both come from one snapshot. Every row carries the count;
   * a page beyond the last is one row with the count and a {@code NULL} {@code data} ({@code data}
   * is {@code NOT NULL} in the table, so {@code NULL} means "no row"). The page subquery selects
   * its sort keys too, so the outer {@code ORDER BY} restores its order after the join (at most
   * {@link PageRequest} size rows). The page is gated on the count ({@code LATERAL ... WHERE
   * total.n > offset}), so a page beyond the last reads only the count, not every row before the
   * offset. Parameters: the filter value (when there is a filter), the offset, the filter value
   * again, {@code LIMIT}, {@code OFFSET}.
   *
   * @throws IllegalArgumentException if the projection name, the filter field or a sort field is
   *     outside the rules
   */
  static String pageSql(ProjectionName projectionName, FieldEquals filter, Sort sort) {
    String table = ReadModelRules.tableName(projectionName);
    String condition = filter == null ? null : filterExpression(filter.field()) + " = ?";
    String where = condition == null ? "" : " WHERE " + condition;
    String gate = " WHERE total.n > ?" + (condition == null ? "" : " AND " + condition);
    List<Sort.Order> keys = ReadModelRules.keysWithIdTiebreak(sort.orders());
    var selected = new ArrayList<String>();
    var inner = new ArrayList<String>();
    var outer = new ArrayList<String>();
    for (int i = 0; i < keys.size(); i++) {
      Sort.Order key = keys.get(i);
      String direction = key.direction() == SortDirection.DESC ? " DESC" : "";
      String expression = keyExpression(key.field());
      selected.add(expression + " AS k" + i);
      inner.add(expression + direction);
      outer.add("page.k" + i + direction);
    }
    return "SELECT total.n, page.data FROM (SELECT count(*) AS n FROM "
        + table
        + where
        + ") AS total LEFT JOIN LATERAL (SELECT data, "
        + String.join(", ", selected)
        + " FROM "
        + table
        + gate
        + " ORDER BY "
        + String.join(", ", inner)
        + " LIMIT ? OFFSET ?) AS page ON true ORDER BY "
        + String.join(", ", outer);
  }

  private Page<String> pageJsonOn(
      Connection conn,
      ProjectionName projectionName,
      String sql,
      FieldEquals filter,
      PageRequest pageRequest) {
    try (var ps = conn.prepareStatement(sql)) {
      int parameter = 1;
      if (filter != null) {
        ps.setString(parameter++, filter.value());
      }
      ps.setLong(parameter++, pageRequest.offset());
      if (filter != null) {
        ps.setString(parameter++, filter.value());
      }
      ps.setInt(parameter++, pageRequest.size());
      ps.setLong(parameter, pageRequest.offset());
      long totalElements = 0;
      List<String> content = new ArrayList<>();
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          totalElements = rs.getLong(1);
          String data = rs.getString(2);
          if (data != null) {
            content.add(data);
          }
        }
      }
      return new Page<>(content, totalElements, pageRequest.page(), pageRequest.size());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find paginated projections: " + ReadModelRules.shown(projectionName), e);
    }
  }

  /**
   * The SQL a sort or index key compares: a repository column as itself (the id in code-point
   * order), any other name as the read model's top-level JSON field, as text in code-point order.
   * The field rule keeps the name safe to splice into the statement.
   */
  private static String keyExpression(String field) {
    ReadModelRules.validateField(field);
    return switch (field) {
      case "id" -> "id COLLATE \"C\"";
      case "version", "updated_at" -> field;
      default -> jsonFieldExpression(field);
    };
  }

  /**
   * The SQL a filter compares: a read-model field ({@link FieldEquals} refuses the column names).
   */
  private static String filterExpression(String field) {
    ReadModelRules.validateField(field);
    return jsonFieldExpression(field);
  }

  private static String jsonFieldExpression(String field) {
    return "(data->>'" + field + "') COLLATE \"C\"";
  }

  // ---- declared indexes ----

  /**
   * Creates {@code index} on its projection's table, creating the table first if it does not exist:
   * {@code CREATE INDEX IF NOT EXISTS <index name> ON <projection>_view (<keys>)}, committed before
   * it returns. Call it at wiring, once per index the projection's reads need, before the
   * application serves them; the Spring, Quarkus and Micronaut integrations call it at startup for
   * each {@link ProjectionIndex} bean, on the application's {@code ProjectionRepository} bean.
   * Idempotent: a valid index of that name on the projection's table is left as it is, and no
   * statement takes a lock on the table then.
   *
   * <p><b>Keys.</b> Each JSON field is indexed as {@code (data->>'field') COLLATE "C"}, the
   * expression the reads compare, so the planner can use the index for the filter and for the
   * {@code ORDER BY}; the id is the last key ({@code id COLLATE "C"}, appended unless the keys name
   * it). Index short scalar fields: a B-tree entry holds at most 2704 bytes, and once the index
   * exists a save whose indexed text is longer fails ({@code 54000}), and its projection batch with
   * it.
   *
   * <p><b>Name.</b> {@code <projection>_<keys>_ix_<hash>}: the projection name and the lower-cased
   * keys ({@code _desc} after a descending one), cut so the name stays within PostgreSQL's 63-byte
   * identifier limit, then the first ten hex digits of a SHA-256 of the table and the exact column
   * list the statement creates, so two indexes that differ in letter case, direction, a cut-off
   * tail or a key expression have two names. {@link #createIndexSql} returns the statement, and it
   * is logged in full at INFO when this method creates the index.
   *
   * <p><b>Not {@code CONCURRENTLY}.</b> {@code CREATE INDEX} holds a {@code SHARE} lock on the
   * table while it builds: reads go on, writes to that projection wait until the build commits. On
   * a new or small table that is milliseconds; on a large one it stalls the projection's batches
   * for the build, and the session's {@code statement_timeout} (this class sets none of its own)
   * bounds it: a build that takes longer fails every start. {@code CREATE INDEX CONCURRENTLY} would
   * not block writes, but it cannot run in a transaction (so not on a pool that hands out
   * manual-commit connections), waits for every open transaction that touched the table, and a
   * failed or interrupted build leaves an {@code INVALID} index behind. For a large existing table,
   * run {@link #createIndexSql}'s statement with {@code CONCURRENTLY} by hand before the deploy
   * that declares it (print it with {@code
   * System.out.println(JdbcProjectionRepository.createIndexSql(index))}); this method then finds
   * the index and does nothing.
   *
   * <p><b>An index of that name that is not usable</b> — {@code INVALID} (a failed manual {@code
   * CONCURRENTLY} build, or one still running), or a relation of that name that is not an index of
   * the projection's table — fails this method with an {@link EventStoreException} naming the
   * {@code DROP INDEX} to run. It is not dropped here: an {@code INVALID} index may be a build an
   * operator is still running.
   *
   * <p><b>Replicas.</b> Several replicas declaring one index at once all succeed: each finds the
   * index missing and builds it; PostgreSQL commits one and fails the others on the catalog ({@code
   * 23505}, {@code 42P07}), and a loser checks again, finds the index and returns, as table
   * creation does ({@link #lostCreationRace}).
   *
   * @param index the index to create
   * @throws IllegalArgumentException if {@code index} is {@code null}, or its projection name or a
   *     key is outside the rules
   * @throws EventStoreException if the table or the index cannot be created, or an index of its
   *     name is not usable
   */
  @Override
  public void declareIndex(ProjectionIndex index) {
    if (index == null) {
      throw new IllegalArgumentException("index cannot be null");
    }
    String ddl = createIndexSql(index); // validates the name and the keys before any I/O
    String name = indexName(index);
    String table = ReadModelRules.tableName(index.projection());
    ensureTableExists(index.projection());
    SQLException lost = null;
    for (int attempt = 1; ; attempt++) {
      try (var conn = dataSource.getConnection()) {
        IndexOutcome outcome =
            PostgresTransactions.commitIfManual(
                conn,
                "projection index creation",
                c -> {
                  if (usableIndexExists(c, name, table)) {
                    return IndexOutcome.EXISTS;
                  }
                  try (var stmt = c.createStatement()) {
                    stmt.execute(ddl);
                    if (!skippedAsExisting(stmt.getWarnings())) {
                      return IndexOutcome.CREATED;
                    }
                  }
                  // IF NOT EXISTS skipped it (42P07 notice): another session's index, committed
                  // between the check and the statement. Checked on this same connection: a
                  // declaration never holds one connection while it borrows a second.
                  return usableIndexExists(c, name, table)
                      ? IndexOutcome.EXISTS
                      : IndexOutcome.SKIPPED_MISSING;
                });
        switch (outcome) {
          case CREATED -> {
            // The statement is framework-built from validated identifiers ([a-z_][a-z0-9_]* and
            // [A-Za-z0-9_]+) and fixed text: logged in full, so an operator can run it by hand.
            log.info("Created projection index: {}", ddl);
            return;
          }
          case EXISTS -> {
            return;
          }
          case SKIPPED_MISSING ->
              throw new SQLException("index " + name + " was skipped but is not there", "42P07");
        }
      } catch (SQLException failure) {
        if (lost != null) {
          failure.addSuppressed(lost);
        }
        if (!lostCreationRace(failure) || attempt == TABLE_CREATION_ATTEMPTS) {
          throw new EventStoreException(
              "Failed to create projection index: " + LogSanitizer.sanitizeForLog(name), failure);
        }
        log.debug(
            "Projection index {} was created concurrently by another session; checking again",
            LogSanitizer.sanitizeForLog(name));
        lost = failure;
      }
    }
  }

  /** What one attempt of {@link #declareIndex} found or did. */
  private enum IndexOutcome {
    EXISTS,
    CREATED,
    SKIPPED_MISSING
  }

  private static boolean skippedAsExisting(SQLWarning warning) {
    for (var w = warning; w != null; w = w.getNextWarning()) {
      if ("42P07".equals(w.getSQLState())) {
        return true;
      }
    }
    return false;
  }

  /**
   * The statement {@link #declareIndex} runs for {@code index}: {@code CREATE INDEX IF NOT EXISTS
   * <index name> ON <projection>_view (<keys>)}. For a large existing table, run it with {@code
   * CONCURRENTLY} inserted after {@code INDEX} before the deploy that declares the index.
   *
   * @param index the index
   * @return the statement
   * @throws IllegalArgumentException if {@code index} is {@code null}, or the projection name or a
   *     key is outside the rules
   */
  public static String createIndexSql(ProjectionIndex index) {
    if (index == null) {
      throw new IllegalArgumentException("index cannot be null");
    }
    String table = ReadModelRules.tableName(index.projection());
    return ReadModelRules.createIndexSql(indexName(index), table, indexColumns(index));
  }

  /** The column list of {@code index}'s statement: each key's expression and direction, id last. */
  private static String indexColumns(ProjectionIndex index) {
    var columns = new ArrayList<String>();
    for (Sort.Order key : ReadModelRules.keysWithIdTiebreak(index.keys())) {
      columns.add(
          keyExpression(key.field()) + (key.direction() == SortDirection.DESC ? " DESC" : ""));
    }
    return String.join(", ", columns);
  }

  /**
   * The deterministic name of {@code index} (see {@link #declareIndex}), from {@link
   * ReadModelRules#indexName} over this backend's column list. The table name is checked before the
   * keys, so a bad projection name fails before a bad key.
   */
  static String indexName(ProjectionIndex index) {
    return ReadModelRules.indexName(
        index, ReadModelRules.tableName(index.projection()), indexColumns(index));
  }

  /**
   * Whether a valid index named {@code name} on {@code table} exists in the schema the projection
   * tables go to; {@code false} when no relation of that name exists there.
   *
   * @throws EventStoreException if a relation of that name exists and is not a valid index of
   *     {@code table}
   */
  private static boolean usableIndexExists(Connection conn, String name, String table)
      throws SQLException {
    try (var ps =
        conn.prepareStatement(
            "SELECT i.indisvalid, t.relname FROM pg_class c"
                + " JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " LEFT JOIN pg_index i ON i.indexrelid = c.oid"
                + " LEFT JOIN pg_class t ON t.oid = i.indrelid"
                + " WHERE c.relname = ? AND n.nspname = current_schema()")) {
      ps.setString(1, name);
      try (var rs = ps.executeQuery()) {
        if (!rs.next()) {
          return false;
        }
        boolean valid = rs.getBoolean(1);
        String indexedTable = rs.getString(2);
        if (indexedTable == null || !indexedTable.equals(table)) {
          throw new EventStoreException(
              "A relation named "
                  + name
                  + " exists and is not an index of "
                  + table
                  + ": the projection index of that name cannot be created; rename or drop it");
        }
        if (!valid) {
          throw new EventStoreException(
              "Projection index "
                  + name
                  + " exists but is INVALID (a failed or still running CREATE INDEX CONCURRENTLY):"
                  + " wait for a running build to finish, or run DROP INDEX "
                  + name
                  + " and start again");
        }
        return true;
      }
    }
  }

  @Override
  public <T> Optional<Versioned<T>> findById(
      ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
    ReadModelRules.validateInputs(projectionName, id);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (lockMode == null) {
      throw new IllegalArgumentException("lockMode cannot be null");
    }
    ensureTableExists(projectionName);
    Optional<Versioned<String>> row;
    try (var conn = dataSource.getConnection()) {
      row = findVersionedJsonByIdOn(conn, projectionName, id, lockMode);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find projection with lock: " + ReadModelRules.shown(projectionName, id), e);
    }
    return rules.fromJson(row, type, projectionName, id);
  }

  private Optional<Versioned<String>> findVersionedJsonByIdOn(
      Connection conn, ProjectionName projectionName, String id, LockMode lockMode) {
    String table = ReadModelRules.tableName(projectionName);
    String sql =
        String.format(
            lockMode == LockMode.PESSIMISTIC ? SELECT_BY_ID_FOR_UPDATE : SELECT_BY_ID_VERSIONED,
            table);
    try (var ps = conn.prepareStatement(sql)) {
      ps.setString(1, id);
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          String data = rs.getString("data");
          long version = rs.getLong("version");
          return Optional.of(new Versioned<>(data, version));
        }
        return Optional.empty();
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find projection with lock: " + ReadModelRules.shown(projectionName, id), e);
    }
  }

  @Override
  public <T> void save(
      ProjectionName projectionName, String id, T readModel, long expectedVersion) {
    ReadModelRules.validateInputs(projectionName, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must be >= 0");
    }
    ensureTableExists(projectionName);
    String json =
        toJson(readModel, "Failed to save projection with version check: ", projectionName, id);
    try (var conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "projection versioned save",
          c -> {
            saveJsonOn(c, projectionName, id, json, expectedVersion);
            return null;
          });
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to save projection with version check: "
              + ReadModelRules.shown(projectionName, id),
          e);
    }
  }

  private void saveJsonOn(
      Connection conn,
      ProjectionName projectionName,
      String id,
      String json,
      long expectedVersion) {
    String table = ReadModelRules.tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(VERSIONED_UPDATE, table))) {
      ps.setString(1, json);
      ps.setString(2, id);
      ps.setLong(3, expectedVersion);
      int updated = ps.executeUpdate();
      if (updated == 0) {
        throw ReadModelRules.optimisticLockConflict(projectionName, id, expectedVersion);
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to save projection with version check: "
              + ReadModelRules.shown(projectionName, id),
          e);
    }
  }

  // The serialisation step, kept apart from every connection scope (see the note above save). The
  // reverse step is rules.fromJson, under the same discipline.

  private String toJson(
      Object readModel, String failure, ProjectionName projectionName, String id) {
    try {
      return objectMapper.writeValueAsString(readModel);
    } catch (JsonProcessingException e) {
      throw new EventStoreException(failure + ReadModelRules.shown(projectionName, id), e);
    }
  }

  private void ensureTableExists(ProjectionName projectionName) {
    if (tablesChecked.contains(projectionName)) {
      return;
    }
    // Reject a name outside the rule before touching the database.
    ReadModelRules.tableName(projectionName);

    synchronized (this) {
      if (tablesChecked.contains(projectionName)) {
        return;
      }

      SQLException lost = null;
      for (int attempt = 1; ; attempt++) {
        try {
          createTableCommitted(projectionName);
          break;
        } catch (SQLException failure) {
          if (lost != null) {
            failure.addSuppressed(lost);
          }
          if (!lostCreationRace(failure) || attempt == TABLE_CREATION_ATTEMPTS) {
            throw new EventStoreException(
                "Failed to create projection table: " + ReadModelRules.tableName(projectionName),
                failure);
          }
          logLostCreationRace(projectionName);
          lost = failure;
        }
      }
      tablesChecked.add(projectionName);
    }
  }

  /**
   * Creates {@code projectionName}'s table if it does not exist, committed before it returns: on a
   * pool that hands out autoCommit=false connections an uncommitted CREATE TABLE is rolled back on
   * return, and a recorded table would then be skipped by the batch transaction that needs it.
   */
  private void createTableCommitted(ProjectionName projectionName) throws SQLException {
    try (var conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "projection table creation",
          c -> {
            createTable(c, projectionName);
            return null;
          });
    }
  }

  /**
   * How often a projection table's creation runs when it loses the creation race ({@link
   * #lostCreationRace}). PostgreSQL reports the loser of concurrent {@code CREATE TABLE IF NOT
   * EXISTS} statements only after the winner's commit, so the next run finds the table; the further
   * attempts cover the instant between the winner's commit becoming visible and its catalog
   * invalidation reaching the loser.
   */
  private static final int TABLE_CREATION_ATTEMPTS = 3;

  private static void logLostCreationRace(ProjectionName projectionName) {
    log.debug(
        "Projection table {} was created concurrently by another session; creating it again finds"
            + " it",
        ReadModelRules.tableName(projectionName));
  }

  /**
   * Whether {@code failure} is how PostgreSQL fails a {@code CREATE TABLE IF NOT EXISTS} that raced
   * another session's committed creation of the same table: a unique violation on the catalog
   * ({@code 23505}), a duplicate table ({@code 42P07}) or a duplicate object ({@code 42710}, the
   * table's row type).
   */
  private static boolean lostCreationRace(SQLException failure) {
    String state = failure.getSQLState();
    return "23505".equals(state) || "42P07".equals(state) || "42710".equals(state);
  }

  private static void createTable(Connection conn, ProjectionName projectionName)
      throws SQLException {
    String table = ReadModelRules.tableName(projectionName);
    String createSql =
        "CREATE TABLE IF NOT EXISTS "
            + table
            + " ("
            + "id VARCHAR(255) COLLATE \"C\" PRIMARY KEY, "
            + "data JSONB NOT NULL, "
            + "version BIGINT NOT NULL DEFAULT 0, "
            + "updated_at TIMESTAMPTZ DEFAULT NOW()"
            + ")";
    try (var stmt = conn.createStatement()) {
      stmt.execute(createSql);
    }
  }

  /**
   * Rejects, at registration, a projection name this repository cannot store read models under —
   * one outside {@code [a-z_][a-z0-9_]{0,57}}. The projection runners call it for every
   * registration before they read an event, so the name fails at startup instead of on the
   * projection's first batch. Because every valid name has a table of its own, the runners'
   * exact-name duplicate check is then also the check that no two registrations share a table, as
   * long as each projection saves under the name it is registered under (see {@link
   * AtomicBatchProcessor#checkProjectionName}).
   *
   * @throws IllegalArgumentException if {@code projectionName} is null or outside the rule
   */
  @Override
  public void checkProjectionName(ProjectionName projectionName) {
    ReadModelRules.validateProjectionName(projectionName);
    ReadModelRules.tableName(projectionName);
  }

  /**
   * Processes a batch of events and saves the offset atomically within a single database
   * transaction. If either the projection processing or offset save fails, the entire operation is
   * rolled back and the offset is not advanced — ensuring events are not silently lost or
   * double-processed.
   *
   * <p>The {@code projectionUpdater} receives a {@link ProjectionRepository} whose operations all
   * run on the transaction's single JDBC connection. The projection must write through that
   * repository so the projection update and the offset save commit — or roll back — together.
   * Writes made through any other repository instance (including this one directly) use separate
   * pooled auto-commit connections and are NOT part of the transaction. The supplied repository
   * never requests a second pooled connection, so a single-connection pool cannot self-deadlock.
   *
   * <p><b>Split-brain double-apply protection.</b> The transaction first {@code SELECT ... FOR
   * UPDATE}-locks this projection's checkpoint row (seeding a 0 checkpoint if none exists) through
   * the package-private {@code PostgresCheckpointFence}, which holds every checkpoint statement and
   * guard, so <em>all</em> writers for one projection serialize on that row. It then rejects a
   * batch whose <b>first</b> offset is at or before the committed checkpoint: such a batch overlaps
   * events another writer already applied — the exact split-brain window where a stale/superseded
   * leader read at an older checkpoint and fetched a range extending past a newer leader's advance.
   * The monotonic guard alone (endpoint {@literal >} stored) would let that overlapping batch
   * commit and re-apply its already-applied prefix a second time, silently corrupting a
   * non-idempotent read model (count++/balance/list-append). The overlap guard rolls the whole
   * transaction back instead. <b>Empty batches</b> (a bare offset advance) are still governed by
   * the monotonic guard on the offset save. This makes the read-model write — the write through the
   * handed repository — exactly-once under an overlapping-batch split-brain; it does <em>not</em>
   * remove the need for single-active-consumer leadership, which is what keeps two writers from
   * racing in the first place.
   *
   * <p><b>Epoch fencing.</b> Under the same {@code FOR UPDATE} lock the transaction also reads the
   * row's stored fencing {@code epoch} and rejects — before the updater writes a single read-model
   * row — any commit whose {@code fencingEpoch} is below it: a superseded leader whose lease a
   * newer one took over (bumping the epoch). On commit the epoch is stamped with {@code
   * GREATEST(stored, caller)} so it never regresses. A {@code fencingEpoch} of {@code 0} is
   * unfenced (single-node / {@link org.streamrune.core.subscription.SubscriptionLeadership#NOOP})
   * and always passes. This epoch fence, the first-offset overlap guard, and the monotonic offset
   * guard on the offset save are the three checks that keep the read-model write through the handed
   * repository exactly-once under split-brain; an HTTP call or a broker publish inside the updater
   * is not covered and is at-least-once.
   *
   * <p><b>After-commit actions.</b> An action registered through the supplied repository's {@link
   * ProjectionRepository#afterCommit} runs after the COMMIT, before this method returns, and is
   * discarded if the transaction rolls back. An action that throws a {@link RuntimeException} is
   * logged and never fails the committed batch. An action that throws an {@link Error} ends the
   * run: after a clean commit the Error propagates from this method, and when {@code commit()} or
   * closing the connection threw, it is added as suppressed to that exception, which stays the one
   * thrown. {@code CacheAwareProjection} evicts the query cache this way. {@link
   * AfterCommitActions} lists the crash and failure points.
   *
   * <p>Implements {@link AtomicBatchProcessor#executeAtomically}. The {@code offsetStore} parameter
   * is ignored — the offset is saved internally, in {@code readmodel_checkpoint}, within the same
   * transaction as the projection update.
   *
   * @param projectionName the name of the projection
   * @param batch the batch of events; its first offset is checked against the committed checkpoint
   *     to reject an overlapping split-brain re-application (empty batches skip that check)
   * @param newOffset the global offset to save after successful processing
   * @param fencingEpoch the caller's held leadership epoch; a commit whose epoch is below the epoch
   *     stored on the checkpoint row is rejected (rolled back) and a successful commit stamps
   *     {@code GREATEST(stored, fencingEpoch)}. {@code 0} means unfenced (single-node / NOOP) and
   *     always commits
   * @param projectionUpdater callback that performs the projection update(s) through the supplied
   *     transaction-scoped repository
   * @param offsetStore ignored (offset is saved internally)
   * @throws RuntimeException if the transaction fails; in this case the offset is NOT saved and the
   *     updater's effects are rolled back
   */
  @Override
  public void executeAtomically(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      OffsetStore offsetStore) {
    // Reject a name outside the rule before touching the database.
    ReadModelRules.tableName(projectionName);
    runCheckpointTransaction(
        projectionName,
        CheckpointTransaction.BATCH,
        (conn, transactionScopedRepository) -> {
          // Lock the checkpoint row and reject a fenced-out (stale-epoch) or overlapping batch
          // before it writes a single read-model row. Both checks read the row under the same FOR
          // UPDATE.
          checkpointFence.lockForBatch(
              conn,
              projectionName,
              fencingEpoch,
              batch.isEmpty() ? null : batch.getFirst().globalOffset());

          // Execute projection update(s) on the transaction's connection
          projectionUpdater.update(transactionScopedRepository);

          // Save offset — both in same transaction, stamping the caller's epoch on the checkpoint.
          checkpointFence.saveLocked(conn, projectionName, newOffset, fencingEpoch);
        });
  }

  /**
   * Creates the {@code <name>_view} table if it does not exist, in a transaction of its own, so the
   * batch transaction that follows carries no DDL. The runners call it before each batch of a
   * registration that writes through the handed repository; a registration that is handed {@code
   * null} ({@code AT_LEAST_ONCE_IDEMPOTENT}) never reaches it, so this repository creates no table
   * for a projection whose read models live elsewhere. After the first call for a name it is a
   * lookup in a set. A write through the handed repository to a table nothing prepared still
   * creates it, inside the batch transaction.
   *
   * @throws IllegalArgumentException if {@code projectionName} is outside the table-name rule
   */
  @Override
  public void prepareReadModel(ProjectionName projectionName) {
    ReadModelRules.validateProjectionName(projectionName);
    ensureTableExists(projectionName);
  }

  /**
   * A replay and a batch of one projection both lock its checkpoint row {@code FOR UPDATE}, so one
   * waits for the other.
   *
   * @return always {@code true}
   */
  @Override
  public boolean serializesReplay() {
    return true;
  }

  /**
   * Applies a dead-lettered range in one transaction that first locks this projection's checkpoint
   * row {@code FOR UPDATE} (seeding a 0 checkpoint if none exists) — the lock {@link
   * #executeAtomically} takes — and never writes that row. A live batch of the same projection, in
   * this process or another, waits for the replay's commit, and the replay waits for an open batch;
   * whichever comes second reads the rows the first one committed.
   *
   * <p>The {@code projectionUpdater} receives a repository bound to the replay's transaction, with
   * the same {@link ProjectionRepository#afterCommit} behaviour as a batch. The range is applied
   * all-or-nothing when the projection writes through it. An at-least-once projection is handed
   * {@code null} by the replayer and writes through its own repository while this transaction holds
   * only the lock.
   *
   * <p>Crash and failure points:
   *
   * <ul>
   *   <li>Before the COMMIT (an updater throw, a lost connection, a killed process): the
   *       transaction rolls back. The writes through the handed repository are gone, the checkpoint
   *       row is as it was, and the lock is released.
   *   <li>{@code commit()} throws: the outcome is unknown, the exception propagates and the caller
   *       keeps its dead-letter entry. If the server did commit, the next replay applies the range
   *       a second time.
   *   <li>After the COMMIT: the range is in the read model. Discarding the dead-letter entry is the
   *       caller's separate step.
   * </ul>
   *
   * @param projectionName the projection whose dead-lettered range is replayed
   * @param batch the re-read range (not inspected here)
   * @param projectionUpdater applies the range through the supplied transaction-scoped repository
   * @throws IllegalArgumentException if {@code projectionName} is outside the table-name rule
   */
  @Override
  public void executeReplay(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      ProjectionUpdater projectionUpdater) {
    ReadModelRules.validateProjectionName(projectionName);
    ReadModelRules.tableName(projectionName);
    runCheckpointTransaction(
        projectionName,
        CheckpointTransaction.REPLAY,
        (conn, transactionScopedRepository) -> {
          // The lock a batch takes, with no check and no checkpoint write.
          checkpointFence.lockForReplay(conn, projectionName);
          projectionUpdater.update(transactionScopedRepository);
        });
  }

  /**
   * What runs inside a checkpoint transaction. Its first statement locks the checkpoint row through
   * {@link #checkpointFence}, so every batch and every replay of the projection serializes there.
   */
  @FunctionalInterface
  private interface CheckpointTransactionBody {
    void run(Connection conn, TransactionScopedRepository repository) throws SQLException;
  }

  /**
   * Runs {@code body} in a checkpoint transaction and then the after-commit actions it registered:
   * after the connection is back in the pool, and also when {@code commit()} or closing the
   * connection failed after the COMMIT was issued. A transaction that rolled back discards them.
   * The actions run with the transaction's exception in flight, and an Error from one rides on it
   * as suppressed instead of replacing it ({@link AfterCommitActions#runCheckpointTransaction}). A
   * failing action is logged at WARN on this repository's logger.
   */
  private void runCheckpointTransaction(
      ProjectionName projectionName, CheckpointTransaction kind, CheckpointTransactionBody body) {
    AfterCommitActions.runCheckpointTransaction(
        projectionName, log, actions -> commitUnderCheckpointLock(kind, body, actions));
  }

  /**
   * One transaction under the projection's checkpoint-row lock, in the shell {@link
   * CheckpointTransaction#run} shares with {@link PostgresSubscriptionCheckpointProcessor}: runs
   * {@code body}, which locks the row first, and commits. {@link #executeAtomically} runs the lock
   * with its fences, the updater and the offset save in it; {@link #executeReplay} runs the lock
   * and the updater. The after-commit actions the updater registers are queued on {@code
   * afterCommitActions}; the caller runs them once this method has returned or thrown.
   */
  private void commitUnderCheckpointLock(
      CheckpointTransaction kind,
      CheckpointTransactionBody body,
      AfterCommitActions afterCommitActions) {
    kind.run(
        dataSource,
        conn -> {
          var transactionScopedRepository =
              new TransactionScopedRepository(conn, afterCommitActions);
          // The body locks the checkpoint row first, so every batch and every replay of this
          // projection serializes there.
          body.run(conn, transactionScopedRepository);

          // From here on the outcome may be "committed" even if commit() throws (see
          // AfterCommitActions), so the after-commit actions run whatever commit() reports.
          afterCommitActions.commitIssued();
          // Tables created inside the transaction survive only after commit
          return () -> tablesChecked.addAll(transactionScopedRepository.tablesEnsuredInTransaction);
        });
  }

  /**
   * {@link ProjectionRepository} view bound to the single connection of an open transaction. Handed
   * to the projection updater by {@link #executeAtomically} so projection writes commit — or roll
   * back — together with the offset save. Never requests a pooled connection itself: table creation
   * also runs on the bound connection, eliminating self-deadlock on exhausted pools.
   *
   * <p><b>The crypto engine is the exception.</b> The read models are (de)serialised while the
   * bound connection is held — the batch transaction cannot be released mid-batch — so a read model
   * with an {@code @Encrypted} component reaches the {@link CryptoEngine} inside the batch. A
   * {@code PostgresCryptoEngine} on the same pool borrows a second connection for each encrypt and
   * for each decrypt the decrypt cache does not answer; a Vault or KMS engine holds the batch
   * connection across its remote call. Size the pool for it: the guide's production page gives the
   * budget.
   */
  private final class TransactionScopedRepository implements ProjectionRepository {

    private final Connection conn;

    // Tables created on the bound connection during this transaction. Folded into the shared
    // tablesChecked cache only after commit — a rollback would undo the CREATE TABLE.
    private final Set<ProjectionName> tablesEnsuredInTransaction = new HashSet<>();

    private final AfterCommitActions afterCommitActions;

    private TransactionScopedRepository(Connection conn, AfterCommitActions afterCommitActions) {
      this.conn = conn;
      this.afterCommitActions = afterCommitActions;
    }

    /**
     * Queues {@code action} to run once this transaction has committed; a rollback discards it (see
     * {@link AfterCommitActions}).
     */
    @Override
    public void afterCommit(Runnable action) {
      if (action == null) {
        throw new IllegalArgumentException("action cannot be null");
      }
      afterCommitActions.add(action);
    }

    /** The outer repository's identity: this view writes to the same store, on its transaction. */
    @Override
    public Object writeTargetIdentity() {
      return JdbcProjectionRepository.this.writeTargetIdentity();
    }

    private void ensureTableOnConnection(ProjectionName projectionName) {
      if (tablesChecked.contains(projectionName)
          || tablesEnsuredInTransaction.contains(projectionName)) {
        return;
      }
      try {
        createTableInTransaction(projectionName);
      } catch (SQLException e) {
        throw new EventStoreException(
            "Failed to create projection table: " + ReadModelRules.tableName(projectionName), e);
      }
      tablesEnsuredInTransaction.add(projectionName);
    }

    /**
     * Creates the table inside this transaction, each attempt under a savepoint, so that losing the
     * creation race to another replica's committed table ({@link #lostCreationRace}) rolls back
     * only the failed statement, and the statement run again finds the table ({@link
     * #TABLE_CREATION_ATTEMPTS}).
     */
    private void createTableInTransaction(ProjectionName projectionName) throws SQLException {
      SQLException lost = null;
      for (int attempt = 1; ; attempt++) {
        Savepoint beforeCreate = conn.setSavepoint();
        try {
          createTable(conn, projectionName);
          conn.releaseSavepoint(beforeCreate);
          return;
        } catch (SQLException failure) {
          if (lost != null) {
            failure.addSuppressed(lost);
          }
          if (!lostCreationRace(failure) || attempt == TABLE_CREATION_ATTEMPTS) {
            throw failure;
          }
          try {
            conn.rollback(beforeCreate);
          } catch (SQLException rollbackFailure) {
            rollbackFailure.addSuppressed(failure);
            throw rollbackFailure;
          }
          logLostCreationRace(projectionName);
          lost = failure;
        }
      }
    }

    @Override
    public <T> void save(ProjectionName projectionName, String id, T readModel) {
      ReadModelRules.validateInputs(projectionName, id);
      if (readModel == null) {
        throw new IllegalArgumentException("readModel cannot be null");
      }
      ensureTableOnConnection(projectionName);
      saveJsonOn(
          conn,
          projectionName,
          id,
          toJson(readModel, "Failed to save projection: ", projectionName, id));
    }

    @Override
    public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
      ReadModelRules.validateInputs(projectionName, id);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      ensureTableOnConnection(projectionName);
      return findJsonByIdOn(conn, projectionName, id)
          .map(j -> rules.fromJson(j, type, "Failed to find projection: ", projectionName, id));
    }

    @Override
    public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
      ReadModelRules.validateProjectionName(projectionName);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      ensureTableOnConnection(projectionName);
      return rules.fromJson(
          findAllJsonOn(conn, projectionName),
          type,
          "Failed to find all projections: ",
          projectionName);
    }

    @Override
    public void delete(ProjectionName projectionName, String id) {
      ReadModelRules.validateInputs(projectionName, id);
      ensureTableOnConnection(projectionName);
      deleteOn(conn, projectionName, id);
    }

    @Override
    public <T> Page<T> findAll(
        ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
      ReadModelRules.validateProjectionName(projectionName);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      if (pageRequest == null) {
        throw new IllegalArgumentException("pageRequest cannot be null");
      }
      rules.refuseEncryptedKeys(type, null, pageRequest.sort());
      String sql = pageSql(projectionName, null, pageRequest.sort());
      ensureTableOnConnection(projectionName);
      return rules.fromJson(
          pageJsonOn(conn, projectionName, sql, null, pageRequest), type, projectionName);
    }

    @Override
    public <T> Page<T> findAllBy(
        ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
      ReadModelRules.validateFilteredRead(projectionName, type, filter, pageRequest);
      rules.refuseEncryptedKeys(type, filter, pageRequest.sort());
      String sql = pageSql(projectionName, filter, pageRequest.sort());
      ensureTableOnConnection(projectionName);
      return rules.fromJson(
          pageJsonOn(conn, projectionName, sql, filter, pageRequest), type, projectionName);
    }

    /** Refused: an index is declared at wiring, on the repository itself, not inside a batch. */
    @Override
    public void declareIndex(ProjectionIndex index) {
      throw new IllegalStateException(
          "declareIndex on a transaction-scoped repository: declare indexes at wiring, on the"
              + " repository itself, not inside a projection batch");
    }

    @Override
    public <T> Optional<Versioned<T>> findById(
        ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
      ReadModelRules.validateInputs(projectionName, id);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      if (lockMode == null) {
        throw new IllegalArgumentException("lockMode cannot be null");
      }
      ensureTableOnConnection(projectionName);
      return rules.fromJson(
          findVersionedJsonByIdOn(conn, projectionName, id, lockMode), type, projectionName, id);
    }

    @Override
    public <T> void save(
        ProjectionName projectionName, String id, T readModel, long expectedVersion) {
      ReadModelRules.validateInputs(projectionName, id);
      if (readModel == null) {
        throw new IllegalArgumentException("readModel cannot be null");
      }
      if (expectedVersion < 0) {
        throw new IllegalArgumentException("expectedVersion must be >= 0");
      }
      ensureTableOnConnection(projectionName);
      saveJsonOn(
          conn,
          projectionName,
          id,
          toJson(readModel, "Failed to save projection with version check: ", projectionName, id),
          expectedVersion);
    }
  }

  /**
   * This repository runs the updater's writes through the handed repository and the checkpoint
   * write in one transaction, and the handed repository is bound to it and never {@code null}. The
   * delivery policy consults this to accept a {@code TRANSACTIONAL_LOCAL} or {@code
   * EXTERNAL_EFFECT} registration.
   *
   * @return always {@code true}
   */
  @Override
  public boolean transactional() {
    return true;
  }

  /**
   * This repository honours the leadership fencing epoch: {@link #executeAtomically} runs the epoch
   * fence under the checkpoint row's {@code FOR UPDATE} lock <em>before</em> the updater writes
   * anything, and {@link #stampFencingEpoch} durably arms the fence at takeover. The
   * leadership-aware runners consult this to refuse booting a multi-replica projection on a
   * processor that cannot fence.
   *
   * @return always {@code true}
   */
  @Override
  public boolean fencesCheckpoint() {
    return true;
  }

  /**
   * The store this repository writes to, for {@link AtomicBatchProcessor#writesTo}: the {@link
   * DataSource} AND the {@link ObjectMapper}, both by reference. Two repositories over one
   * DataSource with the same mapper instance are one write target; with different mappers they are
   * not — the transaction-scoped repository handed to the updater serialises with THIS repository's
   * mapper, so a crypto-aware projection paired with a crypto-blind processor would persist
   * {@code @Encrypted} read-model fields in plaintext inside {@code process} and encrypted outside
   * it.
   *
   * @return a {@link JdbcWriteTarget} over this repository's DataSource and ObjectMapper
   */
  @Override
  public Object writeTargetIdentity() {
    return new JdbcWriteTarget(dataSource, objectMapper);
  }

  /**
   * The write-target identity of a {@link JdbcProjectionRepository}: (DataSource, ObjectMapper).
   * Equality is reference equality on both components, never their own {@code equals}, so a
   * DataSource that overrides {@code equals} cannot make two stores look like one.
   */
  public static final class JdbcWriteTarget {
    private final DataSource dataSource;
    private final ObjectMapper objectMapper;

    JdbcWriteTarget(DataSource dataSource, ObjectMapper objectMapper) {
      this.dataSource = dataSource;
      this.objectMapper = objectMapper;
    }

    /**
     * The DataSource the repository writes through.
     *
     * @return the DataSource, by reference
     */
    public DataSource dataSource() {
      return dataSource;
    }

    /**
     * The ObjectMapper the repository serialises read models with.
     *
     * @return the ObjectMapper, by reference
     */
    public ObjectMapper objectMapper() {
      return objectMapper;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof JdbcWriteTarget t
          && t.dataSource == dataSource
          && t.objectMapper == objectMapper;
    }

    @Override
    public int hashCode() {
      return 31 * System.identityHashCode(dataSource) + System.identityHashCode(objectMapper);
    }

    @Override
    public String toString() {
      return "JdbcProjectionRepository(dataSource="
          + dataSource.getClass().getSimpleName()
          + "@"
          + Integer.toHexString(System.identityHashCode(dataSource))
          + ", objectMapper=ObjectMapper@"
          + Integer.toHexString(System.identityHashCode(objectMapper))
          + ")";
    }
  }

  /**
   * Stamps {@code fencingEpoch} on the projection's checkpoint row <em>without moving the
   * offset</em> — the takeover stamp (see {@link AtomicBatchProcessor#stampFencingEpoch}).
   *
   * <p>The epoch fence compares against the epoch STORED on the checkpoint row, which the offset
   * save stamps only on a committed advance — so from a takeover until the new leader's first
   * committed advance the row still carried the superseded leader's epoch and ALL of that leader's
   * writes passed the fence (its fail-open local lease view can outlive DB lease expiry by a pause
   * length, so a paused-then-resumed old leader could commit a stale SKIP/DLQ forward-jump in that
   * window — silently dropping events). Leadership-aware runners call this immediately after
   * acquiring the lease, before reading anything, which closes that window down to the instants
   * between the acquire and this stamp committing (an in-flight stale transaction that already
   * passed its fence read under the row lock is serialized before the stamp and still lands — no
   * takeover-time stamping can reject a fence check that already happened; every transaction the
   * old leader STARTS after this stamp commits is rejected).
   *
   * <p>Idempotent and monotonic: the {@code WHERE EXCLUDED.epoch > <checkpoint table>.epoch} guard
   * makes a same-epoch re-stamp (every scheduled tick) a 0-row no-op — the stored epoch never
   * regresses (same {@code GREATEST} discipline as the commit stamp), and an unchanged row is not
   * rewritten. The INSERT arm seeds {@code last_offset = 0} for a projection that has never
   * committed — the same floor the batch lock seeds and the offset save's monotonic {@code WHERE}
   * measures against — and the conflict arm never touches {@code last_offset}, so the checkpoint
   * cannot move in either direction. Epoch {@code 0} (unfenced / NOOP leadership) is skipped
   * entirely: there is nothing to fence and stamping would only churn the row.
   *
   * <p><b>A 0-row outcome is not silently accepted.</b> Discarding the {@code executeUpdate()}
   * result would make "same epoch, already stamped" indistinguishable from "the epoch I was handed
   * is BELOW the stored one". The second case is never benign: the caller then runs under an epoch
   * the epoch fence rejects on every commit, and the runners classify that rejection as a benign
   * leadership/CAS signal, so the projection freezes permanently at INFO level while reporting
   * itself LIVE and healthy. On 0 rows this now re-reads the stored epoch and, when it is strictly
   * greater, throws {@link ProjectionEpochRegressionException}. The re-read is a separate
   * statement, so it can observe an epoch newer than the one that actually beat us — harmless,
   * because ANY stored epoch above ours means our epoch is not authoritative and the caller must
   * stop either way. An equal stored epoch (the ordinary re-stamp) still returns quietly, and so
   * does the vanishing case where the row was deleted between the two statements.
   *
   * <p>Runs as a single statement on its own pooled connection (PgBouncer transaction-mode safe),
   * committed whatever autoCommit mode the pool hands out. If a concurrent {@code
   * executeAtomically} holds the checkpoint row's {@code FOR UPDATE} lock, this write queues behind
   * that transaction and applies right after it resolves.
   *
   * @param projectionName the projection whose checkpoint row is stamped
   * @param fencingEpoch the newly acquired leadership epoch; {@code 0} is a no-op
   * @throws ProjectionEpochRegressionException if the checkpoint row already stores an epoch
   *     strictly greater than {@code fencingEpoch}
   */
  @Override
  public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
    ReadModelRules.validateProjectionName(projectionName);
    checkpointFence.stampEpoch(dataSource, projectionName, fencingEpoch);
  }

  /**
   * Rewinds the projection's checkpoint to offset 0, <b>bypassing the monotonic guard</b> on the
   * offset save. This is the reset/rebuild path referenced in the {@link OffsetStore} contract:
   * {@code saveOffset(name, initial())} would no-op the regression against the guard, so a genuine
   * reset must use this direct unguarded write.
   *
   * @param projectionName unique name of the projection to reset
   */
  public void resetOffset(ProjectionName projectionName) {
    ReadModelRules.validateProjectionName(projectionName);
    checkpointFence.reset(dataSource, projectionName);
  }
}
