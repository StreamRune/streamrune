package org.streamrune.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.Versioned;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * PostgreSQL-backed {@link ProjectionRepository} with automatic table creation. Tables are created
 * as: `{projection_name}_view` with JSONB storage — {@code id VARCHAR(255) PRIMARY KEY, data JSONB
 * NOT NULL, version BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ} — through {@code CREATE
 * TABLE IF NOT EXISTS}, so a table of that name the application created itself must already have
 * that shape. A projection name must match {@code [a-z_][a-z0-9_]{0,57}} and is used exactly as
 * given: any other name is rejected with an {@link IllegalArgumentException}, never rewritten, so
 * no two names share a table; the projection runners check the name at registration through {@link
 * #checkProjectionName}. Also implements {@link AtomicBatchProcessor} to provide atomic projection
 * update + offset save within a single transaction, guarded against split-brain double-apply by
 * three checks under one checkpoint-row lock: the epoch fence, the first-offset overlap guard, and
 * the monotonic offset guard (see {@link #executeAtomically}).
 *
 * <p>{@link #supportsFencing()} is {@code true} and means all three of: the updater and the
 * checkpoint write run in one transaction; the repository handed to the updater is bound to it and
 * never {@code null}; a superseded epoch is rejected before any read-model write and stamped
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
 */
public final class JdbcProjectionRepository implements ProjectionRepository, AtomicBatchProcessor {

  private static final Logger log = LoggerFactory.getLogger(JdbcProjectionRepository.class);

  private final DataSource dataSource;
  private final ObjectMapper objectMapper;

  /**
   * The projection names this repository stores read models under: lower-case ASCII letters, digits
   * and underscores, starting with a letter or an underscore, at most 58 characters. The table is
   * the name plus {@code _view}, so it never exceeds PostgreSQL's 63-byte identifier limit (it is
   * never truncated), is never case-folded, and is never a keyword — no keyword ends in {@code
   * _view}. The name is used exactly as given, never rewritten, so two different names always have
   * two different tables.
   */
  private static final Pattern TABLE_NAMED_PROJECTION = Pattern.compile("[a-z_][a-z0-9_]{0,57}");

  private static final Set<String> DIRECT_COLUMNS = Set.of("id", "updated_at", "version");

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
   * @param metrics the crypto-redaction metrics collector (null → {@link StreamRuneMetrics#NOOP})
   * @return a new {@link ObjectMapper}, independent from any other mapper instance
   */
  public static ObjectMapper createObjectMapper(
      CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    if (cryptoEngine != null) {
      mapper.registerModule(
          new org.streamrune.crypto.CryptoShreddingModule(
              cryptoEngine, metrics == null ? StreamRuneMetrics.NOOP : metrics));
    }
    return mapper;
  }

  public JdbcProjectionRepository(DataSource dataSource, ObjectMapper objectMapper) {
    if (dataSource == null) throw new IllegalArgumentException("dataSource is required");
    if (objectMapper == null) throw new IllegalArgumentException("objectMapper is required");
    this.dataSource = dataSource;
    this.objectMapper = objectMapper;
  }

  @Override
  public <T> void save(ProjectionName projectionName, String id, T readModel) {
    validateInputs(projectionName, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
    ensureTableExists(projectionName);
    try (var conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "projection save",
          c -> {
            saveOn(c, projectionName, id, readModel);
            return null;
          });
    } catch (SQLException e) {
      throw new EventStoreException("Failed to save projection: " + shown(projectionName, id), e);
    }
  }

  private <T> void saveOn(Connection conn, ProjectionName projectionName, String id, T readModel) {
    String table = tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(UPSERT, table))) {
      ps.setString(1, id);
      ps.setString(2, objectMapper.writeValueAsString(readModel));
      ps.executeUpdate();
    } catch (SQLException | JsonProcessingException e) {
      throw new EventStoreException("Failed to save projection: " + shown(projectionName, id), e);
    }
  }

  @Override
  public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
    validateInputs(projectionName, id);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    ensureTableExists(projectionName);
    try (var conn = dataSource.getConnection()) {
      return findByIdOn(conn, projectionName, id, type);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to find projection: " + shown(projectionName, id), e);
    }
  }

  private <T> Optional<T> findByIdOn(
      Connection conn, ProjectionName projectionName, String id, Class<T> type) {
    String table = tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(SELECT_BY_ID, table))) {
      ps.setString(1, id);
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          String json = rs.getString("data");
          return Optional.of(objectMapper.readValue(json, type));
        }
        return Optional.empty();
      }
    } catch (SQLException | JsonProcessingException e) {
      throw new EventStoreException("Failed to find projection: " + shown(projectionName, id), e);
    }
  }

  @Override
  public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
    validateProjectionName(projectionName);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    ensureTableExists(projectionName);
    try (var conn = dataSource.getConnection()) {
      // Read inside an explicit transaction so the driver streams the rows in
      // FIND_ALL_FETCH_SIZE batches instead of materializing the whole table.
      conn.setAutoCommit(false);
      try {
        List<T> results = findAllOn(conn, projectionName, type);
        conn.commit();
        return results;
      } catch (RuntimeException e) {
        conn.rollback();
        throw e;
      } finally {
        // Restore auto-commit before the connection returns to the pool
        conn.setAutoCommit(true);
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to find all projections: " + shown(projectionName), e);
    }
  }

  private <T> List<T> findAllOn(Connection conn, ProjectionName projectionName, Class<T> type) {
    String table = tableName(projectionName);
    List<T> results = new ArrayList<>();
    try (var ps = conn.prepareStatement(String.format(SELECT_ALL, table))) {
      // Streams only when the connection is in an explicit transaction: the public findAll
      // opens one, executeAtomically's TransactionScopedRepository already runs in one.
      ps.setFetchSize(FIND_ALL_FETCH_SIZE);
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          String json = rs.getString("data");
          results.add(objectMapper.readValue(json, type));
        }
      }
      return results;
    } catch (SQLException | JsonProcessingException e) {
      throw new EventStoreException("Failed to find all projections: " + shown(projectionName), e);
    }
  }

  @Override
  public void delete(ProjectionName projectionName, String id) {
    validateInputs(projectionName, id);
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
      throw new EventStoreException("Failed to delete projection: " + shown(projectionName, id), e);
    }
  }

  private void deleteOn(Connection conn, ProjectionName projectionName, String id) {
    String table = tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(DELETE, table))) {
      ps.setString(1, id);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new EventStoreException("Failed to delete projection: " + shown(projectionName, id), e);
    }
  }

  @Override
  public <T> Page<T> findAll(
      ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
    validateProjectionName(projectionName);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (pageRequest == null) {
      throw new IllegalArgumentException("pageRequest cannot be null");
    }
    ensureTableExists(projectionName);
    try (var conn = dataSource.getConnection()) {
      return findAllOn(conn, projectionName, type, pageRequest);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find paginated projections: " + shown(projectionName), e);
    }
  }

  private <T> Page<T> findAllOn(
      Connection conn, ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
    String table = tableName(projectionName);
    String orderClause = buildOrderClause(pageRequest.sort());

    String contentSql =
        String.format("SELECT data FROM %s %s LIMIT ? OFFSET ?", table, orderClause);
    String countSql = String.format("SELECT COUNT(*) FROM %s", table);

    try {
      long totalElements;
      try (var countStmt = conn.prepareStatement(countSql);
          var countRs = countStmt.executeQuery()) {
        countRs.next();
        totalElements = countRs.getLong(1);
      }

      List<T> content = new ArrayList<>();
      try (var ps = conn.prepareStatement(contentSql)) {
        ps.setInt(1, pageRequest.size());
        ps.setInt(2, pageRequest.offset());
        try (var rs = ps.executeQuery()) {
          while (rs.next()) {
            content.add(objectMapper.readValue(rs.getString("data"), type));
          }
        }
      }

      return new Page<>(content, totalElements, pageRequest.page(), pageRequest.size());
    } catch (SQLException | JsonProcessingException e) {
      throw new EventStoreException(
          "Failed to find paginated projections: " + shown(projectionName), e);
    }
  }

  private String buildOrderClause(Sort sort) {
    if (sort.orders().isEmpty()) {
      return "ORDER BY id";
    }

    var parts = new ArrayList<String>();
    for (var order : sort.orders()) {
      String field = order.field();
      validateSortField(field);
      String column = DIRECT_COLUMNS.contains(field) ? field : "data->>'" + field + "'";
      String dir = order.direction() == SortDirection.DESC ? "DESC" : "ASC";
      parts.add(column + " " + dir);
    }
    return "ORDER BY " + String.join(", ", parts);
  }

  private void validateSortField(String field) {
    if (!field.matches("[a-zA-Z0-9_]+")) {
      throw new IllegalArgumentException(
          "Sort field must contain only alphanumeric characters and underscores: " + field);
    }
  }

  @Override
  public <T> Optional<Versioned<T>> findById(
      ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
    validateInputs(projectionName, id);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (lockMode == null) {
      throw new IllegalArgumentException("lockMode cannot be null");
    }
    ensureTableExists(projectionName);
    try (var conn = dataSource.getConnection()) {
      return findByIdOn(conn, projectionName, id, type, lockMode);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find projection with lock: " + shown(projectionName, id), e);
    }
  }

  private <T> Optional<Versioned<T>> findByIdOn(
      Connection conn, ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
    String table = tableName(projectionName);
    String sql =
        String.format(
            lockMode == LockMode.PESSIMISTIC ? SELECT_BY_ID_FOR_UPDATE : SELECT_BY_ID_VERSIONED,
            table);
    try (var ps = conn.prepareStatement(sql)) {
      ps.setString(1, id);
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          T data = objectMapper.readValue(rs.getString("data"), type);
          long version = rs.getLong("version");
          return Optional.of(new Versioned<>(data, version));
        }
        return Optional.empty();
      }
    } catch (SQLException | JsonProcessingException e) {
      throw new EventStoreException(
          "Failed to find projection with lock: " + shown(projectionName, id), e);
    }
  }

  @Override
  public <T> void save(
      ProjectionName projectionName, String id, T readModel, long expectedVersion) {
    validateInputs(projectionName, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must be >= 0");
    }
    ensureTableExists(projectionName);
    try (var conn = dataSource.getConnection()) {
      PostgresTransactions.commitIfManual(
          conn,
          "projection versioned save",
          c -> {
            saveOn(c, projectionName, id, readModel, expectedVersion);
            return null;
          });
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to save projection with version check: " + shown(projectionName, id), e);
    }
  }

  private <T> void saveOn(
      Connection conn,
      ProjectionName projectionName,
      String id,
      T readModel,
      long expectedVersion) {
    String table = tableName(projectionName);
    try (var ps = conn.prepareStatement(String.format(VERSIONED_UPDATE, table))) {
      ps.setString(1, objectMapper.writeValueAsString(readModel));
      ps.setString(2, id);
      ps.setLong(3, expectedVersion);
      int updated = ps.executeUpdate();
      if (updated == 0) {
        throw new OptimisticLockException(
            "Optimistic lock conflict on projection '%s/%s': expected version %d"
                .formatted(projectionName.value(), id, expectedVersion));
      }
    } catch (SQLException | JsonProcessingException e) {
      throw new EventStoreException(
          "Failed to save projection with version check: " + shown(projectionName, id), e);
    }
  }

  private void ensureTableExists(ProjectionName projectionName) {
    if (tablesChecked.contains(projectionName)) {
      return;
    }
    tableName(projectionName); // reject a name outside the rule before touching the database

    synchronized (this) {
      if (tablesChecked.contains(projectionName)) {
        return;
      }

      try (var conn = dataSource.getConnection()) {
        // Committed before the table is recorded as created: on a pool that hands out
        // autoCommit=false connections an uncommitted CREATE TABLE is rolled back on return, and a
        // recorded table would then be skipped by the batch transaction that needs it.
        PostgresTransactions.commitIfManual(
            conn,
            "projection table creation",
            c -> {
              createTable(c, projectionName);
              return null;
            });
        tablesChecked.add(projectionName);
      } catch (SQLException e) {
        throw new EventStoreException(
            "Failed to create projection table: " + tableName(projectionName), e);
      }
    }
  }

  private void createTable(Connection conn, ProjectionName projectionName) {
    String table = tableName(projectionName);
    String createSql =
        "CREATE TABLE IF NOT EXISTS "
            + table
            + " ("
            + "id VARCHAR(255) PRIMARY KEY, "
            + "data JSONB NOT NULL, "
            + "version BIGINT NOT NULL DEFAULT 0, "
            + "updated_at TIMESTAMPTZ DEFAULT NOW()"
            + ")";
    try (var stmt = conn.createStatement()) {
      stmt.execute(createSql);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to create projection table: " + table, e);
    }
  }

  /**
   * The read-model table of {@code projectionName}: the name plus {@code _view}. A name outside
   * {@link #TABLE_NAMED_PROJECTION} is rejected, not rewritten: the old mapping replaced every
   * other character with {@code _} and let PostgreSQL fold the case and truncate at 63 bytes, so
   * {@code order-summary}, {@code order.summary} and {@code order_summary}, or {@code Orders} and
   * {@code orders}, silently shared one table — rows overwrote each other, {@code findAll} returned
   * the other projection's rows, and a rebuild of one corrupted the other. The rule also keeps the
   * name safe to splice into SQL. The old reserved-word check ran on the name before {@code _view}
   * was appended and rejected harmless names such as {@code order}; it is gone.
   */
  private static String tableName(ProjectionName projectionName) {
    String name = projectionName.value();
    if (!TABLE_NAMED_PROJECTION.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Projection name '"
              + LogSanitizer.sanitizeForLog(name)
              + "' cannot name a read-model table: it must match [a-z_][a-z0-9_]{0,57} —"
              + " lower-case ASCII letters, digits and underscores, starting with a letter or an"
              + " underscore, at most 58 characters (the table is the name plus '_view'). The name"
              + " is used exactly as given, never rewritten, so that no two projection names share"
              + " a table: rename the projection, e.g. 'order_summary' instead of 'order-summary'"
              + " or 'OrderSummary'.");
    }
    return name + "_view";
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
    validateProjectionName(projectionName);
    tableName(projectionName);
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
   * UPDATE}-locks this projection's {@code projection_offset} row (seeding a 0 checkpoint if none
   * exists), so <em>all</em> writers for one projection serialize on that row. It then rejects a
   * batch whose <b>first</b> offset is at or before the committed checkpoint: such a batch overlaps
   * events another writer already applied — the exact split-brain window where a stale/superseded
   * leader read at an older checkpoint and fetched a range extending past a newer leader's advance.
   * The monotonic guard alone (endpoint {@literal >} stored) would let that overlapping batch
   * commit and re-apply its already-applied prefix a second time, silently corrupting a
   * non-idempotent read model (count++/balance/list-append). The overlap guard rolls the whole
   * transaction back instead. <b>Empty batches</b> (a bare offset advance) are still governed by
   * the monotonic guard in {@link #saveOffsetInternal}. This makes the read-model write — the write
   * through the handed repository — exactly-once under an overlapping-batch split-brain; it does
   * <em>not</em> remove the need for single-active-consumer leadership, which is what keeps two
   * writers from racing in the first place.
   *
   * <p><b>Epoch fencing.</b> Under the same {@code FOR UPDATE} lock the transaction also reads the
   * row's stored fencing {@code epoch} and rejects — before the updater writes a single read-model
   * row — any commit whose {@code fencingEpoch} is below it: a superseded leader whose lease a
   * newer one took over (bumping the epoch). On commit the epoch is stamped with {@code
   * GREATEST(stored, caller)} so it never regresses. A {@code fencingEpoch} of {@code 0} is
   * unfenced (single-node / {@link org.streamrune.core.subscription.SubscriptionLeadership#NOOP})
   * and always passes. This epoch fence, the first-offset overlap guard, and the monotonic offset
   * guard in {@link #saveOffsetInternal} are the three checks that keep the read-model write
   * through the handed repository exactly-once under split-brain; an HTTP call or a broker publish
   * inside the updater is not covered and is at-least-once.
   *
   * <p><b>After-commit actions.</b> An action registered through the supplied repository's {@link
   * ProjectionRepository#afterCommit} runs after the COMMIT, before this method returns, and is
   * discarded if the transaction rolls back. An action that throws a {@link RuntimeException} is
   * logged and never fails the committed batch. An action that throws an {@link Error} ends the
   * run: after a clean commit the Error propagates from this method, and when {@code commit()} or
   * closing the connection threw, it is added as suppressed to that exception, which stays the one
   * thrown. {@code CacheAwareProjection} evicts the query cache this way. The private {@code
   * AfterCommitActions} class lists the crash and failure points.
   *
   * <p>Implements {@link AtomicBatchProcessor#executeAtomically}. The {@code offsetStore} parameter
   * is ignored — the offset is saved internally within the same transaction as the projection
   * update.
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
    ensureTableExists(projectionName);

    var afterCommitActions = new AfterCommitActions(projectionName);
    try {
      commitBatch(
          projectionName, batch, newOffset, fencingEpoch, projectionUpdater, afterCommitActions);
    } catch (Throwable inFlight) {
      // After-commit actions: after the connection is back in the pool, and also when commit() or
      // closing the
      // connection failed after the COMMIT was issued; a no-op unless it was issued, so a
      // rolled-back batch discards its actions. The actions run with this exception in
      // flight, and an Error from one rides on it as suppressed instead of replacing it.
      afterCommitActions.runIfCommitIssued(inFlight);
      throw inFlight;
    }
    afterCommitActions.runIfCommitIssued(null);
  }

  /**
   * The transaction of {@link #executeAtomically}: locks the checkpoint row, runs the fences and
   * the updater, saves the offset and commits. The after-commit actions the updater registers are
   * queued on {@code afterCommitActions}; the caller runs them once this method has returned or
   * thrown.
   */
  private void commitBatch(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      AfterCommitActions afterCommitActions) {
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      var transactionScopedRepository = new TransactionScopedRepository(conn, afterCommitActions);
      boolean committed = false;
      try {
        // Lock the checkpoint row so all writers for this projection serialize here, then reject a
        // fenced-out (stale-epoch) or overlapping batch before it writes a single read-model row
        // (epoch fencing). Both checks read the row under the same FOR UPDATE.
        Checkpoint cp = lockAndReadCheckpoint(conn, projectionName);
        rejectStaleEpoch(projectionName, fencingEpoch, cp.epoch());
        rejectOverlappingBatch(projectionName, batch, cp.offset());

        // Execute projection update(s) on the transaction's connection
        projectionUpdater.update(transactionScopedRepository);

        // Save offset — both in same transaction, stamping the caller's epoch on the checkpoint.
        saveOffsetInternal(conn, projectionName, newOffset, fencingEpoch);

        // From here on the outcome may be "committed" even if commit() throws (see
        // AfterCommitActions), so the after-commit actions run whatever commit() reports.
        afterCommitActions.commitIssued();
        conn.commit();
        committed = true;
        // Tables created inside the transaction survive only after commit
        tablesChecked.addAll(transactionScopedRepository.tablesEnsuredInTransaction);
      } catch (SQLException e) {
        // Wrap the transaction-body SQLException as before (rollback happens once, in the finally).
        throw new EventStoreException("Transaction failed during projection update", e);
      } finally {
        // The SINGLE rollback point for every non-committed exit — a
        // RuntimeException (e.g. an updater IllegalStateException or the rejectOverlappingBatch
        // OptimisticLock), the wrapped SQLException above, OR any OTHER Throwable (an Error such as
        // StackOverflowError / OutOfMemoryError / AssertionError from user mapping code in
        // projectionUpdater.update()). It must run BEFORE restoring autoCommit: pgjdbc's
        // setAutoCommit(true) COMMITS an open transaction, so without this rollback a
        // partially-applied read-model write would become durable WITHOUT the offset advance, and
        // the retried batch would re-apply the same events (the overlap guard passes
        // because the checkpoint never moved) — double-applying a non-idempotent projection.
        // Mirrors PostgresEventStore's append discipline. A FAILED rollback must not
        // be followed by that restore, and the pool must not be left to commit the transaction on
        // return (Agroal resets autoCommit with no rollback first), so the physical connection is
        // aborted instead and autoCommit is left alone (PostgresTransactions). An Error from the
        // rollback propagates after the abort.
        if (committed || PostgresTransactions.rollbackOrAbort(conn, "projection batch")) {
          // Restore auto-commit before the connection returns to the pool. Swallow a throw here:
          // on the success path the tx has already committed (a throw must not fail a durable
          // apply); on the abort path the rollback is already done.
          try {
            conn.setAutoCommit(true);
          } catch (SQLException restoreEx) {
            log.debug(
                "Failed to restore autoCommit before returning connection to pool (benign — pool"
                    + " resets on return): {}",
                restoreEx.getMessage());
          }
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to process projection batch atomically", e);
    }
  }

  /**
   * The actions registered through {@link TransactionScopedRepository#afterCommit} during one
   * {@link #executeAtomically} transaction. They run, in registration order, once the transaction's
   * COMMIT has been issued, and are discarded if the transaction rolls back before that.
   *
   * <p>{@code CacheAwareProjection} registers its query-cache eviction here. Run before the commit,
   * as it used to be, the eviction let a query landing between the projection's writes and the
   * commit read the pre-batch rows (READ COMMITTED: the batch's writes are invisible to other
   * connections until the commit) and cache them, and the cache then served the pre-batch read
   * model until the entry's TTL ran out.
   *
   * <p>Crash and failure points:
   *
   * <ul>
   *   <li>Failure before the COMMIT is issued (an updater throw, a fence or overlap rejection, the
   *       monotonic offset guard): the transaction rolls back and the actions are discarded. The
   *       read model did not change, so whatever the cache holds still matches it.
   *   <li>{@code commit()} throws: the outcome is unknown (the connection can drop after the server
   *       committed), so the actions run anyway. An unneeded eviction costs one cache miss; a
   *       missing one serves the pre-batch read model until the TTL.
   *   <li>An action throws a {@link RuntimeException}: logged, and the remaining actions still run.
   *       It never fails {@link #executeAtomically}: the batch is committed, and reporting it as
   *       failed would make the runner retry it, have the overlap guard reject the retry, and drop
   *       the projection to standby.
   *   <li>An action throws an {@link Error}: the actions after it do not run. After a clean commit
   *       the Error propagates from {@link #executeAtomically}; the batch and its checkpoint stay
   *       committed. When {@code commit()} or closing the connection threw, that exception is
   *       already in flight and stays the one thrown, with the Error added as suppressed: it
   *       reports the commit's unknown outcome, which the runner's retry resolves (the overlap
   *       guard rejects it if the server committed), and an Error from cache-eviction code must not
   *       hide it.
   *   <li>The process dies between the commit and the actions: the batch is committed and the
   *       checkpoint advanced, and the actions are lost. For the query cache that is safe: the
   *       {@code CachingQueryBus} cache lives in the same process's memory and dies with it, so the
   *       restarted process reads the committed read model on its first miss. No eviction is needed
   *       at startup or recovery. An action whose effect outlives the process is not covered and
   *       must not rely on running.
   * </ul>
   *
   * <p>An action registered after the actions ran (from an action, or through a repository kept
   * past {@link #executeAtomically}) runs at once: the writes are committed. One registered after a
   * rollback is discarded. Not thread-safe: the transaction-scoped repository, like the connection
   * it is bound to, belongs to the thread running the transaction.
   */
  private static final class AfterCommitActions {

    private final ProjectionName projectionName;
    private final List<Runnable> queued = new ArrayList<>();
    private boolean commitIssued;
    private boolean ran;

    private AfterCommitActions(ProjectionName projectionName) {
      this.projectionName = projectionName;
    }

    private void add(Runnable action) {
      if (ran) {
        runGuarded(action);
      } else {
        queued.add(action);
      }
    }

    private void commitIssued() {
      commitIssued = true;
    }

    /**
     * Runs the queued actions, in registration order, if the COMMIT was issued.
     *
     * @param inFlight the exception {@link #executeAtomically} is throwing ({@code commit()} or
     *     closing the connection failed after the COMMIT was issued), or {@code null} after a clean
     *     commit
     */
    private void runIfCommitIssued(Throwable inFlight) {
      if (!commitIssued) {
        return;
      }
      ran = true;
      try {
        for (Runnable action : queued) {
          runGuarded(action);
        }
      } catch (Error actionError) {
        // An Error ends the run. With nothing in flight it propagates, as it always has.
        // With the batch's exception in flight it must not replace it: that exception reports the
        // commit's unknown outcome, so the Error rides on it as suppressed. The JVM can throw one
        // preallocated Error instance more than once, and suppressing an exception on itself throws
        // an IllegalArgumentException that would replace the one in flight.
        if (inFlight == null) {
          throw actionError;
        }
        if (actionError != inFlight) {
          inFlight.addSuppressed(actionError);
        }
      }
      queued.clear();
    }

    private void runGuarded(Runnable action) {
      try {
        action.run();
      } catch (RuntimeException e) {
        log.warn(
            "Projection '{}' after-commit action failed; the batch stays committed and the"
                + " remaining after-commit actions still run",
            projectionName.value(),
            e);
      }
    }
  }

  /**
   * The committed checkpoint of a projection: its last applied offset and stamped fencing epoch.
   */
  private record Checkpoint(long offset, long epoch) {}

  /**
   * Seeds a {@code 0} checkpoint for {@code projectionName} if none exists, then {@code SELECT ...
   * FOR UPDATE}-locks the row and returns its stored offset and epoch. Seeding guarantees a row to
   * lock even for a first-ever application, so two concurrent writers for the same projection
   * serialize on this row rather than both proceeding past a missing row. Runs on the transaction's
   * connection so the lock is held until commit/rollback; the epoch read here feeds the {@link
   * #rejectStaleEpoch} fence (epoch fencing).
   */
  private Checkpoint lockAndReadCheckpoint(Connection conn, ProjectionName projectionName)
      throws SQLException {
    String seed =
        "INSERT INTO projection_offset (projection_name, last_offset, updated_at) "
            + "VALUES (?, 0, NOW()) ON CONFLICT (projection_name) DO NOTHING";
    try (var ps = conn.prepareStatement(seed)) {
      ps.setString(1, projectionName.value());
      ps.executeUpdate();
    }
    String select =
        "SELECT last_offset, epoch FROM projection_offset WHERE projection_name = ? FOR UPDATE";
    try (var ps = conn.prepareStatement(select)) {
      ps.setString(1, projectionName.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? new Checkpoint(rs.getLong(1), rs.getLong(2)) : new Checkpoint(0L, 0L);
      }
    }
  }

  /**
   * Fences out a commit whose caller epoch is below the epoch already stored on the checkpoint row
   * — a superseded leader whose lease was taken over by a newer one (which bumped the epoch).
   * Throws {@link OptimisticLockException} so {@link #executeAtomically} rolls the whole
   * transaction back <em>before</em> the updater writes a single read-model row. A {@code
   * fencingEpoch} of {@code 0} is unfenced (single-node / {@link
   * org.streamrune.core.subscription.SubscriptionLeadership#NOOP}) and always passes, so a
   * single-node deployment commits unfenced (epoch fencing).
   */
  private void rejectStaleEpoch(ProjectionName name, long fencingEpoch, long storedEpoch) {
    if (fencingEpoch != 0L && fencingEpoch < storedEpoch) {
      throw new ProjectionCommitFencedException(
          "Projection '"
              + shown(name)
              + "' commit fenced out: caller epoch "
              + fencingEpoch
              + " is below the stored epoch "
              + storedEpoch
              + " — a newer leader has taken over the lease. Rolling back the stale leader's write"
              + " (epoch fencing).");
    }
  }

  /**
   * Rejects a batch whose first event offset is at or before the committed checkpoint — it overlaps
   * events already applied by another writer (a concurrent/failed-over stale leader or a
   * redelivery). A forward batch always starts strictly after the checkpoint (the runners read the
   * global stream from {@code storedOffset}); an overlapping start means part of the batch would be
   * applied a second time, so the whole transaction rolls back. Empty batches carry no first offset
   * and are governed solely by the monotonic guard on the offset advance.
   */
  private void rejectOverlappingBatch(
      ProjectionName projectionName, List<EventEnvelope> batch, long storedOffset) {
    if (batch.isEmpty()) {
      return;
    }
    long firstOffset = batch.getFirst().globalOffset().value();
    if (firstOffset <= storedOffset) {
      throw new ProjectionCommitFencedException(
          "Projection '"
              + shown(projectionName)
              + "' batch starting at offset "
              + firstOffset
              + " overlaps the committed checkpoint ("
              + storedOffset
              + "): those events were already applied by another writer (a concurrent or"
              + " failed-over stale leader, or a redelivery). Rolling back so the read-model write is"
              + " not applied a second time.");
    }
  }

  /**
   * {@link ProjectionRepository} view bound to the single connection of an open transaction. Handed
   * to the projection updater by {@link #executeAtomically} so projection writes commit — or roll
   * back — together with the offset save. Never requests a pooled connection: table creation also
   * runs on the bound connection, eliminating self-deadlock on exhausted pools.
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
      createTable(conn, projectionName);
      tablesEnsuredInTransaction.add(projectionName);
    }

    @Override
    public <T> void save(ProjectionName projectionName, String id, T readModel) {
      validateInputs(projectionName, id);
      if (readModel == null) {
        throw new IllegalArgumentException("readModel cannot be null");
      }
      ensureTableOnConnection(projectionName);
      saveOn(conn, projectionName, id, readModel);
    }

    @Override
    public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
      validateInputs(projectionName, id);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      ensureTableOnConnection(projectionName);
      return findByIdOn(conn, projectionName, id, type);
    }

    @Override
    public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
      validateProjectionName(projectionName);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      ensureTableOnConnection(projectionName);
      return findAllOn(conn, projectionName, type);
    }

    @Override
    public void delete(ProjectionName projectionName, String id) {
      validateInputs(projectionName, id);
      ensureTableOnConnection(projectionName);
      deleteOn(conn, projectionName, id);
    }

    @Override
    public <T> Page<T> findAll(
        ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
      validateProjectionName(projectionName);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      if (pageRequest == null) {
        throw new IllegalArgumentException("pageRequest cannot be null");
      }
      ensureTableOnConnection(projectionName);
      return findAllOn(conn, projectionName, type, pageRequest);
    }

    @Override
    public <T> Optional<Versioned<T>> findById(
        ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
      validateInputs(projectionName, id);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      if (lockMode == null) {
        throw new IllegalArgumentException("lockMode cannot be null");
      }
      ensureTableOnConnection(projectionName);
      return findByIdOn(conn, projectionName, id, type, lockMode);
    }

    @Override
    public <T> void save(
        ProjectionName projectionName, String id, T readModel, long expectedVersion) {
      validateInputs(projectionName, id);
      if (readModel == null) {
        throw new IllegalArgumentException("readModel cannot be null");
      }
      if (expectedVersion < 0) {
        throw new IllegalArgumentException("expectedVersion must be >= 0");
      }
      ensureTableOnConnection(projectionName);
      saveOn(conn, projectionName, id, readModel, expectedVersion);
    }
  }

  private void saveOffsetInternal(
      Connection conn, ProjectionName projectionName, GlobalOffset offset, long fencingEpoch)
      throws SQLException {
    // Monotonic guard (byte-identical to PostgresOffsetStore.UPSERT_OFFSET): a backward/sideways
    // save updates 0 rows. Intentional rewinds use the rebuild/reset path, never saveOffset (see
    // OffsetStore javadoc). The advance also stamps the caller's fencing epoch with GREATEST so the
    // stored epoch is monotonic and never regresses below a newer leader's takeover (epoch
    // fencing);
    // an unfenced epoch-0 caller leaves the stored epoch unchanged. The stamp rides the
    // SAME monotonic WHERE guard, so a rejected (0-row) advance stamps nothing.
    String upsert =
        "INSERT INTO projection_offset (projection_name, last_offset, epoch, updated_at) "
            + "VALUES (?, ?, ?, NOW()) "
            + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = EXCLUDED.last_offset, "
            + "epoch = GREATEST(projection_offset.epoch, EXCLUDED.epoch), "
            + "updated_at = NOW() "
            + "WHERE EXCLUDED.last_offset > projection_offset.last_offset";
    try (var ps = conn.prepareStatement(upsert)) {
      ps.setString(1, projectionName.value());
      ps.setLong(2, offset.value());
      ps.setLong(3, fencingEpoch);
      int updated = ps.executeUpdate();
      // The read-model write (projectionUpdater.update, already run above on
      // this same transaction/connection) and this offset advance MUST be all-or-nothing. This
      // monotonic guard catches a NON-ADVANCING endpoint (newOffset <= stored) — a fully-behind
      // redelivery or a bare offset regression: a 0-row result means the stored offset is already
      // at/past this batch, so committing would DOUBLE-APPLY a non-idempotent projection (count+1,
      // balance, list append). Throw so executeAtomically rolls the whole batch back: the offset
      // does not regress and the read model is not double-applied. An OVERLAPPING batch whose
      // endpoint still ADVANCES past the checkpoint (stale leader read at an older checkpoint,
      // fetched a wider range) would pass THIS guard — it is rejected earlier by the first-offset
      // overlap check in executeAtomically, which holds the checkpoint row lock. Together they make
      // the read-model write exactly-once under split-brain; in normal single-leader forward
      // progress the guard always updates exactly one row and never fires.
      if (updated == 0) {
        throw new ProjectionCommitFencedException(
            "Projection '"
                + shown(projectionName)
                + "' offset advance to "
                + offset.value()
                + " was rejected by the monotonic guard (stored offset already at/past it): the"
                + " batch is already applied. Rolling back so the read-model write is not committed"
                + " a second time.");
      }
    }
  }

  /**
   * This repository honours the leadership fencing epoch: {@link #executeAtomically} runs {@code
   * rejectStaleEpoch} under the checkpoint row's {@code FOR UPDATE} lock <em>before</em> the
   * updater writes anything, and {@link #stampFencingEpoch} durably arms the fence at takeover. The
   * leadership-aware runners consult this to refuse booting a multi-replica projection on a
   * processor that cannot fence.
   *
   * @return always {@code true}
   */
  @Override
  public boolean supportsFencing() {
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
   * <p>{@link #rejectStaleEpoch} compares against the epoch STORED on the checkpoint row, which
   * {@link #saveOffsetInternal} stamps only on a committed advance — so from a takeover until the
   * new leader's first committed advance the row still carried the superseded leader's epoch and
   * ALL of that leader's writes passed the fence (its fail-open local lease view can outlive DB
   * lease expiry by a pause length, so a paused-then-resumed old leader could commit a stale
   * SKIP/DLQ forward-jump in that window — silently dropping events). Leadership-aware runners call
   * this immediately after acquiring the lease, before reading anything, which closes that window
   * down to the instants between the acquire and this stamp committing (an in-flight stale
   * transaction that already passed its fence read under the row lock is serialized before the
   * stamp and still lands — no takeover-time stamping can reject a fence check that already
   * happened; every transaction the old leader STARTS after this stamp commits is rejected).
   *
   * <p>Idempotent and monotonic: the {@code WHERE EXCLUDED.epoch > projection_offset.epoch} guard
   * makes a same-epoch re-stamp (every scheduled tick) a 0-row no-op — the stored epoch never
   * regresses (same {@code GREATEST} discipline as the commit stamp), and an unchanged row is not
   * rewritten. The INSERT arm seeds {@code last_offset = 0} for a projection that has never
   * committed — the same floor {@link #lockAndReadCheckpoint} seeds and {@code
   * saveOffsetInternal}'s monotonic {@code WHERE} measures against — and the conflict arm never
   * touches {@code last_offset}, so the checkpoint cannot move in either direction. Epoch {@code 0}
   * (unfenced / NOOP leadership) is skipped entirely: there is nothing to fence and stamping would
   * only churn the row.
   *
   * <p><b>A 0-row outcome is no longer silently accepted.</b> It used to be — the {@code
   * executeUpdate()} result was discarded — which made "same epoch, already stamped"
   * indistinguishable from "the epoch I was handed is BELOW the stored one". The second case is
   * never benign: the caller then runs under an epoch {@link #rejectStaleEpoch} rejects on every
   * commit, and the runners classify that rejection as a benign leadership/CAS signal, so the
   * projection freezes permanently at INFO level while reporting itself LIVE and healthy. On 0 rows
   * this now re-reads the stored epoch and, when it is strictly greater, throws {@link
   * ProjectionEpochRegressionException}. The re-read is a separate statement, so it can observe an
   * epoch newer than the one that actually beat us — harmless, because ANY stored epoch above ours
   * means our epoch is not authoritative and the caller must stop either way. An equal stored epoch
   * (the ordinary re-stamp) still returns quietly, and so does the vanishing case where the row was
   * deleted between the two statements.
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
    validateProjectionName(projectionName);
    if (fencingEpoch == 0L) {
      return;
    }
    String stamp =
        "INSERT INTO projection_offset (projection_name, last_offset, epoch, updated_at) "
            + "VALUES (?, 0, ?, NOW()) "
            + "ON CONFLICT (projection_name) DO UPDATE SET "
            + "epoch = GREATEST(projection_offset.epoch, EXCLUDED.epoch), updated_at = NOW() "
            + "WHERE EXCLUDED.epoch > projection_offset.epoch";
    try (var conn = dataSource.getConnection()) {
      int stamped =
          PostgresTransactions.commitIfManual(
              conn,
              "projection fencing stamp",
              c -> {
                try (var ps = c.prepareStatement(stamp)) {
                  ps.setString(1, projectionName.value());
                  ps.setLong(2, fencingEpoch);
                  return ps.executeUpdate();
                }
              });
      if (stamped == 0) {
        long stored = readStoredEpoch(conn, projectionName);
        if (stored > fencingEpoch) {
          throw new ProjectionEpochRegressionException(projectionName, fencingEpoch, stored);
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to stamp fencing epoch "
              + fencingEpoch
              + " for projection: "
              + shown(projectionName),
          e);
    }
  }

  /**
   * Reads the epoch currently stored on a projection's checkpoint row, or {@code 0} when the row
   * does not exist. Only called to explain a 0-row stamp.
   */
  private long readStoredEpoch(Connection conn, ProjectionName projectionName) throws SQLException {
    try (var ps =
        conn.prepareStatement("SELECT epoch FROM projection_offset WHERE projection_name = ?")) {
      ps.setString(1, projectionName.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong("epoch") : 0L;
      }
    }
  }

  /**
   * Rewinds the projection's checkpoint to offset 0, <b>bypassing the monotonic guard</b> in {@link
   * #saveOffsetInternal}. This is the reset/rebuild path referenced in the {@link OffsetStore}
   * contract: {@code saveOffset(name, initial())} would no-op the regression against the guard, so
   * a genuine reset must use this direct unguarded write.
   *
   * @param projectionName unique name of the projection to reset
   */
  public void resetOffset(ProjectionName projectionName) {
    validateProjectionName(projectionName);
    String reset =
        "INSERT INTO projection_offset (projection_name, last_offset, updated_at) "
            + "VALUES (?, 0, NOW()) "
            + "ON CONFLICT (projection_name) DO UPDATE SET last_offset = 0, updated_at = NOW()";
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(reset)) {
      ps.setString(1, projectionName.value());
      PostgresTransactions.commitIfManual(conn, "projection offset reset", c -> ps.executeUpdate());
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to reset offset for projection: " + shown(projectionName), e);
    }
  }

  /**
   * A projection name rendered for an exception message: the name and the row id are caller text,
   * so both pass through {@link LogSanitizer#sanitizeForLog} before they are embedded.
   */
  private static String shown(ProjectionName projectionName) {
    return LogSanitizer.sanitizeForLog(projectionName.value());
  }

  /** {@code projection/id} rendered for an exception message, both parts sanitized. */
  private static String shown(ProjectionName projectionName, String id) {
    return shown(projectionName) + "/" + LogSanitizer.sanitizeForLog(id);
  }

  private void validateInputs(ProjectionName projectionName, String id) {
    validateProjectionName(projectionName);
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("id cannot be null or blank");
    }
  }

  private void validateProjectionName(ProjectionName projectionName) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName cannot be null");
    }
  }
}
