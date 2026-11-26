package org.streamrune.sqlite;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamWriteConstraints;
import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
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
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.crypto.CryptoShreddingModule;

/**
 * SQLite-backed {@link ProjectionRepository} and {@link AtomicBatchProcessor}: the SQLite port of
 * the PostgreSQL projection repository, with the same reads, the same order and filter semantics,
 * and the same checkpoint fence.
 *
 * <p><b>Tables.</b> A projection's read models live in {@code <name>_view}, created lazily through
 * {@code CREATE TABLE IF NOT EXISTS}: {@code id TEXT NOT NULL PRIMARY KEY} (at most 255 characters,
 * as PostgreSQL's {@code VARCHAR(255)}), {@code data TEXT NOT NULL} holding valid JSON, {@code
 * version INTEGER NOT NULL DEFAULT 0} and {@code updated_at INTEGER NOT NULL} in epoch
 * microseconds, a {@code STRICT} table. The id compares byte-wise ({@code BINARY}), which in UTF-8
 * is code-point order, PostgreSQL's {@code COLLATE "C"}. A projection name must match {@code
 * [a-z_][a-z0-9_]{0,57}} and is used exactly as given, never rewritten; any other name is refused
 * with an {@link IllegalArgumentException}. A table is created by the first read or write of its
 * name through this repository, by {@link #declareIndex} or by {@link #prepareReadModel}; a
 * checkpoint commit alone creates none. Creating it is a write: it waits for SQLite's write lock.
 * Finding it is not: a table that exists is found with a plain read of the schema, so the first
 * read or write of a projection after a process start creates nothing and takes no lock for it.
 *
 * <p><b>Query semantics: PostgreSQL's text.</b> PostgreSQL filters and sorts on {@code
 * data->>'field'}, the JSON value as text. The paged reads here compare the same text ({@link
 * JsonFieldSql}): a string unquoted, a number in its JSON spelling, a boolean as its word, a
 * missing field and JSON {@code null} as SQL {@code NULL}; byte-wise ({@code COLLATE BINARY}),
 * {@code NULL} after every value ascending and before every value descending, the id breaking every
 * tie. So a read returns the rows, in the order, that PostgreSQL returns for the same data. An
 * object or an array field is the exception: its text is compact JSON here and PostgreSQL's {@code
 * jsonb} layout there (see {@link JsonFieldSql}).
 *
 * <p><b>Numbers are stored as {@code jsonb} stores them.</b> PostgreSQL keeps a JSON number as a
 * {@code numeric} and prints it in plain decimal notation with the scale it was written with:
 * {@code 1.2345678E7} reads back as {@code 12345678}, {@code 1.0E-4} as {@code 0.00010}, {@code
 * 19.90} as {@code 19.90}. A text column keeps whatever text it is given, so before it is stored
 * the JSON a read model serializes to is parsed and written again with every number in that plain
 * spelling (and, as {@code jsonb} does, the last of two equal keys kept). The text compared by the
 * reads, and the document read back, are then the ones PostgreSQL holds. What {@code jsonb} refuses
 * is refused here too, before anything is written: text that is not JSON (a {@code RawValue} can
 * write it), a string or a key holding U+0000, a number whose written exponent is beyond
 * &plusmn;1073741823 (even a zero), and a number with more than 131072 digits before the decimal
 * point or 16383 after it. One limit is SQLite's own: its JSON functions nest at most 1000 levels
 * deep, so a deeper document (which Jackson's default writer does not produce either) is refused
 * too. In a batch each of these refusals aborts the transaction, as the failed INSERT does on
 * PostgreSQL.
 *
 * <p><b>The checkpoint and the fence.</b> Checkpoints live in {@code readmodel_checkpoint} of the
 * same database file, the table {@link SqliteReadModelCheckpoints} reads; a repository built over
 * checkpoints of another file is refused. {@link #executeAtomically} runs the checkpoint fence, the
 * updater's writes and the checkpoint advance in one {@code BEGIN IMMEDIATE} ... {@code COMMIT}
 * transaction (see {@link #executeAtomically}); {@link #executeReplay} runs the same lock and the
 * updater without moving the checkpoint.
 *
 * <p><b>One writer at a time.</b> SQLite's write lock covers the whole database file. A batch or a
 * replay holds it from its {@code BEGIN IMMEDIATE} to its {@code COMMIT}, so every other write to
 * the file, of any projection, through any connection, waits for it, at most the connection's
 * {@code busy_timeout}, and fails after it with an {@link EventStoreException} saying the write
 * lock was not granted. A writer that also waits for a connection, on a pool with fewer connections
 * than writers, can instead fail when the pool's own timeout (HikariCP's {@code connectionTimeout})
 * runs out, with the operation's connection-failure message and the pool's exception as its cause.
 * So the lock is not per projection as on PostgreSQL: a dead-letter replay of one projection waits
 * for an open batch of any other, and the other way round; {@code busy_timeout} must exceed the
 * longest batch or replay the file runs. A read does not wait: in WAL mode it sees the last
 * committed state. The one read that does is the first read of a projection whose table does not
 * exist yet, which creates the table.
 *
 * <p><b>Inside an updater.</b> The updater writes through the repository it is handed. A call
 * through this repository itself, or through another {@code SqliteProjectionRepository} of the same
 * file, joins the updater's transaction when it is made on the updater's thread while the updater
 * runs: it runs on the transaction's connection, sees the updater's uncommitted writes, commits and
 * rolls back with the batch, and its first failed statement aborts the batch as one of the handed
 * repository does. So a projection that is handed no repository (an {@code
 * AT_LEAST_ONCE_IDEMPOTENT} registration routed to this store) and writes through the repository it
 * captured is applied in one transaction with its checkpoint, and a lookup of another projection's
 * read model from an updater needs no second connection. {@link #declareIndex} is refused there,
 * and {@link #afterCommit} queues its action with the batch's. Only these calls join: a thread the
 * updater started, and every other writer of the file (the event store, another store, a nested
 * {@link #executeAtomically}), works on a connection of its own, and a write there waits for the
 * updater's own transaction and fails after {@code busy_timeout}. There is one exception, and it is
 * why such a write is never safe: once SQLite has rolled the updater's transaction back on its own
 * ({@code SQLITE_FULL}, an I/O error), the write lock is released, so such a write does not wait
 * and commits at once, on its own, outside the batch (which still throws and rolls back). The abort
 * of a batch on a failed statement covers the handed repository and the joined calls only.
 *
 * <p>{@link #transactional()} and {@link #fencesCheckpoint()} are both {@code true}. No mode makes
 * an effect outside this database exactly-once.
 */
public final class SqliteProjectionRepository
    implements ProjectionRepository, AtomicBatchProcessor {

  private static final Logger log = LoggerFactory.getLogger(SqliteProjectionRepository.class);

  /**
   * The projection names this repository stores read models under: lower-case ASCII letters, digits
   * and underscores, starting with a letter or an underscore, at most 58 characters, as on
   * PostgreSQL, so a name valid on one backend is valid on the other, except a name starting with
   * {@code sqlite_}, which SQLite reserves ({@link #RESERVED_PREFIX}). The name is used exactly as
   * given, so two names always have two tables, and it is safe to splice into a statement.
   */
  private static final Pattern TABLE_NAMED_PROJECTION = Pattern.compile("[a-z_][a-z0-9_]{0,57}");

  /**
   * The prefix SQLite reserves for its own schema objects: a projection named with it is refused at
   * registration, where PostgreSQL would accept it.
   */
  private static final String RESERVED_PREFIX = "sqlite_";

  private static final Set<String> DIRECT_COLUMNS = Set.of("id", "updated_at", "version");

  /**
   * A sort, filter or index field: a top-level read-model field (or a repository column) of ASCII
   * letters, digits and underscores, as on PostgreSQL. No nested path. {@link JsonFieldSql} escapes
   * any key on its own; this rule keeps the two backends accepting the same fields.
   */
  private static final Pattern FIELD = Pattern.compile("[A-Za-z0-9_]+");

  // The statements; %1$s is the table name, which tableName() validated, and nothing else is
  // spliced in.

  private static final String CREATE_TABLE =
      "CREATE TABLE IF NOT EXISTS %1$s ("
          + "id TEXT NOT NULL PRIMARY KEY CHECK (length(id) <= 255), "
          + "data TEXT NOT NULL CHECK (json_valid(data)), "
          + "version INTEGER NOT NULL DEFAULT 0, "
          + "updated_at INTEGER NOT NULL"
          + ") STRICT";

  // Parameters: id, data, updated_at.
  private static final String UPSERT =
      "INSERT INTO %1$s (id, data, version, updated_at) VALUES (?, ?, 0, ?)"
          + " ON CONFLICT (id) DO UPDATE SET data = excluded.data,"
          + " version = %1$s.version + 1, updated_at = excluded.updated_at";

  // Parameters: data, updated_at, id, expected version.
  private static final String VERSIONED_UPDATE =
      "UPDATE %1$s SET data = ?, version = version + 1, updated_at = ?"
          + " WHERE id = ? AND version = ?";

  private static final String SELECT_BY_ID = "SELECT data FROM %1$s WHERE id = ?";
  private static final String SELECT_BY_ID_VERSIONED =
      "SELECT data, version FROM %1$s WHERE id = ?";
  private static final String SELECT_ALL = "SELECT data FROM %1$s";
  private static final String DELETE = "DELETE FROM %1$s WHERE id = ?";

  // Every schema object of the name, whatever its letter case: SQLite's names are case-insensitive.
  private static final String SCHEMA_OBJECT =
      "SELECT type, tbl_name FROM sqlite_schema WHERE name = ? COLLATE NOCASE";

  // A table or a view of the name, whatever its letter case: what CREATE TABLE IF NOT EXISTS finds.
  private static final String TABLE_EXISTS =
      "SELECT 1 FROM sqlite_schema WHERE type IN ('table', 'view') AND name = ? COLLATE NOCASE";

  /**
   * The deepest nesting SQLite's JSON functions accept ({@code json_valid} of a document nested
   * deeper is false), the document's own object or array included. PostgreSQL's {@code jsonb} has
   * no such limit.
   */
  private static final int MAX_NESTING_DEPTH = 1000;

  /**
   * The largest magnitude of a number's written exponent PostgreSQL's {@code numeric} reads ({@code
   * INT_MAX / 2}); beyond it, it refuses the number as "value overflows numeric format", even a
   * zero.
   */
  private static final long NUMERIC_EXPONENT = 1_073_741_823L;

  private static final String DEPTH_REFUSAL =
      "the document nests deeper than "
          + MAX_NESTING_DEPTH
          + " levels, the most SQLite's JSON functions accept";

  private static final String EXPONENT_REFUSAL =
      "a number's exponent is beyond ±"
          + NUMERIC_EXPONENT
          + ", which PostgreSQL's numeric refuses (value overflows numeric format)";

  private static final String NOT_JSON_REFUSAL =
      "the read model serializes to text that is not JSON, which PostgreSQL's jsonb refuses";

  /**
   * Re-reads the JSON a read model serializes to and writes it back: every decimal number kept as
   * written (a {@code BigDecimal}, trailing zeros included), and no limit on the length of a
   * string, a number or a key: PostgreSQL's {@code jsonb} has none this short. The nesting depth is
   * bounded by {@link #MAX_NESTING_DEPTH}, SQLite's own limit, which also bounds the recursion of
   * the checks that walk the tree. Its limits on numbers are applied by {@link #jsonbText}.
   */
  private static final ObjectMapper JSONB =
      JsonMapper.builder(
              JsonFactory.builder()
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxStringLength(Integer.MAX_VALUE)
                          .maxNumberLength(Integer.MAX_VALUE)
                          .maxNameLength(Integer.MAX_VALUE)
                          .maxNestingDepth(MAX_NESTING_DEPTH)
                          .build())
                  .streamWriteConstraints(
                      StreamWriteConstraints.builder().maxNestingDepth(MAX_NESTING_DEPTH).build())
                  .build())
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          // jsonb refuses text with anything after the document ({} {}), so the re-read does too
          // instead of keeping the first document.
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
          .build();

  /** The most digits PostgreSQL's {@code numeric} stores before the decimal point. */
  private static final int NUMERIC_INTEGER_DIGITS = 131_072;

  /** The most digits PostgreSQL's {@code numeric} stores after the decimal point. */
  private static final int NUMERIC_FRACTION_DIGITS = 16_383;

  private final DataSource dataSource;
  private final Path file;
  private final ObjectMapper objectMapper;

  /** The fence over {@code readmodel_checkpoint}, shared with the checkpoints' reads. */
  private final SqliteCheckpointFence checkpointFence;

  /** Stamps {@code updated_at}: the clock the checkpoints stamp theirs with. */
  private final Clock clock;

  /** The projections whose table this repository created or found, committed. */
  private final Set<ProjectionName> tablesChecked = ConcurrentHashMap.newKeySet();

  /** The JSON names of each read-model type's {@code @Encrypted} components. */
  private final ConcurrentHashMap<Class<?>, Set<String>> encryptedFields =
      new ConcurrentHashMap<>();

  /**
   * Creates a repository over {@code database} whose read models are (de)serialized with {@code
   * objectMapper} and whose checkpoints are {@code checkpoints}. Build the mapper with {@link
   * #createObjectMapper(CryptoEngine, StreamRuneMetrics)} to encrypt {@code @Encrypted} fields at
   * rest; a mapper without {@link CryptoShreddingModule} writes them as plaintext, outside GDPR
   * forget scope. {@code updated_at} is stamped with the checkpoints' clock.
   *
   * @param database the read-model database; its read-model series must be migrated
   * @param objectMapper serializes the read models
   * @param checkpoints the checkpoints of {@code database}, which the batches advance
   * @throws IllegalArgumentException if an argument is {@code null}, or {@code checkpoints} live in
   *     another database file
   */
  public SqliteProjectionRepository(
      SqliteDatabase database, ObjectMapper objectMapper, SqliteReadModelCheckpoints checkpoints) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (objectMapper == null) {
      throw new IllegalArgumentException("objectMapper is required");
    }
    if (checkpoints == null) {
      throw new IllegalArgumentException("checkpoints is required");
    }
    if (!database.file().equals(checkpoints.file())) {
      throw new IllegalArgumentException(
          "The checkpoints live in "
              + checkpoints.file()
              + ", not in the repository's database "
              + database.file()
              + ": a batch advances the checkpoint in its own transaction on the repository's"
              + " database, so the checkpoints must be SqliteReadModelCheckpoints of that same"
              + " database");
    }
    this.dataSource = database.dataSource();
    this.file = database.file();
    this.objectMapper = objectMapper;
    this.checkpointFence = checkpoints.fence();
    this.clock = checkpointFence.clock();
  }

  /**
   * Builds the read-model {@link ObjectMapper} the PostgreSQL repository builds: {@link
   * JavaTimeModule} with dates as ISO-8601 text, plus {@link CryptoShreddingModule} with {@code
   * metrics} when {@code cryptoEngine} is not {@code null}, so {@code @Encrypted} read-model fields
   * are encrypted at rest and fall within GDPR forget scope.
   *
   * @param cryptoEngine the crypto engine, or {@code null} for a crypto-blind mapper that writes
   *     {@code @Encrypted} fields as plaintext
   * @param metrics the crypto-redaction metrics collector; {@link StreamRuneMetrics#NOOP} for none
   * @return a new {@link ObjectMapper}
   * @throws NullPointerException if {@code metrics} is {@code null}
   */
  public static ObjectMapper createObjectMapper(
      CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    Objects.requireNonNull(metrics, "metrics is required");
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    if (cryptoEngine != null) {
      mapper.registerModule(new CryptoShreddingModule(cryptoEngine, metrics));
    }
    return mapper;
  }

  // Connection discipline: a read model is turned into JSON before a connection is borrowed and
  // rows back into objects after it is returned, so a crypto engine that borrows a connection
  // never waits on this repository's. Only the transaction-scoped repository (de)serializes while
  // its transaction is open.

  /**
   * The view of the batch or replay transaction this thread holds on this repository's file while
   * its updater runs, or {@code null} when it holds none: on any other thread, on another file,
   * outside an updater. Every read and write of this repository asks first and, inside such a
   * transaction, runs through the view: on the transaction's connection, with this repository's
   * mapper, committed, rolled back and aborted with the batch (see the class Javadoc).
   */
  private TransactionScopedRepository joined() {
    SqliteWriteTransaction held = SqliteWriteTransaction.heldOn(file);
    return held == null ? null : new TransactionScopedRepository(held.batch());
  }

  @Override
  public <T> void save(ProjectionName projectionName, String id, T readModel) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      joined.save(projectionName, id, readModel);
      return;
    }
    validateInputs(projectionName, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
    ensureTableExists(projectionName);
    String json = toJson(readModel, "Failed to save projection: ", projectionName, id);
    long now = now();
    SqliteTransactions.write(
        dataSource,
        "projection save",
        "save of projection " + shown(projectionName, id),
        "Failed to save projection: " + shown(projectionName, id),
        conn -> {
          upsert(conn, projectionName, id, json, now);
          return null;
        });
  }

  private static void upsert(
      Connection conn, ProjectionName projectionName, String id, String json, long now)
      throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(String.format(UPSERT, tableName(projectionName)))) {
      ps.setString(1, id);
      ps.setString(2, json);
      ps.setLong(3, now);
      ps.executeUpdate();
    }
  }

  @Override
  public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      return joined.findById(projectionName, id, type);
    }
    validateInputs(projectionName, id);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    ensureTableExists(projectionName);
    Optional<String> json;
    try (Connection conn = dataSource.getConnection()) {
      json = findJsonById(conn, projectionName, id);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to find projection: " + shown(projectionName, id), e);
    }
    return json.map(j -> fromJson(j, type, "Failed to find projection: ", projectionName, id));
  }

  private static Optional<String> findJsonById(
      Connection conn, ProjectionName projectionName, String id) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(String.format(SELECT_BY_ID, tableName(projectionName)))) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
      }
    }
  }

  @Override
  public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      return joined.findAll(projectionName, type);
    }
    validateProjectionName(projectionName);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    ensureTableExists(projectionName);
    List<String> rows;
    try (Connection conn = dataSource.getConnection()) {
      rows = findAllJson(conn, projectionName);
    } catch (SQLException e) {
      throw new EventStoreException("Failed to find all projections: " + shown(projectionName), e);
    }
    return fromJson(rows, type, "Failed to find all projections: ", projectionName);
  }

  private static List<String> findAllJson(Connection conn, ProjectionName projectionName)
      throws SQLException {
    var rows = new ArrayList<String>();
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(String.format(SELECT_ALL, tableName(projectionName)))) {
      while (rs.next()) {
        rows.add(rs.getString(1));
      }
    }
    return rows;
  }

  @Override
  public void delete(ProjectionName projectionName, String id) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      joined.delete(projectionName, id);
      return;
    }
    validateInputs(projectionName, id);
    ensureTableExists(projectionName);
    SqliteTransactions.write(
        dataSource,
        "projection delete",
        "delete of projection " + shown(projectionName, id),
        "Failed to delete projection: " + shown(projectionName, id),
        conn -> {
          deleteRow(conn, projectionName, id);
          return null;
        });
  }

  private static void deleteRow(Connection conn, ProjectionName projectionName, String id)
      throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(String.format(DELETE, tableName(projectionName)))) {
      ps.setString(1, id);
      ps.executeUpdate();
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>One statement: the count and the page are two subqueries of one {@code SELECT}, so they read
   * one snapshot. The order is {@code ORDER BY} the sort's expressions, each JSON field as its
   * {@link JsonFieldSql} text {@code COLLATE BINARY} with {@code NULLS LAST} ascending and {@code
   * NULLS FIRST} descending, and the id as {@code id COLLATE BINARY}; {@link
   * ProjectionIndex#forSort} declares the index that serves it.
   */
  @Override
  public <T> Page<T> findAll(
      ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      return joined.findAll(projectionName, type, pageRequest);
    }
    validateProjectionName(projectionName);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (pageRequest == null) {
      throw new IllegalArgumentException("pageRequest cannot be null");
    }
    refuseEncryptedKeys(type, null, pageRequest.sort());
    return page(projectionName, type, null, pageRequest);
  }

  /**
   * {@inheritDoc}
   *
   * <p>One statement, as for {@link #findAll(ProjectionName, Class, PageRequest)}, with {@code
   * WHERE <field text> = ?} in the count and in the page; {@link ProjectionIndex#forFilter}
   * declares the index that serves it.
   */
  @Override
  public <T> Page<T> findAllBy(
      ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      return joined.findAllBy(projectionName, type, filter, pageRequest);
    }
    validateFilteredRead(projectionName, type, filter, pageRequest);
    refuseEncryptedKeys(type, filter, pageRequest.sort());
    return page(projectionName, type, filter, pageRequest);
  }

  private <T> Page<T> page(
      ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
    String sql = pageSql(projectionName, filter, pageRequest.sort()); // validates before any I/O
    ensureTableExists(projectionName);
    Page<String> rows;
    try (Connection conn = dataSource.getConnection()) {
      rows = pageJson(conn, sql, filter, pageRequest);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find paginated projections: " + shown(projectionName), e);
    }
    return fromJson(rows, type, projectionName);
  }

  private static void validateFilteredRead(
      ProjectionName projectionName, Class<?> type, FieldEquals filter, PageRequest pageRequest) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName cannot be null");
    }
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (filter == null) {
      throw new IllegalArgumentException("filter cannot be null");
    }
    if (pageRequest == null) {
      throw new IllegalArgumentException("pageRequest cannot be null");
    }
  }

  /**
   * The statement of a paged read: the count of the selected rows and one page of them, two
   * subqueries joined into one result, so both come from one snapshot. Every row carries the count;
   * a page beyond the last is one row with the count and a {@code NULL} {@code data} ({@code data}
   * is {@code NOT NULL} in the table, so {@code NULL} means "no row"). The page subquery selects
   * its sort keys too, so the outer {@code ORDER BY} restores its order after the join (at most
   * {@link PageRequest} size rows). Parameters: the filter value (when there is a filter), the
   * filter value again, {@code LIMIT}, {@code OFFSET}.
   *
   * @throws IllegalArgumentException if the projection name, the filter field or a sort field is
   *     outside the rules
   */
  static String pageSql(ProjectionName projectionName, FieldEquals filter, Sort sort) {
    String table = tableName(projectionName);
    String where = filter == null ? "" : " WHERE " + filterExpression(filter.field()) + " = ?";
    List<Sort.Order> keys = keysWithIdTiebreak(sort.orders());
    var selected = new ArrayList<String>();
    var inner = new ArrayList<String>();
    var outer = new ArrayList<String>();
    for (int i = 0; i < keys.size(); i++) {
      Sort.Order key = keys.get(i);
      String order = orderOf(key);
      String expression = keyExpression(key.field());
      selected.add(expression + " AS k" + i);
      inner.add(expression + order);
      outer.add("page.k" + i + order);
    }
    return "SELECT total.n, page.data FROM (SELECT count(*) AS n FROM "
        + table
        + where
        + ") AS total LEFT JOIN (SELECT data, "
        + String.join(", ", selected)
        + " FROM "
        + table
        + where
        + " ORDER BY "
        + String.join(", ", inner)
        + " LIMIT ? OFFSET ?) AS page ON 1 ORDER BY "
        + String.join(", ", outer);
  }

  private static Page<String> pageJson(
      Connection conn, String sql, FieldEquals filter, PageRequest pageRequest)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      int parameter = 1;
      if (filter != null) {
        ps.setString(parameter++, filter.value());
        ps.setString(parameter++, filter.value());
      }
      ps.setInt(parameter++, pageRequest.size());
      ps.setLong(parameter, pageRequest.offset());
      long totalElements = 0;
      var content = new ArrayList<String>();
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          totalElements = rs.getLong(1);
          String data = rs.getString(2);
          if (data != null) {
            content.add(data);
          }
        }
      }
      return new Page<>(content, totalElements, pageRequest.page(), pageRequest.size());
    }
  }

  private <T> Page<T> fromJson(Page<String> rows, Class<T> type, ProjectionName projectionName) {
    return new Page<>(
        fromJson(rows.content(), type, "Failed to find paginated projections: ", projectionName),
        rows.totalElements(),
        rows.page(),
        rows.size());
  }

  /** {@code orders}, then {@code id} ascending unless an order already names the id. */
  private static List<Sort.Order> keysWithIdTiebreak(List<Sort.Order> orders) {
    var keys = new ArrayList<>(orders);
    if (orders.stream().noneMatch(order -> "id".equals(order.field()))) {
      keys.add(new Sort.Order("id", SortDirection.ASC));
    }
    return keys;
  }

  /**
   * The direction of a sort key, and for a JSON field where its {@code NULL}s go: PostgreSQL's
   * default, after every value ascending and before every value descending. SQLite's default is the
   * opposite, so the clause is spelled out; the repository columns are never {@code NULL}.
   */
  private static String orderOf(Sort.Order key) {
    boolean descending = key.direction() == SortDirection.DESC;
    if (DIRECT_COLUMNS.contains(key.field())) {
      return descending ? " DESC" : "";
    }
    return descending ? " DESC NULLS FIRST" : " NULLS LAST";
  }

  /**
   * The SQL a sort key compares: a repository column as itself (the id byte-wise), any other name
   * as the read model's top-level JSON field, as text, byte-wise.
   */
  private static String keyExpression(String field) {
    validateField(field);
    return switch (field) {
      case "id" -> "id COLLATE BINARY";
      case "version", "updated_at" -> field;
      default -> JsonFieldSql.text(field) + " COLLATE BINARY";
    };
  }

  /** The SQL a filter compares: a read-model field ({@link FieldEquals} refuses the columns). */
  private static String filterExpression(String field) {
    validateField(field);
    return JsonFieldSql.text(field);
  }

  /** The SQL an index key holds: the expression the reads compare, without the collation. */
  private static String indexExpression(String field) {
    validateField(field);
    return DIRECT_COLUMNS.contains(field) ? field : JsonFieldSql.text(field);
  }

  private static void validateField(String field) {
    if (!FIELD.matcher(field).matches()) {
      throw new IllegalArgumentException(
          "A sort, filter or index field must be a top-level read-model field of ASCII letters,"
              + " digits and underscores (no nested path): "
              + LogSanitizer.sanitizeForLog(field));
    }
  }

  // ---- declared indexes ----

  /**
   * Creates {@code index} on its projection's table, creating the table first if it does not exist:
   * {@code CREATE INDEX IF NOT EXISTS <index name> ON <projection>_view (<keys>)}, committed before
   * it returns. Call it at wiring, once per index the projection's reads need. Idempotent: an index
   * of that name on the projection's table is left as it is.
   *
   * <p><b>Keys.</b> Each JSON field is indexed as its {@link JsonFieldSql} expression, the same
   * text the reads compare: SQLite uses an index on an expression only for a query whose expression
   * is identical. The id is the last key, appended unless the keys name it. The index serves the
   * filter and the {@code ORDER BY} of the first sort key; a later JSON sort key, whose {@code
   * NULL}s go where SQLite's index does not keep them, is ordered after the index lookup.
   *
   * <p><b>Name.</b> {@code <projection>_<keys>_ix_<hash>}, as on PostgreSQL: the projection name
   * and the lower-cased keys ({@code _desc} after a descending one), cut at 63 characters with the
   * hash, then the first ten hex digits of a SHA-256 of the table and the exact column list. {@link
   * #createIndexSql} returns the statement; it is logged at INFO when this method creates the
   * index.
   *
   * <p>The check and the creation run in one write transaction, so two declarations of one index,
   * in this process or through another connection, create it once. The build holds SQLite's write
   * lock: on a large table every write to the file waits for it.
   *
   * @param index the index to create
   * @throws IllegalArgumentException if {@code index} is {@code null}, or its projection name or a
   *     key is outside the rules
   * @throws EventStoreException if the table or the index cannot be created, or a schema object of
   *     the index's name exists that is not an index of the projection's table
   */
  @Override
  public void declareIndex(ProjectionIndex index) {
    if (index == null) {
      throw new IllegalArgumentException("index cannot be null");
    }
    String ddl = createIndexSql(index); // validates the name and the keys before any I/O
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      joined.declareIndex(index); // refused: not inside a batch
      return;
    }
    String name = indexName(index);
    String table = tableName(index.projection());
    ensureTableExists(index.projection());
    IndexOutcome outcome =
        SqliteTransactions.write(
            dataSource,
            "projection index creation",
            "creation of projection index " + name,
            "Failed to create projection index: " + name,
            conn -> {
              IndexOutcome existing = existingIndex(conn, name, table);
              if (existing != IndexOutcome.MISSING) {
                return existing;
              }
              try (Statement st = conn.createStatement()) {
                st.execute(ddl);
              }
              return IndexOutcome.CREATED;
            });
    switch (outcome) {
      case CREATED -> log.info("Created projection index: {}", ddl);
      case EXISTS, MISSING -> {
        // left as it is
      }
      case FOREIGN ->
          throw new EventStoreException(
              "A schema object named "
                  + name
                  + " exists and is not an index of "
                  + table
                  + ": the projection index of that name cannot be created; rename or drop it");
    }
  }

  /** What {@link #declareIndex} found or did. */
  private enum IndexOutcome {
    /** No schema object holds the name. */
    MISSING,
    /** Created by this declaration. */
    CREATED,
    /** An index of the name on the projection's table exists. */
    EXISTS,
    /** Another schema object holds the name. */
    FOREIGN
  }

  /**
   * Whether an index named {@code name} on {@code table} exists ({@link IndexOutcome#EXISTS}), no
   * schema object of that name exists ({@link IndexOutcome#MISSING}), or another object holds the
   * name ({@link IndexOutcome#FOREIGN}).
   */
  private static IndexOutcome existingIndex(Connection conn, String name, String table)
      throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(SCHEMA_OBJECT)) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return IndexOutcome.MISSING;
        }
        return "index".equals(rs.getString(1)) && table.equals(rs.getString(2))
            ? IndexOutcome.EXISTS
            : IndexOutcome.FOREIGN;
      }
    }
  }

  /**
   * The statement {@link #declareIndex} runs for {@code index}: {@code CREATE INDEX IF NOT EXISTS
   * <index name> ON <projection>_view (<keys>)}.
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
    return "CREATE INDEX IF NOT EXISTS "
        + indexName(index)
        + " ON "
        + tableName(index.projection())
        + " ("
        + indexColumns(index)
        + ")";
  }

  /** The column list of {@code index}'s statement: each key's expression and direction, id last. */
  private static String indexColumns(ProjectionIndex index) {
    var columns = new ArrayList<String>();
    for (Sort.Order key : keysWithIdTiebreak(index.keys())) {
      columns.add(
          indexExpression(key.field()) + (key.direction() == SortDirection.DESC ? " DESC" : ""));
    }
    return String.join(", ", columns);
  }

  private static final int INDEX_NAME_HASH_DIGITS = 10;
  private static final String INDEX_NAME_MARK = "_ix_";
  private static final int INDEX_NAME_LENGTH = 63;

  /** The deterministic name of {@code index} (see {@link #declareIndex}). */
  static String indexName(ProjectionIndex index) {
    String table = tableName(index.projection());
    String columns = indexColumns(index); // validates every key
    var readable = new StringBuilder(index.projection().value());
    for (Sort.Order key : keysWithIdTiebreak(index.keys())) {
      readable.append('_').append(key.field().toLowerCase(Locale.ROOT));
      if (key.direction() == SortDirection.DESC) {
        readable.append("_desc");
      }
    }
    int room = INDEX_NAME_LENGTH - INDEX_NAME_MARK.length() - INDEX_NAME_HASH_DIGITS;
    String prefix = readable.length() > room ? readable.substring(0, room) : readable.toString();
    return prefix + INDEX_NAME_MARK + sha256Hex(table + "|" + columns);
  }

  private static String sha256Hex(String text) {
    try {
      byte[] hash =
          MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash).substring(0, INDEX_NAME_HASH_DIGITS);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is required of every Java platform", e);
    }
  }

  @Override
  public <T> Optional<Versioned<T>> findById(
      ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      return joined.findById(projectionName, id, type, lockMode);
    }
    validateInputs(projectionName, id);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (lockMode == null) {
      throw new IllegalArgumentException("lockMode cannot be null");
    }
    ensureTableExists(projectionName);
    Optional<Versioned<String>> row;
    try (Connection conn = dataSource.getConnection()) {
      row = findVersionedJsonById(conn, projectionName, id);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Failed to find projection with lock: " + shown(projectionName, id), e);
    }
    return fromJson(row, type, projectionName, id);
  }

  /**
   * The row and its version. SQLite has no row lock: {@link LockMode#PESSIMISTIC} reads the same
   * way. Inside a batch the transaction already holds the database's write lock, so no other writer
   * changes the row before the batch commits; outside one a PostgreSQL {@code FOR UPDATE} in
   * autocommit releases its lock when the read returns, too.
   */
  private static Optional<Versioned<String>> findVersionedJsonById(
      Connection conn, ProjectionName projectionName, String id) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(String.format(SELECT_BY_ID_VERSIONED, tableName(projectionName)))) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next()
            ? Optional.of(new Versioned<>(rs.getString(1), rs.getLong(2)))
            : Optional.empty();
      }
    }
  }

  private <T> Optional<Versioned<T>> fromJson(
      Optional<Versioned<String>> row, Class<T> type, ProjectionName projectionName, String id) {
    return row.map(
        r ->
            new Versioned<>(
                fromJson(
                    r.data(), type, "Failed to find projection with lock: ", projectionName, id),
                r.version()));
  }

  @Override
  public <T> void save(
      ProjectionName projectionName, String id, T readModel, long expectedVersion) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      joined.save(projectionName, id, readModel, expectedVersion);
      return;
    }
    validateInputs(projectionName, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must be >= 0");
    }
    ensureTableExists(projectionName);
    String json =
        toJson(readModel, "Failed to save projection with version check: ", projectionName, id);
    long now = now();
    int updated =
        SqliteTransactions.write(
            dataSource,
            "projection versioned save",
            "versioned save of projection " + shown(projectionName, id),
            "Failed to save projection with version check: " + shown(projectionName, id),
            conn -> versionedUpdate(conn, projectionName, id, json, expectedVersion, now));
    requireUpdated(updated, projectionName, id, expectedVersion);
  }

  private static int versionedUpdate(
      Connection conn,
      ProjectionName projectionName,
      String id,
      String json,
      long expectedVersion,
      long now)
      throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement(String.format(VERSIONED_UPDATE, tableName(projectionName)))) {
      ps.setString(1, json);
      ps.setLong(2, now);
      ps.setString(3, id);
      ps.setLong(4, expectedVersion);
      return ps.executeUpdate();
    }
  }

  private static void requireUpdated(
      int updated, ProjectionName projectionName, String id, long expectedVersion) {
    if (updated == 0) {
      throw new OptimisticLockException(
          "Optimistic lock conflict on projection '%s/%s': expected version %d"
              .formatted(projectionName.value(), id, expectedVersion));
    }
  }

  /**
   * Runs {@code action} at once, as a write through this repository has already committed when it
   * returns. Inside a batch or replay this thread holds on this repository's file, where such a
   * write commits with the batch, the action is queued with the batch's own instead: it runs after
   * the {@code COMMIT} and is discarded on a rollback.
   *
   * @param action what to run; must not be {@code null}
   */
  @Override
  public void afterCommit(Runnable action) {
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      joined.afterCommit(action);
      return;
    }
    ProjectionRepository.super.afterCommit(action);
  }

  // ---- (de)serialization, kept apart from every connection scope ----

  /**
   * The JSON stored for {@code readModel}: what {@link #objectMapper} writes ({@link #serialized}),
   * read again as PostgreSQL's {@code jsonb} reads it ({@link #jsonbText}).
   */
  private String toJson(
      Object readModel, String failure, ProjectionName projectionName, String id) {
    return jsonbText(
        serialized(readModel, failure, projectionName, id), failure, projectionName, id);
  }

  /**
   * What {@link #objectMapper} writes for {@code readModel}. Its failure is the application's
   * error, raised before any statement, as the PostgreSQL repository's mapper raises it: it never
   * aborts a batch.
   */
  private String serialized(
      Object readModel, String failure, ProjectionName projectionName, String id) {
    try {
      return objectMapper.writeValueAsString(readModel);
    } catch (JsonProcessingException e) {
      throw new EventStoreException(failure + shown(projectionName, id), e);
    }
  }

  /**
   * {@code json}, the text a read model serialized to, as PostgreSQL's {@code jsonb} would store
   * it: parsed and written again with every number in plain notation with its scale, and the last
   * of two equal keys kept. What PostgreSQL refuses when it casts the text to {@code jsonb} is
   * refused here, before anything is written: text that is not JSON (which a {@link RawValue} can
   * write), a string or a key holding U+0000, a number whose written exponent is beyond
   * &plusmn;{@value #NUMERIC_EXPONENT}, and a number with more digits than {@code numeric} stores.
   * So is a document nested deeper than {@value #MAX_NESTING_DEPTH} levels, which SQLite's JSON
   * functions cannot store. Every failure here is the database refusing the document, the failed
   * INSERT on PostgreSQL, so the transaction-scoped repository aborts its batch on it.
   *
   * @throws EventStoreException on every refusal and every failure, naming the read model
   */
  private static String jsonbText(
      String json, String failure, ProjectionName projectionName, String id) {
    String what = failure + shown(projectionName, id);
    boolean exponentRefused;
    JsonNode tree = null;
    try {
      exponentRefused = exponentBeyondNumeric(json);
      if (!exponentRefused) {
        tree = JSONB.readTree(json);
      }
    } catch (StreamConstraintsException e) {
      throw new EventStoreException(what + ": " + DEPTH_REFUSAL, e);
    } catch (IOException | RuntimeException e) {
      throw new EventStoreException(what + ": " + NOT_JSON_REFUSAL, e);
    }
    String reason =
        exponentRefused
            ? EXPONENT_REFUSAL
            : holdsNul(tree)
                ? "a string holds U+0000, which PostgreSQL jsonb cannot store"
                : overflowsNumeric(tree)
                    ? "a number has more digits than PostgreSQL's numeric stores (131072 before"
                        + " the decimal point, 16383 after)"
                    : null;
    if (reason != null) {
      throw new EventStoreException(what + ": " + reason);
    }
    try {
      return JSONB.writeValueAsString(plainNumbers(tree));
    } catch (JsonProcessingException | RuntimeException e) {
      throw new EventStoreException(what, e);
    }
  }

  /**
   * Whether a number of {@code json} has a written exponent PostgreSQL's {@code numeric} refuses:
   * one whose magnitude exceeds {@value #NUMERIC_EXPONENT}, whatever the number's digits ({@code
   * 0E+1073741824} is refused, {@code 0.0E+1073741823} stored as {@code 0}). Only the number's text
   * shows its exponent, so the tokens are scanned before the tree is built; no number is converted
   * here, so an exponent beyond Java's {@code int} range throws nothing.
   *
   * @throws IOException if {@code json} is not JSON or nests deeper than {@value
   *     #MAX_NESTING_DEPTH} levels
   */
  private static boolean exponentBeyondNumeric(String json) throws IOException {
    try (JsonParser parser = JSONB.createParser(json)) {
      for (JsonToken token = parser.nextToken(); token != null; token = parser.nextToken()) {
        if (token == JsonToken.VALUE_NUMBER_FLOAT
            && literalExponentBeyondNumeric(parser.getText())) {
          return true;
        }
      }
    }
    return false;
  }

  /** Whether the JSON number {@code number} has an exponent beyond {@link #NUMERIC_EXPONENT}. */
  private static boolean literalExponentBeyondNumeric(String number) {
    int marker = Math.max(number.indexOf('e'), number.indexOf('E'));
    if (marker < 0) {
      return false;
    }
    int start = marker + 1;
    if (number.charAt(start) == '+' || number.charAt(start) == '-') {
      start++;
    }
    while (start < number.length() - 1 && number.charAt(start) == '0') {
      start++;
    }
    String digits = number.substring(start);
    return digits.length() > 10 || Long.parseLong(digits) > NUMERIC_EXPONENT;
  }

  private static boolean holdsNul(JsonNode node) {
    if (node.isTextual()) {
      return node.textValue().indexOf('\0') >= 0;
    }
    for (var field : node.properties()) {
      if (field.getKey().indexOf('\0') >= 0 || holdsNul(field.getValue())) {
        return true;
      }
    }
    if (node.isArray()) {
      for (JsonNode element : node) {
        if (holdsNul(element)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean overflowsNumeric(JsonNode node) {
    if (node.isBigDecimal()) {
      BigDecimal value = node.decimalValue();
      return value.scale() > NUMERIC_FRACTION_DIGITS
          || (value.signum() != 0
              && (long) value.precision() - value.scale() > NUMERIC_INTEGER_DIGITS);
    }
    if (node.isBigInteger()) {
      return node.bigIntegerValue().abs().toString().length() > NUMERIC_INTEGER_DIGITS;
    }
    for (JsonNode child : node) {
      if (overflowsNumeric(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@code node} with every decimal replaced by its plain text as PostgreSQL prints a {@code
   * numeric}: no exponent, the scale it was written with, at least zero fraction digits ({@code
   * 1.0E-4} is {@code 0.00010}, {@code 1E+3} is {@code 1000}, {@code -0.0} is {@code 0.0}).
   * Containers are changed in place.
   */
  private static JsonNode plainNumbers(JsonNode node) {
    if (node.isBigDecimal()) {
      BigDecimal value = node.decimalValue();
      BigDecimal plain =
          value.signum() == 0
              ? BigDecimal.ZERO.setScale(Math.max(0, value.scale()))
              : value.scale() < 0 ? value.setScale(0) : value;
      return JsonNodeFactory.instance.rawValueNode(new RawValue(plain.toPlainString()));
    }
    if (node instanceof ObjectNode object) {
      for (var field : new ArrayList<>(object.properties())) {
        object.set(field.getKey(), plainNumbers(field.getValue()));
      }
    } else if (node instanceof ArrayNode array) {
      for (int i = 0; i < array.size(); i++) {
        array.set(i, plainNumbers(array.get(i)));
      }
    }
    return node;
  }

  private <T> T fromJson(
      String json, Class<T> type, String failure, ProjectionName projectionName, String id) {
    try {
      return objectMapper.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new EventStoreException(failure + shown(projectionName, id), e);
    }
  }

  private <T> List<T> fromJson(
      List<String> rows, Class<T> type, String failure, ProjectionName projectionName) {
    var results = new ArrayList<T>(rows.size());
    try {
      for (String json : rows) {
        results.add(objectMapper.readValue(json, type));
      }
    } catch (JsonProcessingException e) {
      throw new EventStoreException(failure + shown(projectionName), e);
    }
    return results;
  }

  // ---- encrypted components ----

  /**
   * Refuses a filter or sort on an {@code Encrypted} record component of {@code type}: its stored
   * text is ciphertext under a crypto-aware mapper, plaintext under a crypto-blind one; refused in
   * every wiring, as on PostgreSQL, so the mistake fails at the first call. The component is
   * matched by its Java name and by the JSON name this repository's mapper gives it.
   */
  private void refuseEncryptedKeys(Class<?> type, FieldEquals filter, Sort sort) {
    Set<String> encrypted = encryptedFields.computeIfAbsent(type, this::encryptedJsonFields);
    if (encrypted.isEmpty()) {
      return;
    }
    if (filter != null && encrypted.contains(filter.field())) {
      throw encryptedKey(type, filter.field());
    }
    for (Sort.Order order : sort.orders()) {
      if (!DIRECT_COLUMNS.contains(order.field()) && encrypted.contains(order.field())) {
        throw encryptedKey(type, order.field());
      }
    }
  }

  private Set<String> encryptedJsonFields(Class<?> type) {
    return encryptedJsonFields(type, new HashSet<>());
  }

  /**
   * The JSON names of {@code type}'s {@code Encrypted} components, and of those of the records it
   * embeds with {@code @JsonUnwrapped} (with the annotation's prefix and suffix). A type that is
   * not a record ({@code Map}, a class) is not checked.
   */
  private Set<String> encryptedJsonFields(Class<?> type, Set<Class<?>> visiting) {
    if (!type.isRecord() || !visiting.add(type)) {
      return Set.of();
    }
    var components = new HashSet<String>();
    var names = new HashSet<String>();
    for (RecordComponent component : type.getRecordComponents()) {
      if (component.isAnnotationPresent(Encrypted.class)) {
        components.add(component.getName());
      }
      JsonUnwrapped unwrapped = unwrapped(type, component);
      if (unwrapped != null && unwrapped.enabled()) {
        for (String inner : encryptedJsonFields(component.getType(), visiting)) {
          names.add(unwrapped.prefix() + inner + unwrapped.suffix());
        }
      }
    }
    visiting.remove(type);
    if (!components.isEmpty()) {
      names.addAll(components);
      var description =
          objectMapper.getSerializationConfig().introspect(objectMapper.constructType(type));
      for (var property : description.findProperties()) {
        if (components.contains(property.getInternalName())) {
          names.add(property.getName());
        }
      }
    }
    return Set.copyOf(names);
  }

  /**
   * The {@code @JsonUnwrapped} of a record component: the annotation is not a record-component
   * annotation, so Java puts it on the accessor and the field.
   */
  private static JsonUnwrapped unwrapped(Class<?> type, RecordComponent component) {
    JsonUnwrapped onAccessor = component.getAccessor().getAnnotation(JsonUnwrapped.class);
    if (onAccessor != null) {
      return onAccessor;
    }
    try {
      return type.getDeclaredField(component.getName()).getAnnotation(JsonUnwrapped.class);
    } catch (NoSuchFieldException e) {
      return null;
    }
  }

  private static IllegalArgumentException encryptedKey(Class<?> type, String field) {
    return new IllegalArgumentException(
        "Field '"
            + LogSanitizer.sanitizeForLog(field)
            + "' of "
            + type.getSimpleName()
            + " is @Encrypted: its stored text is ciphertext, so it cannot be filtered or sorted"
            + " (keep a separate, non-sensitive field for the query)");
  }

  // ---- tables ----

  /**
   * Makes sure {@code projectionName}'s table exists, and remembers it. A table that exists is
   * found with a plain read of the schema ({@link #tableExists}), which takes no lock: so the first
   * read of a projection after a process start does not wait for a writer. Only a table that is
   * missing is created, committed before this returns: the {@code CREATE TABLE IF NOT EXISTS} runs
   * under SQLite's write lock, which every writer of the file takes, so it sees any table another
   * connection or process committed since the probe and cannot lose a creation race; one waiting
   * for another's creation proceeds once that commits.
   */
  private void ensureTableExists(ProjectionName projectionName) {
    if (tablesChecked.contains(projectionName)) {
      return;
    }
    String table = tableName(projectionName); // reject a name outside the rule before any I/O
    if (tableExists(table)) {
      tablesChecked.add(projectionName);
      return;
    }
    synchronized (this) {
      if (tablesChecked.contains(projectionName)) {
        return;
      }
      SqliteTransactions.write(
          dataSource,
          "projection table creation",
          "creation of projection table " + table,
          "Failed to create projection table: " + table,
          conn -> {
            createTable(conn, table);
            return null;
          });
      tablesChecked.add(projectionName);
    }
  }

  /**
   * Whether the committed schema holds a table (or a view) named {@code table}, whatever its letter
   * case: exactly what makes {@code CREATE TABLE IF NOT EXISTS} do nothing. One read in autocommit
   * on a borrowed connection; in WAL mode it never waits for a writer.
   *
   * @throws EventStoreException if the schema cannot be read
   */
  private boolean tableExists(String table) {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(TABLE_EXISTS)) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    } catch (SQLException e) {
      throw new EventStoreException("Failed to look up projection table: " + table, e);
    }
  }

  private static void createTable(Connection conn, String table) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(String.format(CREATE_TABLE, table));
    }
  }

  /**
   * The read-model table of {@code projectionName}: the name plus {@code _view}. A name outside
   * {@link #TABLE_NAMED_PROJECTION} is rejected, not rewritten, so no two projection names share a
   * table, and the name is safe to splice into SQL.
   */
  private static String tableName(ProjectionName projectionName) {
    String name = projectionName.value();
    if (name.startsWith(RESERVED_PREFIX)) {
      throw new IllegalArgumentException(
          "Projection name '"
              + LogSanitizer.sanitizeForLog(name)
              + "' cannot name a read-model table on SQLite: SQLite reserves every name that"
              + " starts with 'sqlite_' for its own tables, so '"
              + LogSanitizer.sanitizeForLog(name)
              + "_view' cannot be created. Rename the projection.");
    }
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
   * Rejects, at registration, a projection name this repository cannot store read models under: one
   * outside {@code [a-z_][a-z0-9_]{0,57}}, the rule PostgreSQL's repository applies too, and one
   * starting with {@code sqlite_}, a prefix SQLite reserves for its own tables.
   *
   * @throws IllegalArgumentException if {@code projectionName} is null or outside the rule
   */
  @Override
  public void checkProjectionName(ProjectionName projectionName) {
    validateProjectionName(projectionName);
    tableName(projectionName);
  }

  /**
   * Creates the {@code <name>_view} table if it does not exist, in a transaction of its own, so the
   * batch transaction that follows carries no DDL. After the first call for a name it is a lookup
   * in a set. A write through the handed repository to a table nothing prepared still creates it,
   * inside the batch transaction.
   *
   * @throws IllegalArgumentException if {@code projectionName} is outside the table-name rule
   */
  @Override
  public void prepareReadModel(ProjectionName projectionName) {
    validateProjectionName(projectionName);
    TransactionScopedRepository joined = joined();
    if (joined != null) {
      joined.prepare(projectionName);
      return;
    }
    ensureTableExists(projectionName);
  }

  // ---- the batch and the replay ----

  /**
   * Processes a batch and advances the projection's checkpoint in one SQLite write transaction:
   * either the updater's writes through the handed repository and the advance both commit, or
   * neither does.
   *
   * <p><b>The transaction.</b> On one connection, one {@code BEGIN IMMEDIATE} ... {@code COMMIT}:
   *
   * <ol>
   *   <li>the checkpoint fence ({@link SqliteReadModelCheckpoints}' fence) seeds and reads the
   *       projection's checkpoint row and applies the epoch fence and the first-offset overlap
   *       guard, before the updater writes anything;
   *   <li>the updater runs with a repository bound to the transaction's connection; it never
   *       borrows a second connection, so a pool of one connection cannot deadlock. While it runs,
   *       the transaction is published on this thread for this file ({@link
   *       SqliteWriteTransaction}): a call the updater makes through this repository itself, or
   *       another {@code SqliteProjectionRepository} of the file, runs inside it (see the class
   *       Javadoc), and a SQLite crypto key store on the same file reads and mints keys inside it,
   *       so its keys commit and roll back with the batch;
   *   <li>the advance moves the checkpoint to {@code newOffset} under the monotonic guard and
   *       stamps {@code max(stored, fencingEpoch)} as the epoch.
   * </ol>
   *
   * The three guards are those of the PostgreSQL repository, with the same {@link
   * ProjectionCommitFencedException}s. The {@code BEGIN IMMEDIATE} takes SQLite's write lock up
   * front, waiting at most {@code busy_timeout}; it serializes every batch and every replay of
   * every projection of the file, where PostgreSQL serializes those of one projection on its
   * checkpoint row.
   *
   * <p><b>A failed statement aborts the transaction</b>, as on PostgreSQL. The first statement of
   * the handed repository, or of a call that joined the transaction, that fails (or a read model
   * {@code jsonb} would refuse) marks the transaction aborted: every later call of the handed
   * repository, and every later joined call, throws an {@link EventStoreException} naming that
   * first failure (its cause) without running a statement, and once the updater returns, even if it
   * caught the failure, the batch throws that exception before the advance, and rolls back. So a
   * batch never commits without a write that failed, and after SQLite rolled a transaction back on
   * its own ({@code SQLITE_FULL}, an I/O error) no later write of the updater through the handed
   * repository or a joined call runs, and autocommits, outside the batch. The abort covers those
   * calls only: a write the updater makes through any other writer of the file (the event store, a
   * thread it started) after such a rollback finds the write lock released and commits at once, on
   * its own. An {@link OptimisticLockException} (no row of the expected version) does not abort, as
   * on PostgreSQL.
   *
   * <p><b>Failures.</b> Every exit before the {@code COMMIT} rolls the transaction back. A fence
   * rejection, and an exception or {@link Error} of the updater, propagates unchanged. A {@link
   * SQLException} of a statement or of the {@code COMMIT} becomes an {@link EventStoreException}
   * "Transaction failed during projection update" (one that is SQLite refusing the write lock
   * within {@code busy_timeout}, or {@code SQLITE_LOCKED}, says so instead), one of borrowing or
   * closing the connection "Failed to process projection batch atomically". A connection handed out
   * in manual-commit mode is refused with {@link IllegalStateException} before anything runs.
   *
   * <p><b>Crash points.</b> Killed before the {@code COMMIT}: SQLite rolls the transaction back
   * when the connection or the process dies, so neither the writes nor the advance (nor a table the
   * batch created, nor the seeded checkpoint row) remain; the rerun applies the batch once. Killed
   * after the {@code COMMIT}: both are durable; the rerun of the same range is refused by the
   * overlap guard, and reading on from the stored checkpoint resumes.
   *
   * <p><b>After-commit actions.</b> An action registered through the handed repository's {@link
   * ProjectionRepository#afterCommit} runs after the {@code COMMIT}, before this method returns,
   * and is discarded if the transaction rolls back; when the {@code COMMIT} statement itself fails
   * the outcome is not known, and the actions run anyway. An action that throws a {@link
   * RuntimeException} is logged and never fails the committed batch; an {@link Error} propagates
   * after a clean commit and is added as suppressed to the exception in flight otherwise.
   *
   * @param projectionName the projection
   * @param batch the batch of events; its first offset is checked against the committed checkpoint
   *     (an empty batch skips that check)
   * @param newOffset the offset the checkpoint advances to
   * @param fencingEpoch the caller's leadership epoch; {@code 0} is unfenced
   * @param projectionUpdater applies the batch through the transaction-scoped repository
   * @param offsetStore ignored: the checkpoint is advanced in the transaction
   * @throws IllegalArgumentException if {@code projectionName} is {@code null} or outside the
   *     table-name rule
   * @throws ProjectionCommitFencedException if a guard rejects the batch (rolled back)
   * @throws EventStoreException if the transaction fails (rolled back)
   */
  @Override
  public void executeAtomically(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      OffsetStore offsetStore) {
    validateProjectionName(projectionName);
    tableName(projectionName); // reject a name outside the rule before touching the database
    GlobalOffset firstOffset = batch.isEmpty() ? null : batch.getFirst().globalOffset();
    runCheckpointTransaction(
        projectionName,
        CheckpointTransaction.BATCH,
        (conn, batchTransaction) -> {
          // The lock, the advance and the updater's writes in ONE transaction: the fence checks
          // run under the write lock this transaction holds until its COMMIT.
          checkpointFence.lockForBatch(conn, projectionName, fencingEpoch, firstOffset);
          runUpdater(batchTransaction, projectionUpdater);
          // A statement that failed inside the updater aborted the transaction, even if the
          // updater caught it: never advance or commit past it.
          batchTransaction.requireNotAborted();
          checkpointFence.saveLocked(conn, projectionName, newOffset, fencingEpoch);
        });
  }

  /**
   * A replay and a batch of one projection both run under SQLite's write lock, so one waits for the
   * other. The lock spans every projection of the file: a replay also waits for a batch of another
   * projection (see {@link #executeReplay}).
   *
   * @return always {@code true}
   */
  @Override
  public boolean serializesReplay() {
    return true;
  }

  /**
   * Applies a dead-lettered range in one write transaction that seeds the projection's checkpoint
   * row if none exists and never writes it otherwise: the offset and the epoch stay as they are.
   * The transaction holds SQLite's write lock, so a live batch waits for the replay's commit and
   * the replay for an open batch; whichever comes second reads the rows the first one committed.
   * The lock covers the whole file, not one projection: the replay also waits for an open batch of
   * any other projection, at most {@code busy_timeout}, and fails with an {@link
   * EventStoreException} naming the replay when that batch holds the lock longer. The updater is
   * handed a repository bound to the transaction, with the after-commit behaviour and the abort on
   * a failed statement of a batch; failures are reported as for {@link #executeAtomically}, with
   * the messages "Transaction failed during projection dead-letter replay" and "Failed to replay a
   * projection dead-letter range".
   *
   * <p>Crash points: killed before the {@code COMMIT}, the range's writes are gone and the
   * checkpoint row is as it was; killed after it, the range is in the read model and the caller,
   * which saw a failure, keeps its dead-letter entry, so the next replay applies the range a second
   * time (as on PostgreSQL when {@code commit()} throws after the server committed).
   *
   * @param projectionName the projection whose dead-lettered range is replayed
   * @param batch the re-read range (not inspected here)
   * @param projectionUpdater applies the range through the transaction-scoped repository
   * @throws IllegalArgumentException if {@code projectionName} is {@code null} or outside the
   *     table-name rule
   * @throws EventStoreException if the transaction fails (rolled back)
   */
  @Override
  public void executeReplay(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      ProjectionUpdater projectionUpdater) {
    validateProjectionName(projectionName);
    tableName(projectionName);
    runCheckpointTransaction(
        projectionName,
        CheckpointTransaction.REPLAY,
        (conn, batchTransaction) -> {
          // The lock a batch takes, with no check and no checkpoint write.
          checkpointFence.lockForReplay(conn, projectionName);
          runUpdater(batchTransaction, projectionUpdater);
          batchTransaction.requireNotAborted();
        });
  }

  /**
   * Runs the application's updater with the transaction published on this thread for this file
   * ({@link SqliteWriteTransaction}). While it runs, a call on this thread through a {@code
   * SqliteProjectionRepository} of the file, this one included, runs on the transaction's
   * connection ({@link #joined()}), and the SQLite crypto key store on the same file mints a key
   * there, inside the transaction, instead of waiting for the write lock this thread holds. A
   * failure either reports aborts the transaction as a failed statement of the handed repository
   * does.
   */
  private void runUpdater(BatchTransaction batchTransaction, ProjectionUpdater projectionUpdater)
      throws SQLException {
    SqliteWriteTransaction.publishWhile(
        file,
        batchTransaction,
        failure ->
            batchTransaction.abort(
                new EventStoreException(
                    "A statement run on the projection transaction's connection by a component"
                        + " joining it failed",
                    failure)),
        () -> projectionUpdater.update(new TransactionScopedRepository(batchTransaction)));
  }

  /**
   * The two transactions that hold the projection's checkpoint row, and how each reports a failure:
   * the messages of the PostgreSQL repository's.
   */
  private enum CheckpointTransaction {
    BATCH(
        "projection batch",
        "Transaction failed during projection update",
        "Failed to process projection batch atomically"),
    REPLAY(
        "projection dead-letter replay",
        "Transaction failed during projection dead-letter replay",
        "Failed to replay a projection dead-letter range");

    /** What the transaction is: the log record of a failed rollback, a lock failure's message. */
    private final String label;

    /** The message of a failed statement or {@code COMMIT} inside the transaction. */
    private final String bodyFailure;

    /** The message of a failure to borrow or close the connection. */
    private final String connectionFailure;

    CheckpointTransaction(String label, String bodyFailure, String connectionFailure) {
      this.label = label;
      this.bodyFailure = bodyFailure;
      this.connectionFailure = connectionFailure;
    }
  }

  /** What runs inside a checkpoint transaction; its first statement goes through the fence. */
  @FunctionalInterface
  private interface CheckpointTransactionBody {
    void run(Connection conn, BatchTransaction batchTransaction) throws SQLException;
  }

  /**
   * Runs {@code body} in one write transaction and then the after-commit actions it registered,
   * also when the {@code COMMIT} or closing the connection failed after the {@code COMMIT} was
   * issued. A transaction that rolled back discards them. The actions run with the transaction's
   * exception in flight, and an Error from one rides on it as suppressed instead of replacing it.
   */
  private void runCheckpointTransaction(
      ProjectionName projectionName, CheckpointTransaction kind, CheckpointTransactionBody body) {
    var afterCommitActions = new AfterCommitActions(projectionName);
    try {
      commitUnderWriteLock(projectionName, kind, body, afterCommitActions);
    } catch (Throwable inFlight) {
      afterCommitActions.runIfCommitIssued(inFlight);
      throw inFlight;
    }
    afterCommitActions.runIfCommitIssued(null);
  }

  /**
   * One {@link SqliteTransactions#inTransaction} on one borrowed connection: {@code body} (the
   * fence first), then the {@code COMMIT}; every exit without the commit rolls back there. Then the
   * tables the transaction created are remembered.
   */
  private void commitUnderWriteLock(
      ProjectionName projectionName,
      CheckpointTransaction kind,
      CheckpointTransactionBody body,
      AfterCommitActions afterCommitActions) {
    BatchTransaction committed;
    try (Connection conn = dataSource.getConnection()) {
      try {
        committed =
            SqliteTransactions.inTransaction(
                conn,
                kind.label,
                c -> {
                  var batchTransaction = new BatchTransaction(c, afterCommitActions, now());
                  body.run(c, batchTransaction);
                  // From here on the outcome may be "committed" even if the COMMIT fails, so the
                  // after-commit actions run whatever it reports.
                  afterCommitActions.commitIssued();
                  return batchTransaction;
                });
      } catch (SQLException e) {
        EventStoreException lockFailure =
            SqliteEventStore.lockFailure(e, kind.label + " of projection " + shown(projectionName));
        throw lockFailure != null ? lockFailure : new EventStoreException(kind.bodyFailure, e);
      }
    } catch (SQLException e) {
      throw new EventStoreException(kind.connectionFailure, e);
    }
    // Tables created inside the transaction exist only now that it committed.
    tablesChecked.addAll(committed.tablesCreatedInTransaction);
  }

  /**
   * The actions registered through {@link TransactionScopedRepository#afterCommit} during one
   * transaction. They run, in registration order, once the transaction's {@code COMMIT} has been
   * issued, and are discarded if the transaction rolls back before that. The crash and failure
   * points are those of the PostgreSQL repository: before the {@code COMMIT} the actions are
   * discarded; when the {@code COMMIT} fails they run anyway (an unneeded cache eviction costs one
   * miss); a {@link RuntimeException} from one is logged and the others still run; an {@link Error}
   * ends the run and propagates, or rides on the exception in flight as suppressed; a process that
   * dies between the commit and the actions loses them, which is safe for the in-process query
   * cache they evict.
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
        // With nothing in flight the Error propagates. With the batch's exception in flight it
        // must not replace it (that exception reports the commit's unknown outcome), so it rides
        // on it as suppressed; the JVM can throw one preallocated Error more than once, and
        // suppressing an exception on itself would throw.
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
   * One open batch or replay transaction: its connection and what every repository view of it
   * shares. The views are the repository handed to the updater and, while the updater runs, each
   * {@code SqliteProjectionRepository} of the file called on the updater's thread ({@link
   * #joined()}); all of them run on this connection, stamp this {@code updated_at}, queue their
   * after-commit actions here, and are aborted together by the first failed statement of any of
   * them. Touched by the updater's thread only.
   */
  static final class BatchTransaction {

    private final Connection conn;
    private final AfterCommitActions afterCommitActions;
    private final long now;

    /** Tables created during this transaction, remembered only once it has committed. */
    private final Set<ProjectionName> tablesCreatedInTransaction = new HashSet<>();

    /**
     * The failure that aborted this transaction, or {@code null}: the first failed statement, or a
     * read model refused because PostgreSQL's {@code jsonb} could not store it.
     */
    private EventStoreException abortedBy;

    private BatchTransaction(Connection conn, AfterCommitActions afterCommitActions, long now) {
      this.conn = conn;
      this.afterCommitActions = afterCommitActions;
      this.now = now;
    }

    /** The connection of the open transaction. */
    Connection connection() {
      return conn;
    }

    /**
     * Marks the transaction aborted by {@code failure}, unless an earlier failure already did, and
     * returns {@code failure} to be thrown.
     */
    private EventStoreException abort(EventStoreException failure) {
      if (abortedBy == null) {
        abortedBy = failure;
      }
      return failure;
    }

    /**
     * Refuses to go on once a statement failed: PostgreSQL aborts the transaction at its first
     * failed statement, and so does this one. Called before every statement of every view and by
     * the batch and the replay after the updater returns, so an updater that catches a failure can
     * neither run another statement (after SQLite's own rollback it would commit on its own) nor
     * commit the transaction without the failed write.
     *
     * @throws EventStoreException naming the first failure, which is its cause
     */
    private void requireNotAborted() {
      if (abortedBy != null) {
        throw new EventStoreException(
            "The projection transaction was aborted by an earlier failure, so it runs no further"
                + " statement and rolls back: "
                + abortedBy.getMessage(),
            abortedBy);
      }
    }
  }

  /**
   * A {@link ProjectionRepository} bound to the one connection of an open write transaction ({@link
   * BatchTransaction}): the repository handed to an updater, and the view through which a call on
   * the outer repository joins the transaction its thread holds ({@link #joined()}). Its writes
   * commit or roll back with the checkpoint advance. It never borrows a connection; a table it
   * needs is created on the bound connection, inside the transaction. Its rows share one {@code
   * updated_at}, the transaction's, as PostgreSQL's {@code NOW()} does. The read models are
   * (de)serialized, with the mapper of the repository the view belongs to, while the transaction is
   * open; a SQLite crypto key store on the same file joins the transaction for that ({@link
   * SqliteWriteTransaction}). Its first failed statement, or one a joining component reports,
   * aborts the transaction for every view ({@link BatchTransaction#requireNotAborted}).
   */
  private final class TransactionScopedRepository implements ProjectionRepository {

    private final BatchTransaction transaction;
    private final Connection conn;
    private final long now;

    private TransactionScopedRepository(BatchTransaction transaction) {
      this.transaction = transaction;
      this.conn = transaction.conn;
      this.now = transaction.now;
    }

    private EventStoreException abort(EventStoreException failure) {
      return transaction.abort(failure);
    }

    private void requireNotAborted() {
      transaction.requireNotAborted();
    }

    /**
     * The JSON to store for {@code readModel}, as {@link SqliteProjectionRepository#toJson}. A
     * document the re-read refuses, or any failure of the re-read, aborts the transaction, as the
     * refused INSERT does on PostgreSQL; a failure of the application's mapper does not, as it
     * comes before any statement there too.
     */
    private String storedJson(
        Object readModel, String failure, ProjectionName projectionName, String id) {
      String json = serialized(readModel, failure, projectionName, id);
      try {
        return jsonbText(json, failure, projectionName, id);
      } catch (EventStoreException refusal) {
        throw abort(refusal);
      }
    }

    /** Queues {@code action} to run once this transaction has committed; a rollback discards it. */
    @Override
    public void afterCommit(Runnable action) {
      if (action == null) {
        throw new IllegalArgumentException("action cannot be null");
      }
      transaction.afterCommitActions.add(action);
    }

    /** The outer repository's identity: this view writes to the same store, on its transaction. */
    @Override
    public Object writeTargetIdentity() {
      return SqliteProjectionRepository.this.writeTargetIdentity();
    }

    /** {@code prepareReadModel} inside the transaction: the table is created on its connection. */
    private void prepare(ProjectionName projectionName) {
      requireNotAborted();
      ensureTable(projectionName);
    }

    private void ensureTable(ProjectionName projectionName) {
      if (tablesChecked.contains(projectionName)
          || transaction.tablesCreatedInTransaction.contains(projectionName)) {
        return;
      }
      String table = tableName(projectionName);
      try {
        createTable(conn, table);
      } catch (SQLException e) {
        throw abort(new EventStoreException("Failed to create projection table: " + table, e));
      }
      transaction.tablesCreatedInTransaction.add(projectionName);
    }

    @Override
    public <T> void save(ProjectionName projectionName, String id, T readModel) {
      validateInputs(projectionName, id);
      if (readModel == null) {
        throw new IllegalArgumentException("readModel cannot be null");
      }
      requireNotAborted();
      ensureTable(projectionName);
      String json = storedJson(readModel, "Failed to save projection: ", projectionName, id);
      try {
        upsert(conn, projectionName, id, json, now);
      } catch (SQLException e) {
        throw abort(
            new EventStoreException("Failed to save projection: " + shown(projectionName, id), e));
      }
    }

    @Override
    public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
      validateInputs(projectionName, id);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      requireNotAborted();
      ensureTable(projectionName);
      Optional<String> json;
      try {
        json = findJsonById(conn, projectionName, id);
      } catch (SQLException e) {
        throw abort(
            new EventStoreException("Failed to find projection: " + shown(projectionName, id), e));
      }
      return json.map(j -> fromJson(j, type, "Failed to find projection: ", projectionName, id));
    }

    @Override
    public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
      validateProjectionName(projectionName);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      requireNotAborted();
      ensureTable(projectionName);
      List<String> rows;
      try {
        rows = findAllJson(conn, projectionName);
      } catch (SQLException e) {
        throw abort(
            new EventStoreException("Failed to find all projections: " + shown(projectionName), e));
      }
      return fromJson(rows, type, "Failed to find all projections: ", projectionName);
    }

    @Override
    public void delete(ProjectionName projectionName, String id) {
      validateInputs(projectionName, id);
      requireNotAborted();
      ensureTable(projectionName);
      try {
        deleteRow(conn, projectionName, id);
      } catch (SQLException e) {
        throw abort(
            new EventStoreException(
                "Failed to delete projection: " + shown(projectionName, id), e));
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
      refuseEncryptedKeys(type, null, pageRequest.sort());
      return page(projectionName, type, null, pageRequest);
    }

    @Override
    public <T> Page<T> findAllBy(
        ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
      validateFilteredRead(projectionName, type, filter, pageRequest);
      refuseEncryptedKeys(type, filter, pageRequest.sort());
      return page(projectionName, type, filter, pageRequest);
    }

    private <T> Page<T> page(
        ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
      String sql = pageSql(projectionName, filter, pageRequest.sort());
      requireNotAborted();
      ensureTable(projectionName);
      Page<String> rows;
      try {
        rows = pageJson(conn, sql, filter, pageRequest);
      } catch (SQLException e) {
        throw abort(
            new EventStoreException(
                "Failed to find paginated projections: " + shown(projectionName), e));
      }
      return fromJson(rows, type, projectionName);
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
      validateInputs(projectionName, id);
      if (type == null) {
        throw new IllegalArgumentException("type cannot be null");
      }
      if (lockMode == null) {
        throw new IllegalArgumentException("lockMode cannot be null");
      }
      requireNotAborted();
      ensureTable(projectionName);
      Optional<Versioned<String>> row;
      try {
        row = findVersionedJsonById(conn, projectionName, id);
      } catch (SQLException e) {
        throw abort(
            new EventStoreException(
                "Failed to find projection with lock: " + shown(projectionName, id), e));
      }
      return fromJson(row, type, projectionName, id);
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
      requireNotAborted();
      ensureTable(projectionName);
      String json =
          storedJson(
              readModel, "Failed to save projection with version check: ", projectionName, id);
      int updated;
      try {
        updated = versionedUpdate(conn, projectionName, id, json, expectedVersion, now);
      } catch (SQLException e) {
        throw abort(
            new EventStoreException(
                "Failed to save projection with version check: " + shown(projectionName, id), e));
      }
      requireUpdated(updated, projectionName, id, expectedVersion);
    }
  }

  /**
   * This repository runs the updater's writes through the handed repository and the checkpoint
   * advance in one transaction, and the handed repository is bound to it and never {@code null}.
   *
   * @return always {@code true}
   */
  @Override
  public boolean transactional() {
    return true;
  }

  /**
   * This repository honours the leadership fencing epoch: {@link #executeAtomically} runs the epoch
   * fence under the write lock before the updater writes anything, and {@link #stampFencingEpoch}
   * arms the fence at takeover. On SQLite the runners pass epoch {@code 0}, which always passes.
   *
   * @return always {@code true}
   */
  @Override
  public boolean fencesCheckpoint() {
    return true;
  }

  /**
   * The store this repository writes to, for {@link AtomicBatchProcessor#writesTo}: the database
   * file AND the {@link ObjectMapper}, the mapper by reference. Two repositories over one file with
   * the same mapper are one write target; with different mappers they are not, because the mapper
   * decides how (and whether encrypted) a read model is stored.
   *
   * @return the write target of this repository
   */
  @Override
  public Object writeTargetIdentity() {
    return new WriteTarget(file, objectMapper);
  }

  /** (file, mapper): the file by its canonical path, the mapper by reference. */
  private static final class WriteTarget {
    private final Path file;
    private final ObjectMapper objectMapper;

    private WriteTarget(Path file, ObjectMapper objectMapper) {
      this.file = file;
      this.objectMapper = objectMapper;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof WriteTarget t
          && t.file.equals(file)
          && t.objectMapper == objectMapper;
    }

    @Override
    public int hashCode() {
      return 31 * file.hashCode() + System.identityHashCode(objectMapper);
    }

    @Override
    public String toString() {
      return "SqliteProjectionRepository(file="
          + file
          + ", objectMapper=ObjectMapper@"
          + Integer.toHexString(System.identityHashCode(objectMapper))
          + ")";
    }
  }

  /**
   * Stamps {@code fencingEpoch} on the projection's checkpoint row without moving the offset: the
   * takeover stamp (see {@link AtomicBatchProcessor#stampFencingEpoch}). Epoch {@code 0} is a
   * no-op; a same-epoch re-stamp changes nothing; the stored epoch never regresses.
   *
   * @param projectionName the projection whose checkpoint row is stamped
   * @param fencingEpoch the newly acquired leadership epoch; {@code 0} is a no-op
   * @throws IllegalArgumentException if {@code projectionName} is {@code null}
   * @throws ProjectionEpochRegressionException if the row already stores a greater epoch
   * @throws EventStoreException if the stamp fails
   */
  @Override
  public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
    validateProjectionName(projectionName);
    checkpointFence.stampEpoch(dataSource, projectionName, fencingEpoch);
  }

  /**
   * Rewinds the projection's checkpoint to offset {@code 0}, bypassing the monotonic guard: the
   * reset/rebuild path of the {@link OffsetStore} contract. The epoch is left as it is.
   *
   * @param projectionName the projection to reset
   * @throws IllegalArgumentException if {@code projectionName} is {@code null}
   * @throws EventStoreException if the reset fails
   */
  public void resetOffset(ProjectionName projectionName) {
    validateProjectionName(projectionName);
    checkpointFence.reset(dataSource, projectionName);
  }

  private long now() {
    return SqliteTimestamps.toMicros(clock.instant());
  }

  /** A projection name for a message: caller text, sanitized. */
  private static String shown(ProjectionName projectionName) {
    return LogSanitizer.sanitizeForLog(projectionName.value());
  }

  /** {@code projection/id} for a message, both parts sanitized. */
  private static String shown(ProjectionName projectionName, String id) {
    return shown(projectionName) + "/" + LogSanitizer.sanitizeForLog(id);
  }

  private static void validateInputs(ProjectionName projectionName, String id) {
    validateProjectionName(projectionName);
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("id cannot be null or blank");
    }
  }

  private static void validateProjectionName(ProjectionName projectionName) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName cannot be null");
    }
  }
}
