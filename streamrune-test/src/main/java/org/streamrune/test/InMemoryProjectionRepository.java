package org.streamrune.test;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.Versioned;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * In-memory {@link ProjectionRepository} that is also a fencing-capable {@link
 * AtomicBatchProcessor} and the {@link OffsetStore} of its own checkpoint — the Docker-free way to
 * run a {@code TRANSACTIONAL_LOCAL} projection, including rollback. {@link #executeAtomically}
 * mirrors {@code JdbcProjectionRepository}: one lock per projection name, the epoch fence and the
 * first-offset overlap guard BEFORE the updater, the writes staged in a transaction-scoped view and
 * applied together with the checkpoint, the monotonic guard on the checkpoint move, after-commit
 * actions run after the apply and dropped on rollback. {@link #executeReplay} takes the same
 * per-name lock and leaves the checkpoint alone. Pass the same instance as {@code
 * .offsetStore(...)} and {@code .atomicProcessor(...)} of a runner, exactly as {@code
 * PostgresOffsetStore} and {@code JdbcProjectionRepository} share {@code projection_offset}. Passes
 * {@link AtomicBatchProcessorContract}.
 *
 * <p>Same rules as the JDBC repository: an inserted row starts at version 0 and every save adds
 * one; a versioned save only updates an existing row at the expected version, so a missing row is
 * an {@link OptimisticLockException} at every expected version; the repository methods refuse a
 * {@code null} projection name, a {@code null} or blank id, a {@code null} read model and a
 * negative expected version with an {@link IllegalArgumentException} at the call. The
 * transaction-scoped view refuses every read and write once its batch has committed or rolled back;
 * an after-commit action registered on it after the commit runs at once, one registered after a
 * rollback is discarded.
 *
 * <p>One deliberate difference: an after-commit action that throws a {@link RuntimeException}. Both
 * repositories still run the remaining actions and keep the batch and its checkpoint applied, but
 * where the JDBC repository logs the failure and returns normally, this one rethrows the first
 * failure from {@link #executeAtomically} once every action has run, so a test sees it. A runner
 * then reports that batch as failed although it is applied, which production never does.
 *
 * <p>A reader on another thread sees a batch's rows and its checkpoint together, never one without
 * the other; it is not blocked while the updater runs, only while the batch is applied.
 *
 * <p>The paged reads return what the JDBC repository returns: {@code findAll(name, type,
 * pageRequest)} and {@code findAllBy(name, type, filter, pageRequest)} order by the sort's fields
 * as JSON text in code-point order, a missing field last ascending, the id breaking ties, filter by
 * the field's JSON text (a decimal number as PostgreSQL prints {@code numeric}), and refuse the
 * fields the JDBC repository refuses, {@code Encrypted} record components included. A field is read
 * through the mapper this repository was built with, and a read model that mapper cannot write, or
 * whose JSON holds U+0000 in a string, is refused at save, as the JDBC repository refuses it.
 * Passes {@link ProjectionRepositoryQueryContract}. {@link #declareIndex} checks and records an
 * index and creates nothing ({@link #declaredIndexes()}).
 *
 * <p>Read models are stored as the objects themselves, not serialized: a projection that mutates a
 * read model after saving it mutates the stored row too. Not for production.
 */
public final class InMemoryProjectionRepository
    implements ProjectionRepository, AtomicBatchProcessor, OffsetStore {

  /** A row: the read model, its version, and when it was last written (the write clock's tick). */
  private record Stored(Object value, long version, long writtenAt) {}

  /**
   * A sort, filter or index field, as the JDBC repository accepts it: ASCII letters, digits and
   * underscores, no nested path.
   */
  private static final Pattern FIELD = Pattern.compile("[A-Za-z0-9_]+");

  /** The JDBC repository's own columns, which a sort or an index names as themselves. */
  private static final Set<String> COLUMNS = Set.of("id", "version", "updated_at");

  private record Checkpoint(long offset, long epoch) {
    static final Checkpoint NONE = new Checkpoint(0L, 0L);
  }

  private final ConcurrentHashMap<ProjectionName, ConcurrentSkipListMap<String, Stored>> tables =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ProjectionName, Checkpoint> checkpoints =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ProjectionName, Object> locks = new ConcurrentHashMap<>();

  // Publication lock: every change to the committed rows or checkpoints happens under the write
  // lock and every read of them under the read lock, so a batch's rows and its checkpoint become
  // visible to other threads together. Separate from the per-name monitor so readers never wait for
  // an updater.
  private final ReentrantReadWriteLock publication = new ReentrantReadWriteLock();

  // The updated_at of the JDBC repository: a tick per write (one per committed batch), so a sort on
  // updated_at orders rows by their last write without depending on the clock's resolution.
  private final AtomicLong writeClock = new AtomicLong();

  // Renders a read model's fields as JSON text for the filtered and sorted reads, as the JDBC
  // repository's mapper writes them to its data column.
  private final ObjectMapper fieldMapper;
  private final ObjectReader fieldReader;

  // The JSON names of each read-model type's @Encrypted record components (refuseEncryptedKeys).
  private final ConcurrentHashMap<Class<?>, Set<String>> encryptedFields =
      new ConcurrentHashMap<>();

  // The indexes declared through declareIndex, in declaration order, each once.
  private final Set<ProjectionIndex> declaredIndexes =
      java.util.Collections.synchronizedSet(new java.util.LinkedHashSet<>());

  /**
   * Creates an empty store: no read models, every checkpoint at offset 0 and epoch 0. A filter or a
   * sort on a read-model field reads the field as JSON text rendered by a mapper configured as the
   * JDBC repository's default ({@code JavaTimeModule}, ISO-8601 dates).
   */
  public InMemoryProjectionRepository() {
    this(defaultFieldMapper());
  }

  /**
   * Creates an empty store whose filtered and sorted reads render a read model's fields with {@code
   * fieldMapper}: pass the mapper the production {@code JdbcProjectionRepository} writes with when
   * it is not the default (a naming strategy, custom serializers), so a field compares as it does
   * there.
   *
   * @param fieldMapper the mapper that renders read-model fields as JSON
   * @throws IllegalArgumentException if {@code fieldMapper} is {@code null}
   */
  public InMemoryProjectionRepository(ObjectMapper fieldMapper) {
    if (fieldMapper == null) {
      throw new IllegalArgumentException("fieldMapper cannot be null");
    }
    this.fieldMapper = fieldMapper;
    this.fieldReader =
        fieldMapper
            .reader()
            .with(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .without(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES);
  }

  private static ObjectMapper defaultFieldMapper() {
    var mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    return mapper;
  }

  private ConcurrentSkipListMap<String, Stored> table(ProjectionName name) {
    return tables.computeIfAbsent(name, n -> new ConcurrentSkipListMap<>());
  }

  private Object lockFor(ProjectionName name) {
    return locks.computeIfAbsent(name, n -> new Object());
  }

  private <R> R read(Supplier<R> body) {
    return locked(publication.readLock(), body);
  }

  private void write(Runnable body) {
    locked(
        publication.writeLock(),
        () -> {
          body.run();
          return null;
        });
  }

  private static <R> R locked(Lock lock, Supplier<R> body) {
    lock.lock();
    try {
      return body.get();
    } finally {
      lock.unlock();
    }
  }

  private Stored committedRow(ProjectionName name, String id) {
    var t = tables.get(name);
    return t == null ? null : t.get(id);
  }

  private static void validateName(ProjectionName name) {
    if (name == null) {
      throw new IllegalArgumentException("projectionName cannot be null");
    }
  }

  private static void validateInputs(ProjectionName name, String id) {
    validateName(name);
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("id cannot be null or blank");
    }
  }

  private static void validateWrite(ProjectionName name, String id, Object readModel) {
    validateInputs(name, id);
    if (readModel == null) {
      throw new IllegalArgumentException("readModel cannot be null");
    }
  }

  private static void validateVersionedWrite(
      ProjectionName name, String id, Object readModel, long expectedVersion) {
    validateWrite(name, id, readModel);
    if (expectedVersion < 0) {
      throw new IllegalArgumentException("expectedVersion must be >= 0");
    }
  }

  private static String quoted(ProjectionName name) {
    return "'" + LogSanitizer.sanitizeForLog(name.value()) + "'";
  }

  /** The versioned-save outcome of the JDBC repository's {@code UPDATE ... AND version = ?}. */
  private static Stored versionedUpdate(
      ProjectionName name,
      String id,
      Object readModel,
      long expectedVersion,
      Stored current,
      long writtenAt) {
    if (current == null || current.version() != expectedVersion) {
      throw new OptimisticLockException(
          "Optimistic lock conflict on projection '%s/%s': expected version %d but %s"
              .formatted(
                  LogSanitizer.sanitizeForLog(name.value()),
                  LogSanitizer.sanitizeForLog(id),
                  expectedVersion,
                  current == null ? "no such row exists" : "was " + current.version()));
    }
    return new Stored(readModel, current.version() + 1, writtenAt);
  }

  // ---- paged reads: the JDBC repository's order and filter, on the rows of one snapshot ----

  /**
   * One page of {@code rows} (one consistent snapshot, sorted by id): those whose field holds the
   * filter's text when there is a filter, ordered as the JDBC repository orders them.
   */
  private <T> Page<T> page(
      SortedMap<String, Stored> rows, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
    var fields = new HashMap<String, JsonNode>();
    var selected = new ArrayList<Map.Entry<String, Stored>>();
    for (var row : rows.entrySet()) {
      if (filter == null || filter.value().equals(fieldText(row, filter.field(), fields))) {
        selected.add(row);
      }
    }
    selected.sort(order(pageRequest.sort(), fields));
    long offset = pageRequest.offset();
    int from = (int) Math.min(offset, selected.size());
    int to = Math.min(from + pageRequest.size(), selected.size());
    var content = new ArrayList<T>(to - from);
    for (var row : selected.subList(from, to)) {
      content.add(type.cast(row.getValue().value()));
    }
    return new Page<>(content, selected.size(), pageRequest.page(), pageRequest.size());
  }

  /** The sort's orders, each JSON field compared as text by code point, then the id ascending. */
  private Comparator<Map.Entry<String, Stored>> order(Sort sort, Map<String, JsonNode> fields) {
    Comparator<Map.Entry<String, Stored>> order = (a, b) -> 0;
    boolean byId = false;
    for (Sort.Order key : sort.orders()) {
      Comparator<Map.Entry<String, Stored>> one = key(key.field(), fields);
      order = order.thenComparing(key.direction() == SortDirection.DESC ? one.reversed() : one);
      byId |= "id".equals(key.field());
    }
    return byId ? order : order.thenComparing(key("id", fields));
  }

  private Comparator<Map.Entry<String, Stored>> key(String field, Map<String, JsonNode> fields) {
    return switch (field) {
      case "id" -> (a, b) -> compareCodePoints(a.getKey(), b.getKey());
      case "version" -> Comparator.comparingLong(row -> row.getValue().version());
      case "updated_at" -> Comparator.comparingLong(row -> row.getValue().writtenAt());
      // PostgreSQL's default: NULL after every value ascending, before every value descending.
      default ->
          Comparator.comparing(
              row -> fieldText(row, field, fields),
              Comparator.nullsLast(InMemoryProjectionRepository::compareCodePoints));
    };
  }

  /**
   * The field as PostgreSQL's {@code data->>'field'} renders the stored JSON: a string's
   * characters, a number's or a boolean's JSON text, an object or array as JSON text; {@code null}
   * when the field is missing or JSON {@code null}.
   */
  private String fieldText(
      Map.Entry<String, Stored> row, String field, Map<String, JsonNode> fields) {
    JsonNode node = fields.computeIfAbsent(row.getKey(), id -> tree(row)).get(field);
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      // As PostgreSQL prints the numeric a jsonb number is: no exponent, the scale as written.
      return node.isIntegralNumber()
          ? node.bigIntegerValue().toString()
          : node.decimalValue().toPlainString();
    }
    return node.isValueNode() ? node.asText() : node.toString();
  }

  /**
   * The row's JSON as the field mapper writes it, read back with every decimal kept as written (not
   * a double, trailing zeros kept), so a number's text is the one PostgreSQL stores.
   */
  private JsonNode tree(Map.Entry<String, Stored> row) {
    try {
      return fieldReader.readTree(fieldMapper.writeValueAsBytes(row.getValue().value()));
    } catch (java.io.IOException e) {
      throw new EventStoreException(
          "Failed to read the fields of projection row "
              + LogSanitizer.sanitizeForLog(row.getKey()),
          e);
    }
  }

  /** Unicode code-point order: PostgreSQL's {@code COLLATE "C"} on UTF-8 text. */
  private static int compareCodePoints(String a, String b) {
    int i = 0;
    int j = 0;
    while (i < a.length() && j < b.length()) {
      int ca = a.codePointAt(i);
      int cb = b.codePointAt(j);
      if (ca != cb) {
        return Integer.compare(ca, cb);
      }
      i += Character.charCount(ca);
      j += Character.charCount(cb);
    }
    return Boolean.compare(i < a.length(), j < b.length());
  }

  private static void validateField(String field) {
    if (!FIELD.matcher(field).matches()) {
      throw new IllegalArgumentException(
          "A sort, filter or index field must be a top-level read-model field of ASCII letters,"
              + " digits and underscores (no nested path): "
              + LogSanitizer.sanitizeForLog(field));
    }
  }

  private static void validateSort(Sort sort) {
    sort.orders().forEach(order -> validateField(order.field()));
  }

  private static void validatePagedRead(
      ProjectionName name, Class<?> type, PageRequest pageRequest) {
    validateName(name);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (pageRequest == null) {
      throw new IllegalArgumentException("pageRequest cannot be null");
    }
    validateSort(pageRequest.sort());
  }

  private static void validateFilteredRead(
      ProjectionName name, Class<?> type, FieldEquals filter, PageRequest pageRequest) {
    validateName(name);
    if (type == null) {
      throw new IllegalArgumentException("type cannot be null");
    }
    if (filter == null) {
      throw new IllegalArgumentException("filter cannot be null");
    }
    validatePagedRead(name, type, pageRequest);
    validateField(filter.field()); // FieldEquals itself refuses the column names
  }

  /**
   * Refuses a filter or sort on an {@code Encrypted} record component of {@code type}, as the JDBC
   * repository does: matched by its Java name and by the JSON name the field mapper gives it.
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
      if (!COLUMNS.contains(order.field()) && encrypted.contains(order.field())) {
        throw encryptedKey(type, order.field());
      }
    }
  }

  private Set<String> encryptedJsonFields(Class<?> type) {
    return encryptedJsonFields(type, new HashSet<>());
  }

  /**
   * The JSON names of {@code type}'s {@code Encrypted} components, and of those of the records it
   * embeds with {@code @JsonUnwrapped} (with the annotation's prefix and suffix). One level of
   * {@code @JsonUnwrapped} is what is supported: Jackson does not round-trip a record unwrapped
   * inside an unwrapped record (the walk goes deeper, but such a read model cannot be read back). A
   * type that is not a record ({@code Map}, a class) is not checked.
   */
  private Set<String> encryptedJsonFields(Class<?> type, Set<Class<?>> visiting) {
    if (!type.isRecord() || !visiting.add(type)) {
      return Set.of();
    }
    var components = new HashSet<String>();
    var names = new HashSet<String>();
    for (var component : type.getRecordComponents()) {
      if (component.isAnnotationPresent(Encrypted.class)) {
        components.add(component.getName());
      }
      var unwrapped = unwrapped(type, component);
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
          fieldMapper.getSerializationConfig().introspect(fieldMapper.constructType(type));
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
  private static com.fasterxml.jackson.annotation.JsonUnwrapped unwrapped(
      Class<?> type, java.lang.reflect.RecordComponent component) {
    var onAccessor =
        component.getAccessor().getAnnotation(com.fasterxml.jackson.annotation.JsonUnwrapped.class);
    if (onAccessor != null) {
      return onAccessor;
    }
    try {
      return type.getDeclaredField(component.getName())
          .getAnnotation(com.fasterxml.jackson.annotation.JsonUnwrapped.class);
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

  /**
   * Refuses, as the JDBC repository does at save, a read model the field mapper cannot write, and
   * one whose JSON holds U+0000 in a string or a field name, which PostgreSQL's {@code jsonb}
   * refuses ({@code 22P05}); the stored object itself is kept, not the JSON.
   */
  private void requireWritable(ProjectionName name, String id, Object readModel) {
    try {
      JsonNode tree = fieldReader.readTree(fieldMapper.writeValueAsBytes(readModel));
      if (holdsNul(tree)) {
        throw new EventStoreException(
            "Failed to save projection: "
                + LogSanitizer.sanitizeForLog(name.value())
                + "/"
                + LogSanitizer.sanitizeForLog(id)
                + ": a string holds U+0000, which PostgreSQL jsonb cannot store");
      }
    } catch (java.io.IOException e) {
      throw new EventStoreException(
          "Failed to save projection: "
              + LogSanitizer.sanitizeForLog(name.value())
              + "/"
              + LogSanitizer.sanitizeForLog(id),
          e);
    }
  }

  private static boolean holdsNul(JsonNode node) {
    if (node.isTextual()) {
      return node.textValue().indexOf(0) >= 0;
    }
    if (node.isObject()) {
      for (var fields = node.fields(); fields.hasNext(); ) {
        var field = fields.next();
        if (field.getKey().indexOf(0) >= 0 || holdsNul(field.getValue())) {
          return true;
        }
      }
      return false;
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

  // ---- ProjectionRepository (autocommit face: what a BaseProjection uses outside a batch) ----

  @Override
  public <T> void save(ProjectionName projectionName, String id, T readModel) {
    validateWrite(projectionName, id, readModel);
    requireWritable(projectionName, id, readModel);
    write(
        () ->
            table(projectionName)
                .compute(
                    id,
                    (k, old) ->
                        new Stored(
                            readModel,
                            old == null ? 0L : old.version() + 1,
                            writeClock.incrementAndGet())));
  }

  @Override
  public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
    validateInputs(projectionName, id);
    return read(
        () -> Optional.ofNullable(committedRow(projectionName, id)).map(s -> type.cast(s.value())));
  }

  /** Every row of {@code projectionName}, sorted by id. */
  @Override
  public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
    validateName(projectionName);
    return read(
        () -> {
          var t = tables.get(projectionName);
          return t == null
              ? List.<T>of()
              : t.values().stream().map(s -> type.cast(s.value())).toList();
        });
  }

  @Override
  public void delete(ProjectionName projectionName, String id) {
    validateInputs(projectionName, id);
    write(
        () -> {
          var t = tables.get(projectionName);
          if (t != null) {
            t.remove(id);
          }
        });
  }

  /**
   * Pages the rows in the JDBC repository's order: the sort's fields as JSON text by code point
   * (PostgreSQL's {@code COLLATE "C"}), a missing field last ascending and first descending, the id
   * breaking ties; unsorted by id. The count and the page come from one snapshot.
   */
  @Override
  public <T> Page<T> findAll(
      ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
    validatePagedRead(projectionName, type, pageRequest);
    refuseEncryptedKeys(type, null, pageRequest.sort());
    return read(() -> page(committedRows(projectionName), type, null, pageRequest));
  }

  /**
   * Pages the rows whose top-level field holds the filter's text, compared as the JDBC repository
   * compares {@code data->>'field'}, in the order of {@link #findAll(ProjectionName, Class,
   * PageRequest)}.
   */
  @Override
  public <T> Page<T> findAllBy(
      ProjectionName projectionName, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
    validateFilteredRead(projectionName, type, filter, pageRequest);
    refuseEncryptedKeys(type, filter, pageRequest.sort());
    return read(() -> page(committedRows(projectionName), type, filter, pageRequest));
  }

  private SortedMap<String, Stored> committedRows(ProjectionName name) {
    var t = tables.get(name);
    return t == null ? new TreeMap<>() : t;
  }

  /**
   * Checks {@code index} as the JDBC repository does and records it ({@link #declaredIndexes()});
   * it creates nothing: a map needs no index. Declare the same indexes here as in production, so a
   * key the database would refuse fails in a test too.
   *
   * @param index the index the projection's reads need
   * @throws IllegalArgumentException if {@code index} is {@code null} or a key is outside the field
   *     rule
   */
  @Override
  public void declareIndex(ProjectionIndex index) {
    if (index == null) {
      throw new IllegalArgumentException("index cannot be null");
    }
    index.keys().forEach(key -> validateField(key.field()));
    declaredIndexes.add(index);
  }

  /**
   * The indexes declared on this repository, in declaration order, each once: what a test of the
   * wiring asserts.
   *
   * @return the declared indexes; an unmodifiable copy
   */
  public List<ProjectionIndex> declaredIndexes() {
    synchronized (declaredIndexes) {
      return List.copyOf(declaredIndexes);
    }
  }

  /** The lock mode is accepted and ignored: a single JVM map needs no row lock. */
  @Override
  public <T> Optional<Versioned<T>> findById(
      ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
    validateInputs(projectionName, id);
    return read(
        () ->
            Optional.ofNullable(committedRow(projectionName, id))
                .map(s -> new Versioned<>(type.cast(s.value()), s.version())));
  }

  /**
   * Updates an existing row at {@code expectedVersion}; a missing row is a conflict at every
   * expected version, as with the JDBC repository's {@code UPDATE}.
   */
  @Override
  public <T> void save(
      ProjectionName projectionName, String id, T readModel, long expectedVersion) {
    validateVersionedWrite(projectionName, id, readModel, expectedVersion);
    requireWritable(projectionName, id, readModel);
    write(
        () -> {
          Stored next =
              versionedUpdate(
                  projectionName,
                  id,
                  readModel,
                  expectedVersion,
                  committedRow(projectionName, id),
                  writeClock.incrementAndGet());
          table(projectionName).put(id, next);
        });
  }

  // ---- extra accessors for assertions ----

  /**
   * The committed checkpoint of {@code name}, as any reader outside a batch sees it.
   *
   * @param name the projection
   * @return the committed checkpoint; {@link GlobalOffset#initial()} if none
   */
  public GlobalOffset committedOffset(ProjectionName name) {
    return read(() -> GlobalOffset.of(checkpoints.getOrDefault(name, Checkpoint.NONE).offset()));
  }

  /**
   * The leadership epoch stamped on the checkpoint of {@code name}.
   *
   * @param name the projection
   * @return the stamped epoch; {@code 0} if none
   */
  public long stampedEpoch(ProjectionName name) {
    return read(() -> checkpoints.getOrDefault(name, Checkpoint.NONE).epoch());
  }

  /**
   * {@code true} once any row was ever written under {@code name} (the in-memory "table exists").
   *
   * @param name the projection
   * @return whether {@code name} has a table
   */
  public boolean hasReadModels(ProjectionName name) {
    return read(() -> tables.containsKey(name));
  }

  /**
   * The number of committed rows of {@code name}.
   *
   * @param name the projection
   * @return the row count; {@code 0} if {@code name} has no table
   */
  public int rowCount(ProjectionName name) {
    return read(
        () -> {
          var t = tables.get(name);
          return t == null ? 0 : t.size();
        });
  }

  // ---- OffsetStore (the same checkpoint the processor advances) ----

  @Override
  public GlobalOffset getLastOffset(ProjectionName projectionName) {
    return committedOffset(projectionName);
  }

  /**
   * Monotonic: a backward or sideways move is a silent no-op, as in {@code PostgresOffsetStore}.
   */
  @Override
  public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
    synchronized (lockFor(projectionName)) {
      Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
      if (offset.value() > cp.offset()) {
        write(() -> checkpoints.put(projectionName, new Checkpoint(offset.value(), cp.epoch())));
      }
    }
  }

  /** Unguarded rewind to offset 0; the stamped epoch is kept. */
  @Override
  public void reset(ProjectionName projectionName) {
    synchronized (lockFor(projectionName)) {
      Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
      write(() -> checkpoints.put(projectionName, new Checkpoint(0L, cp.epoch())));
    }
  }

  // ---- AtomicBatchProcessor ----

  @Override
  public boolean transactional() {
    return true;
  }

  @Override
  public boolean fencesCheckpoint() {
    return true;
  }

  /** Monotonic: an older epoch is a no-op, and the offset never moves. */
  @Override
  public void stampFencingEpoch(ProjectionName projectionName, long fencingEpoch) {
    if (fencingEpoch == 0L) {
      return;
    }
    synchronized (lockFor(projectionName)) {
      Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
      write(
          () ->
              checkpoints.put(
                  projectionName, new Checkpoint(cp.offset(), Math.max(cp.epoch(), fencingEpoch))));
    }
  }

  /**
   * Runs the updater against a transaction-scoped view under the projection's lock, then applies
   * its writes and moves the checkpoint together. {@code offsetStore} is ignored: the checkpoint
   * this processor advances is its own. The view refuses every read and write once this method has
   * committed or rolled back.
   */
  @Override
  public void executeAtomically(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      OffsetStore offsetStore) {
    var tx = new Transaction();
    try {
      synchronized (lockFor(projectionName)) {
        commit(projectionName, batch, newOffset, fencingEpoch, projectionUpdater, tx);
      }
    } finally {
      tx.close();
    }
    tx.runAfterCommit();
  }

  /** A replay and a batch of one projection run under the same per-name lock. */
  @Override
  public boolean serializesReplay() {
    return true;
  }

  /**
   * Runs the updater against a transaction-scoped view under the projection's lock — the lock
   * {@link #executeAtomically} takes — and applies its writes together; the checkpoint is not
   * touched. An updater exception drops the staged writes. After-commit actions behave as in {@link
   * #executeAtomically}.
   */
  @Override
  public void executeReplay(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      ProjectionUpdater projectionUpdater) {
    validateName(projectionName);
    var tx = new Transaction();
    try {
      synchronized (lockFor(projectionName)) {
        projectionUpdater.update(tx); // throws → the staged writes are dropped
        write(tx::apply);
      }
    } finally {
      tx.close();
    }
    tx.runAfterCommit();
  }

  private void commit(
      ProjectionName projectionName,
      List<EventEnvelope> batch,
      GlobalOffset newOffset,
      long fencingEpoch,
      ProjectionUpdater projectionUpdater,
      Transaction tx) {
    Checkpoint cp = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
    if (fencingEpoch != 0L && fencingEpoch < cp.epoch()) {
      throw new ProjectionCommitFencedException(
          ProjectionCommitFencedException.Guard.EPOCH_FENCE,
          "Projection "
              + quoted(projectionName)
              + " commit fenced out: caller epoch "
              + fencingEpoch
              + " is below the stored epoch "
              + cp.epoch()
              + " — a newer leader has taken over. Rolling back the stale leader's write.");
    }
    if (!batch.isEmpty() && batch.getFirst().globalOffset().value() <= cp.offset()) {
      throw new ProjectionCommitFencedException(
          ProjectionCommitFencedException.Guard.OVERLAP,
          "Projection "
              + quoted(projectionName)
              + " batch starting at offset "
              + batch.getFirst().globalOffset().value()
              + " overlaps the committed checkpoint ("
              + cp.offset()
              + "). Rolling back so the read-model write is not applied a second time.");
    }
    projectionUpdater.update(tx); // throws → nothing below runs, the staged writes are dropped
    // Re-read like the SQL guard does at UPDATE time: the lock is reentrant, so the updater's own
    // thread could have moved the checkpoint or stamped an epoch through this object.
    Checkpoint current = checkpoints.getOrDefault(projectionName, Checkpoint.NONE);
    if (newOffset.value() <= current.offset()) {
      throw new ProjectionCommitFencedException(
          ProjectionCommitFencedException.Guard.MONOTONIC,
          "Projection "
              + quoted(projectionName)
              + " checkpoint advance to "
              + newOffset.value()
              + " was rejected by the monotonic guard (stored offset "
              + current.offset()
              + "). Rolling back so the read-model write is not committed a second time.");
    }
    write(
        () -> {
          tx.apply();
          checkpoints.put(
              projectionName,
              new Checkpoint(newOffset.value(), Math.max(current.epoch(), fencingEpoch)));
        });
  }

  /** The transaction-scoped view handed to the updater: staged writes, read-your-writes. */
  private final class Transaction implements ProjectionRepository {
    private final Map<ProjectionName, Map<String, Stored>> staged = new HashMap<>();
    private final Map<ProjectionName, Set<String>> deleted = new HashMap<>();
    private final List<Runnable> afterCommit = new ArrayList<>();
    // updated_at of every row this transaction writes: one tick, as PostgreSQL's NOW() is the
    // transaction's start, so the rows of one transaction tie and the id orders them.
    private final long startedAt = writeClock.incrementAndGet();
    private volatile boolean closed;
    private volatile boolean ran;

    private void ensureOpen() {
      if (closed) {
        throw new IllegalStateException(
            "The transaction is over: a transaction-scoped repository can be used only inside"
                + " the updater it was handed to");
      }
    }

    private Optional<Stored> lookup(ProjectionName name, String id) {
      if (deleted.getOrDefault(name, Set.of()).contains(id)) {
        return Optional.empty();
      }
      var s = staged.getOrDefault(name, Map.of()).get(id);
      if (s != null) {
        return Optional.of(s);
      }
      return read(() -> Optional.ofNullable(committedRow(name, id)));
    }

    private void stage(ProjectionName name, String id, Stored row) {
      staged.computeIfAbsent(name, n -> new HashMap<>()).put(id, row);
      var gone = deleted.get(name);
      if (gone != null) {
        gone.remove(id);
      }
    }

    @Override
    public <T> void save(ProjectionName name, String id, T readModel) {
      ensureOpen();
      validateWrite(name, id, readModel);
      requireWritable(name, id, readModel);
      long version = lookup(name, id).map(s -> s.version() + 1).orElse(0L);
      stage(name, id, new Stored(readModel, version, startedAt));
    }

    @Override
    public <T> Optional<T> findById(ProjectionName name, String id, Class<T> type) {
      ensureOpen();
      validateInputs(name, id);
      return lookup(name, id).map(s -> type.cast(s.value()));
    }

    @Override
    public <T> List<T> findAll(ProjectionName name, Class<T> type) {
      ensureOpen();
      validateName(name);
      return merged(name).values().stream().map(s -> type.cast(s.value())).toList();
    }

    /** The committed rows with this transaction's writes and deletes applied, sorted by id. */
    private TreeMap<String, Stored> merged(ProjectionName name) {
      var merged = new TreeMap<String, Stored>();
      read(
          () -> {
            var t = tables.get(name);
            if (t != null) {
              merged.putAll(t);
            }
            return null;
          });
      merged.putAll(staged.getOrDefault(name, Map.of()));
      deleted.getOrDefault(name, Set.of()).forEach(merged::remove);
      return merged;
    }

    @Override
    public void delete(ProjectionName name, String id) {
      ensureOpen();
      validateInputs(name, id);
      var rows = staged.get(name);
      if (rows != null) {
        rows.remove(id);
      }
      deleted.computeIfAbsent(name, n -> new HashSet<>()).add(id);
    }

    @Override
    public <T> Page<T> findAll(ProjectionName name, Class<T> type, PageRequest pageRequest) {
      ensureOpen();
      validatePagedRead(name, type, pageRequest);
      refuseEncryptedKeys(type, null, pageRequest.sort());
      return page(merged(name), type, null, pageRequest);
    }

    @Override
    public <T> Page<T> findAllBy(
        ProjectionName name, Class<T> type, FieldEquals filter, PageRequest pageRequest) {
      ensureOpen();
      validateFilteredRead(name, type, filter, pageRequest);
      refuseEncryptedKeys(type, filter, pageRequest.sort());
      return page(merged(name), type, filter, pageRequest);
    }

    /** Refused, as on the JDBC repository's transaction-scoped view. */
    @Override
    public void declareIndex(ProjectionIndex index) {
      throw new IllegalStateException(
          "declareIndex on a transaction-scoped repository: declare indexes at wiring, on the"
              + " repository itself, not inside a projection batch");
    }

    @Override
    public <T> Optional<Versioned<T>> findById(
        ProjectionName name, String id, Class<T> type, LockMode lockMode) {
      ensureOpen();
      validateInputs(name, id);
      return lookup(name, id).map(s -> new Versioned<>(type.cast(s.value()), s.version()));
    }

    @Override
    public <T> void save(ProjectionName name, String id, T readModel, long expectedVersion) {
      ensureOpen();
      validateVersionedWrite(name, id, readModel, expectedVersion);
      requireWritable(name, id, readModel);
      stage(
          name,
          id,
          versionedUpdate(
              name, id, readModel, expectedVersion, lookup(name, id).orElse(null), startedAt));
    }

    @Override
    public Object writeTargetIdentity() {
      return InMemoryProjectionRepository.this.writeTargetIdentity();
    }

    /**
     * Queued until the commit and dropped on rollback; registered after the commit (from an action,
     * or through a view kept past the batch) it runs at once, as with the JDBC repository.
     */
    @Override
    public void afterCommit(Runnable action) {
      if (action == null) {
        throw new IllegalArgumentException("action cannot be null");
      }
      if (ran) {
        action.run();
      } else {
        afterCommit.add(action);
      }
    }

    void apply() {
      deleted.forEach(
          (name, ids) -> {
            var t = tables.get(name);
            if (t != null) {
              ids.forEach(t::remove);
            }
          });
      staged.forEach((name, rows) -> table(name).putAll(rows));
    }

    void close() {
      closed = true;
    }

    /**
     * Runs every queued action. A {@link RuntimeException} does not stop the rest; the first one is
     * rethrown once all have run, the later ones suppressed on it. An {@link Error} ends the run,
     * as with the JDBC repository.
     */
    void runAfterCommit() {
      ran = true;
      RuntimeException first = null;
      for (Runnable action : afterCommit) {
        try {
          action.run();
        } catch (RuntimeException e) {
          if (first == null) {
            first = e;
          } else if (e != first) {
            first.addSuppressed(e);
          }
        }
      }
      if (first != null) {
        throw first;
      }
    }
  }
}
