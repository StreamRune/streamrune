package org.streamrune.runtime.jdbc;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Internal;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.Versioned;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * The rules a projection repository applies whatever its backend: which projection names have a
 * read-model table and what that table is called, the argument checks and their messages, which
 * sort and filter fields are allowed, the refusal of a filter or sort on an {@code Encrypted}
 * record component, the turning of stored JSON back into read models with the failure messages that
 * name the projection, the id tiebreak of a sort or index, the deterministic index name and the
 * index statement's template, and the optimistic-lock conflict.
 *
 * <p>What a backend renders differently stays in the backend: the SQL expression of a key, its
 * collation and the column list of an index. {@link #indexName} takes the backend's rendered column
 * list, so the same index has a different name on each backend, as each backend's hash covers its
 * own statement.
 *
 * <p><b>Helper rules.</b> It touches no connection and never wraps a {@link java.sql.SQLException}.
 * It does not log. Its one per-instance state, the JSON names of each read-model type's {@code
 * Encrypted} components under the repository's mapper, belongs to the repository instance that
 * built it: a repository builds exactly one, so two repositories with mappers that name properties
 * differently never share it. It keeps no static mutable state.
 *
 * @hidden
 */
@Internal
public final class ReadModelRules {

  /** The repository's own columns: a sort on one of them is never a read-model field. */
  public static final Set<String> DIRECT_COLUMNS = Set.of("id", "updated_at", "version");

  /** The longest index name, in characters: PostgreSQL's identifier limit, applied everywhere. */
  public static final int INDEX_NAME_LENGTH = 63;

  /**
   * The projection names a repository stores read models under: lower-case ASCII letters, digits
   * and underscores, starting with a letter or an underscore, at most 58 characters. The table is
   * the name plus {@code _view}, so it never exceeds a 63-byte identifier limit (it is never
   * truncated), is never case-folded, and is never a keyword — no keyword ends in {@code _view}.
   * The name is used exactly as given, never rewritten, so two different names always have two
   * different tables.
   */
  private static final Pattern TABLE_NAMED_PROJECTION = Pattern.compile("[a-z_][a-z0-9_]{0,57}");

  /**
   * A sort, filter or index field: a top-level read-model field (or a repository column) of ASCII
   * letters, digits and underscores, so it is safe to splice into a statement. No nested path.
   */
  private static final Pattern FIELD = Pattern.compile("[A-Za-z0-9_]+");

  private static final int INDEX_NAME_HASH_DIGITS = 10;
  private static final String INDEX_NAME_MARK = "_ix_";

  private final ObjectMapper objectMapper;

  // The JSON names of each read-model type's @Encrypted record components (see
  // refuseEncryptedKeys).
  private final ConcurrentHashMap<Class<?>, Set<String>> encryptedFields =
      new ConcurrentHashMap<>();

  /**
   * The rules of one repository, whose read models {@code objectMapper} (de)serialises.
   *
   * @param objectMapper the repository's mapper: it reads the stored JSON back and names the JSON
   *     properties the {@code Encrypted} refusal matches
   */
  public ReadModelRules(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  // ---- encrypted components ----

  /**
   * Refuses a filter or sort on an {@code Encrypted} record component of {@code type}: its stored
   * text is ciphertext under a crypto-aware mapper (so it matches and orders nothing meaningful),
   * plaintext under a crypto-blind one; refused in every wiring so the mistake fails at the first
   * call. The component is matched by its Java name and by the JSON name this repository's mapper
   * gives it.
   *
   * @param type the read-model type
   * @param filter the filter, or {@code null} for an unfiltered read
   * @param sort the sort
   * @throws IllegalArgumentException if the filter or a sort key names an encrypted component
   */
  public void refuseEncryptedKeys(Class<?> type, FieldEquals filter, Sort sort) {
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
    var onAccessor = component.getAccessor().getAnnotation(JsonUnwrapped.class);
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

  // ---- stored JSON back to read models ----

  /**
   * One stored row's JSON as a read model.
   *
   * @param json the stored JSON
   * @param type the read-model type
   * @param failure the message prefix of a failure, naming the operation
   * @param projectionName the projection
   * @param id the row id
   * @param <T> the read-model type
   * @return the read model
   * @throws EventStoreException if the JSON cannot be read as {@code type}: {@code failure}, then
   *     the sanitized {@code projection/id}, with the mapper's exception as the cause
   */
  public <T> T fromJson(
      String json, Class<T> type, String failure, ProjectionName projectionName, String id) {
    try {
      return objectMapper.readValue(json, type);
    } catch (JsonProcessingException e) {
      throw new EventStoreException(failure + shown(projectionName, id), e);
    }
  }

  /**
   * Stored rows' JSON as read models, in order.
   *
   * @param rows the stored JSON of each row
   * @param type the read-model type
   * @param failure the message prefix of a failure, naming the operation
   * @param projectionName the projection
   * @param <T> the read-model type
   * @return the read models
   * @throws EventStoreException if a row cannot be read as {@code type}: {@code failure}, then the
   *     sanitized projection name, with the mapper's exception as the cause
   */
  public <T> List<T> fromJson(
      List<String> rows, Class<T> type, String failure, ProjectionName projectionName) {
    List<T> results = new ArrayList<>(rows.size());
    try {
      for (String json : rows) {
        results.add(objectMapper.readValue(json, type));
      }
    } catch (JsonProcessingException e) {
      throw new EventStoreException(failure + shown(projectionName), e);
    }
    return results;
  }

  /**
   * A page of stored JSON as a page of read models, with the same totals.
   *
   * @param rows the page of stored JSON
   * @param type the read-model type
   * @param projectionName the projection
   * @param <T> the read-model type
   * @return the page of read models
   * @throws EventStoreException if a row cannot be read as {@code type} ({@code "Failed to find
   *     paginated projections: "} and the sanitized projection name)
   */
  public <T> Page<T> fromJson(Page<String> rows, Class<T> type, ProjectionName projectionName) {
    return new Page<>(
        fromJson(rows.content(), type, "Failed to find paginated projections: ", projectionName),
        rows.totalElements(),
        rows.page(),
        rows.size());
  }

  /**
   * A versioned row's JSON as a versioned read model, with the same version.
   *
   * @param row the stored JSON and its version, or empty
   * @param type the read-model type
   * @param projectionName the projection
   * @param id the row id
   * @param <T> the read-model type
   * @return the versioned read model, or empty
   * @throws EventStoreException if the JSON cannot be read as {@code type} ({@code "Failed to find
   *     projection with lock: "} and the sanitized {@code projection/id})
   */
  public <T> Optional<Versioned<T>> fromJson(
      Optional<Versioned<String>> row, Class<T> type, ProjectionName projectionName, String id) {
    return row.map(
        r ->
            new Versioned<>(
                fromJson(
                    r.data(), type, "Failed to find projection with lock: ", projectionName, id),
                r.version()));
  }

  // ---- names and arguments ----

  /**
   * The read-model table of {@code projectionName}: the name plus {@code _view}. A name outside
   * {@code [a-z_][a-z0-9_]{0,57}} is rejected, not rewritten, so no two projection names share a
   * table, and the name is safe to splice into SQL.
   *
   * @param projectionName the projection
   * @return the table name
   * @throws IllegalArgumentException if the name is outside the rule
   */
  public static String tableName(ProjectionName projectionName) {
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
   * Rejects a {@code null} projection name.
   *
   * @param projectionName the projection
   * @throws IllegalArgumentException if it is {@code null}
   */
  public static void validateProjectionName(ProjectionName projectionName) {
    if (projectionName == null) {
      throw new IllegalArgumentException("projectionName cannot be null");
    }
  }

  /**
   * Rejects a {@code null} projection name, then a {@code null} or blank id.
   *
   * @param projectionName the projection
   * @param id the row id
   * @throws IllegalArgumentException if either is missing
   */
  public static void validateInputs(ProjectionName projectionName, String id) {
    validateProjectionName(projectionName);
    if (id == null || id.isBlank()) {
      throw new IllegalArgumentException("id cannot be null or blank");
    }
  }

  /**
   * Rejects a missing argument of a filtered paged read, in parameter order.
   *
   * @param projectionName the projection
   * @param type the read-model type
   * @param filter the filter
   * @param pageRequest the page
   * @throws IllegalArgumentException naming the first {@code null} argument
   */
  public static void validateFilteredRead(
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
   * Rejects a sort, filter or index field outside {@code [A-Za-z0-9_]+}.
   *
   * @param field the field
   * @throws IllegalArgumentException if it is outside the rule
   */
  public static void validateField(String field) {
    if (!FIELD.matcher(field).matches()) {
      throw new IllegalArgumentException(
          "A sort, filter or index field must be a top-level read-model field of ASCII letters,"
              + " digits and underscores (no nested path): "
              + LogSanitizer.sanitizeForLog(field));
    }
  }

  /**
   * {@code orders}, then {@code id} ascending unless an order already names the id.
   *
   * @param orders the sort or index keys
   * @return the keys with the id tiebreak, a new list
   */
  public static List<Sort.Order> keysWithIdTiebreak(List<Sort.Order> orders) {
    var keys = new ArrayList<>(orders);
    if (orders.stream().noneMatch(order -> "id".equals(order.field()))) {
      keys.add(new Sort.Order("id", SortDirection.ASC));
    }
    return keys;
  }

  // ---- declared indexes ----

  /**
   * The deterministic name of {@code index}: {@code <projection>_<keys>_ix_<hash>}, the projection
   * name and the lower-cased keys ({@code _desc} after a descending one, the id tiebreak included),
   * cut so the name stays within {@link #INDEX_NAME_LENGTH}, then the first ten hex digits of a
   * SHA-256 of {@code table + "|" + renderedColumns}, so two indexes that differ in letter case,
   * direction, a cut-off tail or a key expression have two names. The caller validates the name and
   * the keys (rendering the columns does) before it calls this.
   *
   * @param index the index
   * @param table the index's table, as {@link #tableName} returns it
   * @param renderedColumns the column list of the backend's statement
   * @return the index name
   */
  public static String indexName(ProjectionIndex index, String table, String renderedColumns) {
    var readable = new StringBuilder(index.projection().value());
    for (Sort.Order key : keysWithIdTiebreak(index.keys())) {
      readable.append('_').append(key.field().toLowerCase(Locale.ROOT));
      if (key.direction() == SortDirection.DESC) {
        readable.append("_desc");
      }
    }
    int room = INDEX_NAME_LENGTH - INDEX_NAME_MARK.length() - INDEX_NAME_HASH_DIGITS;
    String prefix = readable.length() > room ? readable.substring(0, room) : readable.toString();
    return prefix + INDEX_NAME_MARK + sha256Hex(table + "|" + renderedColumns);
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

  /**
   * The statement that declares an index: {@code CREATE INDEX IF NOT EXISTS <indexName> ON <table>
   * (<renderedColumns>)}.
   *
   * @param indexName the index name, as {@link #indexName} returns it
   * @param table the index's table
   * @param renderedColumns the column list of the backend's statement
   * @return the statement
   */
  public static String createIndexSql(String indexName, String table, String renderedColumns) {
    return "CREATE INDEX IF NOT EXISTS "
        + indexName
        + " ON "
        + table
        + " ("
        + renderedColumns
        + ")";
  }

  // ---- failures ----

  /**
   * The conflict of a versioned save whose expected version is not the stored one, or whose row is
   * missing. The projection name and the id are embedded as given.
   *
   * @param projectionName the projection
   * @param id the row id
   * @param expectedVersion the version the save expected
   * @return the exception, for the caller to throw
   */
  public static OptimisticLockException optimisticLockConflict(
      ProjectionName projectionName, String id, long expectedVersion) {
    return new OptimisticLockException(
        "Optimistic lock conflict on projection '%s/%s': expected version %d"
            .formatted(projectionName.value(), id, expectedVersion));
  }

  /**
   * A projection name rendered for an exception message: the name is caller text, so it passes
   * through {@link LogSanitizer#sanitizeForLog} before it is embedded.
   *
   * @param projectionName the projection
   * @return the sanitized name
   */
  public static String shown(ProjectionName projectionName) {
    return LogSanitizer.sanitizeForLog(projectionName.value());
  }

  /**
   * {@code projection/id} rendered for an exception message, both parts sanitized.
   *
   * @param projectionName the projection
   * @param id the row id
   * @return the sanitized {@code projection/id}
   */
  public static String shown(ProjectionName projectionName, String id) {
    return shown(projectionName) + "/" + LogSanitizer.sanitizeForLog(id);
  }
}
