package org.streamrune.core.projection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

/**
 * An index a projection's read queries need, declared by the application through {@link
 * ProjectionRepository#declareIndex}. A repository that stores read models in a database creates it
 * (the PostgreSQL repository with {@code CREATE INDEX IF NOT EXISTS}, see {@code
 * JdbcProjectionRepository.declareIndex}); the in-memory repository checks and records it. In the
 * Spring, Quarkus and Micronaut integrations each {@code ProjectionIndex} bean is declared at
 * startup on the application's {@code ProjectionRepository} bean, before the projection runners
 * start.
 *
 * <p>The keys are read-model fields, or the repository columns {@code id}, {@code version} and
 * {@code updated_at}, each with a direction, in index order. The row id is always the last key: a
 * repository appends {@code id} ascending when the keys do not name it, because every paged read
 * breaks ties by the id (see {@link ProjectionRepository#findAll(ProjectionName, Class,
 * org.streamrune.core.PageRequest)}), so the index serves the whole {@code ORDER BY}.
 *
 * <p>Build one for the read it serves:
 *
 * <ul>
 *   <li>{@link #forFilter forFilter(name, field, sort)} serves {@code findAllBy(name, type,
 *       FieldEquals.of(field, …), PageRequest.of(page, size, sort))};
 *   <li>{@link #forSort forSort(name, sort)} serves {@code findAll(name, type, PageRequest.of(page,
 *       size, sort))}.
 * </ul>
 *
 * <p>The field rule is the repository's (see {@link FieldEquals}): a repository refuses a key
 * outside {@code [A-Za-z0-9_]+} when the index is declared. The index does not know the read-model
 * type, so a key on an {@code Encrypted} record component is not refused here; the reads refuse to
 * filter or sort on one, so such an index is never used. Index short scalar fields: on PostgreSQL
 * an index entry holds at most 2704 bytes, and a save whose indexed text is longer fails.
 *
 * @param projection the projection whose read models the index covers
 * @param keys the index keys in order; not empty, no field twice
 */
public record ProjectionIndex(ProjectionName projection, List<Sort.Order> keys) {

  /**
   * Checks the arguments and copies the keys.
   *
   * @throws NullPointerException if {@code projection}, {@code keys} or a key is {@code null}
   * @throws IllegalArgumentException if {@code keys} is empty or names a field twice
   */
  public ProjectionIndex {
    Objects.requireNonNull(projection, "projection");
    keys = List.copyOf(keys);
    if (keys.isEmpty()) {
      throw new IllegalArgumentException("an index needs at least one key");
    }
    var fields = new HashSet<String>();
    for (Sort.Order key : keys) {
      if (!fields.add(key.field())) {
        throw new IllegalArgumentException(
            "index key named twice: " + LogSanitizer.sanitizeForLog(key.field()));
      }
    }
  }

  /**
   * The index of the filtered read {@code findAllBy(projection, type, FieldEquals.of(field, …),
   * PageRequest.of(page, size, sort))}: {@code field} first, then the sort's orders, then the id.
   * With {@link Sort#unsorted()} the read orders by id, and so does the index after {@code field}.
   *
   * @param projection the projection
   * @param field the filtered field
   * @param sort the sort of the read
   * @return the index
   * @throws IllegalArgumentException if {@code field} is not a filterable field (as for {@link
   *     FieldEquals}: blank, or a column name), or the sort also orders by {@code field}
   */
  public static ProjectionIndex forFilter(ProjectionName projection, String field, Sort sort) {
    Objects.requireNonNull(sort, "sort");
    FieldEquals.of(field, ""); // the filter's own rule: refuses a blank field or a column name
    var keys = new ArrayList<Sort.Order>();
    keys.add(new Sort.Order(field, SortDirection.ASC));
    keys.addAll(sort.orders());
    return new ProjectionIndex(projection, keys);
  }

  /**
   * The index of the sorted read {@code findAll(projection, type, PageRequest.of(page, size,
   * sort))}: the sort's orders, then the id.
   *
   * @param projection the projection
   * @param sort the sort of the read; not unsorted (an unsorted read orders by the id, which the
   *     primary key serves)
   * @return the index
   * @throws IllegalArgumentException if {@code sort} is unsorted
   */
  public static ProjectionIndex forSort(ProjectionName projection, Sort sort) {
    Objects.requireNonNull(sort, "sort");
    if (sort.orders().isEmpty()) {
      throw new IllegalArgumentException(
          "an unsorted read orders by the id, which the primary key already serves");
    }
    return new ProjectionIndex(projection, sort.orders());
  }
}
