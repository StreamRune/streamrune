package org.streamrune.core;

import java.util.List;
import java.util.Objects;

/**
 * Defines ordering for paginated queries.
 *
 * <p>Immutable. Factory methods cover the common cases.
 */
public record Sort(List<Order> orders) {

  public Sort {
    orders = List.copyOf(orders);
  }

  private static final Sort UNSORTED = new Sort(List.of());

  /** Returns a Sort with no ordering specified. */
  public static Sort unsorted() {
    return UNSORTED;
  }

  /** Returns a Sort with a single ascending order on {@code field}. */
  public static Sort by(String field) {
    return new Sort(List.of(new Order(field, SortDirection.ASC)));
  }

  /** Returns a Sort with a single order on {@code field} in the given direction. */
  public static Sort by(String field, SortDirection dir) {
    return new Sort(List.of(new Order(field, dir)));
  }

  /** A single field + direction pair. */
  public record Order(String field, SortDirection direction) {

    public Order {
      Objects.requireNonNull(field, "field");
      if (field.isBlank()) throw new IllegalArgumentException("field must not be blank");
      Objects.requireNonNull(direction, "direction");
    }
  }
}
