package org.streamrune.core;

import java.util.Objects;

/**
 * Pagination request. Zero-based page index.
 *
 * <p>Maximum page size is 1000 entries to prevent runaway queries.
 */
public record PageRequest(int page, int size, Sort sort) {

  public PageRequest {
    if (page < 0) throw new IllegalArgumentException("page must be >= 0");
    if (size < 1) throw new IllegalArgumentException("size must be >= 1");
    if (size > 1000) throw new IllegalArgumentException("size must be <= 1000");
    Objects.requireNonNull(sort, "sort");
  }

  /** Creates a {@link PageRequest} with no sorting. */
  public static PageRequest of(int page, int size) {
    return new PageRequest(page, size, Sort.unsorted());
  }

  /** Creates a {@link PageRequest} with the provided sort. */
  public static PageRequest of(int page, int size, Sort sort) {
    return new PageRequest(page, size, sort);
  }

  /** Returns the row offset for use in SQL {@code OFFSET} clauses. */
  public int offset() {
    return page * size;
  }
}
