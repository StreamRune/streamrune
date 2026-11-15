package org.streamrune.core;

import java.util.List;
import java.util.Objects;

/**
 * A single page of query results.
 *
 * <p>The content list is immutable. {@code totalElements} reflects the total number of items
 * matching the query across all pages.
 *
 * @param <T> the element type
 */
public record Page<T>(List<T> content, long totalElements, int page, int size) {

  public Page {
    Objects.requireNonNull(content, "content");
    if (totalElements < 0) {
      throw new IllegalArgumentException("totalElements must be >= 0");
    }
    if (page < 0) {
      throw new IllegalArgumentException("page must be >= 0");
    }
    if (size < 1) {
      throw new IllegalArgumentException("size must be >= 1");
    }
    content = List.copyOf(content);
    if (content.size() > size) {
      throw new IllegalArgumentException(
          "content size " + content.size() + " exceeds page size " + size);
    }
  }

  /**
   * Returns the total number of pages, or {@link Integer#MAX_VALUE} when there are more (the page
   * index is an {@code int}, so no request reaches a page past it).
   */
  public int totalPages() {
    long pages = totalElements / size + (totalElements % size == 0 ? 0 : 1);
    return (int) Math.min(pages, Integer.MAX_VALUE);
  }

  /**
   * Returns {@code true} if there is at least one more element after this page: computed in {@code
   * long}, so it is {@code false} on a page beyond the last, also at {@code page ==
   * Integer.MAX_VALUE}.
   */
  public boolean hasNext() {
    return ((long) page + 1) * size < totalElements;
  }

  /** Returns {@code true} if this is not the first page. */
  public boolean hasPrevious() {
    return page > 0;
  }

  /** Returns {@code true} if this page contains no elements. */
  public boolean isEmpty() {
    return content.isEmpty();
  }
}
