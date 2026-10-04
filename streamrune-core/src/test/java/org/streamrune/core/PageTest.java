package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PageTest {

  @Test
  void totalPages_roundsUp() {
    // 23 elements / 10 per page = 3 pages (ceil)
    var page = new Page<>(List.of("a", "b", "c", "d", "e"), 23L, 0, 10);
    assertEquals(3, page.totalPages());
  }

  @Test
  void totalPages_exactMultiple() {
    var page = new Page<>(List.of(), 20L, 0, 10);
    assertEquals(2, page.totalPages());
  }

  @Test
  void totalPages_zeroTotal_zero() {
    var page = new Page<>(List.of(), 0L, 0, 10);
    assertEquals(0, page.totalPages());
  }

  @Test
  void hasNext_true_whenMore() {
    // page 0 of 3 total pages → has next
    var page = new Page<>(List.of("a"), 23L, 0, 10);
    assertTrue(page.hasNext());
  }

  @Test
  void hasNext_false_onLastPage() {
    // page 2 of 3 total pages → no next
    var page = new Page<>(List.of("a"), 23L, 2, 10);
    assertFalse(page.hasNext());
  }

  @Test
  void hasPrevious_true_afterFirst() {
    var page = new Page<>(List.of("a"), 20L, 1, 10);
    assertTrue(page.hasPrevious());
  }

  @Test
  void hasPrevious_false_onFirst() {
    var page = new Page<>(List.of("a"), 20L, 0, 10);
    assertFalse(page.hasPrevious());
  }

  @Test
  void emptyContent_isEmpty() {
    var page = new Page<>(List.of(), 0L, 0, 10);
    assertTrue(page.isEmpty());
  }

  @Test
  void contentSizeExceedsPageSize_throws() {
    var ex =
        assertThrows(
            IllegalArgumentException.class, () -> new Page<>(List.of("a", "b", "c"), 100L, 0, 2));
    assertTrue(ex.getMessage().contains("exceeds page size"));
  }

  @Test
  void immutable_modifyingInputDoesNotAffectPage() {
    var mutable = new ArrayList<String>();
    mutable.add("first");
    var page = new Page<>(mutable, 1L, 0, 10);

    mutable.add("second"); // mutate the input list

    assertEquals(1, page.content().size());
    assertThrows(UnsupportedOperationException.class, () -> page.content().add("third"));
  }
}
