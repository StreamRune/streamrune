package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SortTest {

  @Test
  void unsorted_emptyOrders() {
    var sort = Sort.unsorted();
    assertTrue(sort.orders().isEmpty());
  }

  @Test
  void unsorted_returnsSameInstance() {
    assertSame(Sort.unsorted(), Sort.unsorted());
  }

  @Test
  void by_fieldAsc_defaultDirection() {
    var sort = Sort.by("name");
    assertEquals(1, sort.orders().size());
    assertEquals("name", sort.orders().get(0).field());
    assertEquals(SortDirection.ASC, sort.orders().get(0).direction());
  }

  @Test
  void by_fieldAndDirection() {
    var sort = Sort.by("createdAt", SortDirection.DESC);
    assertEquals(1, sort.orders().size());
    assertEquals("createdAt", sort.orders().get(0).field());
    assertEquals(SortDirection.DESC, sort.orders().get(0).direction());
  }

  @Test
  void order_blankField_throws() {
    assertThrows(IllegalArgumentException.class, () -> new Sort.Order("  ", SortDirection.ASC));
  }

  @Test
  void order_nullDirection_throws() {
    assertThrows(NullPointerException.class, () -> new Sort.Order("field", null));
  }
}
