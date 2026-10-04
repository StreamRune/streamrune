package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PageRequestTest {

  @Test
  void negativePage_throws() {
    assertThrows(IllegalArgumentException.class, () -> new PageRequest(-1, 10, Sort.unsorted()));
  }

  @Test
  void zeroSize_throws() {
    assertThrows(IllegalArgumentException.class, () -> new PageRequest(0, 0, Sort.unsorted()));
  }

  @Test
  void sizeOver1000_throws() {
    assertThrows(IllegalArgumentException.class, () -> new PageRequest(0, 1001, Sort.unsorted()));
  }

  @Test
  void nullSort_throws() {
    assertThrows(NullPointerException.class, () -> new PageRequest(0, 10, null));
  }

  @Test
  void offset_correct() {
    var req = new PageRequest(3, 20, Sort.unsorted());
    assertEquals(60, req.offset());
  }

  @Test
  void of_twoArg_usesUnsorted() {
    var req = PageRequest.of(0, 10);
    assertEquals(Sort.unsorted(), req.sort());
    assertEquals(0, req.page());
    assertEquals(10, req.size());
  }

  @Test
  void of_threeArg_usesProvidedSort() {
    var sort = Sort.by("name", SortDirection.DESC);
    var req = PageRequest.of(1, 20, sort);
    assertEquals(sort, req.sort());
    assertEquals(1, req.page());
    assertEquals(20, req.size());
  }
}
