package org.streamrune.core.projection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.types.ProjectionName;

class ProjectionIndexTest {

  private static final ProjectionName ORDERS = ProjectionName.of("orders");
  private static final Sort NEWEST_FIRST = Sort.by("createdAt", SortDirection.DESC);

  @Test
  void forFilterPutsTheFilteredFieldFirstThenTheSort() {
    var index = ProjectionIndex.forFilter(ORDERS, "customerId", NEWEST_FIRST);

    assertEquals(ORDERS, index.projection());
    assertEquals(
        List.of(
            new Sort.Order("customerId", SortDirection.ASC),
            new Sort.Order("createdAt", SortDirection.DESC)),
        index.keys());
  }

  @Test
  void forFilterOfAnUnsortedReadIsTheFieldAlone() {
    assertEquals(
        List.of(new Sort.Order("customerId", SortDirection.ASC)),
        ProjectionIndex.forFilter(ORDERS, "customerId", Sort.unsorted()).keys());
  }

  @Test
  void forFilterRefusesWhatAFilterRefuses() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ProjectionIndex.forFilter(ORDERS, "version", Sort.unsorted()));
    assertThrows(
        IllegalArgumentException.class, () -> ProjectionIndex.forFilter(ORDERS, " ", NEWEST_FIRST));
    assertThrows(
        NullPointerException.class, () -> ProjectionIndex.forFilter(ORDERS, "customerId", null));
  }

  @Test
  void forFilterRefusesASortOnTheFilteredField() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ProjectionIndex.forFilter(ORDERS, "createdAt", NEWEST_FIRST));
  }

  @Test
  void forSortIsTheSortsOrders() {
    var index = ProjectionIndex.forSort(ORDERS, NEWEST_FIRST);

    assertEquals(NEWEST_FIRST.orders(), index.keys());
  }

  @Test
  void forSortRefusesAnUnsortedSort() {
    assertThrows(
        IllegalArgumentException.class, () -> ProjectionIndex.forSort(ORDERS, Sort.unsorted()));
    assertThrows(NullPointerException.class, () -> ProjectionIndex.forSort(ORDERS, null));
  }

  @Test
  void theKeysAreCopiedAndNotEmpty() {
    var keys = new ArrayList<>(List.of(new Sort.Order("a", SortDirection.ASC)));
    var index = new ProjectionIndex(ORDERS, keys);
    keys.add(new Sort.Order("b", SortDirection.ASC));

    assertEquals(1, index.keys().size());
    assertThrows(
        UnsupportedOperationException.class,
        () -> index.keys().add(new Sort.Order("c", SortDirection.ASC)));
    assertThrows(IllegalArgumentException.class, () -> new ProjectionIndex(ORDERS, List.of()));
    assertThrows(NullPointerException.class, () -> new ProjectionIndex(null, keys));
  }

  @Test
  void aKeyNamedTwiceIsRefusedWithTheFieldSanitized() {
    var twice =
        List.of(
            new Sort.Order("a\nb", SortDirection.ASC), new Sort.Order("a\nb", SortDirection.DESC));

    var refused =
        assertThrows(IllegalArgumentException.class, () -> new ProjectionIndex(ORDERS, twice));

    assertFalse(refused.getMessage().contains("\n"), refused.getMessage());
  }
}
