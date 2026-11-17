package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.test.InMemoryProjectionRepository;

class ReadModelStoresTest {

  private static ReadModelStore store(String name) {
    var repository = new InMemoryProjectionRepository();
    return ReadModelStore.of(name, repository, repository, repository);
  }

  @Test
  void createsNothingUntilAsked_thenSelectsOnce() {
    var defaultCalls = new AtomicInteger();
    var applicationCalls = new AtomicInteger();
    var postgres = store("postgres");
    var search = store("search");
    var stores =
        new ReadModelStores(
            () -> {
              defaultCalls.incrementAndGet();
              return Optional.of(postgres);
            },
            () -> {
              applicationCalls.incrementAndGet();
              return List.of(search);
            });
    assertThat(defaultCalls).hasValue(0);
    assertThat(applicationCalls).hasValue(0);

    assertThat(stores.stores()).containsExactly(postgres, search);
    assertThat(stores.stores()).containsExactly(postgres, search);
    assertThat(defaultCalls).as("selected once").hasValue(1);
    assertThat(applicationCalls).hasValue(1);
  }

  @Test
  void theDefaultRepository_isTheRepositoryOfTheStoreNamedPostgres() {
    var replacement = store("postgres");
    var stores =
        new ReadModelStores(() -> Optional.of(store("postgres")), () -> List.of(replacement));

    assertThat(stores.defaultRepository()).isSameAs(replacement.repository());
  }

  @Test
  void withoutAStoreNamedPostgres_theDefaultRepositoryRefuses() {
    var stores = new ReadModelStores(Optional::empty, () -> List.of(store("search")));

    assertThatThrownBy(stores::defaultRepository)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No read-model store is named 'postgres' (stores: 'search')");
  }

  @Test
  void anInvalidStoreSet_failsEachTimeItIsAskedFor() {
    var applicationCalls = new AtomicInteger();
    var stores =
        new ReadModelStores(
            Optional::empty,
            () -> {
              applicationCalls.incrementAndGet();
              return List.of(store("search"), store("search"));
            });

    for (int i = 0; i < 2; i++) {
      assertThatThrownBy(stores::stores)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Two read-model stores are named 'search'");
    }
    assertThat(applicationCalls).as("a failure is not remembered as a selection").hasValue(2);
  }

  @Test
  void refusesNulls() {
    assertThatThrownBy(() -> new ReadModelStores(null, List::of))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("defaultStore is required");
    assertThatThrownBy(() -> new ReadModelStores(Optional::empty, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("applicationStores is required");
    assertThatThrownBy(() -> new ReadModelStores(Optional::empty, () -> null).stores())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("applicationStores must supply a list");
  }
}
