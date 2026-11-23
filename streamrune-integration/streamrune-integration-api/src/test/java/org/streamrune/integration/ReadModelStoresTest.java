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
            "postgres",
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
        new ReadModelStores(
            "postgres", () -> Optional.of(store("postgres")), () -> List.of(replacement));

    assertThat(stores.defaultRepository()).isSameAs(replacement.repository());
  }

  @Test
  void withoutAStoreNamedPostgres_theDefaultRepositoryRefuses() {
    var stores = new ReadModelStores("postgres", Optional::empty, () -> List.of(store("search")));

    assertThatThrownBy(stores::defaultRepository)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("No read-model store is named 'postgres' (stores: 'search')");
  }

  @Test
  void anInvalidStoreSet_failsEachTimeItIsAskedFor() {
    var applicationCalls = new AtomicInteger();
    var stores =
        new ReadModelStores(
            "postgres",
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
    assertThatThrownBy(() -> new ReadModelStores("postgres", null, List::of))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("defaultStore is required");
    assertThatThrownBy(() -> new ReadModelStores("postgres", Optional::empty, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("applicationStores is required");
    assertThatThrownBy(() -> new ReadModelStores("postgres", Optional::empty, () -> null).stores())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("applicationStores must supply a list");
  }

  @Test
  void theDefaultStoreName_isTheBackends() {
    var sqlite = store("sqlite");
    var stores = new ReadModelStores("sqlite", () -> Optional.of(sqlite), List::of);

    assertThat(stores.defaultStoreName()).isEqualTo("sqlite");
    assertThat(stores.stores()).containsExactly(sqlite);
    assertThat(stores.defaultRepository()).isSameAs(sqlite.repository());
  }

  @Test
  void anApplicationStoreNamedLikeTheBackend_replacesTheDefault_andGivesTheRepository() {
    var defaultCalls = new AtomicInteger();
    var replacement = store("sqlite");
    var stores =
        new ReadModelStores(
            "sqlite",
            () -> {
              defaultCalls.incrementAndGet();
              return Optional.of(store("sqlite"));
            },
            () -> List.of(store("postgres"), replacement));

    assertThat(stores.defaultRepository()).isSameAs(replacement.repository());
    assertThat(defaultCalls).as("the replaced default is never created").hasValue(0);
  }

  @Test
  void anApplicationStoreNamedDefault_isRefused() {
    ReadModelStore named =
        new ReadModelStore() {
          private final InMemoryProjectionRepository repository =
              new InMemoryProjectionRepository();

          @Override
          public String name() {
            return ReadModelStore.DEFAULT;
          }

          @Override
          public org.streamrune.core.projection.ProjectionRepository repository() {
            return repository;
          }

          @Override
          public org.streamrune.core.projection.OffsetStore checkpoints() {
            return repository;
          }

          @Override
          public org.streamrune.core.projection.AtomicBatchProcessor processor() {
            return repository;
          }
        };
    var stores = new ReadModelStores("postgres", Optional::empty, () -> List.of(named));

    assertThatThrownBy(stores::stores)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("'default' is reserved: it names the backend's default read-model store");
  }

  @Test
  void theDefaultStoreName_isValidatedAtConstruction() {
    assertThatThrownBy(() -> new ReadModelStores(null, Optional::empty, List::of))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("defaultStoreName is required");
    assertThatThrownBy(() -> new ReadModelStores(" ", Optional::empty, List::of))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("defaultStoreName must not be blank");
    assertThatThrownBy(() -> new ReadModelStores("default", Optional::empty, List::of))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not the alias 'default'");
  }
}
