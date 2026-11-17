package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.test.InMemoryProjectionRepository;

class DeferredReadModelStoreTest {

  @Test
  void theNameCreatesNothing_thePartsCreateTheStoreOnce() {
    var creations = new AtomicInteger();
    var repository = new InMemoryProjectionRepository();
    var deferred =
        new DeferredReadModelStore(
            "postgres",
            () -> {
              creations.incrementAndGet();
              return ReadModelStore.of("postgres", repository, repository, repository);
            });

    assertThat(deferred.name()).isEqualTo("postgres");
    assertThat(deferred).hasToString("DeferredReadModelStore[postgres]");
    assertThat(creations).hasValue(0);

    assertThat(deferred.repository()).isSameAs(repository);
    assertThat(deferred.checkpoints()).isSameAs(repository);
    assertThat(deferred.processor()).isSameAs(repository);
    assertThat(creations).as("created once").hasValue(1);
  }

  @Test
  void aFailedCreation_isThrownToEachCaller_andRetried() {
    var attempts = new AtomicInteger();
    var deferred =
        new DeferredReadModelStore(
            "postgres",
            () -> {
              attempts.incrementAndGet();
              throw new IllegalStateException("no database");
            });

    for (int i = 0; i < 2; i++) {
      assertThatThrownBy(deferred::repository).hasMessage("no database");
    }
    assertThat(attempts).hasValue(2);
    assertThatThrownBy(() -> new DeferredReadModelStore("postgres", () -> null).processor())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("the default store was not created");
  }
}
