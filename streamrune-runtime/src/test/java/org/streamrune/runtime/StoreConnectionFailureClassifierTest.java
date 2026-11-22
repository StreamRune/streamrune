package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.test.ForwardingSagaStore;
import org.streamrune.test.InMemorySagaStore;

/** The saga store's classifier, read on first use, once, null taken as GENERIC. */
class StoreConnectionFailureClassifierTest {

  private static final SQLException CONNECTION_LOST = new SQLException("down", "08006");
  private static final SQLException INTERNAL = new SQLException("internal", "XX000");

  /** A store that counts its reads and answers what {@code answer} says. */
  private static final class CountingStore extends ForwardingSagaStore {
    final AtomicInteger reads = new AtomicInteger();
    volatile java.util.function.Supplier<ConnectionFailureClassifier> answer =
        () -> f -> f == INTERNAL;

    CountingStore() {
      super(new InMemorySagaStore());
    }

    @Override
    public ConnectionFailureClassifier connectionFailureClassifier() {
      reads.incrementAndGet();
      return answer.get();
    }
  }

  @Test
  void theStoreIsNotReadUntilTheFirstUse_thenOnce() {
    var store = new CountingStore();
    var classifier = new StoreConnectionFailureClassifier(store);
    assertThat(store.reads).hasValue(0);

    assertThat(classifier.isConnectionFailure(INTERNAL)).isTrue();
    assertThat(classifier.isConnectionFailure(CONNECTION_LOST)).isFalse();
    assertThat(store.reads).hasValue(1);
  }

  @Test
  void aStoreThatAnswersNullIsGeneric() {
    var store = new CountingStore();
    store.answer = () -> null;
    var classifier = new StoreConnectionFailureClassifier(store);

    assertThat(classifier.isConnectionFailure(CONNECTION_LOST)).isTrue();
    assertThat(classifier.isConnectionFailure(INTERNAL)).isFalse();
    assertThat(store.reads).hasValue(1);
  }

  @Test
  void aReadThatThrowsIsNotKept_andTheNextUseReadsAgain() {
    var store = new CountingStore();
    store.answer =
        () -> {
          throw new IllegalStateException("not yet");
        };
    var classifier = new StoreConnectionFailureClassifier(store);

    assertThatThrownBy(() -> classifier.isConnectionFailure(INTERNAL))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("not yet");

    store.answer = () -> f -> f == INTERNAL;
    assertThat(classifier.isConnectionFailure(INTERNAL)).isTrue();
    assertThat(classifier.isConnectionFailure(INTERNAL)).isTrue();
    assertThat(store.reads).hasValue(2);
  }

  @Test
  void racingFirstUsesReadOnce() throws Exception {
    var store = new CountingStore();
    var classifier = new StoreConnectionFailureClassifier(store);
    var go = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 16; i++) {
      threads.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      go.await();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                    classifier.isConnectionFailure(INTERNAL);
                  }));
    }
    go.countDown();
    for (Thread t : threads) {
      t.join(5000);
    }
    assertThat(store.reads).hasValue(1);
  }

  @Test
  void theStoreIsRequired() {
    assertThatThrownBy(() -> new StoreConnectionFailureClassifier((SagaStore) null))
        .isInstanceOf(NullPointerException.class);
  }
}
