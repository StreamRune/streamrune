package org.streamrune.runtime;

import java.util.Objects;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.saga.SagaStore;

/**
 * A saga store's {@link SagaStore#connectionFailureClassifier()}, read on its first use and kept.
 *
 * <p>Not read when the runner is built, where a store's call count is a test's observable, and
 * never more than once, also when the first uses race — several steps of a page whose commands fail
 * at the same time, or the delivering thread deciding how a one-saga page ended. A store that
 * answers {@code null} (a mock) is taken as {@link ConnectionFailureClassifier#GENERIC}. A read
 * that throws is not kept: the failure reaches the caller (the page's stop rule, which then fails
 * and stops the page) and the next use reads again.
 */
final class StoreConnectionFailureClassifier implements ConnectionFailureClassifier {

  private final SagaStore store;
  private volatile ConnectionFailureClassifier resolved;

  StoreConnectionFailureClassifier(SagaStore store) {
    this.store = Objects.requireNonNull(store, "store is required");
  }

  @Override
  public boolean isConnectionFailure(Throwable failure) {
    return resolved().isConnectionFailure(failure);
  }

  private ConnectionFailureClassifier resolved() {
    ConnectionFailureClassifier classifier = resolved;
    if (classifier == null) {
      synchronized (this) {
        classifier = resolved;
        if (classifier == null) {
          ConnectionFailureClassifier fromStore = store.connectionFailureClassifier();
          classifier = fromStore != null ? fromStore : ConnectionFailureClassifier.GENERIC;
          resolved = classifier;
        }
      }
    }
    return classifier;
  }
}
