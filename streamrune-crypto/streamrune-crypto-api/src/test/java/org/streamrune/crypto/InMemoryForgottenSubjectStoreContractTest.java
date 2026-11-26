package org.streamrune.crypto;

import org.streamrune.test.ForgottenSubjectStoreContract;

/** {@link InMemoryForgottenSubjectStore} against the shared contract (heap-only: not durable). */
class InMemoryForgottenSubjectStoreContractTest extends ForgottenSubjectStoreContract {

  @Override
  protected ForgottenSubjectStore newStore() {
    return new InMemoryForgottenSubjectStore();
  }
}
