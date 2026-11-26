package org.streamrune.test;

import org.streamrune.core.crypto.CryptoEngine;

/** {@link InMemoryCryptoEngine} against the shared key-store contract (heap-only: not durable). */
class InMemoryCryptoEngineContractTest extends CryptoKeyStoreContract {

  @Override
  protected CryptoEngine newEngine() {
    return new InMemoryCryptoEngine();
  }

  @Override
  protected boolean durable() {
    return false;
  }

  @Override
  protected CryptoEngine sameKeysNewInstance(CryptoEngine engine) {
    throw new UnsupportedOperationException("not durable");
  }
}
