package org.streamrune.crypto;

import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.test.CryptoKeyStoreContract;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * {@link CachedCryptoEngine} over an {@link InMemoryCryptoEngine} against the shared key-store
 * contract: a cached key must not survive {@code deleteKey}, and a cached refusal must not survive
 * {@code reinstate}. A "restart" is a cold cache over the same wrapped engine.
 */
class CachedCryptoEngineContractTest extends CryptoKeyStoreContract {

  private CryptoEngine delegate;

  @Override
  protected CryptoEngine newEngine() {
    delegate = new InMemoryCryptoEngine();
    return CachedCryptoEngine.builder().delegate(delegate).build();
  }

  @Override
  protected CryptoEngine sameKeysNewInstance(CryptoEngine engine) {
    return CachedCryptoEngine.builder().delegate(delegate).build();
  }
}
