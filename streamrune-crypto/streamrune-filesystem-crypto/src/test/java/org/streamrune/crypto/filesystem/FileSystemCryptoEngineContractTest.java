package org.streamrune.crypto.filesystem;

import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.test.CryptoKeyStoreContract;

/** {@link FileSystemCryptoEngine} against the shared key-store contract. */
class FileSystemCryptoEngineContractTest extends CryptoKeyStoreContract {

  @TempDir Path keyDirectory;

  @Override
  protected CryptoEngine newEngine() {
    return open();
  }

  @Override
  protected CryptoEngine sameKeysNewInstance(CryptoEngine engine) {
    return open();
  }

  private CryptoEngine open() {
    return FileSystemCryptoEngine.builder().keyDirectory(keyDirectory).build();
  }
}
