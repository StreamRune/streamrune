package org.streamrune.core.crypto;

/**
 * Base exception for crypto engine operation failures.
 *
 * <p>This exception is thrown when an encryption, decryption, key generation, or key storage
 * operation fails.
 */
public class CryptoOperationException extends RuntimeException {

  public CryptoOperationException(String message) {
    super(message);
  }

  public CryptoOperationException(String message, Throwable cause) {
    super(message, cause);
  }
}
