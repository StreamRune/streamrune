package org.streamrune.core.crypto;

/**
 * Exception thrown when a cryptographic key is not found for a given subject.
 *
 * <p>This typically occurs during decryption when attempting to decrypt ciphertext for a subject
 * whose key has been deleted (crypto-shredded) or never created.
 */
public class KeyNotFoundException extends RuntimeException {

  public KeyNotFoundException(String message) {
    super(message);
  }

  public KeyNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
