package org.streamrune.crypto;

import javax.crypto.SecretKey;
import org.streamrune.core.Internal;

/**
 * A subject's AES key together with its generation, the key-version byte of every ciphertext sealed
 * under it. The first mint is generation {@code 1}; each mint after an erasure uses the generation
 * above the highest one an erasure destroyed.
 *
 * @param key the subject's AES-256 key
 * @param version the key's generation
 * @hidden
 */
@Internal
public record VersionedKey(SecretKey key, int version) {}
