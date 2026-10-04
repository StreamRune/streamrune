package org.streamrune.spring;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.crypto.CryptoEngine;

/**
 * Registers the immutable {@link StreamRuneCryptoProperties} record, bound once from the {@code
 * streamrune.crypto.*} configuration namespace by constructor binding.
 *
 * <p>The four backend crypto configs each inject the single bean, so a backend that activates
 * ({@code streamrune.crypto.<backend>.enabled=true}) always finds it. The configuration is present
 * whenever the crypto API is on the classpath — mirroring the backend configs'
 * {@code @ConditionalOnClass(CryptoEngine.class)} guard — and is harmless when crypto is disabled:
 * the record is a plain value holder and creates no engine.
 */
@Configuration
@ConditionalOnClass(CryptoEngine.class)
@EnableConfigurationProperties(StreamRuneCryptoProperties.class)
public class CryptoPropertiesConfiguration {}
