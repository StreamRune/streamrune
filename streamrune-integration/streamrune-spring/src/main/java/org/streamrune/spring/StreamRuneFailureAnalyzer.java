package org.streamrune.spring;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

public class StreamRuneFailureAnalyzer
    extends AbstractFailureAnalyzer<NoSuchBeanDefinitionException> {

  @Override
  protected FailureAnalysis analyze(Throwable rootFailure, NoSuchBeanDefinitionException cause) {
    Class<?> missingType = cause.getBeanType();
    String beanType = missingType != null ? missingType.getName() : "";

    if (beanType.contains("DataSource")) {
      return new FailureAnalysis(
          "StreamRune requires a DataSource bean but none was found. "
              + "The streamrune-eventstore-postgres module is on the classpath and needs a database connection.",
          "Add spring-boot-starter-jdbc to your dependencies and configure a DataSource, "
              + "or remove streamrune-eventstore-postgres if you don't need persistence.",
          cause);
    }

    if (beanType.contains("CryptoEngine")) {
      // Name only knobs that actually exist. The old remedy named a single
      // "provider" selector key that NOTHING reads (a pre-1.0 leftover) — each backend is enabled
      // by its own streamrune.crypto.<backend>.enabled flag (@ConditionalOnProperty on the four
      // *CryptoEngineConfiguration classes) plus that backend's required companion settings. A
      // guard test asserts every streamrune.* key this class names is a real, bound-or-read
      // property (StreamRuneFailureAnalyzerTest), so a renamed knob can never strand an operator
      // in a boot-failure outage again.
      return new FailureAnalysis(
          "StreamRune encryption is enabled but no CryptoEngine bean was found.",
          "Register a CryptoEngine bean, or enable exactly one built-in backend: "
              + "streamrune.crypto.filesystem.enabled=true "
              + "(with streamrune.crypto.filesystem.key-directory), "
              + "streamrune.crypto.postgres.enabled=true (requires a DataSource bean), "
              + "streamrune.crypto.vault.enabled=true (with streamrune.crypto.vault.token; "
              + "a DataSource is required for the durable erasure tombstone store), or "
              + "streamrune.crypto.aws.enabled=true (with streamrune.crypto.aws.region and "
              + "streamrune.crypto.aws.kms-key-id; a DataSource is required for the durable "
              + "erasure tombstone store).",
          cause);
    }

    return null;
  }
}
