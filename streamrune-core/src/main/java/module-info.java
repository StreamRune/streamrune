open module org.streamrune.core {
  requires com.fasterxml.jackson.annotation;
  requires com.fasterxml.jackson.databind;
  // ProjectionErrorClassifier.DEFAULT classifies java.sql.SQLException causes as TRANSIENT.
  requires java.sql;

  exports org.streamrune.core;
  exports org.streamrune.core.audit;
  exports org.streamrune.core.crypto;
  exports org.streamrune.core.gdpr;
  exports org.streamrune.core.metrics;
  exports org.streamrune.core.outbox;
  exports org.streamrune.core.projection;
  exports org.streamrune.core.saga;
  exports org.streamrune.core.subscription;
  exports org.streamrune.core.types;
  exports org.streamrune.core.upcasting;
}
