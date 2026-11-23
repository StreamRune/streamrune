package org.streamrune.sqlite;

import java.util.List;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditStore;

/**
 * A test stand-in for a store of the SQLite backend: it lives in that backend's package, so {@code
 * PostgresBackendTest} can check that the foreign-store refusal names the store's backend ({@code
 * 'sqlite'}) rather than its class. The real SQLite module is not on this module's class path.
 */
public final class OtherBackendEventAuditStore implements EventAuditStore {

  @Override
  public void save(EventAuditEntry entry) {
    throw new UnsupportedOperationException("test stand-in");
  }

  @Override
  public void saveAll(List<EventAuditEntry> entries) {
    throw new UnsupportedOperationException("test stand-in");
  }
}
