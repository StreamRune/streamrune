package org.streamrune.test;

import org.junit.jupiter.api.Nested;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.runtime.InMemoryEventAuditStore;

/** The event audit contracts against {@link InMemoryEventAuditStore}, which is store and query. */
class InMemoryEventAuditStoreContractTest {

  @Nested
  class Store extends EventAuditStoreContract {
    @Override
    protected EventAuditStore newStore() {
      return new InMemoryEventAuditStore();
    }
  }

  @Nested
  class Query extends EventAuditQueryContract {
    private final InMemoryEventAuditStore shared = new InMemoryEventAuditStore();

    @Override
    protected EventAuditStore writer() {
      return shared;
    }

    @Override
    protected EventAuditQuery query() {
      return shared;
    }
  }
}
