package org.streamrune.test;

import java.time.Instant;
import org.streamrune.core.types.IdempotencyKey;

/**
 * The command-inbox contract against {@link InMemoryCommandInbox}, written by an in-memory store.
 */
class InMemoryCommandInboxContractTest extends CommandInboxContract {

  @Override
  protected Inboxed newStore() {
    var inbox = new InMemoryCommandInbox();
    return new Inboxed(new InMemoryEventStore().withCommandInbox(inbox), inbox);
  }

  @Override
  protected void setProcessedAt(Inboxed inboxed, IdempotencyKey key, Instant processedAt) {
    ((InMemoryCommandInbox) inboxed.inbox()).setProcessedAt(key, processedAt);
  }
}
