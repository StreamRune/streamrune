package org.streamrune.test;

import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventStore;

class InMemoryCommandInboxContractTest extends CommandInboxContract {

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final InMemoryEventStore events = new InMemoryEventStore().withCommandInbox(inbox);

  @Override
  protected CommandInbox inbox() {
    return inbox;
  }

  @Override
  protected EventStore eventStore() {
    return events;
  }
}
