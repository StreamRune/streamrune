package org.streamrune.runtime;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;

/**
 * In-memory implementation of {@link EventAuditStore} and {@link EventAuditQuery} for unit tests.
 *
 * <p>Thread-safe via {@link CopyOnWriteArrayList}. Not suitable for production use — all entries
 * are held in memory and never evicted.
 *
 * <p>All {@code find*} methods return results sorted by {@link EventAuditEntry#occurredAt()}
 * ascending. {@link #traceFullCausationChain(EventId)} uses iterative breadth-first search (BFS)
 * starting from the root event, following causation links downstream.
 */
public final class InMemoryEventAuditStore implements EventAuditStore, EventAuditQuery {

  private final List<EventAuditEntry> entries = new CopyOnWriteArrayList<>();

  /**
   * Persists a single event audit entry.
   *
   * @param entry the event audit entry to save; must not be {@code null}
   * @throws NullPointerException if {@code entry} is {@code null}
   */
  @Override
  public void save(EventAuditEntry entry) {
    Objects.requireNonNull(entry, "entry is required");
    entries.add(entry);
  }

  /**
   * Persists a batch of event audit entries.
   *
   * @param entries the list of event audit entries to save; must not be {@code null}
   * @throws NullPointerException if {@code entries} is {@code null}
   */
  @Override
  public void saveAll(List<EventAuditEntry> entries) {
    Objects.requireNonNull(entries, "entries is required");
    this.entries.addAll(entries);
  }

  /**
   * Returns all event audit entries sharing the given correlation ID, sorted by {@code occurredAt}
   * ascending.
   *
   * @param correlationId the correlation token to filter by; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if {@code correlationId} is {@code null}
   */
  @Override
  public List<EventAuditEntry> findByCorrelationId(CorrelationId correlationId) {
    Objects.requireNonNull(correlationId, "correlationId is required");
    return entries.stream()
        .filter(e -> correlationId.equals(e.correlationId()))
        .sorted(Comparator.comparing(EventAuditEntry::occurredAt))
        .toList();
  }

  /**
   * Returns all event audit entries produced by the given command, sorted by {@code occurredAt}
   * ascending.
   *
   * @param commandId the command ID to filter by; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if {@code commandId} is {@code null}
   */
  @Override
  public List<EventAuditEntry> findByCommandId(CommandId commandId) {
    Objects.requireNonNull(commandId, "commandId is required");
    return entries.stream()
        .filter(e -> commandId.equals(e.commandId()))
        .sorted(Comparator.comparing(EventAuditEntry::occurredAt))
        .toList();
  }

  /**
   * Returns all event audit entries that were caused by the given causation ID, sorted by {@code
   * occurredAt} ascending.
   *
   * @param causationId the causation ID to filter by; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if {@code causationId} is {@code null}
   */
  @Override
  public List<EventAuditEntry> findByCausationId(CausationId causationId) {
    Objects.requireNonNull(causationId, "causationId is required");
    return entries.stream()
        .filter(e -> causationId.equals(e.causationId()))
        .sorted(Comparator.comparing(EventAuditEntry::occurredAt))
        .toList();
  }

  /**
   * Returns all event audit entries for the given stream within the specified time window
   * (inclusive on both ends), sorted by {@code occurredAt} ascending.
   *
   * @param streamId the stream to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  @Override
  public List<EventAuditEntry> findByStreamId(StreamId streamId, Instant from, Instant to) {
    Objects.requireNonNull(streamId, "streamId is required");
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    return entries.stream()
        .filter(e -> streamId.equals(e.streamId()))
        .filter(e -> !e.occurredAt().isBefore(from) && !e.occurredAt().isAfter(to))
        .sorted(Comparator.comparing(EventAuditEntry::occurredAt))
        .toList();
  }

  /**
   * Returns all event audit entries attributed to the given user within the specified time window
   * (inclusive on both ends), sorted by {@code occurredAt} ascending. Entries with a {@code null}
   * userId are excluded.
   *
   * @param userId the user identity to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  @Override
  public List<EventAuditEntry> findByUserId(UserId userId, Instant from, Instant to) {
    Objects.requireNonNull(userId, "userId is required");
    Objects.requireNonNull(from, "from is required");
    Objects.requireNonNull(to, "to is required");
    return entries.stream()
        .filter(e -> userId.equals(e.userId()))
        .filter(e -> !e.occurredAt().isBefore(from) && !e.occurredAt().isAfter(to))
        .sorted(Comparator.comparing(EventAuditEntry::occurredAt))
        .toList();
  }

  /**
   * Reconstructs the full downstream causal chain starting from the event identified by {@code
   * eventId} using iterative breadth-first search (BFS).
   *
   * <p>The root event is included in the result. All events whose {@code causationId} matches the
   * {@code eventId} of an already-visited event are added to the chain iteratively until no more
   * descendants are found.
   *
   * <p>Returns an empty list if no event with the given {@code eventId} exists in this store.
   *
   * @param eventId the event to start from; must not be {@code null}
   * @return all causally linked entries (root + descendants) ordered by {@code occurredAt}
   *     ascending; never {@code null}
   * @throws NullPointerException if {@code eventId} is {@code null}
   */
  @Override
  public List<EventAuditEntry> traceFullCausationChain(EventId eventId) {
    Objects.requireNonNull(eventId, "eventId is required");

    // Find the root event — return empty list if not found
    EventAuditEntry root =
        entries.stream()
            .filter(e -> eventId.value().equals(e.eventId().value()))
            .findFirst()
            .orElse(null);

    if (root == null) {
      return List.of();
    }

    // Iterative BFS: traverse downstream via causationId links
    List<EventAuditEntry> chain = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    Deque<EventAuditEntry> queue = new ArrayDeque<>();

    queue.add(root);
    visited.add(root.eventId().value());

    while (!queue.isEmpty()) {
      EventAuditEntry current = queue.poll();
      chain.add(current);

      String currentEventId = current.eventId().value();
      // Find all entries whose causationId points to this event
      for (EventAuditEntry e : entries) {
        if (e.causationId() != null
            && currentEventId.equals(e.causationId().value())
            && visited.add(e.eventId().value())) {
          queue.add(e);
        }
      }
    }

    chain.sort(Comparator.comparing(EventAuditEntry::occurredAt));
    return List.copyOf(chain);
  }
}
