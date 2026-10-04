package org.streamrune.core.audit;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;

/**
 * Read-side interface for querying the event audit log.
 *
 * <p>All query methods return results ordered by {@link EventAuditEntry#occurredAt()} ascending. An
 * empty list is returned (never {@code null}) when no matching entries are found.
 *
 * <p><b>Pagination.</b> The unpaged {@code find*} methods materialize every matching entry in
 * memory — on a production audit log a wide time window can return millions of rows. Prefer the
 * {@link PageRequest} overloads. Their default implementations delegate to the unpaged variant and
 * slice the result in memory (no better for the store, but a stable API), so implementations backed
 * by a database should override them with store-side paging (e.g. SQL {@code LIMIT}/{@code
 * OFFSET}). Paged results keep the fixed {@code occurredAt} ascending order; the {@link
 * PageRequest#sort()} component is ignored.
 */
public interface EventAuditQuery {

  /**
   * Returns all event audit entries sharing the given correlation ID.
   *
   * @param correlationId the correlation token to filter by; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if {@code correlationId} is {@code null}
   */
  List<EventAuditEntry> findByCorrelationId(CorrelationId correlationId);

  /**
   * Returns one page of the event audit entries sharing the given correlation ID.
   *
   * <p>This default implementation delegates to {@link #findByCorrelationId(CorrelationId)} and
   * slices in memory; override it for store-side paging.
   *
   * @param correlationId the correlation token to filter by; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  default Page<EventAuditEntry> findByCorrelationId(
      CorrelationId correlationId, PageRequest pageRequest) {
    return slice(findByCorrelationId(correlationId), pageRequest);
  }

  /**
   * Returns all event audit entries produced by the given command.
   *
   * @param commandId the command ID to filter by; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if {@code commandId} is {@code null}
   */
  List<EventAuditEntry> findByCommandId(CommandId commandId);

  /**
   * Returns one page of the event audit entries produced by the given command.
   *
   * <p>This default implementation delegates to {@link #findByCommandId(CommandId)} and slices in
   * memory; override it for store-side paging.
   *
   * @param commandId the command ID to filter by; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  default Page<EventAuditEntry> findByCommandId(CommandId commandId, PageRequest pageRequest) {
    return slice(findByCommandId(commandId), pageRequest);
  }

  /**
   * Returns all event audit entries that were caused by the given causation ID.
   *
   * @param causationId the causation ID to filter by; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if {@code causationId} is {@code null}
   */
  List<EventAuditEntry> findByCausationId(CausationId causationId);

  /**
   * Returns one page of the event audit entries that were caused by the given causation ID.
   *
   * <p>This default implementation delegates to {@link #findByCausationId(CausationId)} and slices
   * in memory; override it for store-side paging.
   *
   * @param causationId the causation ID to filter by; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  default Page<EventAuditEntry> findByCausationId(
      CausationId causationId, PageRequest pageRequest) {
    return slice(findByCausationId(causationId), pageRequest);
  }

  /**
   * Returns all event audit entries for the given stream within the specified time window.
   *
   * @param streamId the stream to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  List<EventAuditEntry> findByStreamId(StreamId streamId, Instant from, Instant to);

  /**
   * Returns one page of the event audit entries for the given stream within the specified time
   * window.
   *
   * <p>This default implementation delegates to {@link #findByStreamId(StreamId, Instant, Instant)}
   * and slices in memory; override it for store-side paging.
   *
   * @param streamId the stream to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  default Page<EventAuditEntry> findByStreamId(
      StreamId streamId, Instant from, Instant to, PageRequest pageRequest) {
    return slice(findByStreamId(streamId, from, to), pageRequest);
  }

  /**
   * Returns all event audit entries attributed to the given user within the specified time window.
   *
   * @param userId the user identity to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @return matching entries ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  List<EventAuditEntry> findByUserId(UserId userId, Instant from, Instant to);

  /**
   * Returns one page of the event audit entries attributed to the given user within the specified
   * time window.
   *
   * <p>This default implementation delegates to {@link #findByUserId(UserId, Instant, Instant)} and
   * slices in memory; override it for store-side paging.
   *
   * @param userId the user identity to filter by; must not be {@code null}
   * @param from the inclusive start of the time window; must not be {@code null}
   * @param to the inclusive end of the time window; must not be {@code null}
   * @param pageRequest the page to return; sort component is ignored; must not be {@code null}
   * @return the requested page ordered by {@code occurredAt} ascending; never {@code null}
   * @throws NullPointerException if any argument is {@code null}
   */
  default Page<EventAuditEntry> findByUserId(
      UserId userId, Instant from, Instant to, PageRequest pageRequest) {
    return slice(findByUserId(userId, from, to), pageRequest);
  }

  /**
   * Reconstructs the full downstream causal chain starting from the given event, following
   * causation links to find all events that were directly or transitively caused by it.
   *
   * <p>The returned list includes the event identified by {@code eventId} plus all descendants
   * reachable via {@link EventAuditEntry#causationId()} links. Returns an empty list if no event
   * with the given ID exists.
   *
   * <p><b>Unbounded result.</b> The list grows with the size of the causal subtree and is not
   * paged. Causal chains are bounded by real causality (typically small), but a saga or projector
   * feedback loop can make them large — callers tracing events in such topologies should bound the
   * window at the application level.
   *
   * @param eventId the root event to start from; must not be {@code null}
   * @return root event plus all descendants ordered by {@code occurredAt} ascending; never {@code
   *     null}
   * @throws NullPointerException if {@code eventId} is {@code null}
   */
  List<EventAuditEntry> traceFullCausationChain(EventId eventId);

  private static Page<EventAuditEntry> slice(List<EventAuditEntry> all, PageRequest pageRequest) {
    Objects.requireNonNull(pageRequest, "pageRequest is required");
    // long arithmetic: page * size may exceed Integer.MAX_VALUE before clamping.
    int from = (int) Math.min((long) pageRequest.page() * pageRequest.size(), all.size());
    int to = Math.min(from + pageRequest.size(), all.size());
    return new Page<>(all.subList(from, to), all.size(), pageRequest.page(), pageRequest.size());
  }
}
