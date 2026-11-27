package org.streamrune.runtime.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import org.streamrune.core.Internal;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.LogSanitizer;

/**
 * The rules both outbox stores apply around their statements, and the decoding of the rows they
 * read: the argument preconditions, the strict channel's refusal of an entry without a stream, the
 * binding of an entry's trace link, the refusal of a delete of a {@code FAILED} entry, and the
 * mapping of an {@code outbox_events} row to {@link OutboxEntry}.
 *
 * <p>It holds no statement: each store keeps its own SQL, binds, transaction and lock scope, claim
 * order and clock. The binder and the row mapper take the caller's {@link PreparedStatement} or
 * {@link ResultSet}, read every column by name, read the timestamp columns through the backend's
 * {@link JdbcInstantCodec} and let every {@link SQLException} propagate unwrapped. The delete
 * refusal is returned, not thrown, so the store throws it where it throws it today. The helper
 * never opens, commits, rolls back or closes a connection, never decides a lock, does not log and
 * keeps no state.
 *
 * @hidden
 */
@Internal
public final class OutboxRules {

  private OutboxRules() {}

  /**
   * A strict channel refuses an entry without a stream before any write, unwrapped.
   *
   * @param mode the channel's ordering mode
   * @param entry the entry to save, not {@code null}
   * @throws OutboxOrderingViolationException if {@code mode} is {@link
   *     OutboxOrderingMode#STRICT_PER_AGGREGATE} and the entry has no stream
   */
  public static void requireStreamIfStrict(OutboxOrderingMode mode, OutboxEntry entry) {
    if (mode == OutboxOrderingMode.STRICT_PER_AGGREGATE && entry.streamId() == null) {
      throw new OutboxOrderingViolationException(entry.id(), entry.payloadType());
    }
  }

  /**
   * Binds {@code link} to the {@code trace_id} and {@code span_id} parameters at {@code index} and
   * {@code index + 1}: both ids when they are valid W3C ids, two {@code NULL}s for {@link
   * SpanLink#NONE} or ids that are not valid, so the columns hold only ids a tracing library
   * accepts.
   *
   * @param ps the caller's statement
   * @param index the parameter index of {@code trace_id}
   * @param link the entry's trace link
   * @throws SQLException if a bind fails
   */
  public static void bindTraceLink(PreparedStatement ps, int index, SpanLink link)
      throws SQLException {
    boolean valid = link.isPresent() && link.isW3c();
    ps.setString(index, valid ? link.traceId().value() : null);
    ps.setString(index + 1, valid ? link.spanId().value() : null);
  }

  /**
   * The id precondition of every operation that takes an entry id.
   *
   * @param id the entry id
   * @throws IllegalArgumentException if {@code id} is {@code null}
   */
  public static void requireId(OutboxEntryId id) {
    if (id == null) {
      throw new IllegalArgumentException("id is required");
    }
  }

  /**
   * The preconditions of a store's options, the lease checked before the mode. The store checks its
   * own connection source first.
   *
   * @param claimLease how long a claim is honored
   * @param mode the channel's ordering mode
   * @throws IllegalArgumentException if {@code claimLease} is {@code null}, zero or negative, or
   *     {@code mode} is {@code null}
   */
  public static void requireOptions(Duration claimLease, OutboxOrderingMode mode) {
    if (claimLease == null || claimLease.isZero() || claimLease.isNegative()) {
      throw new IllegalArgumentException("claimLease must be positive");
    }
    if (mode == null) {
      throw new IllegalArgumentException("orderingMode is required");
    }
  }

  /**
   * The precondition of a read or claim that takes a row limit.
   *
   * @param limit the most rows to return
   * @throws IllegalArgumentException if {@code limit} is below 1
   */
  public static void requireClaimLimit(int limit) {
    if (limit < 1) {
      throw new IllegalArgumentException("limit must be >= 1");
    }
  }

  /**
   * The refusal of a delete that found the entry {@code FAILED}: its only exits are the audited
   * skip and replay, and a raw delete of a blocking head would release its aggregate with no
   * record. The id is caller text, so it is sanitized.
   *
   * @param id the entry id
   * @return the exception to throw
   */
  public static IllegalStateException deleteRefusal(OutboxEntryId id) {
    return new IllegalStateException(
        "outbox entry "
            + LogSanitizer.sanitizeForLog(id.value())
            + " is FAILED; resolve it with OutboxFailedReplayer.skip or .replay — delete would"
            + " release its aggregate with no record");
  }

  /**
   * The one row mapper of every outbox read: each store's entry columns and both claim statements'
   * result columns name the same columns, and every column is read by name. A {@code claimed} row
   * reports a {@code null} {@code processedAt}: a claim leaves the column untouched, and a replayed
   * row still carries the stamp of its earlier {@code FAILED}, which is not this claim's outcome.
   * The skip audit is read iff the row is {@code SKIPPED} (enforced by the {@link OutboxEntry}
   * invariant). {@code NULL} trace columns, or a stored trace id or span id that is not a valid W3C
   * id, read back as {@link SpanLink#NONE} rather than an exception, so a corrupt value never stops
   * the entry's delivery.
   *
   * <p>{@code created_at} and {@code skipped_at} are read through {@link
   * JdbcInstantCodec#readInstant}, {@code processed_at} and {@code next_retry_at} through {@link
   * JdbcInstantCodec#readNullableInstant}; what an unexpected SQL {@code NULL} in a required column
   * becomes is the codec's decision.
   *
   * @param rs the caller's result set, positioned on a row
   * @param codec the backend's timestamp decoding
   * @param claimed whether the row was read by a claim
   * @return the entry
   * @throws SQLException if a read fails
   * @throws IllegalArgumentException if the status column holds no {@link OutboxStatus}, or a
   *     stored value breaks an invariant of {@link OutboxEntry}
   */
  public static OutboxEntry mapRow(ResultSet rs, JdbcInstantCodec codec, boolean claimed)
      throws SQLException {
    var status = OutboxStatus.valueOf(rs.getString("status"));
    OutboxEntry.SkipRecord skip = null;
    if (status == OutboxStatus.SKIPPED) {
      skip =
          new OutboxEntry.SkipRecord(
              codec.readInstant(rs, "skipped_at"),
              rs.getString("skipped_by"),
              rs.getString("skip_reason"));
    }
    return new OutboxEntry(
        OutboxEntryId.of(rs.getString("entry_id")),
        rs.getString("payload"),
        rs.getString("payload_type"),
        // The decode door — the lenient constructors, never the ingress factories — so a stored
        // stream carrying a control character still reads back.
        StreamKeys.read(rs),
        status,
        rs.getInt("attempts"),
        rs.getString("last_error"),
        codec.readInstant(rs, "created_at"),
        claimed ? null : codec.readNullableInstant(rs, "processed_at"),
        codec.readNullableInstant(rs, "next_retry_at"),
        skip,
        SpanLink.ofW3c(rs.getString("trace_id"), rs.getString("span_id")));
  }
}
