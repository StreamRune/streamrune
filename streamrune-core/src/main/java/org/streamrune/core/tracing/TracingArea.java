package org.streamrune.core.tracing;

/**
 * A group of spans that is switched on and off together. Every span StreamRune emits belongs to
 * exactly one area, and {@link StreamRuneTracing#enabled(TracingArea)} answers per area.
 */
public enum TracingArea {
  /** The span of one command execution, {@code command/{CommandType}}. */
  COMMAND,
  /** The span of one subscription delivery batch, {@code subscription/{name}/deliver}. */
  SUBSCRIPTION,
  /** The span of one projection batch, {@code projection/{name}/process}. */
  PROJECTION,
  /** Event store appends, loads, reads and snapshots. */
  EVENT_STORE,
  /** Aggregate lock acquisition. */
  LOCK,
  /** The outbox relay and the publish of each outbox entry. */
  OUTBOX,
  /** Saga steps, compensations and timeouts. */
  SAGA
}
