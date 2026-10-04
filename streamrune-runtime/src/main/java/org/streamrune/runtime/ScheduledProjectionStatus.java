package org.streamrune.runtime;

import java.time.Instant;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Snapshot of a scheduled projection.
 *
 * @param name projection name
 * @param state current state
 * @param lastOffset last advanced offset
 * @param lastFireAt instant of last cron fire (null before first fire)
 * @param nextFireAt instant of next cron fire
 * @param batchesProcessedLastFire number of batches processed during the last fire
 * @param lastError exception message if state is ERROR (null otherwise)
 */
public record ScheduledProjectionStatus(
    ProjectionName name,
    ScheduledProjectionState state,
    GlobalOffset lastOffset,
    Instant lastFireAt,
    Instant nextFireAt,
    long batchesProcessedLastFire,
    String lastError) {}
