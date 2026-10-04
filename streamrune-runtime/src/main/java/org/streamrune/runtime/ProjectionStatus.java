package org.streamrune.runtime;

import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Snapshot of a projection's current state, offset, and last error.
 *
 * @param projectionName the projection name
 * @param state current lifecycle state
 * @param lastOffset last successfully processed global offset
 * @param lastError last error message, or null if no error
 */
public record ProjectionStatus(
    ProjectionName projectionName,
    ProjectionState state,
    GlobalOffset lastOffset,
    String lastError) {}
