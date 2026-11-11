/**
 * The tracing seam: how StreamRune components emit spans without depending on a tracing library.
 *
 * <p>A component holds a {@link org.streamrune.core.tracing.StreamRuneTracing} and asks it, per
 * {@link org.streamrune.core.tracing.TracingArea}, whether that area is traced before it builds any
 * attribute. {@link org.streamrune.core.tracing.StreamRuneTracing#NOOP} answers {@code false} for
 * every area, so a component without tracing pays one branch and allocates nothing. {@code
 * org.streamrune.runtime.OpenTelemetryTracing} implements the seam with the OpenTelemetry API; it
 * is the only implementation the framework ships, and no type of this package references
 * OpenTelemetry.
 */
package org.streamrune.core.tracing;
