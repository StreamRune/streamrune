package org.streamrune.core.projection;

/**
 * Emission produced by a windowed projection.
 *
 * @param window the time window
 * @param state the accumulated state at emission time
 * @param isFinal true on close (watermark passed end+grace); false for late re-emissions
 */
public record WindowResult<W>(Window window, W state, boolean isFinal) {

  public WindowResult {
    if (window == null) {
      throw new IllegalArgumentException("window is required");
    }
  }
}
