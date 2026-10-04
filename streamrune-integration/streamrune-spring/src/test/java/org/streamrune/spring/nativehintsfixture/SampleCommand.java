package org.streamrune.spring.nativehintsfixture;

/**
 * Fixture sealed command hierarchy with a nested sealed level, for the native-hints scanning tests:
 * a native image lists a sealed type's permitted subclasses only when the sealed type itself is
 * registered for reflection, at every level.
 */
public sealed interface SampleCommand {
  record Place(String id) implements SampleCommand {}

  sealed interface Refund extends SampleCommand {
    record Full(String id) implements Refund {}
  }
}
