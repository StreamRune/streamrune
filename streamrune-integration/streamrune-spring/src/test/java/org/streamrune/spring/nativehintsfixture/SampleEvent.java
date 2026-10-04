package org.streamrune.spring.nativehintsfixture;

/**
 * Fixture sealed event hierarchy for {@code StreamRuneRuntimeHints} native-hints scanning tests.
 * Mirrors a real domain event: nested record subtypes, one carrying a value object.
 */
public sealed interface SampleEvent {
  record Created(String id, SampleMoney price) implements SampleEvent {}

  record Renamed(String id, String name) implements SampleEvent {}
}
