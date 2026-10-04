package org.streamrune.spring.nativehintsfixture;

/** Fixture value object nested inside {@link SampleEvent} records. */
public record SampleMoney(long amount, String currency) {}
