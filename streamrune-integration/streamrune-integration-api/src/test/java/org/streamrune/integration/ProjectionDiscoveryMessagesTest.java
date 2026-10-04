package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

class ProjectionDiscoveryMessagesTest {

  private static final String FORGED = "orders\n2026-10-04 WARN forged\r\u0085\u202e";

  @Test
  void ordinaryNamesKeepTheMessagesReadable() {
    assertEquals(
        "Duplicate @ProjectionConfig.name: 'orders'",
        ProjectionDiscoveryMessages.duplicateName("orders"));
    assertEquals(
        "@ProjectionConfig.cron is required for SCHEDULED mode (projection 'orders')",
        ProjectionDiscoveryMessages.cronRequired("orders"));
    assertEquals(
        "Invalid error-strategy 'shrug' for projection 'orders'",
        ProjectionDiscoveryMessages.invalidErrorStrategy("shrug", "orders"));
  }

  @Test
  void aForgedNameIsSanitizedInEveryMessage() {
    assertClean(ProjectionDiscoveryMessages.duplicateName(FORGED));
    assertClean(ProjectionDiscoveryMessages.cronRequired(FORGED));
    assertClean(ProjectionDiscoveryMessages.invalidErrorStrategy("ok", FORGED));
  }

  @Test
  void aForgedStrategyValueIsSanitizedAsFreeText() {
    String message = ProjectionDiscoveryMessages.invalidErrorStrategy("halt\nforged", "p");

    assertClean(message);
    assertEquals("Invalid error-strategy 'halt forged' for projection 'p'", message);
  }

  private static void assertClean(String message) {
    assertFalse(message.chars().anyMatch(Character::isISOControl), message);
    assertFalse(message.contains("\u202e"), message);
  }
}
