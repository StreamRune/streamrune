package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/** The refusal of two leadership beans that Spring and Quarkus share. */
class SubscriptionLeadershipResolutionTest {

  @Test
  void theRefusalNamesEveryCandidateAndHowToChooseOne() {
    IllegalStateException refusal =
        SubscriptionLeadershipResolution.ambiguous(List.of("com.acme.A", "com.acme.B"), "@Primary");

    assertThat(refusal)
        .hasMessage(
            "2 SubscriptionLeadership beans and none is the default (com.acme.A, com.acme.B):"
                + " StreamRune threads one leadership into every projection runner, the"
                + " dead-letter retry runner and the saga sweepers, and does not pick one, because"
                + " two leaderships let two replicas lead the same consumer. Keep one, or mark the"
                + " one to use @Primary.");
  }
}
