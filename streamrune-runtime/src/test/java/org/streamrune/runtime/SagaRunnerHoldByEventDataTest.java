package org.streamrune.runtime;

/**
 * {@link SagaRunnerHoldTest} with correlated events that carry no saga origin: the orchestrator's
 * {@code correlateByEventData} alone brings each event to its saga.
 */
class SagaRunnerHoldByEventDataTest extends SagaRunnerHoldTest {

  SagaRunnerHoldByEventDataTest() {
    super(Route.EVENT_DATA);
  }
}
