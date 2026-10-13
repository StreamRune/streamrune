package org.streamrune.runtime;

/**
 * {@link SagaRunnerHoldTest} with correlated events that carry the saga's origin, and an
 * orchestrator that routes nothing by event data: the origin alone brings each event to its saga.
 */
class SagaRunnerHoldByOriginTest extends SagaRunnerHoldTest {

  SagaRunnerHoldByOriginTest() {
    super(Route.ORIGIN);
  }
}
