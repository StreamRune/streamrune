package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * The contract every {@link OffsetStore} must honour. Run by {@code
 * InMemoryOffsetStoreContractTest} and {@code PostgresOffsetStoreContractTest}; extend it and
 * implement {@link #newStore()} to prove a custom store.
 *
 * <p>The promises, as the {@link OffsetStore} javadoc states them:
 *
 * <ol>
 *   <li>A name that never had an offset stored reads {@link GlobalOffset#initial()}.
 *   <li>A saved offset is read back; a later, greater offset replaces it.
 *   <li>Offsets are kept per name: a save or a reset under one name never moves another.
 *   <li>{@link OffsetStore#reset} rewinds a name to {@link GlobalOffset#initial()}, also from an
 *       offset the monotonic guard would never let {@code saveOffset} move back from, and forward
 *       saves resume after it. Every store implements it: the {@code UnsupportedOperationException}
 *       of the interface's default is not a permitted answer.
 *   <li>The monotonic guard: a save that would move a name's offset backward or leave it where it
 *       is changes nothing and throws nothing. The shipped stores apply it; a store without it
 *       overrides {@link #guardsAgainstRegression()} to return {@code false}, and the guard's cases
 *       are skipped.
 * </ol>
 */
public abstract class OffsetStoreContract {

  /** A fresh, empty store for each test. */
  protected abstract OffsetStore newStore();

  /**
   * Whether the store applies the monotonic guard of the {@link OffsetStore} javadoc: {@code true}
   * by default, as every shipped store does. Override it to return {@code false} for a store
   * without the guard.
   */
  protected boolean guardsAgainstRegression() {
    return true;
  }

  protected static final ProjectionName ORDERS = ProjectionName.of("contract-orders");
  protected static final ProjectionName CUSTOMERS = ProjectionName.of("contract-customers");

  protected OffsetStore store;

  @BeforeEach
  void freshStore() {
    store = newStore();
  }

  @Test
  public void anUnknownNameReadsTheInitialOffset() {
    assertEquals(GlobalOffset.initial(), store.getLastOffset(ORDERS));
  }

  @Test
  public void aSavedOffsetIsReadBack() {
    store.saveOffset(ORDERS, GlobalOffset.of(42));

    assertEquals(GlobalOffset.of(42), store.getLastOffset(ORDERS));
  }

  @Test
  public void aGreaterOffsetReplacesTheSavedOne() {
    store.saveOffset(ORDERS, GlobalOffset.of(10));
    store.saveOffset(ORDERS, GlobalOffset.of(50));

    assertEquals(GlobalOffset.of(50), store.getLastOffset(ORDERS));
  }

  @Test
  public void aFirstSaveOfTheInitialOffsetReadsTheInitialOffset() {
    store.saveOffset(ORDERS, GlobalOffset.initial());

    assertEquals(GlobalOffset.initial(), store.getLastOffset(ORDERS));
    store.saveOffset(ORDERS, GlobalOffset.of(1));
    assertEquals(GlobalOffset.of(1), store.getLastOffset(ORDERS));
  }

  @Test
  public void offsetsAreKeptPerName() {
    store.saveOffset(ORDERS, GlobalOffset.of(100));
    store.saveOffset(CUSTOMERS, GlobalOffset.of(200));

    assertEquals(GlobalOffset.of(100), store.getLastOffset(ORDERS));
    assertEquals(GlobalOffset.of(200), store.getLastOffset(CUSTOMERS));
  }

  @Test
  public void resetRewindsToTheInitialOffset() {
    store.saveOffset(ORDERS, GlobalOffset.of(500));

    store.reset(ORDERS);

    assertEquals(GlobalOffset.initial(), store.getLastOffset(ORDERS));
  }

  @Test
  public void forwardSavesResumeAfterAReset() {
    store.saveOffset(ORDERS, GlobalOffset.of(500));
    store.reset(ORDERS);

    store.saveOffset(ORDERS, GlobalOffset.of(10));

    assertEquals(GlobalOffset.of(10), store.getLastOffset(ORDERS));
  }

  @Test
  public void resetOfAnUnknownNameReadsTheInitialOffset() {
    store.reset(ORDERS);

    assertEquals(GlobalOffset.initial(), store.getLastOffset(ORDERS));
  }

  @Test
  public void resetLeavesOtherNamesAlone() {
    store.saveOffset(ORDERS, GlobalOffset.of(7));
    store.saveOffset(CUSTOMERS, GlobalOffset.of(9));

    store.reset(ORDERS);

    assertEquals(GlobalOffset.of(9), store.getLastOffset(CUSTOMERS));
  }

  @Test
  public void aBackwardSaveChangesNothing() {
    assumeTrue(guardsAgainstRegression(), "the store applies no monotonic guard");
    store.saveOffset(ORDERS, GlobalOffset.of(300));

    store.saveOffset(ORDERS, GlobalOffset.of(200));

    assertEquals(GlobalOffset.of(300), store.getLastOffset(ORDERS));
    store.saveOffset(ORDERS, GlobalOffset.of(301));
    assertEquals(GlobalOffset.of(301), store.getLastOffset(ORDERS), "forward saves still advance");
  }

  @Test
  public void aSidewaysSaveChangesNothing() {
    assumeTrue(guardsAgainstRegression(), "the store applies no monotonic guard");
    store.saveOffset(ORDERS, GlobalOffset.of(300));

    store.saveOffset(ORDERS, GlobalOffset.of(300));

    assertEquals(GlobalOffset.of(300), store.getLastOffset(ORDERS));
  }

  @Test
  public void aSaveOfTheInitialOffsetDoesNotRewind() {
    assumeTrue(guardsAgainstRegression(), "the store applies no monotonic guard");
    store.saveOffset(ORDERS, GlobalOffset.of(5));

    store.saveOffset(ORDERS, GlobalOffset.initial());

    assertEquals(
        GlobalOffset.of(5), store.getLastOffset(ORDERS), "only reset rewinds a guarded store");
  }
}
