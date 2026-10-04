package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.Versioned;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;

/** Integration tests for JdbcProjectionRepository. */
class JdbcProjectionRepositoryTest {

  static PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_proj_test");

  static org.postgresql.ds.PGSimpleDataSource dataSource;
  static JdbcProjectionRepository repo;

  record TestView(String id, String data) {}

  @BeforeAll
  static void setup() {
    PG.start();

    dataSource = new org.postgresql.ds.PGSimpleDataSource();
    dataSource.setURL(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // The shipped event-store baseline, applied the way the factory applies it: executeAtomically
    // locks and advances the projection_offset row.
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();

    // Create repository directly without factory (for testing purposes)
    repo = new JdbcProjectionRepository(dataSource);
  }

  @AfterAll
  static void teardown() {
    PG.stop();
  }

  @Test
  void shouldSaveAndFindById() {
    repo.save(ProjectionName.of("test"), "id-1", new TestView("id-1", "data-1"));

    Optional<TestView> found = repo.findById(ProjectionName.of("test"), "id-1", TestView.class);
    assertTrue(found.isPresent());
    assertEquals("id-1", found.get().id());
    assertEquals("data-1", found.get().data());
  }

  @Test
  void shouldUpsertOnSave() {
    repo.save(ProjectionName.of("test"), "id-1", new TestView("id-1", "data-1"));
    repo.save(ProjectionName.of("test"), "id-1", new TestView("id-1", "updated-data"));

    var found = repo.findById(ProjectionName.of("test"), "id-1", TestView.class);
    assertTrue(found.isPresent());
    assertEquals("updated-data", found.get().data());
  }

  @Test
  void shouldFindAll() {
    repo.save(ProjectionName.of("test"), "id-1", new TestView("id-1", "data-1"));
    repo.save(ProjectionName.of("test"), "id-2", new TestView("id-2", "data-2"));

    List<TestView> all = repo.findAll(ProjectionName.of("test"), TestView.class);
    assertEquals(2, all.size());
  }

  @Test
  void shouldFindAllAcrossDriverFetchBatches() throws Exception {
    // 1100 rows > 2x the driver fetch size (500): findAll streams the result set in batches
    // inside an explicit transaction. Every row must arrive.
    repo.save(
        ProjectionName.of("bulk"), "seed", new TestView("seed", "seed-data")); // creates the table
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "INSERT INTO bulk_view (id, data, version) "
              + "SELECT 'row-' || i, jsonb_build_object('id', 'row-' || i, 'data', 'd' || i), 0 "
              + "FROM generate_series(1, 1100) AS i");
    }

    List<TestView> all = repo.findAll(ProjectionName.of("bulk"), TestView.class);

    assertEquals(1101, all.size());
  }

  @Test
  void shouldReturnEmptyListWhenNoData() {
    List<TestView> all = repo.findAll(ProjectionName.of("empty_projection"), TestView.class);
    assertTrue(all.isEmpty());
  }

  @Test
  void shouldDelete() {
    repo.save(ProjectionName.of("test"), "id-1", new TestView("id-1", "data-1"));
    repo.delete(ProjectionName.of("test"), "id-1");

    assertTrue(repo.findById(ProjectionName.of("test"), "id-1", TestView.class).isEmpty());
  }

  @Test
  void shouldHandleMultipleProjections() {
    // First projection
    repo.save(ProjectionName.of("orders"), "order-1", new TestView("order-1", "order-data"));
    // Second projection (separate table)
    repo.save(
        ProjectionName.of("products"), "product-1", new TestView("product-1", "product-data"));

    assertTrue(repo.findById(ProjectionName.of("orders"), "order-1", TestView.class).isPresent());
    assertTrue(
        repo.findById(ProjectionName.of("products"), "product-1", TestView.class).isPresent());
    assertTrue(repo.findById(ProjectionName.of("orders"), "product-1", TestView.class).isEmpty());
    assertTrue(repo.findById(ProjectionName.of("products"), "order-1", TestView.class).isEmpty());
  }

  @Test
  void shouldRejectNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new JdbcProjectionRepository(null));
  }

  @Test
  void shouldRejectNullObjectMapper() {
    var ds = new org.postgresql.ds.PGSimpleDataSource();
    ds.setURL(PG.getJdbcUrl());
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    assertThrows(IllegalArgumentException.class, () -> new JdbcProjectionRepository(ds, null));
  }

  @Test
  void shouldRejectBlankId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.save(ProjectionName.of("test"), "  ", new TestView("id", "data")));
  }

  @Test
  void blankProjectionNameIsRejectedByProjectionNameBeforeTheRepositorySeesIt() {
    // The repository never sees a blank name: ProjectionName's own constructor rejects a blank
    // value, so save, findById and findAll cannot be called with one.
    assertThrows(IllegalArgumentException.class, () -> ProjectionName.of("  "));
  }

  @Test
  void shouldRejectNullTypeInFindById() {
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.findById(ProjectionName.of("test"), "id-1", null));
  }

  @Test
  void shouldRejectNullTypeInFindAll() {
    assertThrows(
        IllegalArgumentException.class, () -> repo.findAll(ProjectionName.of("test"), null));
  }

  @Test
  void shouldRejectNullIdInFindById() {
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.findById(ProjectionName.of("test"), null, TestView.class));
  }

  @Test
  void shouldRejectBlankIdInDelete() {
    assertThrows(
        IllegalArgumentException.class, () -> repo.delete(ProjectionName.of("test"), "  "));
  }

  @Test
  void shouldRejectNullIdInDelete() {
    assertThrows(
        IllegalArgumentException.class, () -> repo.delete(ProjectionName.of("test"), null));
  }

  @Test
  void shouldRejectNullProjectionNameInFindById() {
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.findById((ProjectionName) null, "id-1", TestView.class));
  }

  @Test
  void shouldRejectNullProjectionNameInFindAll() {
    assertThrows(
        IllegalArgumentException.class, () -> repo.findAll((ProjectionName) null, TestView.class));
  }

  @Test
  void findAll_paginated_firstPage() {
    for (int i = 1; i <= 5; i++) {
      repo.save(ProjectionName.of("paged"), "id-" + i, new TestView("id-" + i, "data-" + i));
    }

    Page<TestView> page =
        repo.findAll(ProjectionName.of("paged"), TestView.class, PageRequest.of(0, 2));
    assertEquals(2, page.content().size());
    assertEquals(5, page.totalElements());
    assertTrue(page.hasNext());
    assertFalse(page.hasPrevious());
  }

  @Test
  void findAll_paginated_lastPage() {
    for (int i = 1; i <= 5; i++) {
      repo.save(ProjectionName.of("paged2"), "id-" + i, new TestView("id-" + i, "data-" + i));
    }

    Page<TestView> page =
        repo.findAll(ProjectionName.of("paged2"), TestView.class, PageRequest.of(2, 2));
    assertEquals(1, page.content().size());
    assertEquals(5, page.totalElements());
    assertFalse(page.hasNext());
    assertTrue(page.hasPrevious());
  }

  @Test
  void findAll_paginated_emptyProjection() {
    Page<TestView> page =
        repo.findAll(ProjectionName.of("empty_paged"), TestView.class, PageRequest.of(0, 10));
    assertTrue(page.isEmpty());
    assertEquals(0, page.totalElements());
  }

  @Test
  void findAll_paginated_sortById() {
    repo.save(ProjectionName.of("sorted"), "b", new TestView("b", "second"));
    repo.save(ProjectionName.of("sorted"), "a", new TestView("a", "first"));
    repo.save(ProjectionName.of("sorted"), "c", new TestView("c", "third"));

    Page<TestView> page =
        repo.findAll(
            ProjectionName.of("sorted"), TestView.class, PageRequest.of(0, 10, Sort.by("id")));
    assertEquals(3, page.content().size());
    assertEquals("a", page.content().get(0).id());
    assertEquals("b", page.content().get(1).id());
    assertEquals("c", page.content().get(2).id());
  }

  @Test
  void findAll_paginated_sortByJsonField() {
    repo.save(ProjectionName.of("json_sorted"), "1", new TestView("1", "banana"));
    repo.save(ProjectionName.of("json_sorted"), "2", new TestView("2", "apple"));
    repo.save(ProjectionName.of("json_sorted"), "3", new TestView("3", "cherry"));

    Page<TestView> page =
        repo.findAll(
            ProjectionName.of("json_sorted"),
            TestView.class,
            PageRequest.of(0, 10, Sort.by("data", SortDirection.ASC)));
    assertEquals("apple", page.content().get(0).data());
    assertEquals("banana", page.content().get(1).data());
    assertEquals("cherry", page.content().get(2).data());
  }

  @Test
  void findAll_paginated_sortDescending() {
    repo.save(ProjectionName.of("desc_sorted"), "a", new TestView("a", "first"));
    repo.save(ProjectionName.of("desc_sorted"), "b", new TestView("b", "second"));
    repo.save(ProjectionName.of("desc_sorted"), "c", new TestView("c", "third"));

    Page<TestView> page =
        repo.findAll(
            ProjectionName.of("desc_sorted"),
            TestView.class,
            PageRequest.of(0, 10, Sort.by("id", SortDirection.DESC)));
    assertEquals("c", page.content().get(0).id());
    assertEquals("b", page.content().get(1).id());
    assertEquals("a", page.content().get(2).id());
  }

  @Test
  void findAll_paginated_rejectsNullPageRequest() {
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.findAll(ProjectionName.of("test"), TestView.class, null));
  }

  @Test
  void findAll_paginated_rejectsInvalidSortField() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            repo.findAll(
                ProjectionName.of("test"),
                TestView.class,
                PageRequest.of(0, 10, Sort.by("invalid;field"))));
  }

  @Test
  void findById_optimistic_returnsVersioned() {
    repo.save(ProjectionName.of("opt_lock"), "id-1", new TestView("id-1", "data-1"));

    Optional<Versioned<TestView>> found =
        repo.findById(ProjectionName.of("opt_lock"), "id-1", TestView.class, LockMode.OPTIMISTIC);
    assertTrue(found.isPresent());
    assertEquals("id-1", found.get().data().id());
    assertEquals(0, found.get().version());
  }

  @Test
  void findById_optimistic_versionIncrementsOnSave() {
    repo.save(ProjectionName.of("opt_ver"), "id-1", new TestView("id-1", "v1"));
    repo.save(ProjectionName.of("opt_ver"), "id-1", new TestView("id-1", "v2"));
    repo.save(ProjectionName.of("opt_ver"), "id-1", new TestView("id-1", "v3"));

    var found =
        repo.findById(ProjectionName.of("opt_ver"), "id-1", TestView.class, LockMode.OPTIMISTIC);
    assertTrue(found.isPresent());
    assertEquals("v3", found.get().data().data());
    assertEquals(2, found.get().version()); // 0 -> 1 -> 2
  }

  @Test
  void findById_optimistic_notFound() {
    var found =
        repo.findById(
            ProjectionName.of("opt_miss"), "nonexistent", TestView.class, LockMode.OPTIMISTIC);
    assertTrue(found.isEmpty());
  }

  @Test
  void findById_pessimistic_returnsVersioned() {
    repo.save(ProjectionName.of("pess_lock"), "id-1", new TestView("id-1", "data-1"));

    var found =
        repo.findById(ProjectionName.of("pess_lock"), "id-1", TestView.class, LockMode.PESSIMISTIC);
    assertTrue(found.isPresent());
    assertEquals("id-1", found.get().data().id());
    assertEquals(0, found.get().version());
  }

  @Test
  void findById_pessimistic_notFound() {
    var found =
        repo.findById(
            ProjectionName.of("pess_miss"), "nonexistent", TestView.class, LockMode.PESSIMISTIC);
    assertTrue(found.isEmpty());
  }

  @Test
  void findById_lockMode_rejectsNullLockMode() {
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.findById(ProjectionName.of("test"), "id-1", TestView.class, null));
  }

  @Test
  void save_versioned_succeeds() {
    repo.save(ProjectionName.of("ver_save"), "id-1", new TestView("id-1", "v1"));

    var found =
        repo.findById(ProjectionName.of("ver_save"), "id-1", TestView.class, LockMode.OPTIMISTIC);
    assertEquals(0, found.get().version());

    repo.save(ProjectionName.of("ver_save"), "id-1", new TestView("id-1", "v2"), 0);

    var updated =
        repo.findById(ProjectionName.of("ver_save"), "id-1", TestView.class, LockMode.OPTIMISTIC);
    assertEquals("v2", updated.get().data().data());
    assertEquals(1, updated.get().version());
  }

  @Test
  void save_versioned_throwsOnMismatch() {
    repo.save(ProjectionName.of("ver_conflict"), "id-1", new TestView("id-1", "v1"));

    assertThrows(
        OptimisticLockException.class,
        () -> repo.save(ProjectionName.of("ver_conflict"), "id-1", new TestView("id-1", "v2"), 99));
  }

  @Test
  void save_versioned_throwsOnNonexistentRow() {
    assertThrows(
        OptimisticLockException.class,
        () ->
            repo.save(ProjectionName.of("ver_missing"), "nonexistent", new TestView("x", "y"), 0));
  }

  @Test
  void save_versioned_concurrentModification() {
    repo.save(ProjectionName.of("ver_race"), "id-1", new TestView("id-1", "initial"));

    // Both readers see version 0
    var read1 =
        repo.findById(ProjectionName.of("ver_race"), "id-1", TestView.class, LockMode.OPTIMISTIC);
    var read2 =
        repo.findById(ProjectionName.of("ver_race"), "id-1", TestView.class, LockMode.OPTIMISTIC);
    assertEquals(0, read1.get().version());
    assertEquals(0, read2.get().version());

    // First writer succeeds
    repo.save(ProjectionName.of("ver_race"), "id-1", new TestView("id-1", "writer1"), 0);

    // Second writer fails — version is now 1, not 0
    assertThrows(
        OptimisticLockException.class,
        () -> repo.save(ProjectionName.of("ver_race"), "id-1", new TestView("id-1", "writer2"), 0));

    // Verify first writer's data persisted
    var result =
        repo.findById(ProjectionName.of("ver_race"), "id-1", TestView.class, LockMode.OPTIMISTIC);
    assertEquals("writer1", result.get().data().data());
    assertEquals(1, result.get().version());
  }

  // --- projection names map to tables one to one, exactly as given ---

  /**
   * The reserved-word check ran on the name BEFORE {@code _view} was appended, so a projection
   * named {@code order} (or user, group, table, ...) threw on every save/find and halted on its
   * first event, although {@code order_view} is no keyword. The check is gone.
   */
  @Test
  void aProjectionNamedLikeASqlKeywordRoundTrips_d10() {
    for (String name : List.of("order", "user", "group", "table", "check", "window", "grant")) {
      var pn = ProjectionName.of(name);
      repo.save(pn, "id-1", new TestView("id-1", name));
      assertEquals(name, repo.findById(pn, "id-1", TestView.class).orElseThrow().data(), name);
      assertEquals(1, repo.findAll(pn, TestView.class).size(), name);
    }
  }

  /** Saves under the name BaseProjection derives from its class name: {@code order}. */
  static final class OrderProjection extends BaseProjection {
    OrderProjection(ProjectionRepository repository) {
      super(repository);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      save("derived-1", new TestView("derived-1", "order"));
    }
  }

  /** Saves under the name BaseProjection derives from its class name: {@code cart_view}. */
  static final class CartViewProjection extends BaseProjection {
    CartViewProjection(ProjectionRepository repository) {
      super(repository);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      save("derived-1", new TestView("derived-1", "cart view"));
    }
  }

  /**
   * D10 as the triage states it: {@code class OrderProjection extends BaseProjection} with the
   * default constructor halted on its first event, because the derived name {@code Order} was
   * rejected. The derived name is now snake_case ({@code order}, {@code cart_view}), so a
   * projection that never names itself round-trips.
   */
  @Test
  void aBaseProjectionWithTheDerivedDefaultNameRoundTrips_d10() {
    var order = ProjectionName.of("order");
    var cartView = ProjectionName.of("cart_view");
    try {
      new OrderProjection(repo).process(List.of());
      new CartViewProjection(repo).process(List.of());

      assertEquals("order", repo.findById(order, "derived-1", TestView.class).orElseThrow().data());
      assertEquals(
          "cart view", repo.findById(cartView, "derived-1", TestView.class).orElseThrow().data());
    } finally {
      // order_view is shared with aProjectionNamedLikeASqlKeywordRoundTrips_d10, which counts rows.
      repo.delete(order, "derived-1");
      repo.delete(cartView, "derived-1");
    }
  }

  /**
   * A name that is not {@code [a-z_][a-z0-9_]{0,57}} is rejected with a message naming the rule —
   * never rewritten. The old sanitizer turned {@code d11-reject} into the table {@code
   * d11_reject_view}, and PostgreSQL folded {@code D11_Reject_view} to the same table; nothing may
   * be created under that name now.
   */
  @Test
  void aNameOutsideTheRuleIsRejected_neverRewritten_d11() throws Exception {
    for (String name :
        List.of(
            "d11-reject",
            "d11.reject",
            "d11 reject",
            "D11_Reject",
            "D11Reject",
            "1d11_reject",
            "d11_rejé",
            "p" + "x".repeat(58))) {
      var pn = ProjectionName.of(name);
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> repo.save(pn, "id-1", new TestView("id-1", "x")),
              name);
      assertTrue(ex.getMessage().contains("[a-z_][a-z0-9_]{0,57}"), ex.getMessage());
    }
    assertFalse(tableExists("d11_reject_view"), "no table under the old rewritten/folded name");
  }

  /** Every entry point applies the rule, before it touches the database. */
  @Test
  void everyEntryPointRejectsANameOutsideTheRule_d11() {
    var pn = ProjectionName.of("order-summary");
    var jdbc = (JdbcProjectionRepository) repo;
    var view = new TestView("id-1", "x");
    assertThrows(IllegalArgumentException.class, () -> repo.save(pn, "id-1", view));
    assertThrows(IllegalArgumentException.class, () -> repo.save(pn, "id-1", view, 0L));
    assertThrows(IllegalArgumentException.class, () -> repo.findById(pn, "id-1", TestView.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.findById(pn, "id-1", TestView.class, LockMode.OPTIMISTIC));
    assertThrows(IllegalArgumentException.class, () -> repo.findAll(pn, TestView.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> repo.findAll(pn, TestView.class, PageRequest.of(0, 10)));
    assertThrows(IllegalArgumentException.class, () -> repo.delete(pn, "id-1"));
    var updaterRan = new java.util.concurrent.atomic.AtomicBoolean();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            jdbc.executeAtomically(
                pn,
                List.of(),
                org.streamrune.core.types.GlobalOffset.of(1),
                0L,
                r -> updaterRan.set(true),
                null));
    assertFalse(updaterRan.get(), "rejected before the transaction began");
  }

  /**
   * The names the old mapping sent to one table. {@code order_summary} is valid; each of its former
   * aliases is rejected, so none can overwrite its rows. Two different valid names always give two
   * different tables — the name is used exactly as given.
   */
  @Test
  void twoNamesCanNeverShareATable_d11() {
    repo.save(ProjectionName.of("order_summary"), "id-1", new TestView("id-1", "mine"));
    for (String alias :
        List.of("order-summary", "order.summary", "Order_Summary", "ORDER_SUMMARY")) {
      assertThrows(
          IllegalArgumentException.class,
          () -> repo.save(ProjectionName.of(alias), "id-1", new TestView("id-1", "foreign")),
          alias);
    }
    assertEquals(
        "mine",
        repo.findById(ProjectionName.of("order_summary"), "id-1", TestView.class)
            .orElseThrow()
            .data());
  }

  /**
   * The longest valid name has 58 characters, so its table name has exactly 63 bytes — PostgreSQL's
   * identifier limit — and is stored untruncated. One more character is rejected: PostgreSQL used
   * to truncate such a table name, so two long names sharing a 63-byte prefix shared one table.
   */
  @Test
  void theLongestValidNameKeepsItsWholeTableName_oneCharacterMoreIsRejected_d11() throws Exception {
    String longest = "p" + "x".repeat(57);
    repo.save(ProjectionName.of(longest), "id-1", new TestView("id-1", "long"));
    assertEquals(
        "long",
        repo.findById(ProjectionName.of(longest), "id-1", TestView.class).orElseThrow().data());
    assertTrue(tableExists(longest + "_view"), "the 63-byte table name is not truncated");

    assertThrows(
        IllegalArgumentException.class,
        () -> repo.save(ProjectionName.of(longest + "y"), "id-1", new TestView("id-1", "x")));
  }

  /**
   * D11 registration-time check ({@code AtomicBatchProcessor.checkProjectionName}): the runners
   * call it for every registration before reading an event, so a name this repository cannot store
   * under fails at startup instead of on the projection's first batch.
   */
  @Test
  void checkProjectionName_acceptsAValidName_rejectsAnyOther_d11() {
    var jdbc = (JdbcProjectionRepository) repo;
    jdbc.checkProjectionName(ProjectionName.of("order_summary"));
    jdbc.checkProjectionName(ProjectionName.of("_orders2"));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> jdbc.checkProjectionName(ProjectionName.of("order-summary")));
    assertTrue(ex.getMessage().contains("order-summary"), ex.getMessage());
    assertTrue(ex.getMessage().contains("[a-z_][a-z0-9_]{0,57}"), ex.getMessage());
    assertThrows(IllegalArgumentException.class, () -> jdbc.checkProjectionName(null));
  }

  @Test
  void writeTargetIdentity_isTheDataSourceAndTheObjectMapper_byReference() {
    var mapper = new ObjectMapper();
    var a = new JdbcProjectionRepository(dataSource, mapper);
    var b = new JdbcProjectionRepository(dataSource, mapper);
    assertEquals(a.writeTargetIdentity(), b.writeTargetIdentity(), "same DataSource + same mapper");
    assertEquals(a.writeTargetIdentity().hashCode(), b.writeTargetIdentity().hashCode());
    assertNotEquals(
        a.writeTargetIdentity(),
        new JdbcProjectionRepository(dataSource, new ObjectMapper()).writeTargetIdentity(),
        "another ObjectMapper serialises differently (encryption at rest may differ)");
    var otherDs = new org.postgresql.ds.PGSimpleDataSource();
    otherDs.setUrl(dataSource.getUrl());
    otherDs.setUser(dataSource.getUser());
    otherDs.setPassword(dataSource.getPassword());
    assertNotEquals(
        a.writeTargetIdentity(),
        new JdbcProjectionRepository(otherDs, mapper).writeTargetIdentity(),
        "another DataSource object is another store");
    assertTrue(a.writesTo(b));
    assertTrue(a.writeTargetIdentity().toString().contains("JdbcProjectionRepository"));
  }

  @Test
  void transactionScopedRepository_reportsTheOuterIdentity() {
    var name = ProjectionName.of("identity_tx");
    var seen = new java.util.concurrent.atomic.AtomicReference<Object>();
    repo.executeAtomically(
        name, List.of(), GlobalOffset.of(1), 0L, tx -> seen.set(tx.writeTargetIdentity()), null);
    assertEquals(repo.writeTargetIdentity(), seen.get());
  }

  @Test
  void writesTo_isTrueThroughAJdkProxyOfTheSameRepository() {
    Object proxy =
        java.lang.reflect.Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {ProjectionRepository.class, AtomicBatchProcessor.class},
            (p, m, a) -> m.invoke(repo, a));
    assertTrue(repo.writesTo((ProjectionRepository) proxy));
    assertTrue(((AtomicBatchProcessor) proxy).writesTo(repo));
  }

  @Test
  void jdbcWriteTarget_exposesItsComponents() {
    var id = (JdbcProjectionRepository.JdbcWriteTarget) repo.writeTargetIdentity();
    assertSame(dataSource, id.dataSource());
    assertNotNull(id.objectMapper());
  }

  private static boolean tableExists(String table) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?")) {
      ps.setString(1, table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1) > 0;
      }
    }
  }
}
