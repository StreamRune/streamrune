package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;

class BackendSelectionTest {

  /** A backend that does nothing: selection only creates and returns it. */
  static class StubBackend implements StreamRuneBackend {
    private final String name;

    StubBackend(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public void verify(DataSource dataSource) {}

    @Override
    public BackendStores open(BackendContext context) {
      throw new UnsupportedOperationException("selection never opens a backend");
    }
  }

  /** Loaded by name: a public no-argument constructor, like the SQLite backend's. */
  public static final class LoadableBackend extends StubBackend {
    public LoadableBackend() {
      super("sqlite");
    }
  }

  /** A class of the right shape that is not a backend. */
  public static final class NotABackend {
    public NotABackend() {}
  }

  /** A backend whose constructor fails. */
  public static final class ThrowingBackend extends StubBackend {
    public ThrowingBackend() {
      super("sqlite");
      throw new IllegalStateException("constructor failed");
    }
  }

  /** A backend without a no-argument constructor. */
  public static final class NoNoArgBackend extends StubBackend {
    public NoNoArgBackend(String name) {
      super(name);
    }
  }

  /** A backend whose class initialization fails, as a missing driver class would. */
  public static final class FailingInitBackend extends StubBackend {
    static final int VALUE = fail();

    public FailingInitBackend() {
      super("sqlite");
    }

    private static int fail() {
      throw new IllegalStateException("static init failed");
    }
  }

  private static String binaryName(Class<?> type) {
    return type.getName();
  }

  /** Counts its calls: the Postgres backend is created only when it is selected. */
  private static final class CountingSupplier implements Supplier<StreamRuneBackend> {
    private final StreamRuneBackend backend;
    final AtomicInteger calls = new AtomicInteger();

    CountingSupplier(StreamRuneBackend backend) {
      this.backend = backend;
    }

    @Override
    public StreamRuneBackend get() {
      calls.incrementAndGet();
      return backend;
    }
  }

  // ---------- constants ----------

  @Test
  void theConstants() {
    assertThat(BackendSelection.PROPERTY).isEqualTo("streamrune.backend");
    assertThat(BackendSelection.SQLITE_BACKEND_CLASS)
        .isEqualTo("org.streamrune.sqlite.SqliteBackend");
    assertThat(BackendSelection.SQLITE_CRYPTO_BACKEND_CLASS)
        .isEqualTo("org.streamrune.crypto.sqlite.SqliteCryptoBackend");
  }

  // ---------- parse ----------

  @Test
  void parse_nullOrBlank_isPostgres() {
    assertThat(BackendSelection.parse(null)).isEqualTo("postgres");
    assertThat(BackendSelection.parse("")).isEqualTo("postgres");
    assertThat(BackendSelection.parse("   ")).isEqualTo("postgres");
  }

  @Test
  void parse_trimsAndLowerCases() {
    assertThat(BackendSelection.parse("postgres")).isEqualTo("postgres");
    assertThat(BackendSelection.parse("sqlite")).isEqualTo("sqlite");
    assertThat(BackendSelection.parse(" SQLite ")).isEqualTo("sqlite");
    assertThat(BackendSelection.parse("\tPOSTGRES\n")).isEqualTo("postgres");
  }

  @Test
  void parse_anythingElse_isRefusedNamingTheRawValueAndTheValidOnes() {
    assertThatThrownBy(() -> BackendSelection.parse("oracle"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("streamrune.backend='oracle' is not a backend; valid values: postgres, sqlite");
    assertThatThrownBy(() -> BackendSelection.parse(" Oracle "))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.backend=' Oracle ' is not a backend; valid values: postgres, sqlite");
    assertThatThrownBy(() -> BackendSelection.parse("postgresql"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'postgresql'");
  }

  // ---------- resolve ----------

  @Test
  void resolve_postgres_isTheSuppliedBackend() {
    var postgres = new StubBackend("postgres");
    var supplier = new CountingSupplier(postgres);

    assertThat(BackendSelection.resolve("postgres", supplier)).isSameAs(postgres);
    assertThat(supplier.calls).hasValue(1);
  }

  @Test
  void resolve_sqlite_withoutTheModule_namesTheModule_andNeverCreatesPostgres() {
    var supplier = new CountingSupplier(new StubBackend("postgres"));

    assertThatThrownBy(() -> BackendSelection.resolve("sqlite", supplier))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamrune.backend=sqlite needs the module org.streamrune:streamrune-sqlite on the"
                + " classpath")
        .hasCauseInstanceOf(ClassNotFoundException.class);
    assertThat(supplier.calls).hasValue(0);
  }

  @Test
  void resolve_sqlite_loadsTheClassByName() {
    var supplier = new CountingSupplier(new StubBackend("postgres"));

    StreamRuneBackend backend =
        BackendSelection.resolve("sqlite", supplier, binaryName(LoadableBackend.class));

    assertThat(backend).isInstanceOf(LoadableBackend.class);
    assertThat(backend.name()).isEqualTo("sqlite");
    assertThat(supplier.calls).hasValue(0);
  }

  @Test
  void resolve_sqlite_aClassThatIsNotABackend_isRefusedNamingIt() {
    String name = binaryName(NotABackend.class);

    assertThatThrownBy(
            () -> BackendSelection.resolve("sqlite", () -> new StubBackend("postgres"), name))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(name + " is not a " + StreamRuneBackend.class.getName());
  }

  @Test
  void resolve_sqlite_aThrowingConstructor_isWrappedNamingTheClass() {
    String name = binaryName(ThrowingBackend.class);

    assertThatThrownBy(
            () -> BackendSelection.resolve("sqlite", () -> new StubBackend("postgres"), name))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("The constructor of " + name + " failed: constructor failed")
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("constructor failed");
  }

  @Test
  void resolve_sqlite_noPublicNoArgConstructor_isWrappedNamingTheClass() {
    String name = binaryName(NoNoArgBackend.class);

    assertThatThrownBy(
            () -> BackendSelection.resolve("sqlite", () -> new StubBackend("postgres"), name))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create " + name + " through its public no-argument constructor")
        .hasCauseInstanceOf(NoSuchMethodException.class);
  }

  @Test
  void resolve_sqlite_aClassThatFailsToInitialize_isWrappedNamingTheClass() {
    String name = binaryName(FailingInitBackend.class);

    assertThatThrownBy(
            () -> BackendSelection.resolve("sqlite", () -> new StubBackend("postgres"), name))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Cannot load " + name + ": ")
        .hasCauseInstanceOf(ExceptionInInitializerError.class);
  }

  @Test
  void resolve_anUnparsedName_isRefused() {
    for (String name : new String[] {"oracle", "SQLite", " postgres", ""}) {
      assertThatThrownBy(() -> BackendSelection.resolve(name, () -> new StubBackend("postgres")))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "name must be a parsed backend name (postgres, sqlite), got '"
                  + name
                  + "': parse streamrune.backend with BackendSelection.parse first");
    }
  }

  @Test
  void resolve_refusesNulls() {
    assertThatThrownBy(() -> BackendSelection.resolve(null, () -> new StubBackend("postgres")))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("name is required");
    assertThatThrownBy(() -> BackendSelection.resolve("postgres", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("postgres is required");
    assertThatThrownBy(() -> BackendSelection.resolve("postgres", () -> null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("postgres must supply a backend");
  }
}
