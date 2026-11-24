package org.streamrune.integration;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.streamrune.core.backend.StreamRuneBackend;

/**
 * Which storage backend an application runs on, read from {@value #PROPERTY}: {@code postgres} (the
 * default) or {@code sqlite}. Every framework integration parses the property with {@link #parse}
 * and resolves the backend with {@link #resolve}.
 *
 * <p>This module cannot see the backend modules, so the integration supplies the Postgres backend,
 * and the SQLite backend is loaded by its class name, {@value #SQLITE_BACKEND_CLASS}, from the
 * classpath; its crypto half, {@value #SQLITE_CRYPTO_BACKEND_CLASS}, likewise through {@link
 * #resolveCrypto}. That is the only reflection: no {@code ServiceLoader}, no scanning. Each
 * integration registers both classes' public no-argument constructors for native-image reflection.
 *
 * <p><b>Class loader.</b> Both classes are loaded with this class's own class loader. The backend
 * modules are dependencies of the application like this module, so they share its class loader: the
 * application class loader on the JVM and in Spring and Micronaut, Quarkus's runtime class loader
 * in Quarkus dev and test mode, and the image itself in a native image, where the registration
 * makes the name resolvable.
 *
 * <p><b>The loaded backend's name.</b> A loaded or supplied backend must report the name it was
 * selected by, so the default read-model store, named after it, cannot drift from the property.
 */
public final class BackendSelection {

  /** The property that selects the backend, {@value}. */
  public static final String PROPERTY = "streamrune.backend";

  /**
   * The SQLite backend's class, {@value}, in the module {@code org.streamrune:streamrune-sqlite}.
   */
  public static final String SQLITE_BACKEND_CLASS = "org.streamrune.sqlite.SqliteBackend";

  /**
   * The SQLite crypto backend's class, {@value}, in the module {@code
   * org.streamrune:streamrune-sqlite-crypto}.
   */
  public static final String SQLITE_CRYPTO_BACKEND_CLASS =
      "org.streamrune.crypto.sqlite.SqliteCryptoBackend";

  private static final String POSTGRES = "postgres";
  private static final String SQLITE = "sqlite";

  private BackendSelection() {}

  /**
   * The backend {@code raw} names.
   *
   * @param raw the property's value, or {@code null} when it is not set
   * @return {@code "postgres"} for {@code null} or blank; otherwise the trimmed, lower-cased value
   * @throws IllegalStateException when the value names neither {@code postgres} nor {@code sqlite},
   *     quoting the raw value and listing the valid ones
   */
  public static String parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return POSTGRES;
    }
    String name = raw.trim().toLowerCase(Locale.ROOT);
    if (POSTGRES.equals(name) || SQLITE.equals(name)) {
      return name;
    }
    throw new IllegalStateException(
        PROPERTY + "='" + raw + "' is not a backend; valid values: postgres, sqlite");
  }

  /**
   * The backend a parsed name selects.
   *
   * @param name a name {@link #parse} returned: {@code postgres} or {@code sqlite}
   * @param postgres supplies the Postgres backend; called only when {@code name} is {@code
   *     postgres}
   * @return the selected backend
   * @throws NullPointerException if an argument is {@code null}, or {@code postgres} supplies
   *     {@code null}
   * @throws IllegalArgumentException if {@code name} is not one {@link #parse} returns
   * @throws IllegalStateException for {@code sqlite} when {@value #SQLITE_BACKEND_CLASS} is not on
   *     the classpath, or cannot be created through its public no-argument constructor; or when the
   *     backend reports another name than {@code name}
   */
  public static StreamRuneBackend resolve(String name, Supplier<StreamRuneBackend> postgres) {
    return resolve(name, postgres, SQLITE_BACKEND_CLASS);
  }

  /**
   * The crypto half of the backend a parsed name selects: the {@code CryptoBackend} of {@code
   * streamrune-crypto-api}, which this module cannot name, so the integration passes its type.
   * Called only when a crypto engine is enabled.
   *
   * @param <T> the crypto backend type, {@code org.streamrune.crypto.CryptoBackend}
   * @param name a name {@link #parse} returned: {@code postgres} or {@code sqlite}
   * @param type the crypto backend type the loaded class must implement
   * @param nameOf reads a crypto backend's name, which must equal {@code name}
   * @param postgres supplies the Postgres crypto backend; called only when {@code name} is {@code
   *     postgres}
   * @return the selected crypto backend
   * @throws NullPointerException if an argument is {@code null}, or {@code postgres} supplies
   *     {@code null}
   * @throws IllegalArgumentException if {@code name} is not one {@link #parse} returns
   * @throws IllegalStateException for {@code sqlite} when {@value #SQLITE_CRYPTO_BACKEND_CLASS} is
   *     not on the classpath (naming the module {@code org.streamrune:streamrune-sqlite-crypto}) or
   *     cannot be created, or when the crypto backend reports another name than {@code name}
   */
  public static <T> T resolveCrypto(
      String name,
      Class<T> type,
      Function<? super T, String> nameOf,
      Supplier<? extends T> postgres) {
    return resolveCrypto(name, type, nameOf, postgres, SQLITE_CRYPTO_BACKEND_CLASS);
  }

  /**
   * {@link #resolveCrypto(String, Class, Function, Supplier)} loading the SQLite crypto backend
   * from {@code sqliteClassName}: the test seam.
   */
  static <T> T resolveCrypto(
      String name,
      Class<T> type,
      Function<? super T, String> nameOf,
      Supplier<? extends T> postgres,
      String sqliteClassName) {
    Objects.requireNonNull(name, "name is required");
    Objects.requireNonNull(type, "type is required");
    Objects.requireNonNull(nameOf, "nameOf is required");
    Objects.requireNonNull(postgres, "postgres is required");
    T backend =
        switch (name) {
          case POSTGRES -> Objects.requireNonNull(postgres.get(), "postgres must supply a backend");
          case SQLITE ->
              load(
                  sqliteClassName,
                  type,
                  PROPERTY
                      + "=sqlite with a crypto engine enabled needs the module"
                      + " org.streamrune:streamrune-sqlite-crypto on the classpath");
          default -> throw unparsed(name);
        };
    return requireName(name, backend, nameOf.apply(backend));
  }

  /**
   * {@link #resolve(String, Supplier)} loading the SQLite backend from {@code sqliteClassName}: the
   * seam the tests use to reach the reflective failures without a class in the SQLite package.
   */
  static StreamRuneBackend resolve(
      String name, Supplier<StreamRuneBackend> postgres, String sqliteClassName) {
    Objects.requireNonNull(name, "name is required");
    Objects.requireNonNull(postgres, "postgres is required");
    StreamRuneBackend backend =
        switch (name) {
          case POSTGRES -> Objects.requireNonNull(postgres.get(), "postgres must supply a backend");
          case SQLITE ->
              load(
                  sqliteClassName,
                  StreamRuneBackend.class,
                  PROPERTY
                      + "=sqlite needs the module org.streamrune:streamrune-sqlite on the"
                      + " classpath");
          default -> throw unparsed(name);
        };
    return requireName(name, backend, backend.name());
  }

  private static IllegalArgumentException unparsed(String name) {
    return new IllegalArgumentException(
        "name must be a parsed backend name (postgres, sqlite), got '"
            + name
            + "': parse "
            + PROPERTY
            + " with BackendSelection.parse first");
  }

  /** The backend, when it reports the name it was selected by. */
  private static <T> T requireName(String selected, T backend, String reported) {
    if (!selected.equals(reported)) {
      throw new IllegalStateException(
          backend.getClass().getName()
              + " reports the backend name '"
              + reported
              + "', but "
              + PROPERTY
              + "="
              + selected
              + " selected it");
    }
    return backend;
  }

  /**
   * Creates a backend class from its name, every failure an IllegalStateException; {@code missing}
   * is the message when the class is not on the classpath.
   */
  private static <T> T load(String className, Class<T> expected, String missing) {
    Class<?> type;
    try {
      type = Class.forName(className, true, BackendSelection.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(missing, e);
    } catch (LinkageError e) {
      throw new IllegalStateException("Cannot load " + className + ": " + e, e);
    }
    if (!expected.isAssignableFrom(type)) {
      throw new IllegalStateException(className + " is not a " + expected.getName());
    }
    try {
      return expected.cast(type.getDeclaredConstructor().newInstance());
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      throw new IllegalStateException(
          "The constructor of " + className + " failed: " + cause.getMessage(), cause);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Cannot create " + className + " through its public no-argument constructor", e);
    }
  }
}
