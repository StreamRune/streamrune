package org.streamrune.integration;

import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Supplier;
import org.streamrune.core.backend.StreamRuneBackend;

/**
 * Which storage backend an application runs on, read from {@value #PROPERTY}: {@code postgres} (the
 * default) or {@code sqlite}. Every framework integration parses the property with {@link #parse}
 * and resolves the backend with {@link #resolve}.
 *
 * <p>This module cannot see the backend modules, so the integration supplies the Postgres backend,
 * and the SQLite backend is loaded by its class name, {@value #SQLITE_BACKEND_CLASS}, from the
 * classpath. That is the only reflection: no {@code ServiceLoader}, no scanning.
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
   *     the classpath, or cannot be created through its public no-argument constructor
   */
  public static StreamRuneBackend resolve(String name, Supplier<StreamRuneBackend> postgres) {
    return resolve(name, postgres, SQLITE_BACKEND_CLASS);
  }

  /**
   * {@link #resolve(String, Supplier)} loading the SQLite backend from {@code sqliteClassName}: the
   * seam the tests use to reach the reflective failures without a class in the SQLite package.
   */
  static StreamRuneBackend resolve(
      String name, Supplier<StreamRuneBackend> postgres, String sqliteClassName) {
    Objects.requireNonNull(name, "name is required");
    Objects.requireNonNull(postgres, "postgres is required");
    return switch (name) {
      case POSTGRES -> Objects.requireNonNull(postgres.get(), "postgres must supply a backend");
      case SQLITE -> load(sqliteClassName);
      default ->
          throw new IllegalArgumentException(
              "name must be a parsed backend name (postgres, sqlite), got '"
                  + name
                  + "': parse "
                  + PROPERTY
                  + " with BackendSelection.parse first");
    };
  }

  /** Creates the SQLite backend from its class name, every failure an IllegalStateException. */
  private static StreamRuneBackend load(String className) {
    Class<?> type;
    try {
      type = Class.forName(className, true, BackendSelection.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          PROPERTY + "=sqlite needs the module org.streamrune:streamrune-sqlite on the classpath",
          e);
    } catch (LinkageError e) {
      throw new IllegalStateException("Cannot load " + className + ": " + e, e);
    }
    if (!StreamRuneBackend.class.isAssignableFrom(type)) {
      throw new IllegalStateException(className + " is not a " + StreamRuneBackend.class.getName());
    }
    try {
      return (StreamRuneBackend) type.getDeclaredConstructor().newInstance();
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
