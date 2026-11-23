package org.streamrune.quarkus;

/**
 * Recognises Agroal types by name, so that an application without Agroal on its classpath never
 * loads an Agroal type (Agroal is a compile-only dependency of this module).
 */
final class AgroalTypes {

  private static final String AGROAL_DATA_SOURCE = "io.agroal.api.AgroalDataSource";

  private AgroalTypes() {}

  /** Whether {@code type} implements {@code io.agroal.api.AgroalDataSource}. */
  static boolean isAgroal(Class<?> type) {
    if (type == null) {
      return false;
    }
    if (AGROAL_DATA_SOURCE.equals(type.getName())) {
      return true;
    }
    for (Class<?> implemented : type.getInterfaces()) {
      if (isAgroal(implemented)) {
        return true;
      }
    }
    return isAgroal(type.getSuperclass());
  }
}
